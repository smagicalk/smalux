//! Service 内部上报构建。

use crate::config::AgentConfig;
#[cfg(test)]
use crate::config::model::ExportFormat;
use crate::export::export_format_needs_basic_info;
use crate::service::outbound::{
    BasicInfoEnvelope, OutboundEvent, OutboundSender, OutboundSequence, ReportEnvelope,
};
#[cfg(test)]
use crate::telemetry::ReportEvent;
#[cfg(test)]
use crate::telemetry::TelemetryUpdate;
use crate::telemetry::{LatestTelemetry, TelemetryAggregator};
use smalux_protocol::OutboundReport;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};

use super::message::TelemetryUpdateReceiver;
#[cfg(test)]
use super::message::telemetry_update_channel;

/// reporter 控制命令队列容量。
const REPORTER_COMMAND_QUEUE_CAPACITY: usize = 32;
/// 强制 snapshot 等待检查间隔。
const FORCE_SNAPSHOT_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// reporter 控制命令发送端。
pub(crate) type ReporterCommandSender = mpsc::Sender<ReporterCommand>;
/// reporter 控制命令接收端。
pub(crate) type ReporterCommandReceiver = mpsc::Receiver<ReporterCommand>;

/// 创建 reporter 控制命令队列。
pub(crate) fn reporter_command_channel() -> (ReporterCommandSender, ReporterCommandReceiver) {
    mpsc::channel(REPORTER_COMMAND_QUEUE_CAPACITY)
}

/// reporter 控制命令。
#[derive(Debug)]
pub(crate) enum ReporterCommand {
    /// server 请求尽快发送完整 snapshot。
    ForceSnapshot {
        /// 请求原因，便于日志定位。
        reason: Option<String>,
    },
}

/// 按上报频率从缓存构建内部上报语义。
///
/// 测试和单次构建使用的辅助函数，默认序号为 0；循环发送时会使用真实递增序号。
#[cfg(test)]
pub(crate) fn reporter_tick_once(
    state: &LatestTelemetry,
    agent_version: &str,
) -> anyhow::Result<OutboundReport> {
    let mut aggregator = TelemetryAggregator::default();
    let config = AgentConfig::default();
    let sequence = OutboundSequence::default();
    aggregator
        .next_report_event(state, agent_version, &config, || sequence.next())?
        .map(ReportEvent::into_outbound)
        .ok_or_else(|| anyhow::anyhow!("No report event generated"))
}

/// reporter loop 的运行依赖。
pub(crate) struct ReporterLoopParts {
    /// reporter 持有的最新遥测状态。
    pub(crate) state: LatestTelemetry,
    /// agent 版本号，写入每份 report metadata。
    pub(crate) agent_version: &'static str,
    /// 动态配置订阅。
    pub(crate) config_rx: watch::Receiver<AgentConfig>,
    /// 服务关闭信号。
    pub(crate) shutdown: watch::Receiver<bool>,
    /// 出站事件队列。
    pub(crate) outbound_tx: OutboundSender,
    /// 全局出站序号。
    pub(crate) sequence: OutboundSequence,
    /// 采集更新队列。
    pub(crate) telemetry_updates: TelemetryUpdateReceiver,
    /// reporter 控制命令队列。
    pub(crate) reporter_commands: ReporterCommandReceiver,
}

/// 按配置频率构建 report，并放入导出队列。
pub(crate) async fn reporter_loop(parts: ReporterLoopParts) {
    let ReporterLoopParts {
        mut state,
        agent_version,
        mut config_rx,
        mut shutdown,
        outbound_tx,
        sequence,
        mut telemetry_updates,
        mut reporter_commands,
    } = parts;

    let mut config = config_rx.borrow().clone();
    state.configure_metric_groups(
        config.core.enabled,
        config.disk.enabled,
        config.network.enabled,
        config.processes.enabled,
        config.sockets.enabled,
    );
    let mut report_tick = interval(config.report.interval);
    report_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut basic_info_tick = delayed_interval(config.outbound.basic_info.refresh_interval);
    let mut force_snapshot_tick = interval(FORCE_SNAPSHOT_CHECK_INTERVAL);
    force_snapshot_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut config_rx_open = true;
    let mut telemetry_updates_open = true;
    let mut reporter_commands_open = true;
    let mut aggregator = TelemetryAggregator::default();
    let mut force_snapshot_pending = false;
    let mut force_snapshot_reason = None;
    let mut last_forced_snapshot_at = None;
    let mut basic_info_initial_sent = !basic_info_should_send_on_start(&config);
    if !basic_info_initial_sent {
        match queue_basic_info_if_ready(&state, agent_version, &outbound_tx, &sequence).await {
            Ok(true) => basic_info_initial_sent = true,
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(error = ?err, "reporter loop stopping");
                return;
            }
        }
    }

    loop {
        tokio::select! {
            _ = report_tick.tick(), if config.report.enabled => {
                if force_snapshot_pending
                    && force_snapshot_due(last_forced_snapshot_at, config.report.force_snapshot_min_interval)
                {
                    match queue_forced_snapshot(
                        &state,
                        agent_version,
                        &outbound_tx,
                        &sequence,
                        &mut aggregator,
                        force_snapshot_reason.as_deref(),
                    ).await {
                        Ok(()) => {
                            force_snapshot_pending = false;
                            force_snapshot_reason = None;
                            last_forced_snapshot_at = Some(Instant::now());
                        }
                        Err(err) => tracing::debug!(error = ?err, "forced agent snapshot not ready"),
                    }
                    continue;
                }

                if let Err(err) = queue_next_report(
                    &state,
                    agent_version,
                    &config,
                    &outbound_tx,
                    &sequence,
                    &mut aggregator,
                ).await {
                    tracing::warn!(error = ?err, "reporter loop stopping");
                    break;
                }
            }
            _ = basic_info_tick.tick(), if basic_info_enabled(&config) => {
                match queue_basic_info_if_ready(&state, agent_version, &outbound_tx, &sequence).await {
                    Ok(true) => basic_info_initial_sent = true,
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(error = ?err, "reporter loop stopping");
                        break;
                    }
                }
            }
            _ = force_snapshot_tick.tick(), if config.report.enabled && force_snapshot_pending => {
                if !force_snapshot_due(last_forced_snapshot_at, config.report.force_snapshot_min_interval) {
                    continue;
                }
                match queue_forced_snapshot(
                    &state,
                    agent_version,
                    &outbound_tx,
                    &sequence,
                    &mut aggregator,
                    force_snapshot_reason.as_deref(),
                ).await {
                    Ok(()) => {
                        force_snapshot_pending = false;
                        force_snapshot_reason = None;
                        last_forced_snapshot_at = Some(Instant::now());
                    }
                    Err(err) => tracing::debug!(error = ?err, "forced agent snapshot not ready"),
                }
            }
            update = telemetry_updates.recv(), if telemetry_updates_open => {
                let Some(update) = update else {
                    telemetry_updates_open = false;
                    tracing::debug!("Telemetry update channel closed");
                    continue;
                };
                let kind = update.kind();
                state.apply_update(update);
                tracing::debug!(kind, "Telemetry update applied");
                if !config.report.enabled {
                    continue;
                }
                if let Err(err) = queue_next_report(
                    &state,
                    agent_version,
                    &config,
                    &outbound_tx,
                    &sequence,
                    &mut aggregator,
                ).await {
                    tracing::warn!(error = ?err, "reporter loop stopping");
                    break;
                }
                if !basic_info_initial_sent && basic_info_should_send_on_start(&config) {
                    match queue_basic_info_if_ready(
                        &state,
                        agent_version,
                        &outbound_tx,
                        &sequence,
                    ).await {
                        Ok(true) => basic_info_initial_sent = true,
                        Ok(false) => {}
                        Err(err) => {
                            tracing::warn!(error = ?err, "reporter loop stopping");
                            break;
                        }
                    }
                }
            }
            command = reporter_commands.recv(), if reporter_commands_open => {
                let Some(command) = command else {
                    reporter_commands_open = false;
                    tracing::debug!("Reporter command channel closed");
                    continue;
                };
                match command {
                    ReporterCommand::ForceSnapshot { reason } => {
                        if reason.is_some() {
                            force_snapshot_reason = reason;
                        }
                        force_snapshot_pending = true;
                        if !config.report.enabled {
                            tracing::debug!("Forced snapshot requested while reporting is disabled");
                            continue;
                        }
                        if !force_snapshot_due(last_forced_snapshot_at, config.report.force_snapshot_min_interval) {
                            tracing::debug!("Forced snapshot request coalesced by minimum interval");
                            continue;
                        }
                        match queue_forced_snapshot(
                            &state,
                            agent_version,
                            &outbound_tx,
                            &sequence,
                            &mut aggregator,
                            force_snapshot_reason.as_deref(),
                        ).await {
                            Ok(()) => {
                                force_snapshot_pending = false;
                                force_snapshot_reason = None;
                                last_forced_snapshot_at = Some(Instant::now());
                            }
                            Err(err) => tracing::debug!(error = ?err, "forced agent snapshot not ready"),
                        }
                    }
                }
            }
            changed = config_rx.changed(), if config_rx_open => {
                if changed.is_err() {
                    config_rx_open = false;
                    tracing::debug!("Reporter config channel closed");
                    continue;
                }

                let next = config_rx.borrow().clone();
                let previous_basic_info = config.outbound.basic_info;
                let previous_format = config.export.format;
                if next.report.interval != config.report.interval {
                    report_tick = interval(next.report.interval);
                    report_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
                }
                if next.outbound.basic_info.refresh_interval
                    != config.outbound.basic_info.refresh_interval
                {
                    basic_info_tick = delayed_interval(next.outbound.basic_info.refresh_interval);
                }
                config = next;
                state.configure_metric_groups(
                    config.core.enabled,
                    config.disk.enabled,
                    config.network.enabled,
                    config.processes.enabled,
                    config.sockets.enabled,
                );
                if previous_format != config.export.format
                    || previous_basic_info.enabled != config.outbound.basic_info.enabled
                    || previous_basic_info.send_on_start
                        != config.outbound.basic_info.send_on_start
                {
                    basic_info_initial_sent = !basic_info_should_send_on_start(&config);
                }
                if !basic_info_initial_sent && basic_info_should_send_on_start(&config) {
                    match queue_basic_info_if_ready(
                        &state,
                        agent_version,
                        &outbound_tx,
                        &sequence,
                    ).await {
                        Ok(true) => basic_info_initial_sent = true,
                        Ok(false) => {}
                        Err(err) => {
                            tracing::warn!(error = ?err, "reporter loop stopping");
                            break;
                        }
                    }
                }
                tracing::info!("Reporter config updated");
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("Reporter loop shutting down");
                    break;
                }
            }
        }
    }
}

/// 创建第一次触发延后到 interval 之后的 tick。
fn delayed_interval(duration: std::time::Duration) -> tokio::time::Interval {
    let mut tick = interval_at(Instant::now() + duration, duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick
}

/// 判断当前格式是否需要 reporter 产生 basic info 事件。
fn basic_info_enabled(config: &AgentConfig) -> bool {
    config.outbound.basic_info.enabled && export_format_needs_basic_info(config.export.format)
}

/// 判断是否需要在第一份 ready state 出现后立即发送 basic info。
fn basic_info_should_send_on_start(config: &AgentConfig) -> bool {
    basic_info_enabled(config) && config.outbound.basic_info.send_on_start
}

/// 判断强制 snapshot 是否已经超过最小间隔。
fn force_snapshot_due(previous: Option<Instant>, min_interval: std::time::Duration) -> bool {
    previous
        .map(|previous| previous.elapsed() >= min_interval)
        .unwrap_or(true)
}

/// 如果当前状态已经 ready，则构建并投递 basic info 事件。
async fn queue_basic_info_if_ready(
    state: &LatestTelemetry,
    agent_version: &str,
    outbound_tx: &OutboundSender,
    sequence: &OutboundSequence,
) -> anyhow::Result<bool> {
    let report = match state.build_report(agent_version) {
        Ok(report) => report,
        Err(err) => {
            tracing::debug!(error = ?err, "basic info report not ready");
            return Ok(false);
        }
    };
    let sequence = sequence.next();
    let event = OutboundEvent::BasicInfo(Box::new(BasicInfoEnvelope::new(sequence, report)));
    outbound_tx
        .send(event)
        .await
        .map_err(|_| anyhow::anyhow!("outbound event queue is closed"))?;
    tracing::debug!(sequence, "basic info queued");
    Ok(true)
}

/// 构建并投递普通上报事件。
async fn queue_next_report(
    state: &LatestTelemetry,
    agent_version: &str,
    config: &AgentConfig,
    outbound_tx: &OutboundSender,
    sequence: &OutboundSequence,
    aggregator: &mut TelemetryAggregator,
) -> anyhow::Result<()> {
    let event = aggregator.next_report_event(state, agent_version, config, || sequence.next());

    match event {
        Ok(Some(event)) => queue_report_event(outbound_tx, event.into_outbound()).await,
        Ok(None) => {
            tracing::debug!("Agent report unchanged; no business heartbeat due");
            Ok(())
        }
        Err(err) => {
            tracing::debug!(error = ?err, "Agent report not ready");
            Ok(())
        }
    }
}

/// 构建并投递 server 请求的完整 snapshot。
async fn queue_forced_snapshot(
    state: &LatestTelemetry,
    agent_version: &str,
    outbound_tx: &OutboundSender,
    sequence: &OutboundSequence,
    aggregator: &mut TelemetryAggregator,
    reason: Option<&str>,
) -> anyhow::Result<()> {
    let outbound = aggregator
        .force_snapshot_event(state, agent_version, || sequence.next())?
        .into_outbound();
    let sequence = outbound.sequence;
    queue_report_event(outbound_tx, outbound).await?;
    tracing::info!(sequence, reason, "Forced agent snapshot queued");
    Ok(())
}

/// 把协议上报包装成出站事件并投递。
async fn queue_report_event(
    outbound_tx: &OutboundSender,
    outbound: OutboundReport,
) -> anyhow::Result<()> {
    let sequence = outbound.sequence;
    let event = OutboundEvent::Report(ReportEnvelope::from_outbound(outbound));
    outbound_tx
        .send(event)
        .await
        .map_err(|_| anyhow::anyhow!("outbound event queue is closed"))?;
    tracing::debug!(sequence, "Agent report queued");
    Ok(())
}

#[cfg(test)]
mod tests {
    //! 上报调度测试。

    use super::*;
    use crate::collect::{CoreSample, DiskSample, NetworkSample, ProcessSample, SocketSample};
    use crate::service::outbound::outbound_channel;
    use smalux_core::model::info::{
        CoreInfo, DiskInfo, IdentityInfo, NetworkInfo, ProcessInfo, SocketAccuracy, SocketInfo,
        SocketSource, SystemInfo,
    };
    use std::time::Duration;

    /// 构造已经满足上报条件的 telemetry 状态。
    fn ready_state() -> LatestTelemetry {
        let mut state = LatestTelemetry::default();
        state.set_identity(IdentityInfo {
            agent_id: "agent-test".to_string(),
            ..IdentityInfo::default()
        });
        state.set_system(SystemInfo::default());
        state.set_core(CoreSample {
            sampled_at: 1,
            value: CoreInfo::default(),
        });
        state.set_disk(DiskSample {
            sampled_at: 2,
            value: DiskInfo::default(),
        });
        state.set_network(NetworkSample {
            sampled_at: 3,
            value: NetworkInfo::default(),
        });
        state.set_processes(ProcessSample {
            sampled_at: 4,
            value: ProcessInfo::ready(42),
        });
        state.set_sockets(SocketSample {
            sampled_at: 5,
            value: SocketInfo::ready(
                10,
                3,
                SocketSource::SocketTable,
                SocketAccuracy::SocketTable,
            ),
        });
        state
    }

    /// 验证 reporter loop 会把 ready state 组装为最新内部上报。
    #[tokio::test]
    async fn reporter_loop_updates_latest_report() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.interval = Duration::from_millis(100);
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (_telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state,
            agent_version: "0.1.0-test",
            config_rx,
            shutdown: shutdown_rx,
            outbound_tx,
            sequence,
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        let event = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let OutboundEvent::Report(report) = event else {
            panic!("expected report event");
        };
        shutdown_tx.send_replace(true);
        task.await.unwrap();

        match &report.outbound.kind {
            smalux_protocol::OutboundReportKind::Snapshot { report } => {
                assert_eq!(report.meta.agent_version.as_str(), "0.1.0-test");
            }
            _ => panic!("expected snapshot report"),
        }
        assert_eq!(report.sequence, 1);
    }

    /// 验证 Komari basic info 在启动 ready 时会立即进入出站队列。
    #[tokio::test]
    async fn reporter_loop_queues_komari_basic_info_on_start() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.report.interval = Duration::from_secs(60);
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (_telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state,
            agent_version: "0.1.0-test",
            config_rx,
            shutdown: shutdown_rx,
            outbound_tx,
            sequence,
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        let event = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send_replace(true);
        task.await.unwrap();

        let OutboundEvent::BasicInfo(info) = event else {
            panic!("expected basic info event");
        };
        assert_eq!(info.sequence, 1);
        assert_eq!(info.report.meta.agent_version, "0.1.0-test");
        assert_eq!(info.report.identity.agent_id, "agent-test");
    }

    /// 验证 send_on_start=false 时 basic info 等待自己的 refresh_interval。
    #[tokio::test]
    async fn reporter_loop_queues_komari_basic_info_on_interval() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.report.enabled = false;
        config.outbound.basic_info.send_on_start = false;
        config.outbound.basic_info.refresh_interval = Duration::from_millis(50);
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (_telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state,
            agent_version: "0.1.0-test",
            config_rx,
            shutdown: shutdown_rx,
            outbound_tx,
            sequence,
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        assert!(
            tokio::time::timeout(Duration::from_millis(20), outbound_rx.recv())
                .await
                .is_err()
        );
        let event = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send_replace(true);
        task.await.unwrap();

        assert!(matches!(event, OutboundEvent::BasicInfo(_)));
    }

    /// 验证关闭 basic info 出站事件后不会产生 basic info 事件。
    #[tokio::test]
    async fn reporter_loop_skips_disabled_basic_info() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.report.interval = Duration::from_secs(60);
        config.outbound.basic_info.enabled = false;
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (_telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state,
            agent_version: "0.1.0-test",
            config_rx,
            shutdown: shutdown_rx,
            outbound_tx,
            sequence,
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        let event = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send_replace(true);
        task.await.unwrap();

        assert!(matches!(event, OutboundEvent::Report(_)));
    }

    /// 验证采集 update 会驱动 reporter 立即生成下一条上报。
    #[tokio::test]
    async fn reporter_loop_queues_report_when_metric_update_arrives() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.interval = Duration::from_secs(60);
        config.report.delta_enabled = true;
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state,
            agent_version: "0.1.0-test",
            config_rx,
            shutdown: shutdown_rx,
            outbound_tx,
            sequence,
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        let _initial = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        telemetry_tx
            .send(TelemetryUpdate::Core(CoreSample {
                sampled_at: 99,
                value: CoreInfo::default(),
            }))
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send_replace(true);
        task.await.unwrap();

        let OutboundEvent::Report(report) = event else {
            panic!("expected report event");
        };
        match &report.outbound.kind {
            smalux_protocol::OutboundReportKind::Delta { delta } => {
                assert_eq!(
                    delta.core.as_ref().unwrap().as_ref().unwrap().sampled_at,
                    99
                );
            }
            _ => panic!("expected delta report"),
        }
    }

    /// 验证 server 强制 snapshot 请求会绕过普通 tick 立即进入出站队列。
    #[tokio::test]
    async fn reporter_loop_queues_forced_snapshot() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.interval = Duration::from_secs(60);
        config.report.force_snapshot_min_interval = Duration::from_millis(100);
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (_telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state,
            agent_version: "0.1.0-test",
            config_rx,
            shutdown: shutdown_rx,
            outbound_tx,
            sequence,
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        let _first = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        reporter_command_tx
            .send(ReporterCommand::ForceSnapshot {
                reason: Some("manual".to_string()),
            })
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send_replace(true);
        task.await.unwrap();

        let OutboundEvent::Report(report) = event else {
            panic!("expected forced report event");
        };
        assert!(matches!(
            report.outbound.kind,
            smalux_protocol::OutboundReportKind::Snapshot { .. }
        ));
    }
}
