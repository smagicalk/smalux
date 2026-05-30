//! Service 内部上报构建。

use crate::config::AgentConfig;
use crate::service::outbound::{OutboundEvent, OutboundSender, OutboundSequence, ReportEnvelope};
#[cfg(test)]
use crate::telemetry::ReportEvent;
use crate::telemetry::{TelemetryAggregator, TelemetryState};
use smalux_protocol::OutboundReport;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval};

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
    state: &TelemetryState,
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

/// 按配置频率构建 report，并放入导出队列。
pub(crate) async fn reporter_loop(
    state: Arc<RwLock<TelemetryState>>,
    agent_version: &'static str,
    mut config_rx: watch::Receiver<AgentConfig>,
    mut shutdown: watch::Receiver<bool>,
    outbound_tx: OutboundSender,
    sequence: OutboundSequence,
    mut reporter_commands: ReporterCommandReceiver,
) {
    let mut config = config_rx.borrow().clone();
    let mut report_tick = interval(config.report.interval);
    report_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut force_snapshot_tick = interval(FORCE_SNAPSHOT_CHECK_INTERVAL);
    force_snapshot_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut config_rx_open = true;
    let mut reporter_commands_open = true;
    let mut aggregator = TelemetryAggregator::default();
    let mut force_snapshot_pending = false;
    let mut force_snapshot_reason = None;
    let mut last_forced_snapshot_at = None;

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
                if next.report.interval != config.report.interval {
                    report_tick = interval(next.report.interval);
                    report_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
                }
                config = next;
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

/// 判断强制 snapshot 是否已经超过最小间隔。
fn force_snapshot_due(previous: Option<Instant>, min_interval: std::time::Duration) -> bool {
    previous
        .map(|previous| previous.elapsed() >= min_interval)
        .unwrap_or(true)
}

/// 构建并投递普通上报事件。
async fn queue_next_report(
    state: &Arc<RwLock<TelemetryState>>,
    agent_version: &str,
    config: &AgentConfig,
    outbound_tx: &OutboundSender,
    sequence: &OutboundSequence,
    aggregator: &mut TelemetryAggregator,
) -> anyhow::Result<()> {
    let event = {
        let state = state.read().await;
        aggregator.next_report_event(&state, agent_version, config, || sequence.next())
    };

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
    state: &Arc<RwLock<TelemetryState>>,
    agent_version: &str,
    outbound_tx: &OutboundSender,
    sequence: &OutboundSequence,
    aggregator: &mut TelemetryAggregator,
    reason: Option<&str>,
) -> anyhow::Result<()> {
    let outbound = {
        let state = state.read().await;
        aggregator.force_snapshot_event(&state, agent_version, || sequence.next())?
    }
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
    fn ready_state() -> TelemetryState {
        let mut state = TelemetryState::default();
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
        let state = Arc::new(RwLock::new(ready_state()));
        let mut config = AgentConfig::default();
        config.report.interval = Duration::from_millis(100);
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(
            state,
            "0.1.0-test",
            config_rx,
            shutdown_rx,
            outbound_tx,
            sequence,
            reporter_command_rx,
        ));

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

    /// 验证 server 强制 snapshot 请求会绕过普通 tick 立即进入出站队列。
    #[tokio::test]
    async fn reporter_loop_queues_forced_snapshot() {
        let state = Arc::new(RwLock::new(ready_state()));
        let mut config = AgentConfig::default();
        config.report.interval = Duration::from_secs(60);
        config.report.force_snapshot_min_interval = Duration::from_millis(100);
        let (_config_tx, config_rx) = watch::channel(config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (outbound_tx, mut outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (reporter_command_tx, reporter_command_rx) = reporter_command_channel();

        let task = tokio::spawn(reporter_loop(
            state,
            "0.1.0-test",
            config_rx,
            shutdown_rx,
            outbound_tx,
            sequence,
            reporter_command_rx,
        ));

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
