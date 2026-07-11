//! Telemetry 上报聚合器。
//!
//! 这里负责把最新 telemetry 状态转换为 snapshot、delta 或 heartbeat。导出层只消费
//! 已经聚合好的 `ReportEvent`，不直接理解采样缓存细节。

use super::{LatestTelemetry, ReportEvent};
use crate::collect::unix_timestamp_secs;
use crate::config::AgentConfig;
use smalux_core::model::info::AgentReport;
use smalux_protocol::{ClientEvent, DeltaReport, Heartbeat};
use std::time::{Duration, Instant};

/// 根据上次发送状态决定本次发 snapshot、delta、heartbeat 或跳过。
#[derive(Debug, Default)]
pub(crate) struct TelemetryAggregator {
    /// 最近一次发送给导出层的完整报告。
    last_report: Option<AgentReport>,
    /// 最近一次发送给导出层的监控状态序号。
    last_report_sequence: Option<u64>,
    /// 最近一次发送 snapshot、delta 或 heartbeat 的单调时间。
    last_outbound_instant: Option<Instant>,
    /// 最近一次发送完整 snapshot 的时间。
    last_snapshot_at: Option<u64>,
    /// 最近一次发送完整 snapshot 的单调时间，用于保留亚秒间隔精度。
    last_snapshot_instant: Option<Instant>,
    /// 最近一次发送完整 snapshot 的序号。
    last_snapshot_sequence: Option<u64>,
}

impl TelemetryAggregator {
    /// 从 telemetry 状态和配置构建下一条待发送事件。
    pub(crate) fn next_report_event(
        &mut self,
        state: &LatestTelemetry,
        agent_version: &str,
        config: &AgentConfig,
        next_sequence: impl FnMut() -> u64,
    ) -> anyhow::Result<Option<ReportEvent>> {
        let Some(outbound) = self.next_outbound(state, agent_version, config, next_sequence)?
        else {
            return Ok(None);
        };

        Ok(Some(ReportEvent::new(outbound)))
    }

    /// 从当前 telemetry 状态强制构建完整 snapshot。
    pub(crate) fn force_snapshot_event(
        &mut self,
        state: &LatestTelemetry,
        agent_version: &str,
        mut next_sequence: impl FnMut() -> u64,
    ) -> anyhow::Result<ReportEvent> {
        let report = state.build_report(agent_version)?;
        let now = unix_timestamp_secs();
        let now_instant = Instant::now();
        Ok(ReportEvent::new(self.snapshot(
            report,
            now,
            now_instant,
            next_sequence(),
        )))
    }

    /// 从 telemetry 状态和配置构建下一条内部上报语义。
    fn next_outbound(
        &mut self,
        state: &LatestTelemetry,
        agent_version: &str,
        config: &AgentConfig,
        mut next_sequence: impl FnMut() -> u64,
    ) -> anyhow::Result<Option<ClientEvent>> {
        let report = state.build_report(agent_version)?;
        let now = unix_timestamp_secs();
        let now_instant = Instant::now();
        tracing::trace!(
            has_last_report = self.last_report.is_some(),
            last_report_sequence = self.last_report_sequence,
            last_snapshot_sequence = self.last_snapshot_sequence,
            delta_enabled = config.report.delta_enabled,
            heartbeat_enabled = config.report.heartbeat_enabled,
            "telemetry aggregator evaluating report policy"
        );

        if self.should_send_snapshot(now_instant, config) {
            tracing::trace!(
                reason = snapshot_reason(self.last_report.is_some(), config.report.delta_enabled),
                snapshot_interval_ms = config.report.snapshot_interval.as_millis(),
                "telemetry aggregator selected snapshot"
            );
            return Ok(Some(self.snapshot(
                report,
                now,
                now_instant,
                next_sequence(),
            )));
        }

        if config.report.delta_enabled
            && let Some(delta) = self.build_delta(&report, now)
        {
            tracing::trace!(
                base_sequence = delta.base_sequence,
                identity_changed = delta.identity.is_some(),
                core_changed = delta.core.is_some(),
                disk_changed = delta.disk.is_some(),
                network_changed = delta.network.is_some(),
                processes_changed = delta.processes.is_some(),
                sockets_changed = delta.sockets.is_some(),
                "telemetry aggregator selected delta"
            );
            return Ok(Some(self.delta(
                report,
                delta,
                now,
                now_instant,
                next_sequence(),
            )));
        }

        if self.should_send_heartbeat(now_instant, config) {
            tracing::trace!(
                heartbeat_interval_ms = config.report.heartbeat_interval.as_millis(),
                last_snapshot_sequence = self.last_snapshot_sequence,
                "telemetry aggregator selected heartbeat"
            );
            return Ok(Some(self.heartbeat(
                &report,
                now,
                now_instant,
                next_sequence(),
            )));
        }

        tracing::trace!("telemetry aggregator skipped report; no delta or heartbeat due");
        Ok(None)
    }

    /// 判断是否需要发送完整 snapshot。
    fn should_send_snapshot(&self, now: Instant, config: &AgentConfig) -> bool {
        if self.last_report.is_none() {
            return true;
        }
        if !config.report.delta_enabled {
            return true;
        }

        elapsed_at_least(
            self.last_snapshot_instant,
            now,
            config.report.snapshot_interval,
        )
    }

    /// 判断是否需要发送业务级 heartbeat。
    fn should_send_heartbeat(&self, now: Instant, config: &AgentConfig) -> bool {
        config.report.heartbeat_enabled
            && elapsed_at_least(
                self.last_outbound_instant,
                now,
                config.report.heartbeat_interval,
            )
    }

    /// 构造完整 snapshot，并更新策略状态。
    fn snapshot(
        &mut self,
        report: AgentReport,
        now: u64,
        now_instant: Instant,
        sequence: u64,
    ) -> ClientEvent {
        self.last_snapshot_at = Some(now);
        self.last_snapshot_instant = Some(now_instant);
        self.last_outbound_instant = Some(now_instant);
        self.last_snapshot_sequence = Some(sequence);
        self.last_report_sequence = Some(sequence);
        self.last_report = Some(report.clone());
        ClientEvent::snapshot(sequence, now, report)
    }

    /// 构造 delta，并更新策略状态。
    fn delta(
        &mut self,
        report: AgentReport,
        delta: DeltaReport,
        now: u64,
        now_instant: Instant,
        sequence: u64,
    ) -> ClientEvent {
        let agent_id = report.identity.agent_id.clone();
        self.last_report_sequence = Some(sequence);
        self.last_outbound_instant = Some(now_instant);
        self.last_report = Some(report);
        ClientEvent::delta(agent_id, sequence, now, delta)
    }

    /// 构造业务级 heartbeat。
    fn heartbeat(
        &mut self,
        report: &AgentReport,
        now: u64,
        now_instant: Instant,
        sequence: u64,
    ) -> ClientEvent {
        self.last_outbound_instant = Some(now_instant);
        ClientEvent::heartbeat(
            report.identity.agent_id.clone(),
            sequence,
            now,
            Heartbeat {
                last_report_at: self.last_snapshot_at,
                last_report_sequence: self.last_snapshot_sequence,
            },
        )
    }

    /// 对比当前 report 和上一份状态，构造有变化的 delta。
    fn build_delta(&self, report: &AgentReport, now: u64) -> Option<DeltaReport> {
        let previous = self.last_report.as_ref()?;
        let base_sequence = self.last_report_sequence?;
        let mut delta = DeltaReport {
            base_sequence,
            report_at: now,
            ..DeltaReport::default()
        };
        let mut changed = false;

        if json_changed(&previous.identity, &report.identity) {
            delta.identity = Some(report.identity.clone());
            changed = true;
        }
        if json_changed(&previous.core, &report.core) {
            delta.core = Some(report.core.clone());
            changed = true;
        }
        if json_changed(&previous.disk, &report.disk) {
            delta.disk = Some(report.disk.clone());
            changed = true;
        }
        if json_changed(&previous.network, &report.network) {
            delta.network = Some(report.network.clone());
            changed = true;
        }
        if json_changed(&previous.processes, &report.processes) {
            delta.processes = Some(report.processes.clone());
            changed = true;
        }
        if json_changed(&previous.sockets, &report.sockets) {
            delta.sockets = Some(report.sockets.clone());
            changed = true;
        }

        changed.then_some(delta)
    }
}

/// 判断指定时间点距离上次事件是否已经达到配置间隔。
fn elapsed_at_least(previous_at: Option<Instant>, now: Instant, interval: Duration) -> bool {
    previous_at
        .map(|previous| now.duration_since(previous) >= interval)
        .unwrap_or(true)
}

/// 返回 snapshot 被选中的原因，方便 trace 日志解释聚合策略。
fn snapshot_reason(has_last_report: bool, delta_enabled: bool) -> &'static str {
    if !has_last_report {
        "initial_report"
    } else if !delta_enabled {
        "delta_disabled"
    } else {
        "snapshot_interval_due"
    }
}

/// 用 serde JSON 值比较两个上报片段，避免要求所有模型派生 PartialEq。
fn json_changed<T: serde::Serialize>(previous: &T, current: &T) -> bool {
    match (
        serde_json::to_value(previous),
        serde_json::to_value(current),
    ) {
        (Ok(previous), Ok(current)) => previous != current,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    //! Telemetry 聚合策略测试。

    use super::*;
    use crate::collect::{CoreSample, DiskSample, NetworkSample, ProcessSample, SocketSample};
    use smalux_core::model::info::{
        CoreInfo, DiskInfo, IdentityInfo, MemoryInfo, NetworkInfo, ProcessInfo, SocketAccuracy,
        SocketInfo, SocketSource, SystemInfo,
    };
    use smalux_protocol::ClientEventKind;

    /// 构造已经满足上报条件的状态。
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

    /// 从 aggregator 中构建下一条事件。
    fn next_report(
        aggregator: &mut TelemetryAggregator,
        state: &LatestTelemetry,
        config: &AgentConfig,
    ) -> Option<ClientEvent> {
        let mut next_sequence = aggregator.last_report_sequence.unwrap_or(0);
        aggregator
            .next_report_event(state, "0.1.0-test", config, || {
                next_sequence = next_sequence.saturating_add(1);
                next_sequence
            })
            .unwrap()
            .map(ReportEvent::into_outbound)
    }

    /// 验证默认策略持续发送完整 snapshot，保持兼容。
    #[test]
    fn aggregator_keeps_snapshot_mode_by_default() {
        let state = ready_state();
        let config = AgentConfig::default();
        let mut aggregator = TelemetryAggregator::default();

        let first = next_report(&mut aggregator, &state, &config).unwrap();
        let second = next_report(&mut aggregator, &state, &config).unwrap();

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        assert!(matches!(second.kind, ClientEventKind::Snapshot { .. }));
        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
    }

    /// 验证启用 delta 后，只变化的分组会进入 delta。
    #[test]
    fn aggregator_emits_delta_for_changed_group() {
        let mut state = ready_state();
        let mut config = AgentConfig::default();
        config.report.delta_enabled = true;
        let mut aggregator = TelemetryAggregator::default();
        let first = next_report(&mut aggregator, &state, &config).unwrap();

        state.set_core(CoreSample {
            sampled_at: 10,
            value: CoreInfo {
                memory: MemoryInfo {
                    memory_usage: 42,
                    ..MemoryInfo::default()
                },
                ..CoreInfo::default()
            },
        });
        let second = next_report(&mut aggregator, &state, &config).unwrap();

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        match second.kind {
            ClientEventKind::Delta { delta } => {
                assert_eq!(delta.base_sequence, 1);
                assert!(delta.identity.is_none());
                assert!(delta.core.is_some());
                assert!(delta.disk.is_none());
                assert!(delta.network.is_none());
                assert!(delta.processes.is_none());
                assert!(delta.sockets.is_none());
            }
            _ => panic!("expected delta report"),
        }
    }

    /// 验证进程和 Socket 变化也会进入 delta。
    #[test]
    fn aggregator_emits_delta_for_process_and_socket_changes() {
        let mut state = ready_state();
        let mut config = AgentConfig::default();
        config.report.delta_enabled = true;
        let mut aggregator = TelemetryAggregator::default();
        let first = next_report(&mut aggregator, &state, &config).unwrap();

        state.set_processes(ProcessSample {
            sampled_at: 10,
            value: ProcessInfo::ready(43),
        });
        state.set_sockets(SocketSample {
            sampled_at: 11,
            value: SocketInfo::ready(
                11,
                4,
                SocketSource::SocketTable,
                SocketAccuracy::SocketTable,
            ),
        });
        let second = next_report(&mut aggregator, &state, &config).unwrap();

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        match second.kind {
            ClientEventKind::Delta { delta } => {
                assert_eq!(delta.base_sequence, 1);
                assert!(delta.identity.is_none());
                assert!(delta.core.is_none());
                assert!(delta.disk.is_none());
                assert!(delta.network.is_none());
                assert_eq!(delta.processes.unwrap().unwrap().value.count, 43);
                let sockets = delta.sockets.unwrap().unwrap().value;
                assert_eq!(sockets.tcp, 11);
                assert_eq!(sockets.udp, 4);
            }
            _ => panic!("expected delta report"),
        }
    }

    /// 验证启用 delta 后，无变化且未到心跳时间时会跳过。
    #[test]
    fn aggregator_skips_unchanged_report_without_heartbeat() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.delta_enabled = true;
        let mut aggregator = TelemetryAggregator::default();
        let first = next_report(&mut aggregator, &state, &config).unwrap();
        let second = next_report(&mut aggregator, &state, &config);

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        assert!(second.is_none());
    }

    /// 验证亚秒 snapshot 间隔不会被截断成 0。
    #[test]
    fn aggregator_respects_subsecond_snapshot_interval() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.delta_enabled = true;
        config.report.snapshot_interval = Duration::from_millis(500);
        let mut aggregator = TelemetryAggregator::default();
        let first = next_report(&mut aggregator, &state, &config).unwrap();
        let second = next_report(&mut aggregator, &state, &config);

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        assert!(second.is_none());
    }

    /// 验证亚秒业务心跳间隔不会被截断成 0。
    #[test]
    fn aggregator_respects_subsecond_heartbeat_interval() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.delta_enabled = true;
        config.report.heartbeat_enabled = true;
        config.report.heartbeat_interval = Duration::from_millis(500);
        let mut aggregator = TelemetryAggregator::default();
        let first = next_report(&mut aggregator, &state, &config).unwrap();
        let second = next_report(&mut aggregator, &state, &config);

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        assert!(second.is_none());
    }

    /// 验证启用业务心跳后，无变化时可以发送 heartbeat。
    #[test]
    fn aggregator_emits_heartbeat_when_idle() {
        let state = ready_state();
        let mut config = AgentConfig::default();
        config.report.delta_enabled = true;
        config.report.heartbeat_enabled = true;
        config.report.heartbeat_interval = Duration::from_secs(0);
        let mut aggregator = TelemetryAggregator::default();
        let first = next_report(&mut aggregator, &state, &config).unwrap();
        let second = next_report(&mut aggregator, &state, &config).unwrap();

        assert!(matches!(first.kind, ClientEventKind::Snapshot { .. }));
        match second.kind {
            ClientEventKind::Heartbeat { heartbeat } => {
                assert_eq!(heartbeat.last_report_sequence, Some(1));
            }
            _ => panic!("expected heartbeat report"),
        }
    }
}
