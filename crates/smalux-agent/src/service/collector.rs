//! Service 本机指标采集调度。

use crate::collect::LocalCollector;
use crate::config::AgentConfig;
use crate::telemetry::TelemetryUpdate;
use smalux_core::model::info::MetricLevel;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};

use super::message::TelemetryUpdateSender;

/// 采集控制命令队列容量。
const COLLECTOR_COMMAND_CAPACITY: usize = 16;
/// 所有采样组都关闭时的占位休眠时间；配置变化和 shutdown 会打断该等待。
const DISABLED_COLLECTOR_SLEEP: Duration = Duration::from_secs(3600);

/// 采集控制命令发送端。
pub(crate) type CollectorCommandSender = mpsc::Sender<CollectorCommand>;

/// 采集控制命令接收端。
pub(crate) type CollectorCommandReceiver = mpsc::Receiver<CollectorCommand>;

/// 创建采集控制命令通道。
pub(crate) fn collector_command_channel() -> (CollectorCommandSender, CollectorCommandReceiver) {
    mpsc::channel(COLLECTOR_COMMAND_CAPACITY)
}

/// 采集循环支持的控制命令。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum CollectorCommand {
    /// 立即采样一次进程信息。
    SampleProcessesOnce {
        /// 本次采集级别。
        level: MetricLevel,
        /// 本次返回条数上限。
        limit: usize,
    },
    /// 立即采样一次 Socket 信息。
    SampleSocketsOnce {
        /// 本次采集级别。
        level: MetricLevel,
        /// 本次返回条数上限。
        limit: usize,
    },
}

/// 单次调度点中到期的指标组。
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct DueMetricGroups {
    /// 核心指标是否到期。
    core: bool,
    /// 磁盘指标是否到期。
    disk: bool,
    /// 网络指标是否到期。
    network: bool,
    /// 进程指标是否到期。
    processes: bool,
    /// Socket 指标是否到期。
    sockets: bool,
}

impl DueMetricGroups {
    /// 判断是否至少有一个采样组到期。
    fn any(self) -> bool {
        self.core || self.disk || self.network || self.processes || self.sockets
    }

    /// 返回本次到期的采样组数量，用于日志排查调度合并是否符合预期。
    fn count(self) -> usize {
        [
            self.core,
            self.disk,
            self.network,
            self.processes,
            self.sockets,
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
    }
}

/// 采集调度表。
///
/// 所有指标共用一个调度器，避免多个 interval 同时到期时被拆成多条上报。
#[derive(Debug, Clone)]
struct MetricSchedule {
    /// 下一次核心指标采样时间。
    core_next: Instant,
    /// 下一次磁盘指标采样时间。
    disk_next: Instant,
    /// 下一次网络指标采样时间。
    network_next: Instant,
    /// 下一次进程指标采样时间。
    processes_next: Instant,
    /// 下一次 Socket 指标采样时间。
    sockets_next: Instant,
}

impl MetricSchedule {
    /// 按当前时间创建采集调度表。
    fn new(config: &AgentConfig) -> Self {
        Self::new_at(config, Instant::now())
    }

    /// 按指定起点创建采集调度表，测试可用固定起点验证对齐行为。
    fn new_at(config: &AgentConfig, now: Instant) -> Self {
        Self {
            core_next: now + config.core.interval,
            disk_next: now + config.disk.interval,
            network_next: now + config.network.interval,
            processes_next: now + config.processes.interval,
            sockets_next: now + config.sockets.interval,
        }
    }

    /// 判断当前是否存在启用的采样组。
    fn has_enabled_groups(&self, config: &AgentConfig) -> bool {
        config.core.enabled
            || config.disk.enabled
            || config.network.enabled
            || config.processes.enabled
            || config.sockets.enabled
    }

    /// 返回下一次需要唤醒的采集时间。
    fn next_due(&self, config: &AgentConfig) -> Instant {
        let mut next = None;
        push_next_due(&mut next, config.core.enabled, self.core_next);
        push_next_due(&mut next, config.disk.enabled, self.disk_next);
        push_next_due(&mut next, config.network.enabled, self.network_next);
        push_next_due(&mut next, config.processes.enabled, self.processes_next);
        push_next_due(&mut next, config.sockets.enabled, self.sockets_next);
        next.unwrap_or_else(|| Instant::now() + DISABLED_COLLECTOR_SLEEP)
    }

    /// 计算当前调度点到期的采样组。
    fn due_groups(&self, now: Instant, config: &AgentConfig) -> DueMetricGroups {
        DueMetricGroups {
            core: config.core.enabled && self.core_next <= now,
            disk: config.disk.enabled && self.disk_next <= now,
            network: config.network.enabled && self.network_next <= now,
            processes: config.processes.enabled && self.processes_next <= now,
            sockets: config.sockets.enabled && self.sockets_next <= now,
        }
    }

    /// 将已采样的 group 推进到下一次调度点；错过的旧 tick 会被跳过。
    fn reschedule(&mut self, due: DueMetricGroups, config: &AgentConfig, now: Instant) {
        if due.core {
            advance_due(&mut self.core_next, config.core.interval, now);
        }
        if due.disk {
            advance_due(&mut self.disk_next, config.disk.interval, now);
        }
        if due.network {
            advance_due(&mut self.network_next, config.network.interval, now);
        }
        if due.processes {
            advance_due(&mut self.processes_next, config.processes.interval, now);
        }
        if due.sockets {
            advance_due(&mut self.sockets_next, config.sockets.interval, now);
        }
    }
}

/// 本机指标采集循环。
///
/// 这个循环由单个任务持有 `LocalCollector`，避免多个异步任务同时刷新同一组 sysinfo 状态。
pub(crate) async fn collector_loop(
    mut collector: LocalCollector,
    telemetry_tx: TelemetryUpdateSender,
    mut config_rx: watch::Receiver<AgentConfig>,
    mut command_rx: CollectorCommandReceiver,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut config = config_rx.borrow().clone();
    let mut schedule = MetricSchedule::new(&config);
    let mut config_rx_open = true;
    let mut command_rx_open = true;

    loop {
        let next_due = schedule.next_due(&config);
        tokio::select! {
            _ = sleep_until(next_due), if schedule.has_enabled_groups(&config) => {
                let now = Instant::now();
                let due = schedule.due_groups(now, &config);
                if !due.any() {
                    continue;
                }
                tracing::debug!(
                    due_group_count = due.count(),
                    core_due = due.core,
                    disk_due = due.disk,
                    network_due = due.network,
                    processes_due = due.processes,
                    sockets_due = due.sockets,
                    "collector sampling due metric groups"
                );
                if let Some(update) = sample_due_groups(&mut collector, &config, due)
                    && !publish_update(&telemetry_tx, update).await
                {
                    break;
                }
                schedule.reschedule(due, &config, Instant::now());
            }
            command = command_rx.recv(), if command_rx_open => {
                match command {
                    Some(command) => {
                        if !handle_collector_command(&mut collector, &telemetry_tx, command).await {
                            break;
                        }
                    }
                    None => {
                        command_rx_open = false;
                        tracing::debug!("Collector command channel closed");
                    }
                }
            }
            changed = config_rx.changed(), if config_rx_open => {
                if changed.is_err() {
                    config_rx_open = false;
                    tracing::debug!("Collector config channel closed");
                    continue;
                }

                let next = config_rx.borrow().clone();
                schedule = MetricSchedule::new(&next);
                config = next;
                tracing::info!(
                    core_enabled = config.core.enabled,
                    core_interval_ms = config.core.interval.as_millis(),
                    disk_enabled = config.disk.enabled,
                    disk_interval_ms = config.disk.interval.as_millis(),
                    network_enabled = config.network.enabled,
                    network_interval_ms = config.network.interval.as_millis(),
                    processes_enabled = config.processes.enabled,
                    processes_interval_ms = config.processes.interval.as_millis(),
                    sockets_enabled = config.sockets.enabled,
                    sockets_interval_ms = config.sockets.interval.as_millis(),
                    "Collector config updated"
                );
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("Collector loop shutting down");
                    break;
                }
            }
        }
    }
}

/// 按同一调度点到期的 group 执行采样，并压缩成一条 telemetry update。
fn sample_due_groups(
    collector: &mut LocalCollector,
    config: &AgentConfig,
    due: DueMetricGroups,
) -> Option<TelemetryUpdate> {
    let mut updates = Vec::new();

    if due.core {
        let core = collector.sample_core();
        tracing::debug!(
            sampled_at = core.sampled_at,
            cpu_usage = core.value.cpu.cpu_usage,
            memory_usage_bytes = core.value.memory.memory_usage,
            memory_total_bytes = core.value.memory.memory_total,
            load1 = core.value.load_avg.one,
            "core metrics sampled"
        );
        updates.push(TelemetryUpdate::Core(core));
    }

    if due.disk {
        let disk = collector.sample_disk(config.disk.include_per_device);
        tracing::debug!(
            sampled_at = disk.sampled_at,
            warmed_up = disk.value.warmed_up,
            include_per_device = config.disk.include_per_device,
            device_count = disk.value.disks.len(),
            total_space_bytes = disk.value.total_space,
            available_space_bytes = disk.value.available_space,
            io_bytes_per_sec = disk.value.io_bytes_per_sec,
            "disk metrics sampled"
        );
        updates.push(TelemetryUpdate::Disk(disk));
    }

    if due.network {
        let network = collector.sample_network(
            config.network.include_per_interface,
            &config.network.include_interfaces,
            &config.network.exclude_interfaces,
        );
        tracing::debug!(
            sampled_at = network.sampled_at,
            warmed_up = network.value.warmed_up,
            include_per_interface = config.network.include_per_interface,
            interface_count = network.value.networks.len(),
            received_bytes_per_sec = network.value.received_bytes_per_sec,
            transmitted_bytes_per_sec = network.value.transmitted_bytes_per_sec,
            network_bytes_per_sec = network.value.network_bytes_per_sec,
            used_traffic_bytes = network.value.used_traffic_bytes,
            "network metrics sampled"
        );
        updates.push(TelemetryUpdate::Network(network));
    }

    if due.processes {
        let processes = collector.sample_processes(config.processes.level, config.processes.limit);
        tracing::debug!(
            sampled_at = processes.sampled_at,
            level = ?processes.value.level,
            status = ?processes.value.status,
            process_count = processes.value.count,
            light_count = processes.value.light.as_ref().map(|light| light.items.len()),
            detail_count = processes.value.details.as_ref().map(|details| details.items.len()),
            "process metrics sampled"
        );
        updates.push(TelemetryUpdate::Processes(processes));
    }

    if due.sockets {
        let sockets = collector.sample_sockets(config.sockets.level, config.sockets.limit);
        tracing::debug!(
            sampled_at = sockets.sampled_at,
            level = ?sockets.value.level,
            status = ?sockets.value.status,
            tcp_count = sockets.value.tcp,
            udp_count = sockets.value.udp,
            source = ?sockets.value.source,
            accuracy = ?sockets.value.accuracy,
            detail_count = sockets.value.details.as_ref().map(|details| details.items.len()),
            "socket metrics sampled"
        );
        updates.push(TelemetryUpdate::Sockets(sockets));
    }

    compact_updates(updates)
}

/// 将一组更新压缩成单条内部事件。
fn compact_updates(mut updates: Vec<TelemetryUpdate>) -> Option<TelemetryUpdate> {
    match updates.len() {
        0 => None,
        1 => updates.pop(),
        _ => Some(TelemetryUpdate::Batch(updates)),
    }
}

/// 执行采集控制命令并发布采样更新。
async fn handle_collector_command(
    collector: &mut LocalCollector,
    telemetry_tx: &TelemetryUpdateSender,
    command: CollectorCommand,
) -> bool {
    match command {
        CollectorCommand::SampleProcessesOnce { level, limit } => {
            let processes = collector.sample_processes(level, limit);
            if !publish_update(telemetry_tx, TelemetryUpdate::Processes(processes)).await {
                return false;
            }
            tracing::info!(level = ?level, limit, "one-shot process collection completed");
        }
        CollectorCommand::SampleSocketsOnce { level, limit } => {
            let sockets = collector.sample_sockets(level, limit);
            if !publish_update(telemetry_tx, TelemetryUpdate::Sockets(sockets)).await {
                return false;
            }
            tracing::info!(level = ?level, limit, "one-shot socket collection completed");
        }
    }
    true
}

/// 将采样结果提交给 reporter。
async fn publish_update(telemetry_tx: &TelemetryUpdateSender, update: TelemetryUpdate) -> bool {
    let kind = update.kind();
    match telemetry_tx.send(update).await {
        Ok(()) => {
            tracing::debug!(
                kind,
                queue_capacity = telemetry_tx.max_capacity(),
                "telemetry update published"
            );
            true
        }
        Err(_) => {
            tracing::warn!(
                kind,
                "Collector stopped because telemetry update channel is closed"
            );
            false
        }
    }
}

/// 将候选唤醒时间并入最早唤醒时间。
fn push_next_due(next: &mut Option<Instant>, enabled: bool, candidate: Instant) {
    if !enabled {
        return;
    }
    *next = Some(
        next.map(|current| current.min(candidate))
            .unwrap_or(candidate),
    );
}

/// 推进单个采样组的下一次调度时间；落后的 tick 会被跳过，避免补采旧数据。
fn advance_due(next: &mut Instant, interval: Duration, now: Instant) {
    while *next <= now {
        *next += interval;
    }
}

#[cfg(test)]
mod tests {
    //! 采集控制命令测试。

    use super::*;

    /// 验证不同采样间隔会在共同到期点合并。
    #[test]
    fn metric_schedule_coalesces_common_due_time() {
        let mut config = AgentConfig::default();
        config.core.enabled = false;
        config.disk.enabled = true;
        config.disk.interval = Duration::from_secs(2);
        config.network.enabled = true;
        config.network.interval = Duration::from_secs(3);
        config.processes.enabled = false;
        config.sockets.enabled = false;
        let start = Instant::now();
        let mut schedule = MetricSchedule::new_at(&config, start);

        let due = schedule.due_groups(start + Duration::from_secs(2), &config);
        assert_eq!(
            due,
            DueMetricGroups {
                disk: true,
                ..DueMetricGroups::default()
            }
        );
        schedule.reschedule(due, &config, start + Duration::from_secs(2));

        let due = schedule.due_groups(start + Duration::from_secs(3), &config);
        assert_eq!(
            due,
            DueMetricGroups {
                network: true,
                ..DueMetricGroups::default()
            }
        );
        schedule.reschedule(due, &config, start + Duration::from_secs(3));

        let due = schedule.due_groups(start + Duration::from_secs(4), &config);
        assert_eq!(
            due,
            DueMetricGroups {
                disk: true,
                ..DueMetricGroups::default()
            }
        );
        schedule.reschedule(due, &config, start + Duration::from_secs(4));

        let due = schedule.due_groups(start + Duration::from_secs(6), &config);
        assert_eq!(
            due,
            DueMetricGroups {
                disk: true,
                network: true,
                ..DueMetricGroups::default()
            }
        );
    }

    /// 验证同一调度点的多个采样结果会合并成一个 batch update。
    #[test]
    fn sample_due_groups_batches_multiple_updates() {
        let mut collector = LocalCollector::new();
        let config = AgentConfig::default();

        let update = sample_due_groups(
            &mut collector,
            &config,
            DueMetricGroups {
                core: true,
                disk: true,
                ..DueMetricGroups::default()
            },
        )
        .unwrap();

        match update {
            TelemetryUpdate::Batch(updates) => {
                assert_eq!(updates.len(), 2);
                assert!(matches!(&updates[0], TelemetryUpdate::Core(_)));
                assert!(matches!(&updates[1], TelemetryUpdate::Disk(_)));
            }
            _ => panic!("expected batch telemetry update"),
        }
    }

    /// 验证单个采样组不会额外包一层 batch。
    #[test]
    fn sample_due_groups_keeps_single_update_flat() {
        let mut collector = LocalCollector::new();
        let config = AgentConfig::default();

        let update = sample_due_groups(
            &mut collector,
            &config,
            DueMetricGroups {
                network: true,
                ..DueMetricGroups::default()
            },
        )
        .unwrap();

        assert!(matches!(update, TelemetryUpdate::Network(_)));
    }

    /// 验证没有到期采样组时不会提交空 update。
    #[test]
    fn sample_due_groups_skips_empty_due_groups() {
        let mut collector = LocalCollector::new();
        let config = AgentConfig::default();

        let update = sample_due_groups(&mut collector, &config, DueMetricGroups::default());

        assert!(update.is_none());
    }

    /// 验证一次性进程采集命令会写入缓存。
    #[tokio::test]
    async fn collector_command_samples_processes_once() {
        let mut collector = LocalCollector::new();
        let (telemetry_tx, mut telemetry_rx) = mpsc::channel(4);

        assert!(
            handle_collector_command(
                &mut collector,
                &telemetry_tx,
                CollectorCommand::SampleProcessesOnce {
                    level: MetricLevel::Count,
                    limit: 10,
                },
            )
            .await
        );

        let update = telemetry_rx.recv().await.unwrap();
        assert!(matches!(update, TelemetryUpdate::Processes(_)));
    }

    /// 验证一次性 Socket 采集命令会写入缓存。
    #[tokio::test]
    async fn collector_command_samples_sockets_once() {
        let mut collector = LocalCollector::new();
        let (telemetry_tx, mut telemetry_rx) = mpsc::channel(4);

        assert!(
            handle_collector_command(
                &mut collector,
                &telemetry_tx,
                CollectorCommand::SampleSocketsOnce {
                    level: MetricLevel::Count,
                    limit: 10,
                },
            )
            .await
        );

        let update = telemetry_rx.recv().await.unwrap();
        assert!(matches!(update, TelemetryUpdate::Sockets(_)));
    }
}
