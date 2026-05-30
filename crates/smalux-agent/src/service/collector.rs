//! Service 本机指标采集调度。

use crate::collect::LocalCollector;
use crate::config::AgentConfig;
use crate::telemetry::TelemetryState;
use smalux_core::model::info::MetricLevel;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, watch};
use tokio::time::{Interval, MissedTickBehavior, interval_at};

/// 采集控制命令队列容量。
const COLLECTOR_COMMAND_CAPACITY: usize = 16;

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

/// 根据当前配置刷新一次启用的指标组。
#[cfg(test)]
pub(crate) fn sample_enabled_groups(
    collector: &mut LocalCollector,
    store: &mut TelemetryState,
    config: &AgentConfig,
) {
    store.configure_metric_groups(
        config.core.enabled,
        config.disk.enabled,
        config.network.enabled,
        config.processes.enabled,
        config.sockets.enabled,
    );

    if config.core.enabled {
        store.set_core(collector.sample_core());
    }

    if config.disk.enabled {
        store.set_disk(collector.sample_disk(config.disk.include_per_device));
    }

    if config.network.enabled {
        store.set_network(collector.sample_network(
            config.network.include_per_interface,
            &config.network.include_interfaces,
            &config.network.exclude_interfaces,
        ));
    }

    if config.processes.enabled {
        store.set_processes(
            collector.sample_processes(config.processes.level, config.processes.limit),
        );
    }

    if config.sockets.enabled {
        store.set_sockets(collector.sample_sockets(config.sockets.level, config.sockets.limit));
    }
}

/// 本机指标采集循环。
///
/// 这个循环由单个任务持有 `LocalCollector`，避免多个异步任务同时刷新同一组 sysinfo 状态。
pub(crate) async fn collector_loop(
    mut collector: LocalCollector,
    store: Arc<RwLock<TelemetryState>>,
    mut config_rx: watch::Receiver<AgentConfig>,
    mut command_rx: CollectorCommandReceiver,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut config = config_rx.borrow().clone();
    let mut core_tick = metric_interval(config.core.interval);
    let mut disk_tick = metric_interval(config.disk.interval);
    let mut network_tick = metric_interval(config.network.interval);
    let mut processes_tick = metric_interval(config.processes.interval);
    let mut sockets_tick = metric_interval(config.sockets.interval);
    let mut config_rx_open = true;
    let mut command_rx_open = true;
    store.write().await.configure_metric_groups(
        config.core.enabled,
        config.disk.enabled,
        config.network.enabled,
        config.processes.enabled,
        config.sockets.enabled,
    );

    loop {
        tokio::select! {
            _ = core_tick.tick(), if config.core.enabled => {
                let core = collector.sample_core();
                store.write().await.set_core(core);
            }
            _ = disk_tick.tick(), if config.disk.enabled => {
                let disk = collector.sample_disk(config.disk.include_per_device);
                store.write().await.set_disk(disk);
            }
            _ = network_tick.tick(), if config.network.enabled => {
                let network = collector.sample_network(
                    config.network.include_per_interface,
                    &config.network.include_interfaces,
                    &config.network.exclude_interfaces,
                );
                store.write().await.set_network(network);
            }
            _ = processes_tick.tick(), if config.processes.enabled => {
                let processes = collector.sample_processes(config.processes.level, config.processes.limit);
                store.write().await.set_processes(processes);
            }
            _ = sockets_tick.tick(), if config.sockets.enabled => {
                let sockets = collector.sample_sockets(config.sockets.level, config.sockets.limit);
                store.write().await.set_sockets(sockets);
            }
            command = command_rx.recv(), if command_rx_open => {
                match command {
                    Some(command) => {
                        handle_collector_command(&mut collector, &store, command).await;
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
                if next.core.interval != config.core.interval {
                    core_tick = metric_interval(next.core.interval);
                }
                if next.disk.interval != config.disk.interval {
                    disk_tick = metric_interval(next.disk.interval);
                }
                if next.network.interval != config.network.interval {
                    network_tick = metric_interval(next.network.interval);
                }
                if next.processes.interval != config.processes.interval {
                    processes_tick = metric_interval(next.processes.interval);
                }
                if next.sockets.interval != config.sockets.interval {
                    sockets_tick = metric_interval(next.sockets.interval);
                }
                config = next;
                store.write().await.configure_metric_groups(
                    config.core.enabled,
                    config.disk.enabled,
                    config.network.enabled,
                    config.processes.enabled,
                    config.sockets.enabled,
                );
                tracing::info!("Collector config updated");
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

/// 执行采集控制命令并写入最新缓存。
async fn handle_collector_command(
    collector: &mut LocalCollector,
    store: &Arc<RwLock<TelemetryState>>,
    command: CollectorCommand,
) {
    match command {
        CollectorCommand::SampleProcessesOnce { level, limit } => {
            let processes = collector.sample_processes(level, limit);
            store.write().await.set_processes(processes);
            tracing::info!(level = ?level, limit, "one-shot process collection completed");
        }
        CollectorCommand::SampleSocketsOnce { level, limit } => {
            let sockets = collector.sample_sockets(level, limit);
            store.write().await.set_sockets(sockets);
            tracing::info!(level = ?level, limit, "one-shot socket collection completed");
        }
    }
}

/// 创建指标采样定时器；首次 tick 延迟到配置间隔之后，避免启动后立即重复采样。
fn metric_interval(interval: Duration) -> Interval {
    let mut tick = interval_at(tokio::time::Instant::now() + interval, interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick
}

#[cfg(test)]
mod tests {
    //! 采集控制命令测试。

    use super::*;

    /// 验证一次性进程采集命令会写入缓存。
    #[tokio::test]
    async fn collector_command_samples_processes_once() {
        let mut collector = LocalCollector::new();
        let store = Arc::new(RwLock::new(TelemetryState::default()));

        handle_collector_command(
            &mut collector,
            &store,
            CollectorCommand::SampleProcessesOnce {
                level: MetricLevel::Count,
                limit: 10,
            },
        )
        .await;

        assert!(store.read().await.processes.ready().is_some());
    }

    /// 验证一次性 Socket 采集命令会写入缓存。
    #[tokio::test]
    async fn collector_command_samples_sockets_once() {
        let mut collector = LocalCollector::new();
        let store = Arc::new(RwLock::new(TelemetryState::default()));

        handle_collector_command(
            &mut collector,
            &store,
            CollectorCommand::SampleSocketsOnce {
                level: MetricLevel::Count,
                limit: 10,
            },
        )
        .await;

        assert!(store.read().await.sockets.ready().is_some());
    }
}
