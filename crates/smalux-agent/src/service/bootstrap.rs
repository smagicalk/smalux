//! Service 启动采样和第一包身份门禁。

use super::options::ServiceState;
use crate::collect::{LocalCollector, get_info};
use crate::config::AgentConfig;
use crate::telemetry::TelemetryState;
use smalux_core::model::info::{PublicIpInfo, PublicIpStatus};
use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior, interval_at};

/// 执行启动采样。
///
/// 公网 IP 默认只写入状态；只有显式开启第一包门禁时，调用方才会继续重试。
pub(crate) async fn bootstrap_once(
    collector: &mut LocalCollector,
    config_rx: &watch::Receiver<AgentConfig>,
) -> ServiceState {
    let mut store = TelemetryState::default();
    let config = config_rx.borrow().clone();

    tracing::info!("Starting initial metric bootstrap");
    store.configure_metric_groups(
        config.core.enabled,
        config.disk.enabled,
        config.network.enabled,
        config.processes.enabled,
        config.sockets.enabled,
    );
    store.set_system(get_info());
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

    let identity = collector
        .sample_identity(config.agent_id.clone(), &config.public_ip)
        .await;
    log_public_ip_status("bootstrap", &identity.public_ip);
    store.set_identity(identity);

    let first_report_ready = store.first_report_ready();
    ServiceState {
        store,
        first_report_ready,
    }
}

/// 重试公网 IP 身份信息采集，成功后更新缓存。
pub(crate) async fn retry_identity_until_ready(
    collector: &mut LocalCollector,
    store: &mut TelemetryState,
    config_rx: &mut watch::Receiver<AgentConfig>,
) {
    let mut config = config_rx.borrow().clone();
    if !public_ip_required_for_first_report(&config) {
        tracing::info!("Public IP retry skipped; first report no longer requires public IP");
        return;
    }

    let mut retry_tick = public_ip_retry_interval(&config);
    let mut config_rx_open = true;

    while !store.public_ip_ready() {
        tokio::select! {
            _ = retry_tick.tick() => {}
            changed = config_rx.changed(), if config_rx_open => {
                if changed.is_err() {
                    config_rx_open = false;
                    tracing::debug!("Public IP retry config channel closed");
                    continue;
                }

                config = config_rx.borrow().clone();
                if !public_ip_required_for_first_report(&config) {
                    tracing::info!("Public IP retry stopped; first report no longer requires public IP");
                    break;
                }
                retry_tick = public_ip_retry_interval(&config);
                tracing::info!("Public IP retry config updated");
                continue;
            }
        }
        let identity = collector
            .sample_identity(config.agent_id.clone(), &config.public_ip)
            .await;
        log_public_ip_status("retry", &identity.public_ip);
        store.set_identity(identity);
        if store.public_ip_ready() {
            break;
        }
    }
}

/// 判断第一包是否仍需要等待公网 IP。
pub(super) fn public_ip_required_for_first_report(config: &AgentConfig) -> bool {
    config.public_ip.enabled && config.public_ip.required_for_first_report
}

/// 创建公网 IP 重试定时器；第一次 tick 延迟到 retry_interval 之后。
fn public_ip_retry_interval(config: &AgentConfig) -> Interval {
    let mut retry_tick = interval_at(
        tokio::time::Instant::now() + config.public_ip.retry_interval,
        config.public_ip.retry_interval,
    );
    retry_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    retry_tick
}

/// 按公网 IP 状态输出启动和重试日志。
fn log_public_ip_status(stage: &'static str, public_ip: &PublicIpInfo) {
    match public_ip.status {
        PublicIpStatus::Ready => {
            if let Some(ip) = public_ip.ip {
                tracing::info!(
                    stage = stage,
                    public_ip = %ip,
                    source = ?public_ip.source.as_ref(),
                    "Public IP ready"
                );
            } else {
                tracing::warn!(stage = stage, "Public IP marked ready without IP address");
            }
        }
        PublicIpStatus::Failed => {
            tracing::warn!(
                stage = stage,
                error = public_ip.error.as_deref().unwrap_or("unknown"),
                "Public IP lookup failed"
            );
        }
        PublicIpStatus::Disabled => {
            tracing::info!(stage = stage, "Public IP collection disabled");
        }
        PublicIpStatus::Stale => {
            tracing::warn!(
                stage = stage,
                public_ip = ?public_ip.ip,
                error = public_ip.error.as_deref().unwrap_or("unknown"),
                "Public IP stale"
            );
        }
        PublicIpStatus::Pending => {
            tracing::debug!(stage = stage, "Public IP lookup pending");
        }
    }
}

#[cfg(test)]
mod tests {
    //! 启动采样和身份重试测试。

    use super::*;
    use std::time::Duration;

    /// 验证第一包公网 IP 门禁同时受 enabled 和 required_for_first_report 控制。
    #[test]
    fn public_ip_required_for_first_report_respects_enabled_and_required_flags() {
        let mut config = AgentConfig::default();

        assert!(!public_ip_required_for_first_report(&config));

        config.public_ip.required_for_first_report = true;
        assert!(public_ip_required_for_first_report(&config));
        config.public_ip.enabled = false;
        assert!(!public_ip_required_for_first_report(&config));
    }

    /// 验证等待期间关闭第一包公网 IP 门禁后，重试循环会退出。
    #[tokio::test]
    async fn retry_identity_stops_when_first_report_no_longer_requires_public_ip() {
        let mut config = AgentConfig::default();
        config.public_ip.required_for_first_report = true;
        config.public_ip.retry_interval = Duration::from_millis(200);
        let (sender, mut config_rx) = watch::channel(config.clone());
        let mut collector = LocalCollector::new();
        let mut store = TelemetryState::default();

        let sender_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            config.public_ip.required_for_first_report = false;
            sender.send_replace(config);
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            retry_identity_until_ready(&mut collector, &mut store, &mut config_rx),
        )
        .await
        .unwrap();
        sender_task.await.unwrap();

        assert!(!store.first_report_ready());
    }
}
