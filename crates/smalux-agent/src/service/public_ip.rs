//! Service 公网 IP 低频刷新调度。

use crate::collect::LocalCollector;
use crate::config::AgentConfig;
use crate::telemetry::TelemetryUpdate;
use smalux_core::model::info::{IdentityInfo, PublicIpInfo, PublicIpStatus};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior, interval_at};

use super::message::TelemetryUpdateSender;

/// 低频刷新公网 IP。
pub(crate) async fn refresh_identity_once(
    collector: &mut LocalCollector,
    config: &AgentConfig,
) -> IdentityInfo {
    let identity = collector
        .sample_identity(config.agent_id.clone(), &config.public_ip)
        .await;
    log_public_ip_refresh_status(&identity.public_ip);
    identity
}

/// 公网 IP 低频刷新循环。
pub(crate) async fn public_ip_refresh_loop(
    mut collector: LocalCollector,
    telemetry_tx: TelemetryUpdateSender,
    mut config_rx: watch::Receiver<AgentConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut config = config_rx.borrow().clone();
    let mut refresh_tick = public_ip_refresh_interval(config.public_ip.refresh_interval);
    let mut config_rx_open = true;

    loop {
        tokio::select! {
            _ = refresh_tick.tick(), if config.public_ip.enabled => {
                let identity = refresh_identity_once(&mut collector, &config).await;
                if !publish_identity_refresh(&telemetry_tx, identity).await {
                    break;
                }
            }
            changed = config_rx.changed(), if config_rx_open => {
                if changed.is_err() {
                    config_rx_open = false;
                    tracing::debug!("Public IP refresh config channel closed");
                    continue;
                }

                let next = config_rx.borrow().clone();
                if next.public_ip.refresh_interval != config.public_ip.refresh_interval {
                    refresh_tick = public_ip_refresh_interval(next.public_ip.refresh_interval);
                }
                let refresh_immediately = public_ip_identity_inputs_changed(&config, &next);
                config = next;
                tracing::info!("Public IP refresh config updated");
                if refresh_immediately {
                    let identity = refresh_identity_once(&mut collector, &config).await;
                    if !publish_identity_refresh(&telemetry_tx, identity).await {
                        break;
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("Public IP refresh loop shutting down");
                    break;
                }
            }
        }
    }
}

/// 将身份刷新结果提交给 reporter。
async fn publish_identity_refresh(
    telemetry_tx: &TelemetryUpdateSender,
    identity: IdentityInfo,
) -> bool {
    match telemetry_tx
        .send(TelemetryUpdate::IdentityRefresh(identity))
        .await
    {
        Ok(()) => true,
        Err(_) => {
            tracing::warn!("Public IP refresh stopped because telemetry update channel is closed");
            false
        }
    }
}

/// 创建公网 IP 低频刷新定时器；启动后先等待 refresh_interval。
fn public_ip_refresh_interval(interval: Duration) -> Interval {
    let mut tick = interval_at(tokio::time::Instant::now() + interval, interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick
}

/// 判断配置变化是否需要立刻刷新身份里的公网 IP 状态。
fn public_ip_identity_inputs_changed(previous: &AgentConfig, next: &AgentConfig) -> bool {
    previous.agent_id != next.agent_id
        || previous.public_ip.enabled != next.public_ip.enabled
        || previous.public_ip.prefer_interface_candidate
            != next.public_ip.prefer_interface_candidate
        || previous.public_ip.verify_interface_candidate
            != next.public_ip.verify_interface_candidate
        || previous.public_ip.lookup_timeout != next.public_ip.lookup_timeout
        || previous.public_ip.max_concurrency != next.public_ip.max_concurrency
}

/// 按刷新结果输出英文日志。
fn log_public_ip_refresh_status(public_ip: &PublicIpInfo) {
    match public_ip.status {
        PublicIpStatus::Ready => {
            if let Some(ip) = public_ip.ip {
                tracing::info!(
                    public_ip = %ip,
                    source = ?public_ip.source.as_ref(),
                    "Public IP refreshed"
                );
            } else {
                tracing::warn!("Public IP refresh returned ready without IP address");
            }
        }
        PublicIpStatus::Failed => {
            tracing::warn!(
                error = public_ip.error.as_deref().unwrap_or("unknown"),
                "Public IP refresh failed"
            );
        }
        PublicIpStatus::Disabled => {
            tracing::info!("Public IP refresh disabled");
        }
        PublicIpStatus::Stale => {
            tracing::warn!(
                public_ip = ?public_ip.ip,
                error = public_ip.error.as_deref().unwrap_or("unknown"),
                "Public IP refresh stale"
            );
        }
        PublicIpStatus::Pending => {
            tracing::debug!("Public IP refresh pending");
        }
    }
}

#[cfg(test)]
mod tests {
    //! 公网 IP 低频刷新测试。

    use super::*;

    /// 验证影响公网 IP 结果的配置变化会触发立即刷新。
    #[test]
    fn public_ip_identity_inputs_changed_detects_lookup_input_changes() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();
        assert!(!public_ip_identity_inputs_changed(&previous, &next));

        next.public_ip.enabled = !previous.public_ip.enabled;
        assert!(public_ip_identity_inputs_changed(&previous, &next));
    }
}
