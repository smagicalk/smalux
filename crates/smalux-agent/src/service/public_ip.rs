//! Service 公网 IP 低频刷新调度。

use crate::collect::{LocalCollector, unix_timestamp_secs};
use crate::config::AgentConfig;
use crate::telemetry::TelemetryState;
use smalux_core::model::info::{IdentityInfo, PublicIpInfo, PublicIpStatus};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};
use tokio::time::{Interval, MissedTickBehavior, interval_at};

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
    store: Arc<RwLock<TelemetryState>>,
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
                let mut store = store.write().await;
                apply_identity_refresh_result(&mut store, identity);
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
                    let mut store = store.write().await;
                    apply_identity_refresh_result(&mut store, identity);
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

/// 创建公网 IP 低频刷新定时器；启动后先等待 refresh_interval。
fn public_ip_refresh_interval(interval: Duration) -> Interval {
    let mut tick = interval_at(tokio::time::Instant::now() + interval, interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick
}

/// 写入身份刷新结果；公网 IP 刷新失败时尽量保留旧公网 IP。
fn apply_identity_refresh_result(store: &mut TelemetryState, mut identity: IdentityInfo) {
    if matches!(identity.public_ip.status, PublicIpStatus::Failed) {
        if let Some(previous) = store.identity.ready() {
            let error = identity
                .public_ip
                .error
                .clone()
                .unwrap_or_else(|| "Public IP lookup failed".to_string());
            let last_attempt_at = identity
                .public_ip
                .last_attempt_at
                .unwrap_or_else(unix_timestamp_secs);
            identity.public_ip =
                PublicIpInfo::stale_or_failed(&previous.public_ip, error, last_attempt_at);
        }
    }
    store.set_identity(identity);
}

/// 判断配置变化是否需要立刻刷新身份里的公网 IP 状态。
fn public_ip_identity_inputs_changed(previous: &AgentConfig, next: &AgentConfig) -> bool {
    previous.agent_id != next.agent_id
        || previous.public_ip.enabled != next.public_ip.enabled
        || previous.public_ip.prefer_interface_candidate
            != next.public_ip.prefer_interface_candidate
        || previous.public_ip.verify_interface_candidate
            != next.public_ip.verify_interface_candidate
        || previous.public_ip.startup_timeout != next.public_ip.startup_timeout
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

    /// 验证低频刷新失败会保留旧公网 IP，并标记为 stale。
    #[test]
    fn identity_refresh_failure_marks_existing_public_ip_stale() {
        let mut store = TelemetryState::default();
        store.set_identity(IdentityInfo {
            agent_id: "agent-test".to_string(),
            public_ip: PublicIpInfo::ready(
                "8.8.8.8".parse().unwrap(),
                smalux_core::model::info::PublicIpSource::ExternalHttp,
                1,
                Some(1),
            ),
            ..IdentityInfo::default()
        });

        apply_identity_refresh_result(
            &mut store,
            IdentityInfo {
                agent_id: "agent-test".to_string(),
                public_ip: PublicIpInfo::failed("temporary failure".to_string(), 2),
                ..IdentityInfo::default()
            },
        );

        let identity = store.identity.ready().unwrap();
        assert_eq!(identity.agent_id, "agent-test");
        assert_eq!(identity.public_ip.status, PublicIpStatus::Stale);
        assert_eq!(identity.public_ip.ip, Some("8.8.8.8".parse().unwrap()));
        assert_eq!(
            identity.public_ip.error.as_deref(),
            Some("temporary failure")
        );
    }

    /// 验证没有可用旧 IP 时会记录 failed 状态。
    #[test]
    fn identity_refresh_failure_is_recorded_when_identity_missing() {
        let mut store = TelemetryState::default();

        apply_identity_refresh_result(
            &mut store,
            IdentityInfo {
                agent_id: "agent-test".to_string(),
                public_ip: PublicIpInfo::failed("temporary failure".to_string(), 2),
                ..IdentityInfo::default()
            },
        );

        let identity = store.identity.ready().unwrap();
        assert_eq!(identity.public_ip.status, PublicIpStatus::Failed);
        assert_eq!(
            identity.public_ip.error.as_deref(),
            Some("temporary failure")
        );
    }

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
