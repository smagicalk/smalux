//! Service 采集模块的身份信息低频刷新调度。
//!
//! 该模块归属采集层，但不并入 `collector_loop` 主调度，避免公网请求慢或超时时
//! 阻塞 core/disk/network/processes/sockets 这类本机指标采样。

use crate::collect::LocalCollector;
use crate::config::AgentConfig;
use crate::service::message::TelemetryUpdateSender;
use crate::telemetry::TelemetryUpdate;
use smalux_core::model::info::{IdentityInfo, PublicIpInfo, PublicIpStatus};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior, interval_at};

/// 低频采样身份信息。
async fn sample_identity_once(
    collector: &mut LocalCollector,
    config: &AgentConfig,
) -> IdentityInfo {
    let identity = collector
        .sample_identity(config.agent_id.clone(), &config.public_ip)
        .await;
    log_identity_status(&identity.public_ip);
    identity
}

/// 身份信息低频刷新循环。
pub(crate) async fn identity_refresh_loop(
    mut collector: LocalCollector,
    telemetry_tx: TelemetryUpdateSender,
    mut config_rx: watch::Receiver<AgentConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut config = config_rx.borrow().clone();
    let mut refresh_tick = identity_refresh_interval(config.public_ip.refresh_interval);
    let mut config_rx_open = true;

    loop {
        tokio::select! {
            _ = refresh_tick.tick(), if config.public_ip.enabled => {
                let identity = sample_identity_once(&mut collector, &config).await;
                if !publish_identity_refresh(&telemetry_tx, identity).await {
                    break;
                }
            }
            changed = config_rx.changed(), if config_rx_open => {
                if changed.is_err() {
                    config_rx_open = false;
                    tracing::debug!("identity refresh config channel closed");
                    continue;
                }

                let next = config_rx.borrow().clone();
                let change = identity_refresh_config_change(&config, &next);
                if change.refresh_interval_changed {
                    refresh_tick = identity_refresh_interval(next.public_ip.refresh_interval);
                }
                config = next;
                log_identity_refresh_config_change(change);
                if change.sample_inputs_changed {
                    let identity = sample_identity_once(&mut collector, &config).await;
                    if !publish_identity_refresh(&telemetry_tx, identity).await {
                        break;
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("identity refresh loop shutting down");
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
            tracing::warn!("identity refresh stopped because telemetry update channel is closed");
            false
        }
    }
}

/// 创建身份低频刷新定时器；启动后先等待 refresh_interval。
fn identity_refresh_interval(interval: Duration) -> Interval {
    let mut tick = interval_at(tokio::time::Instant::now() + interval, interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick
}

/// 身份刷新相关配置变化。
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct IdentityRefreshConfigChange {
    /// 低频刷新间隔是否变化。
    refresh_interval_changed: bool,
    /// 采样输入是否变化；变化后需要立即刷新一次身份信息。
    sample_inputs_changed: bool,
}

impl IdentityRefreshConfigChange {
    /// 是否存在身份刷新模块关心的配置变化。
    fn any(self) -> bool {
        self.refresh_interval_changed || self.sample_inputs_changed
    }
}

/// 计算身份刷新模块关心的配置变化。
fn identity_refresh_config_change(
    previous: &AgentConfig,
    next: &AgentConfig,
) -> IdentityRefreshConfigChange {
    IdentityRefreshConfigChange {
        refresh_interval_changed: previous.public_ip.refresh_interval
            != next.public_ip.refresh_interval,
        sample_inputs_changed: identity_sample_inputs_changed(previous, next),
    }
}

/// 判断配置变化是否需要立刻刷新身份里的公网 IP 状态。
fn identity_sample_inputs_changed(previous: &AgentConfig, next: &AgentConfig) -> bool {
    IdentitySampleInputs::from_config(previous) != IdentitySampleInputs::from_config(next)
}

/// 会影响身份采样结果的运行期输入。
#[derive(Debug, Clone, Eq, PartialEq)]
struct IdentitySampleInputs<'a> {
    /// Agent 识别 ID。
    agent_id: &'a str,
    /// 是否启用公网 IP 采集。
    public_ip_enabled: bool,
    /// 是否优先使用网卡公网候选地址。
    prefer_interface_candidate: bool,
    /// 使用网卡候选地址后是否继续用外部服务校验。
    verify_interface_candidate: bool,
    /// 单轮外部公网 IP 探测超时。
    lookup_timeout: Duration,
    /// 外部公网 IP 服务最大并发数。
    max_concurrency: usize,
}

impl<'a> IdentitySampleInputs<'a> {
    /// 从完整配置中提取会影响身份采样结果的字段。
    fn from_config(config: &'a AgentConfig) -> Self {
        Self {
            agent_id: &config.agent_id,
            public_ip_enabled: config.public_ip.enabled,
            prefer_interface_candidate: config.public_ip.prefer_interface_candidate,
            verify_interface_candidate: config.public_ip.verify_interface_candidate,
            lookup_timeout: config.public_ip.lookup_timeout,
            max_concurrency: config.public_ip.max_concurrency,
        }
    }
}

/// 输出身份刷新配置变更日志。
fn log_identity_refresh_config_change(change: IdentityRefreshConfigChange) {
    if change.any() {
        tracing::info!(
            refresh_interval_changed = change.refresh_interval_changed,
            sample_inputs_changed = change.sample_inputs_changed,
            "identity refresh config updated"
        );
    } else {
        tracing::debug!("identity refresh ignored unrelated config update");
    }
}

/// 按身份刷新里的公网 IP 状态输出英文日志。
fn log_identity_status(public_ip: &PublicIpInfo) {
    match public_ip.status {
        PublicIpStatus::Ready => {
            if let Some(ip) = public_ip.ip {
                tracing::info!(
                    public_ip = %ip,
                    source = ?public_ip.source.as_ref(),
                    "Identity public IP refreshed"
                );
            } else {
                tracing::warn!("Identity public IP returned ready without IP address");
            }
        }
        PublicIpStatus::Failed => {
            tracing::warn!(
                error = public_ip.error.as_deref().unwrap_or("unknown"),
                "Identity public IP refresh failed"
            );
        }
        PublicIpStatus::Disabled => {
            tracing::info!("Identity public IP refresh disabled");
        }
        PublicIpStatus::Stale => {
            tracing::warn!(
                public_ip = ?public_ip.ip,
                error = public_ip.error.as_deref().unwrap_or("unknown"),
                "Identity public IP refresh stale"
            );
        }
        PublicIpStatus::Pending => {
            tracing::debug!("Identity public IP refresh pending");
        }
    }
}

#[cfg(test)]
mod tests {
    //! 身份低频刷新测试。

    use super::*;

    /// 验证每个影响公网 IP 结果的配置变化都会触发立即刷新。
    #[test]
    fn identity_sample_inputs_changed_detects_public_ip_lookup_input_changes() {
        let previous = AgentConfig::default();

        let cases: &[fn(&mut AgentConfig)] = &[
            |config| config.public_ip.enabled = !config.public_ip.enabled,
            |config| {
                config.public_ip.prefer_interface_candidate =
                    !config.public_ip.prefer_interface_candidate;
            },
            |config| {
                config.public_ip.verify_interface_candidate =
                    !config.public_ip.verify_interface_candidate;
            },
            |config| config.public_ip.lookup_timeout += Duration::from_secs(1),
            |config| config.public_ip.max_concurrency += 1,
        ];

        for mutate in cases {
            let mut next = previous.clone();
            mutate(&mut next);

            assert!(identity_sample_inputs_changed(&previous, &next));
        }
    }

    /// 验证完全相同的配置不会触发身份信息重新采样。
    #[test]
    fn identity_sample_inputs_changed_ignores_identical_config() {
        let previous = AgentConfig::default();
        let next = previous.clone();

        assert!(!identity_sample_inputs_changed(&previous, &next));
    }

    /// 验证启动门禁字段不会进入运行期身份采样输入集合。
    #[test]
    fn identity_sample_inputs_ignore_startup_gate_fields() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();

        next.public_ip.required_for_first_report = !previous.public_ip.required_for_first_report;
        next.public_ip.retry_interval = previous.public_ip.retry_interval + Duration::from_secs(1);

        assert_eq!(
            IdentitySampleInputs::from_config(&previous),
            IdentitySampleInputs::from_config(&next)
        );
    }

    /// 验证公网 IP 开关重新打开时会触发立即刷新，而不是等待低频间隔。
    #[test]
    fn identity_sample_inputs_changed_detects_public_ip_enabled_again() {
        let mut previous = AgentConfig::default();
        previous.public_ip.enabled = false;
        let mut next = previous.clone();

        next.public_ip.enabled = true;

        assert!(identity_sample_inputs_changed(&previous, &next));
    }

    /// 验证 agent_id 变化会触发身份信息重新采样。
    #[test]
    fn identity_sample_inputs_changed_detects_agent_id_change() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();

        next.agent_id = "agent-next".to_string();

        assert!(identity_sample_inputs_changed(&previous, &next));
    }

    /// 验证刷新间隔变化只重建定时器，不需要立即采样。
    #[test]
    fn identity_refresh_config_change_detects_interval_only_change() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();

        next.public_ip.refresh_interval = previous.public_ip.refresh_interval * 2;

        assert_eq!(
            identity_refresh_config_change(&previous, &next),
            IdentityRefreshConfigChange {
                refresh_interval_changed: true,
                sample_inputs_changed: false,
            }
        );
    }

    /// 验证刷新间隔和采样输入同时变化时，会同时重建定时器并立即采样。
    #[test]
    fn identity_refresh_config_change_detects_interval_and_sample_input_change() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();

        next.public_ip.refresh_interval = previous.public_ip.refresh_interval * 2;
        next.public_ip.lookup_timeout = previous.public_ip.lookup_timeout + Duration::from_secs(1);

        assert_eq!(
            identity_refresh_config_change(&previous, &next),
            IdentityRefreshConfigChange {
                refresh_interval_changed: true,
                sample_inputs_changed: true,
            }
        );
    }

    /// 验证无关采样配置变化不会触发身份刷新日志升级或立即采样。
    #[test]
    fn identity_refresh_config_change_ignores_unrelated_metric_change() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();

        next.disk.enabled = !previous.disk.enabled;

        assert_eq!(
            identity_refresh_config_change(&previous, &next),
            IdentityRefreshConfigChange::default()
        );
    }

    /// 验证启动门禁字段不会触发运行期低频身份刷新。
    #[test]
    fn identity_refresh_config_change_ignores_startup_gate_fields() {
        let previous = AgentConfig::default();
        let mut next = previous.clone();

        next.public_ip.required_for_first_report = !previous.public_ip.required_for_first_report;
        next.public_ip.retry_interval = previous.public_ip.retry_interval + Duration::from_secs(1);

        assert_eq!(
            identity_refresh_config_change(&previous, &next),
            IdentityRefreshConfigChange::default()
        );
    }
}
