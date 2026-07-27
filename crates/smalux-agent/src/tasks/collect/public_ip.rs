//! 公网 IPv4/IPv6 周期采集任务。

use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
pub use smalux_protocol::agent::v1::PublicIpTaskConfig;
use smalux_protocol::agent::v1::{SampleMetadata, TaskResult, task_result};
use tokio::sync::Mutex;

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::ip::collect_public_families,
};

use super::{
    IpFamilySelection,
    sample::{duration_ms, unix_timestamp_ms},
    selection::requested_ip_families,
};

/// 公网 IP Proto 配置无法编译为可执行参数。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicIpConfigError {
    /// 必须显式配置单个公网地址请求的超时。
    #[error("public IP timeout is required")]
    MissingTimeout,
    /// protobuf Duration 不是正的、可表示的 Rust Duration。
    #[error("public IP timeout must be a positive protobuf duration")]
    InvalidTimeout,
    /// IP 协议族不能使用 protobuf 零值或未知值。
    #[error("public IP family is unspecified or unknown")]
    InvalidFamily,
}

/// 使用显式端点超时查询公网 IPv4 与 IPv6 的调度任务。
pub struct PublicIpTask {
    config: PublicIpTaskConfig,
    timeout: Duration,
    family: IpFamilySelection,
    last_sampled_at: Mutex<Option<Instant>>,
}

impl PublicIpTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.public_ip.v1";

    /// 创建同时请求 IPv4/IPv6 的 Task，并显式指定请求超时。
    pub fn new(timeout: Duration) -> Result<Self, PublicIpConfigError> {
        let seconds =
            i64::try_from(timeout.as_secs()).map_err(|_| PublicIpConfigError::InvalidTimeout)?;
        Self::with_config(PublicIpTaskConfig {
            timeout: Some(prost_types::Duration {
                seconds,
                nanos: timeout.subsec_nanos() as i32,
            }),
            family: IpFamilySelection::Both as i32,
        })
    }

    /// 校验完整 Proto 请求配置并创建 Task。
    pub fn with_config(config: PublicIpTaskConfig) -> Result<Self, PublicIpConfigError> {
        let timeout = compile_timeout(config.timeout.as_ref())?;
        let family = IpFamilySelection::try_from(config.family)
            .ok()
            .filter(|family| *family != IpFamilySelection::Unspecified)
            .ok_or(PublicIpConfigError::InvalidFamily)?;
        Ok(Self {
            config,
            timeout,
            family,
            last_sampled_at: Mutex::new(None),
        })
    }

    /// 返回公网地址 HTTP 请求超时。
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// 返回当前生效的请求配置。
    pub fn config(&self) -> &PublicIpTaskConfig {
        &self.config
    }
}

#[async_trait]
impl ReportingTask for PublicIpTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let cancellation = context.cancellation;
        let mut last_sampled_at = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(TaskError::Transient(anyhow!("public IP collection cancelled before start")));
            }
            guard = self.last_sampled_at.lock() => guard,
        };
        let sampled_at = Instant::now();
        let sampled_at_ms = unix_timestamp_ms();
        let sample_interval_ms =
            last_sampled_at.map(|previous| duration_ms(sampled_at.duration_since(previous)));

        let (request_ipv4, request_ipv6) = requested_ip_families(self.family);
        let snapshot = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(TaskError::Transient(anyhow!("public IP collection cancelled")));
            }
            snapshot = collect_public_families(
                self.timeout,
                request_ipv4,
                request_ipv6,
            ) => snapshot,
        };
        *last_sampled_at = Some(sampled_at);

        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms,
                sample_interval_ms,
            }),
            result: Some(task_result::Result::PublicIp(snapshot)),
        })
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }
}

fn compile_timeout(
    timeout: Option<&prost_types::Duration>,
) -> Result<Duration, PublicIpConfigError> {
    let timeout = timeout.ok_or(PublicIpConfigError::MissingTimeout)?;
    if timeout.seconds < 0 || !(0..1_000_000_000).contains(&timeout.nanos) {
        return Err(PublicIpConfigError::InvalidTimeout);
    }
    let timeout = Duration::new(timeout.seconds as u64, timeout.nanos as u32);
    if timeout.is_zero() {
        return Err(PublicIpConfigError::InvalidTimeout);
    }
    Ok(timeout)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::scheduler::ReportingTask;
    use crate::tasks::collect::IpFamilySelection;

    use super::{PublicIpConfigError, PublicIpTask, PublicIpTaskConfig};

    #[test]
    fn public_ip_task_requires_explicit_timeout() {
        let timeout = Duration::from_secs(3);
        let task = PublicIpTask::new(timeout).unwrap();

        assert_eq!(task.timeout(), timeout);
        assert_eq!(task.kind(), PublicIpTask::KIND);
        assert_eq!(task.config().family, IpFamilySelection::Both as i32);
    }

    #[test]
    fn public_ip_task_accepts_family_config() {
        let task = PublicIpTask::with_config(PublicIpTaskConfig {
            timeout: Some(prost_types::Duration {
                seconds: 2,
                nanos: 0,
            }),
            family: IpFamilySelection::V4 as i32,
        })
        .unwrap();

        assert_eq!(task.timeout(), Duration::from_secs(2));
        assert_eq!(task.config().family, IpFamilySelection::V4 as i32);
    }

    #[test]
    fn public_ip_task_rejects_missing_timeout_and_unspecified_family() {
        let missing_timeout = PublicIpTask::with_config(PublicIpTaskConfig {
            timeout: None,
            family: IpFamilySelection::Both as i32,
        });
        assert!(matches!(
            missing_timeout,
            Err(PublicIpConfigError::MissingTimeout)
        ));

        let unspecified_family = PublicIpTask::with_config(PublicIpTaskConfig {
            timeout: Some(prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
            family: IpFamilySelection::Unspecified as i32,
        });
        assert!(matches!(
            unspecified_family,
            Err(PublicIpConfigError::InvalidFamily)
        ));
    }
}
