//! 公网 IPv4/IPv6 周期采集任务。

use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::ip::{PublicIpSnapshot, collect_public_families},
};

use super::{
    IpFamilySelection, MetricSample,
    sample::{duration_ms, unix_timestamp_ms},
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// 公网 IP Task 的请求配置。
pub struct PublicIpTaskConfig {
    /// 单个公网地址 HTTP 请求的超时，不等同于 Scheduler Task 超时。
    pub timeout: Duration,
    /// 本次采集请求的 IP 协议族。
    pub family: IpFamilySelection,
}

/// 使用显式端点超时查询公网 IPv4 与 IPv6 的调度任务。
pub struct PublicIpTask {
    config: PublicIpTaskConfig,
    last_sampled_at: Mutex<Option<Instant>>,
}

impl PublicIpTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.public_ip.v1";

    /// 创建同时请求 IPv4/IPv6 的 Task，并显式指定请求超时。
    pub fn new(timeout: Duration) -> Self {
        Self::with_config(PublicIpTaskConfig {
            timeout,
            family: IpFamilySelection::Both,
        })
    }

    /// 使用完整请求配置创建 Task。
    pub fn with_config(config: PublicIpTaskConfig) -> Self {
        Self {
            config,
            last_sampled_at: Mutex::new(None),
        }
    }

    /// 返回公网地址 HTTP 请求超时。
    pub fn timeout(&self) -> Duration {
        self.config.timeout
    }

    /// 返回当前生效的请求配置。
    pub fn config(&self) -> &PublicIpTaskConfig {
        &self.config
    }
}

#[async_trait]
impl ValueTask for PublicIpTask {
    type Output = MetricSample<PublicIpSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
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

        let (request_ipv4, request_ipv6) = self.config.family.requested();
        let snapshot = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(TaskError::Transient(anyhow!("public IP collection cancelled")));
            }
            snapshot = collect_public_families(
                self.config.timeout,
                request_ipv4,
                request_ipv6,
            ) => snapshot,
        };
        *last_sampled_at = Some(sampled_at);

        Ok(MetricSample::new(
            sampled_at_ms,
            sample_interval_ms,
            snapshot,
        ))
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::scheduler::ValueTask;
    use crate::tasks::collect::IpFamilySelection;

    use super::{PublicIpTask, PublicIpTaskConfig};

    #[test]
    fn public_ip_task_requires_explicit_timeout() {
        let timeout = Duration::from_secs(3);
        let task = PublicIpTask::new(timeout);

        assert_eq!(task.timeout(), timeout);
        assert_eq!(task.kind(), PublicIpTask::KIND);
        assert_eq!(task.config().family, IpFamilySelection::Both);
    }

    #[test]
    fn public_ip_task_accepts_family_config() {
        let task = PublicIpTask::with_config(PublicIpTaskConfig {
            timeout: Duration::from_secs(2),
            family: IpFamilySelection::V4,
        });

        assert_eq!(task.config().timeout, Duration::from_secs(2));
        assert_eq!(task.config().family, IpFamilySelection::V4);
    }
}
