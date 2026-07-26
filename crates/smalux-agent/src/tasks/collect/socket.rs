//! 本机 TCP/UDP socket 周期采集任务。

use std::num::NonZeroUsize;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::socket::{
        SocketAddressFamilySelection, SocketCollector, SocketProtocolSelection, SocketSnapshot,
    },
};

use super::{CollectionMode, MetricSample, blocking::CollectState};

/// Socket Task 的分级查询配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketTaskConfig {
    /// 汇总、轻量明细或带 PID 的详细明细。
    pub mode: CollectionMode,
    /// 查询 TCP、UDP 或两者。
    pub protocols: SocketProtocolSelection,
    /// 查询 IPv4、IPv6 或两者。
    pub address_families: SocketAddressFamilySelection,
    /// Basic/Detailed 返回列表上限；`None` 使用对应档位默认值。
    pub max_entries: Option<NonZeroUsize>,
}

impl Default for SocketTaskConfig {
    fn default() -> Self {
        Self {
            mode: CollectionMode::Summary,
            protocols: SocketProtocolSelection::Both,
            address_families: SocketAddressFamilySelection::Both,
            max_entries: None,
        }
    }
}

/// 使用独立阻塞查询采集 TCP/UDP socket 的调度任务。
pub struct SocketTask {
    state: CollectState<SocketCollector>,
    config: SocketTaskConfig,
}

impl SocketTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.socket.v1";

    /// 使用指定详细档位和其他默认筛选创建 Task。
    pub fn new(mode: CollectionMode) -> Self {
        Self::with_config(SocketTaskConfig {
            mode,
            ..SocketTaskConfig::default()
        })
    }

    /// 使用完整协议、地址族和列表上限配置创建 Task。
    pub fn with_config(config: SocketTaskConfig) -> Self {
        Self {
            state: CollectState::new(SocketCollector),
            config,
        }
    }

    /// 返回当前生效的查询配置。
    pub fn config(&self) -> &SocketTaskConfig {
        &self.config
    }
}

impl Default for SocketTask {
    fn default() -> Self {
        Self::new(CollectionMode::Summary)
    }
}

#[async_trait]
impl ValueTask for SocketTask {
    type Output = MetricSample<SocketSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let config = self.config.clone();
        self.state
            .try_collect(context, move |_| {
                SocketCollector::collect(
                    config.mode,
                    config.protocols,
                    config.address_families,
                    config.max_entries,
                )
                .map_err(|error| anyhow!(error))
            })
            .await
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn cancellation_mode(&self) -> crate::scheduler::TaskCancellationMode {
        crate::scheduler::TaskCancellationMode::NonCancellable
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, num::NonZeroUsize};

    use crate::{
        scheduler::ValueTask,
        tasks::collect::{
            CollectionMode, SocketAddressFamilySelection, SocketProtocolSelection, context,
        },
    };

    use super::{SocketTask, SocketTaskConfig};

    #[tokio::test]
    async fn socket_task_returns_the_configured_basic_snapshot() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = SocketTask::with_config(SocketTaskConfig {
            mode: CollectionMode::Basic,
            protocols: SocketProtocolSelection::Tcp,
            address_families: SocketAddressFamilySelection::Ipv4,
            max_entries: NonZeroUsize::new(16_384),
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), SocketTask::KIND);
        assert_eq!(output.snapshot.mode, CollectionMode::Basic);
        let entry = output
            .snapshot
            .entries
            .iter()
            .find(|entry| entry.local_port == port)
            .expect("the listening socket should be returned");
        assert!(entry.associated_pids.is_none());
    }
}
