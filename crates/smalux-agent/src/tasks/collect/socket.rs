//! 本机 TCP/UDP socket 周期采集任务。

use std::num::NonZeroUsize;

use anyhow::anyhow;
use async_trait::async_trait;
pub use smalux_protocol::agent::v1::SocketTaskConfig;
use smalux_protocol::agent::v1::{
    CollectionMode, SampleMetadata, SocketAddressFamily, SocketAddressFamilySelection,
    SocketAvailability, SocketCollectionStatus, SocketEntry, SocketProtocol,
    SocketProtocolSelection, SocketSnapshot, TaskResult, TcpConnectionState, TcpStateCount,
    task_result,
};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::socket::{
        SocketAddressFamily as CollectedAddressFamily, SocketCollectionStatus as CollectedStatus,
        SocketCollector, SocketEntry as CollectedEntry, SocketProtocol as CollectedProtocol,
        SocketSnapshot as CollectedSocketSnapshot, TcpConnectionState as CollectedTcpState,
    },
};

use super::blocking::CollectState;

/// Socket Proto 配置无法编译为可执行参数。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SocketConfigError {
    /// 采集档位不能为零值或未知值。
    #[error("socket collection mode is unspecified or unknown")]
    InvalidMode,
    /// 协议选择不能为零值或未知值。
    #[error("socket protocol selection is unspecified or unknown")]
    InvalidProtocols,
    /// 地址族选择不能为零值或未知值。
    #[error("socket address family selection is unspecified or unknown")]
    InvalidAddressFamilies,
    /// 显式列表上限必须大于零。
    #[error("socket max_entries must be greater than zero")]
    InvalidMaxEntries,
}

/// 使用独立阻塞查询采集 TCP/UDP socket 的调度任务。
pub struct SocketTask {
    state: CollectState<SocketCollector>,
    config: SocketTaskConfig,
    mode: CollectionMode,
    protocols: SocketProtocolSelection,
    address_families: SocketAddressFamilySelection,
    max_entries: Option<NonZeroUsize>,
}

impl SocketTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.socket.v1";

    /// 使用指定详细档位和其他默认筛选创建 Task。
    pub fn new(mode: CollectionMode) -> Self {
        Self::with_config(SocketTaskConfig {
            mode: mode as i32,
            protocols: SocketProtocolSelection::Both as i32,
            address_families: SocketAddressFamilySelection::Both as i32,
            max_entries: None,
        })
        .expect("built-in socket task config must be valid")
    }

    /// 使用完整协议、地址族和列表上限配置创建 Task。
    pub fn with_config(config: SocketTaskConfig) -> Result<Self, SocketConfigError> {
        let mode = CollectionMode::try_from(config.mode)
            .ok()
            .filter(|mode| *mode != CollectionMode::Unspecified)
            .ok_or(SocketConfigError::InvalidMode)?;
        let protocols = SocketProtocolSelection::try_from(config.protocols)
            .ok()
            .filter(|value| *value != SocketProtocolSelection::Unspecified)
            .ok_or(SocketConfigError::InvalidProtocols)?;
        let address_families = SocketAddressFamilySelection::try_from(config.address_families)
            .ok()
            .filter(|value| *value != SocketAddressFamilySelection::Unspecified)
            .ok_or(SocketConfigError::InvalidAddressFamilies)?;
        let max_entries = config
            .max_entries
            .map(|value| {
                NonZeroUsize::new(value as usize).ok_or(SocketConfigError::InvalidMaxEntries)
            })
            .transpose()?;
        Ok(Self {
            state: CollectState::new(SocketCollector),
            config,
            mode,
            protocols,
            address_families,
            max_entries,
        })
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
impl ReportingTask for SocketTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let mode = self.mode;
        let protocols = self.protocols;
        let address_families = self.address_families;
        let max_entries = self.max_entries;
        let output = self
            .state
            .try_collect(context, move |_| {
                SocketCollector::collect(mode, protocols, address_families, max_entries)
                    .map_err(|error| anyhow!(error))
            })
            .await?;
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::Socket(into_proto_snapshot(
                output.snapshot,
            ))),
        })
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn cancellation_mode(&self) -> crate::scheduler::TaskCancellationMode {
        crate::scheduler::TaskCancellationMode::NonCancellable
    }
}

pub(super) fn into_proto_snapshot(snapshot: CollectedSocketSnapshot) -> SocketSnapshot {
    SocketSnapshot {
        mode: snapshot.mode as i32,
        status: Some(match snapshot.status {
            CollectedStatus::Available => SocketCollectionStatus {
                availability: SocketAvailability::Available as i32,
                message: None,
            },
            CollectedStatus::Unavailable { message } => SocketCollectionStatus {
                availability: SocketAvailability::Unavailable as i32,
                message: Some(message),
            },
        }),
        tcp_total: snapshot.tcp_total.try_into().unwrap_or(u32::MAX),
        udp_total: snapshot.udp_total.try_into().unwrap_or(u32::MAX),
        tcp_states: snapshot
            .tcp_states
            .into_iter()
            .map(|state| TcpStateCount {
                state: into_proto_tcp_state(state.state) as i32,
                count: state.count.try_into().unwrap_or(u32::MAX),
            })
            .collect(),
        entries: snapshot.entries.into_iter().map(into_proto_entry).collect(),
        truncated: snapshot.truncated,
    }
}

fn into_proto_entry(entry: CollectedEntry) -> SocketEntry {
    let (associated_pids, pids_included) = match entry.associated_pids {
        Some(pids) => (pids, true),
        None => (Vec::new(), false),
    };
    SocketEntry {
        protocol: match entry.protocol {
            CollectedProtocol::Tcp => SocketProtocol::Tcp as i32,
            CollectedProtocol::Udp => SocketProtocol::Udp as i32,
        },
        address_family: match entry.address_family {
            CollectedAddressFamily::Ipv4 => SocketAddressFamily::Ipv4 as i32,
            CollectedAddressFamily::Ipv6 => SocketAddressFamily::Ipv6 as i32,
        },
        local_address: entry.local_address.to_string(),
        local_port: entry.local_port.into(),
        remote_address: entry.remote_address.map(|address| address.to_string()),
        remote_port: entry.remote_port.map(u32::from),
        tcp_state: entry
            .tcp_state
            .map(|state| into_proto_tcp_state(state) as i32),
        associated_pids,
        pids_included,
    }
}

const fn into_proto_tcp_state(state: CollectedTcpState) -> TcpConnectionState {
    match state {
        CollectedTcpState::Closed => TcpConnectionState::Closed,
        CollectedTcpState::Listen => TcpConnectionState::Listen,
        CollectedTcpState::SynSent => TcpConnectionState::SynSent,
        CollectedTcpState::SynReceived => TcpConnectionState::SynReceived,
        CollectedTcpState::Established => TcpConnectionState::Established,
        CollectedTcpState::FinWait1 => TcpConnectionState::FinWait1,
        CollectedTcpState::FinWait2 => TcpConnectionState::FinWait2,
        CollectedTcpState::CloseWait => TcpConnectionState::CloseWait,
        CollectedTcpState::Closing => TcpConnectionState::Closing,
        CollectedTcpState::LastAck => TcpConnectionState::LastAck,
        CollectedTcpState::TimeWait => TcpConnectionState::TimeWait,
        CollectedTcpState::DeleteTcb => TcpConnectionState::DeleteTcb,
        CollectedTcpState::Unknown => TcpConnectionState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use smalux_protocol::agent::v1::task_result;
    use std::net::TcpListener;

    use crate::{
        scheduler::ReportingTask,
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
            mode: CollectionMode::Basic as i32,
            protocols: SocketProtocolSelection::Tcp as i32,
            address_families: SocketAddressFamilySelection::Ipv4 as i32,
            max_entries: Some(16_384),
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), SocketTask::KIND);
        let Some(task_result::Result::Socket(snapshot)) = output.result else {
            panic!("socket task must return TaskResult.socket");
        };
        assert_eq!(snapshot.mode, CollectionMode::Basic as i32);
        let entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.local_port == u32::from(port))
            .expect("the listening socket should be returned");
        assert!(!entry.pids_included);
    }

    #[test]
    fn socket_task_rejects_unspecified_protocols() {
        let result = SocketTask::with_config(SocketTaskConfig {
            mode: CollectionMode::Summary as i32,
            protocols: SocketProtocolSelection::Unspecified as i32,
            address_families: SocketAddressFamilySelection::Both as i32,
            max_entries: None,
        });

        assert!(matches!(
            result,
            Err(super::SocketConfigError::InvalidProtocols)
        ));
    }
}
