//! 本机指标采集任务及其统一输出。

#![warn(missing_docs)]

mod blocking;
pub mod collectors;
mod cpu;
mod disk_io;
mod host;
mod load;
mod local_ip;
mod memory;
mod network_io;
mod probe;
mod process;
mod public_ip;
mod sample;
mod selection;
mod socket;
mod system;

/// 采集结果的详细程度；档位越高，系统查询和输出体积通常越大。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionMode {
    /// 只返回完整汇总，不保留逐项列表。
    #[default]
    Summary,
    /// 返回不含高成本关联字段的轻量列表。
    Basic,
    /// 返回 PID、资源或命令等高成本详细字段。
    Detailed,
}

pub use collectors::{
    SystemSnapshot,
    cpu::{CpuCoreSnapshot, CpuSnapshot},
    host::HostSnapshot,
    io::{DiskDeviceSnapshot, DiskIoSnapshot, NetworkInterfaceSnapshot, NetworkIoSnapshot},
    ip::{InterfaceAddress, IpScope, IpSnapshot, PublicIpSnapshot, PublicIpState},
    load::LoadSnapshot,
    memory::MemorySnapshot,
    process::{
        ProcessConfigError, ProcessDetails, ProcessEntry, ProcessRanking, ProcessSelection,
        ProcessSnapshot, ProcessState, ProcessStateCount,
    },
    socket::{
        SocketAddressFamily, SocketAddressFamilySelection, SocketCollectionError,
        SocketCollectionStatus, SocketEntry, SocketProtocol, SocketProtocolSelection,
        SocketSnapshot, TcpConnectionState, TcpStateCount,
    },
};
pub use cpu::CpuTask;
pub use disk_io::{DiskIoTask, DiskIoTaskConfig};
pub use host::HostTask;
pub use load::LoadTask;
pub use local_ip::{LocalIpTask, LocalIpTaskConfig};
pub use memory::MemoryTask;
pub use network_io::{NetworkIoTask, NetworkIoTaskConfig};
pub use probe::{
    HttpStatusRange, ProbeAttemptSnapshot, ProbeConfigError, ProbeNodeConfig, ProbeNodeSnapshot,
    ProbeProtocol, ProbeSnapshot, ProbeTarget, ProbeTask, ProbeTaskConfig,
};
pub use process::{ProcessTask, ProcessTaskConfig};
pub use public_ip::{PublicIpTask, PublicIpTaskConfig};
pub use sample::MetricSample;
pub use selection::{DiskSelection, InterfaceSelection, IpFamilySelection};
pub use socket::{SocketTask, SocketTaskConfig};
pub use system::{SystemTask, SystemTaskConfig};

#[cfg(test)]
fn context() -> crate::scheduler::TaskContext {
    crate::scheduler::TaskContext {
        job_id: uuid::Uuid::new_v4(),
        version: 0,
        run_id: uuid::Uuid::new_v4(),
        attempt: 1,
        scheduled_at: chrono::Utc::now(),
        started_at: chrono::Utc::now(),
        cancellation: tokio_util::sync::CancellationToken::new(),
    }
}
