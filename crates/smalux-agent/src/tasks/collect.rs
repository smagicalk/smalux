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

pub use smalux_protocol::agent::v1::{
    CollectionMode, HttpTarget, IcmpEchoTarget, ProbeAttemptSnapshot, ProbeNodeConfig,
    ProbeNodeSnapshot, ProbeProtocol, ProbeSnapshot, ProcessDetails, ProcessEntry, ProcessRanking,
    ProcessSelection, ProcessSnapshot, ProcessState, ProcessStateCount, SocketAddressFamily,
    SocketAddressFamilySelection, SocketAvailability, SocketCollectionStatus, SocketEntry,
    SocketProtocol, SocketProtocolSelection, SocketSnapshot, SystemSnapshot, TcpConnectTarget,
    TcpConnectionState, TcpStateCount,
};

pub use collectors::{
    cpu::{CpuCoreSnapshot, CpuSnapshot},
    host::HostSnapshot,
    io::{DiskDeviceSnapshot, DiskIoSnapshot, NetworkInterfaceSnapshot, NetworkIoSnapshot},
    ip::{InterfaceAddress, IpScope, IpSnapshot, PublicIpSnapshot, PublicIpState, PublicIpStatus},
    load::LoadSnapshot,
    memory::MemorySnapshot,
    process::ProcessConfigError,
    socket::SocketCollectionError,
};
pub use cpu::CpuTask;
pub use disk_io::{DiskIoTask, DiskIoTaskConfig};
pub use host::HostTask;
pub use load::LoadTask;
pub use local_ip::{LocalIpTask, LocalIpTaskConfig};
pub use memory::MemoryTask;
pub use network_io::{NetworkIoTask, NetworkIoTaskConfig};
pub use probe::{ProbeConfigError, ProbeTask, ProbeTaskConfig};
pub use process::{ProcessTask, ProcessTaskConfig};
pub use public_ip::{PublicIpConfigError, PublicIpTask, PublicIpTaskConfig};
pub use selection::{DiskSelection, InterfaceSelection, IpFamilySelection};
pub use socket::{SocketConfigError, SocketTask, SocketTaskConfig};
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
