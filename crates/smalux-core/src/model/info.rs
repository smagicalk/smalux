//! 系统采集信息模型。
//!
//! 这些结构体是 agent 采集后的内部表示，server 和协议转换层可以复用。

/// 高频核心指标模型。
mod core_metrics;
/// CPU 信息模型。
pub mod cpu;
/// 磁盘信息模型。
pub mod disk;
/// agent 身份信息模型。
mod identity;
/// 内存信息模型。
pub mod memory;
/// agent 上报元信息模型。
mod meta;
/// 通用指标状态模型。
mod metric;
/// 网络地址和网卡信息模型。
mod network;
/// 进程汇总信息模型。
mod process;
/// 公网 IP 信息模型。
mod public_ip;
/// agent 监控上报数据模型。
mod report;
/// Socket 汇总信息模型。
mod socket;
/// 操作系统和主机基础信息模型。
mod system;

pub use self::core_metrics::{CoreInfo, LoadAverageInfo};
pub use self::cpu::{Cpu, CpuInfo};
pub use self::disk::{Disk, DiskInfo};
pub use self::identity::IdentityInfo;
pub use self::memory::MemoryInfo;
pub use self::meta::{AGENT_REPORT_SCHEMA_VERSION, ReportMeta};
pub use self::metric::{MetricLevel, MetricStatus, Stamped};
pub use self::network::{Ip, Network, NetworkInfo};
pub use self::process::{
    ProcessDetail, ProcessDetailInfo, ProcessInfo, ProcessLight, ProcessLightInfo,
};
pub use self::public_ip::{PublicIpInfo, PublicIpSource, PublicIpStatus};
pub use self::report::AgentReport;
pub use self::socket::{
    SocketAccuracy, SocketDetail, SocketDetailInfo, SocketInfo, SocketLightInfo, SocketProtocol,
    SocketSource, TcpStateCount,
};
pub use self::system::SystemInfo;
