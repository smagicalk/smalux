//! 系统采集信息模型。
//!
//! 这些结构体是 agent 采集后的内部表示，server 和协议转换层可以复用。

/// 网络地址和网卡信息模型。
mod network;

pub use network::Ip;
pub use network::Network;
pub use network::NetworkInfo;
use serde::{Deserialize, Serialize};

/// 进程汇总信息模型。
mod process;
pub use process::{ProcessDetail, ProcessDetailInfo, ProcessInfo, ProcessLight, ProcessLightInfo};

/// Socket 汇总信息模型。
mod socket;
pub use socket::{
    SocketAccuracy, SocketDetail, SocketDetailInfo, SocketInfo, SocketLightInfo, SocketProtocol,
    SocketSource, TcpStateCount,
};

/// 磁盘信息模型。
pub mod disk;
pub use disk::Disk;
pub use disk::DiskInfo;

/// 内存信息模型。
pub mod memory;
pub use memory::MemoryInfo;

/// CPU 信息模型。
pub mod cpu;
pub use cpu::Cpu;
pub use cpu::CpuInfo;
use std::net::IpAddr;

/// 上报模型版本。
pub const AGENT_REPORT_SCHEMA_VERSION: u16 = 5;

/// 通用指标采集状态。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricStatus {
    /// 最近一次采集成功。
    Ready,
    /// 保留旧值，但最近一次采集失败。
    Stale,
    /// 采集失败，且没有可用旧值。
    Failed,
    /// 当前平台或运行环境不支持该指标。
    Unsupported,
}

impl Default for MetricStatus {
    /// 默认表示尚未拿到有效数据。
    fn default() -> Self {
        Self::Failed
    }
}

/// 指标采集详细级别。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricLevel {
    /// 只采集总数，适合默认高频上报。
    Count,
    /// 采集轻量聚合或 top 列表，适合临时排查。
    Light,
    /// 采集完整明细，适合按需诊断。
    Details,
}

impl Default for MetricLevel {
    /// 默认只上报总数。
    fn default() -> Self {
        Self::Count
    }
}

impl MetricLevel {
    /// 是否需要轻量信息。
    pub fn includes_light(self) -> bool {
        matches!(self, Self::Light)
    }

    /// 是否需要完整明细。
    pub fn includes_details(self) -> bool {
        matches!(self, Self::Details)
    }
}

/// 带采样时间的数据。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stamped<T> {
    /// Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// 采样值。
    pub value: T,
}

/// agent 上报元信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReportMeta {
    /// 上报模型版本。
    pub schema_version: u16,
    /// agent 版本。
    pub agent_version: String,
    /// 本次上报时间，Unix 时间戳，单位秒。
    pub report_at: u64,
}

/// 公网 IP 来源。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicIpSource {
    /// 从网卡公网候选地址获取。
    InterfaceCandidate,
    /// 从外部 HTTP 服务获取。
    ExternalHttp,
}

impl Default for PublicIpSource {
    /// 默认按外部 HTTP 服务处理。
    fn default() -> Self {
        Self::ExternalHttp
    }
}

/// 公网 IP 获取状态。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicIpStatus {
    /// 未启用公网 IP 采集。
    Disabled,
    /// 尚未完成首次尝试。
    Pending,
    /// 最近一次获取成功。
    Ready,
    /// 获取失败，且没有可用旧值。
    Failed,
    /// 保留旧公网 IP，但最近一次刷新失败。
    Stale,
}

impl Default for PublicIpStatus {
    /// 默认处于等待采集状态。
    fn default() -> Self {
        Self::Pending
    }
}

/// 公网 IP 信息。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct PublicIpInfo {
    /// 公网 IP 获取状态。
    pub status: PublicIpStatus,
    /// 公网 IP 地址，获取成功或使用旧值时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<IpAddr>,
    /// 公网 IP 来源，获取成功或使用旧值时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<PublicIpSource>,
    /// 成功采样时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at: Option<u64>,
    /// 外部服务校验时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<u64>,
    /// 最近一次尝试时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<u64>,
    /// 最近一次失败原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Default for PublicIpInfo {
    /// 默认表示尚未完成公网 IP 采集。
    fn default() -> Self {
        Self {
            status: PublicIpStatus::Pending,
            ip: None,
            source: None,
            sampled_at: None,
            verified_at: None,
            last_attempt_at: None,
            error: None,
        }
    }
}

impl PublicIpInfo {
    /// 构造禁用状态。
    pub fn disabled() -> Self {
        Self {
            status: PublicIpStatus::Disabled,
            ..Self::default()
        }
    }

    /// 构造获取成功状态。
    pub fn ready(
        ip: IpAddr,
        source: PublicIpSource,
        sampled_at: u64,
        verified_at: Option<u64>,
    ) -> Self {
        Self {
            status: PublicIpStatus::Ready,
            ip: Some(ip),
            source: Some(source),
            sampled_at: Some(sampled_at),
            verified_at,
            last_attempt_at: Some(sampled_at),
            error: None,
        }
    }

    /// 构造获取失败状态。
    pub fn failed(error: String, last_attempt_at: u64) -> Self {
        Self {
            status: PublicIpStatus::Failed,
            last_attempt_at: Some(last_attempt_at),
            error: Some(error),
            ..Self::default()
        }
    }

    /// 根据旧值构造 stale；没有旧 IP 时降级为 failed。
    pub fn stale_or_failed(previous: &Self, error: String, last_attempt_at: u64) -> Self {
        let Some(ip) = previous.ip else {
            return Self::failed(error, last_attempt_at);
        };

        Self {
            status: PublicIpStatus::Stale,
            ip: Some(ip),
            source: previous.source.clone(),
            sampled_at: previous.sampled_at,
            verified_at: previous.verified_at,
            last_attempt_at: Some(last_attempt_at),
            error: Some(error),
        }
    }
}

/// 本机身份信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IdentityInfo {
    /// agent 实例 ID。
    pub agent_id: String,
    /// 主机名。
    pub hostname: String,
    /// 公网 IP 获取结果。
    pub public_ip: PublicIpInfo,
    /// 本地网卡 IP 列表。
    pub local_ips: Vec<Ip>,
}

/// 系统负载信息。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct LoadAverageInfo {
    /// 1 分钟平均负载。
    pub one: f64,
    /// 5 分钟平均负载。
    pub five: f64,
    /// 15 分钟平均负载。
    pub fifteen: f64,
    /// 当前平台是否可靠支持平均负载。
    pub supported: bool,
}

/// 高频核心指标。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoreInfo {
    /// CPU 信息。
    pub cpu: CpuInfo,
    /// 内存信息。
    pub memory: MemoryInfo,
    /// 平均负载。
    pub load_avg: LoadAverageInfo,
}

/// agent 监控上报数据。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentReport {
    /// 上报元信息。
    pub meta: ReportMeta,
    /// 身份信息。
    pub identity: IdentityInfo,
    /// 静态系统信息。
    pub system: SystemInfo,
    /// 高频核心指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core: Option<Stamped<CoreInfo>>,
    /// 磁盘指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk: Option<Stamped<DiskInfo>>,
    /// 网络指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<Stamped<NetworkInfo>>,
    /// 进程汇总指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processes: Option<Stamped<ProcessInfo>>,
    /// Socket 汇总指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sockets: Option<Stamped<SocketInfo>>,
}

/// 操作系统和主机基础信息。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq, Default)]
pub struct SystemInfo {
    /// 系统名称。
    pub name: String,
    /// 内核版本。
    pub kernel_version: String,
    /// 内核完整版本。
    pub kernel_long_version: String,
    /// 操作系统版本。
    pub os_version: String,
    /// 操作系统完整版本。
    pub long_os_version: String,
    /// 主机名。
    pub hostname: String,
    /// 发行版标识。
    pub distribution_id: String,
    /// 系统运行时长，单位秒。
    pub uptime: u64,
    /// 系统启动时间戳。
    pub boot_time: u64,
    /// 当前平台是否被 sysinfo 支持。
    pub supported: bool,
    /// 物理核心数。
    pub core_num: usize,
    /// CPU 架构。
    pub cpu_arch: String,
}
