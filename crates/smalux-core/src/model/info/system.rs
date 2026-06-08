//! 操作系统和主机基础信息模型。

use serde::{Deserialize, Serialize};

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
