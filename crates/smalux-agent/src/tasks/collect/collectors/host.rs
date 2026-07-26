//! 主机和操作系统基础信息采集。

use serde::Serialize;
use sysinfo::System;

/// 主机基础信息快照。
#[derive(Debug, Clone, Serialize)]
pub struct HostSnapshot {
    /// 主机名，无法读取时为 `unknown`。
    pub hostname: String,
    /// 操作系统名称，无法读取时为 `unknown`。
    pub os_name: String,
    /// 操作系统版本，无法读取时为 `unknown`。
    pub os_version: String,
    /// 内核版本，无法读取时为 `unknown`。
    pub kernel_version: String,
    /// CPU 架构，例如 `x86_64` 或 `aarch64`。
    pub architecture: String,
    /// 从系统启动到采样时的秒数。
    pub uptime_seconds: u64,
    /// Unix 纪元到系统启动时间的秒数。
    pub boot_time_seconds: u64,
}

/// 采集变化频率较低的主机基础信息。
pub(crate) fn collect() -> HostSnapshot {
    HostSnapshot {
        hostname: System::host_name().unwrap_or_else(|| "unknown".to_owned()),
        os_name: System::name().unwrap_or_else(|| "unknown".to_owned()),
        os_version: System::os_version().unwrap_or_else(|| "unknown".to_owned()),
        kernel_version: System::kernel_version().unwrap_or_else(|| "unknown".to_owned()),
        architecture: System::cpu_arch(),
        uptime_seconds: System::uptime(),
        boot_time_seconds: System::boot_time(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_snapshot_has_stable_fallbacks() {
        let snapshot = collect();
        assert!(!snapshot.hostname.is_empty());
        assert!(!snapshot.os_name.is_empty());
        assert!(!snapshot.architecture.is_empty());
    }
}
