//! 主机和操作系统基础信息采集。

pub use smalux_protocol::agent::v1::HostSnapshot;
use sysinfo::System;

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
