//! CPU 指标采集。

pub use smalux_protocol::agent::v1::{CpuCoreSnapshot, CpuSnapshot};
use sysinfo::System;

/// 从已刷新的 `System` 中提取 CPU 指标。
pub(super) fn collect(system: &System, warmed_up: bool) -> CpuSnapshot {
    let cpus = system
        .cpus()
        .iter()
        .map(|cpu| CpuCoreSnapshot {
            name: cpu.name().to_owned(),
            brand: cpu.brand().to_owned(),
            vendor_id: cpu.vendor_id().to_owned(),
            frequency_mhz: cpu.frequency(),
            usage_percent: cpu.cpu_usage(),
        })
        .collect::<Vec<_>>();

    CpuSnapshot {
        warmed_up,
        physical_cpu_count: System::physical_core_count().and_then(|count| count.try_into().ok()),
        logical_cpu_count: cpus.len().try_into().unwrap_or(u32::MAX),
        global_usage_percent: system.global_cpu_usage(),
        cpus,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_snapshot_matches_system_cpu_count() {
        let mut system = System::new_all();
        system.refresh_cpu_all();

        let snapshot = collect(&system, false);

        assert_eq!(snapshot.logical_cpu_count as usize, system.cpus().len());
        assert_eq!(snapshot.cpus.len(), system.cpus().len());
        assert!(snapshot.global_usage_percent.is_finite());
    }
}
