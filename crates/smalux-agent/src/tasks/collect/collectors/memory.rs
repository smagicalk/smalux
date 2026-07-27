//! 内存与交换空间指标采集。

pub use smalux_protocol::agent::v1::MemorySnapshot;
use sysinfo::System;

/// 从已刷新的 `System` 中提取内存指标。
pub(super) fn collect(system: &System) -> MemorySnapshot {
    MemorySnapshot {
        total_bytes: system.total_memory(),
        used_bytes: system.used_memory(),
        available_bytes: system.available_memory(),
        free_bytes: system.free_memory(),
        usage_percent: percent(system.used_memory(), system.total_memory()),
        swap_total_bytes: system.total_swap(),
        swap_used_bytes: system.used_swap(),
        swap_free_bytes: system.free_swap(),
        swap_usage_percent: percent(system.used_swap(), system.total_swap()),
    }
}

/// 计算容量使用百分比，并安全处理总量为零的平台结果。
fn percent(used: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    used as f64 * 100.0 / total as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_handles_zero_total() {
        assert_eq!(percent(1, 0), 0.0);
        assert_eq!(percent(50, 100), 50.0);
    }

    #[test]
    fn memory_snapshot_preserves_capacity_invariants() {
        let mut system = System::new_all();
        system.refresh_memory();

        let snapshot = collect(&system);

        assert!(snapshot.total_bytes >= snapshot.used_bytes);
        assert!(snapshot.swap_total_bytes >= snapshot.swap_used_bytes);
        assert!(snapshot.usage_percent.is_finite());
    }
}
