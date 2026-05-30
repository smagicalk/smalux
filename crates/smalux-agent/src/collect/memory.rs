//! 内存采集实现。

use smalux_core::model::info::MemoryInfo;
use sysinfo::System;

/// 从已刷新的系统对象构建物理内存与 swap 的容量和使用情况。
pub(crate) fn build_memory_info(system: &System) -> MemoryInfo {
    // 所有容量单位保持为 sysinfo 返回的字节数，展示层再决定格式化方式。
    let mut res_memory = MemoryInfo::default();
    res_memory.memory_usage = system.used_memory();
    res_memory.memory_total = system.total_memory();
    res_memory.memory_available = system.available_memory();
    res_memory.memory_free = system.free_memory();
    res_memory.swap_total = system.total_swap();
    res_memory.swap_usage = system.used_swap();
    res_memory.swap_free = system.free_swap();

    res_memory
}

#[cfg(test)]
mod tests {
    //! 内存信息映射测试。

    use super::*;

    /// 验证内存模型字段来自同一个 `System` 快照。
    #[test]
    fn test_build_memory_info() {
        let mut system = System::new_all();
        system.refresh_memory();

        let memory_info = build_memory_info(&system);

        assert_eq!(memory_info.memory_total, system.total_memory());
        assert_eq!(memory_info.memory_usage, system.used_memory());
        assert!(memory_info.memory_total >= memory_info.memory_usage);
        assert!(memory_info.swap_total >= memory_info.swap_usage);
    }
}
