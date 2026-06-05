//! CPU 采集实现。
//!
//! `sysinfo` 的 CPU 使用率需要两次刷新之间存在最小间隔，调用方应在采样循环中控制节奏。

use smalux_core::model::info::{Cpu, CpuInfo};
use sysinfo::System;

/// 从已刷新的系统对象构建 CPU 汇总信息和每个逻辑 CPU 的明细。
pub(crate) fn build_cpu_info(system: &System) -> CpuInfo {
    let cpus = system.cpus();
    let mut res_cpu = CpuInfo {
        cpu_num: cpus.len(),
        cpu_usage: system.global_cpu_usage(),
        cpus: Vec::with_capacity(cpus.len()),
    };

    for cpu in cpus {
        // 保留每个逻辑 CPU 的名称、品牌、供应商、频率和使用率，方便上层做明细展示。
        let cpu_info = Cpu {
            name: cpu.name().to_string(),
            brand: cpu.brand().to_string(),
            vendor_id: cpu.vendor_id().to_string(),
            usage: cpu.cpu_usage(),
            frequency: cpu.frequency(),
        };
        res_cpu.cpus.push(cpu_info);
    }

    res_cpu
}

#[cfg(test)]
mod tests {
    //! CPU 信息映射测试。

    use super::*;

    /// 验证 CPU 模型字段来自同一个 `System` 快照。
    #[test]
    fn test_build_cpu_info() {
        let mut system = System::new_all();
        system.refresh_cpu_all();

        let cpu_info = build_cpu_info(&system);

        assert_eq!(cpu_info.cpu_num, system.cpus().len());
        assert_eq!(cpu_info.cpus.len(), system.cpus().len());
        assert!(cpu_info.cpu_usage.is_finite());
    }
}
