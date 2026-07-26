//! CPU 指标采集。

use serde::Serialize;
use sysinfo::System;

/// 单个逻辑 CPU 的指标。
#[derive(Debug, Clone, Serialize)]
pub struct CpuCoreSnapshot {
    /// sysinfo 提供的逻辑核心名称，例如 `cpu0`。
    pub name: String,
    /// 处理器品牌字符串。
    pub brand: String,
    /// 处理器厂商标识。
    pub vendor_id: String,
    /// 当前报告频率，单位 MHz。
    pub frequency_mhz: u64,
    /// 最近有效采样周期内的核心使用率，范围通常为 0 到 100。
    pub usage_percent: f32,
}

/// CPU 汇总快照。
#[derive(Debug, Clone, Serialize)]
pub struct CpuSnapshot {
    /// CPU 使用率是否已经完成至少一个有效采样周期。
    pub warmed_up: bool,
    /// 操作系统可识别的物理核心数；平台不支持时为 None。
    pub physical_cpu_count: Option<usize>,
    /// sysinfo 返回的逻辑核心数量。
    pub logical_cpu_count: usize,
    /// 全部逻辑核心的汇总使用率。
    pub global_usage_percent: f32,
    /// 每个逻辑核心的指标。
    pub cpus: Vec<CpuCoreSnapshot>,
}

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
        physical_cpu_count: System::physical_core_count(),
        logical_cpu_count: cpus.len(),
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

        assert_eq!(snapshot.logical_cpu_count, system.cpus().len());
        assert_eq!(snapshot.cpus.len(), system.cpus().len());
        assert!(snapshot.global_usage_percent.is_finite());
    }
}
