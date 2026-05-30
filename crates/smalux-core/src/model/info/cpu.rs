//! CPU 信息模型。

use serde::{Deserialize, Serialize};

/// 单个逻辑 CPU 的采集信息。
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Cpu {
    /// CPU 名称。
    pub name: String,
    /// CPU 品牌。
    pub brand: String,
    /// 供应商 ID。
    pub vendor_id: String,
    /// 当前使用率百分比。
    pub usage: f32,
    /// 当前频率，单位 MHz。
    pub frequency: u64,
}

/// CPU 汇总信息。
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct CpuInfo {
    /// 逻辑 CPU 数量。
    pub cpu_num: usize,
    /// 全局 CPU 使用率百分比。
    pub cpu_usage: f32,
    /// 每个逻辑 CPU 的明细。
    pub cpus: Vec<Cpu>,
}
