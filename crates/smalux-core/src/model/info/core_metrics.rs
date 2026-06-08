//! 高频核心指标模型。

use super::{CpuInfo, MemoryInfo};
use serde::{Deserialize, Serialize};

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
