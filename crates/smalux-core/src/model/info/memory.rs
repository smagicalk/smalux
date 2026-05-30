//! 内存信息模型。

use serde::{Deserialize, Serialize};

/// 物理内存和 swap 的采集信息。
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryInfo {
    /// 物理内存总量，单位字节。
    pub memory_total: u64,
    /// 已使用物理内存，单位字节。
    pub memory_usage: u64,
    /// 可用物理内存，单位字节。
    pub memory_available: u64,
    /// 空闲物理内存，单位字节。
    pub memory_free: u64,
    /// swap 总量，单位字节。
    pub swap_total: u64,
    /// 已使用 swap，单位字节。
    pub swap_usage: u64,
    /// 空闲 swap，单位字节。
    pub swap_free: u64,
}
