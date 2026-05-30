//! 进程信息模型。

use super::{MetricLevel, MetricStatus};
use serde::{Deserialize, Serialize};

/// 进程轻量信息。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessLight {
    /// 进程 ID。
    pub pid: u32,
    /// 进程名称。
    pub name: String,
    /// 进程状态，使用平台字符串表示。
    pub status: String,
    /// CPU 使用率百分比。
    pub cpu_usage: f32,
    /// 常驻内存，单位 byte。
    pub memory_bytes: u64,
}

/// 进程完整明细。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessDetail {
    /// 进程 ID。
    pub pid: u32,
    /// 父进程 ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_pid: Option<u32>,
    /// 进程名称。
    pub name: String,
    /// 进程状态，使用平台字符串表示。
    pub status: String,
    /// CPU 使用率百分比。
    pub cpu_usage: f32,
    /// 常驻内存，单位 byte。
    pub memory_bytes: u64,
    /// 虚拟内存，单位 byte。
    pub virtual_memory_bytes: u64,
    /// 启动时间，Unix 时间戳，单位秒。
    pub start_time: u64,
    /// 运行时长，单位秒。
    pub run_time: u64,
    /// 可执行文件路径。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exe: Option<String>,
    /// 命令行参数。
    pub cmd: Vec<String>,
}

/// 进程轻量采样结果。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessLightInfo {
    /// 返回条数上限。
    pub limit: usize,
    /// 是否因为上限截断。
    pub truncated: bool,
    /// 默认按内存占用从高到低返回的进程列表。
    pub items: Vec<ProcessLight>,
}

/// 进程明细采样结果。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessDetailInfo {
    /// 返回条数上限。
    pub limit: usize,
    /// 是否因为上限截断。
    pub truncated: bool,
    /// 默认按内存占用从高到低返回的进程列表。
    pub items: Vec<ProcessDetail>,
}

/// 进程信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ProcessInfo {
    /// 当前进程数量。
    pub count: u64,
    /// 采集状态。
    pub status: MetricStatus,
    /// 本次采集详细级别。
    pub level: MetricLevel,
    /// 轻量进程信息，仅 `level=light` 时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub light: Option<ProcessLightInfo>,
    /// 进程完整明细，仅 `level=details` 时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ProcessDetailInfo>,
    /// 最近一次失败原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ProcessInfo {
    /// 构造成功状态。
    pub fn ready(count: u64) -> Self {
        Self::ready_with_level(count, MetricLevel::Count, None, None)
    }

    /// 构造带详细级别的成功状态。
    pub fn ready_with_level(
        count: u64,
        level: MetricLevel,
        light: Option<ProcessLightInfo>,
        details: Option<ProcessDetailInfo>,
    ) -> Self {
        Self {
            count,
            status: MetricStatus::Ready,
            level,
            light,
            details,
            error: None,
        }
    }

    /// 构造当前平台不支持状态。
    pub fn unsupported(error: String) -> Self {
        Self {
            count: 0,
            status: MetricStatus::Unsupported,
            level: MetricLevel::Count,
            light: None,
            details: None,
            error: Some(error),
        }
    }
}
