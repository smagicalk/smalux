//! 远程非交互任务协议模型。

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 远程非交互任务执行状态。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteTaskStatus {
    /// 命令执行成功，退出码为 0。
    Success,
    /// 命令执行完成，但退出码非 0 或启动失败。
    Failed,
    /// 命令超过 agent 配置的最大运行时间。
    TimedOut,
    /// agent 拒绝执行，例如能力未开启或并发已满。
    Rejected,
}

/// 远程非交互任务执行结果。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskResult {
    /// server 下发的任务 ID。
    pub task_id: String,
    /// 执行状态。
    pub status: RemoteTaskStatus,
    /// 进程退出码；启动失败、超时或拒绝执行时为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// 标准输出，超过 agent 配置上限会被截断。
    pub stdout: String,
    /// 标准错误，超过 agent 配置上限会被截断。
    pub stderr: String,
    /// 开始执行时间，Unix 时间戳，单位秒。
    pub started_at: u64,
    /// 完成时间，Unix 时间戳，单位秒。
    pub finished_at: u64,
    /// 执行耗时，单位毫秒。
    pub duration_ms: u64,
    /// 是否因为超时结束。
    pub timed_out: bool,
    /// 标准输出是否被截断。
    pub stdout_truncated: bool,
    /// 标准错误是否被截断。
    pub stderr_truncated: bool,
    /// 面向日志和调试的错误说明。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 远程非交互任务请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskRequest {
    /// server 侧生成的任务 ID。
    pub task_id: String,
    /// 要执行的程序路径或程序名。
    pub program: String,
    /// 直接传给程序的参数；agent 不做 shell 拼接。
    #[serde(default)]
    pub args: Vec<String>,
    /// 本次任务期望超时；agent 会限制在当前配置允许范围内。
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout: Option<Duration>,
}
