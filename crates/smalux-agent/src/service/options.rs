//! Service 运行参数和运行状态。

use super::{shell::RemoteShellOptions, task::RemoteTaskOptions};
use crate::telemetry::LatestTelemetry;
use smalux_core::model::info::MetricLevel;

/// Agent 运行入口参数。
#[derive(Debug, Clone)]
pub(crate) struct ServiceOptions {
    /// 当前 agent 版本。
    pub agent_version: &'static str,
    /// 诊断采集静态权限。
    pub diagnostics: DiagnosticOptions,
    /// 远程交互式 shell 静态选项。
    pub remote_shell: RemoteShellOptions,
    /// 远程非交互任务静态选项。
    pub remote_task: RemoteTaskOptions,
}

impl Default for ServiceOptions {
    /// 使用 crate 编译版本作为 agent 版本。
    fn default() -> Self {
        Self {
            agent_version: env!("CARGO_PKG_VERSION"),
            diagnostics: DiagnosticOptions::default(),
            remote_shell: RemoteShellOptions::default(),
            remote_task: RemoteTaskOptions::default(),
        }
    }
}

impl ServiceOptions {
    /// 校验服务静态选项。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.diagnostics.validate()?;
        self.remote_shell.validate()?;
        self.remote_task.validate()?;
        Ok(())
    }
}

/// 远程采样权限等级。
///
/// 权限等级只限制 server 远程触发或调整采样级别，不影响 agent 启动时本地配置的采样级别。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum RemoteMetricPermission {
    /// server 不能远程触发该类采样。
    None,
    /// server 只能触发 count 级别。
    Count,
    /// server 可以触发 count/light 级别。
    Light,
    /// server 可以触发 count/light/details 级别。
    Details,
}

impl Default for RemoteMetricPermission {
    /// 默认允许低成本 count 级别，避免 server 无法获取基础汇总。
    fn default() -> Self {
        Self::Count
    }
}

impl RemoteMetricPermission {
    /// 判断当前权限是否允许指定采样级别。
    pub(crate) fn allows(self, level: MetricLevel) -> bool {
        self.rank() >= Self::rank_for_metric_level(level)
    }

    /// 权限名称，用于错误信息和日志。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Count => "count",
            Self::Light => "light",
            Self::Details => "details",
        }
    }

    /// 权限排序值。
    fn rank(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Count => 1,
            Self::Light => 2,
            Self::Details => 3,
        }
    }

    /// 采样级别对应的最低权限排序值。
    fn rank_for_metric_level(level: MetricLevel) -> u8 {
        match level {
            MetricLevel::Count => 1,
            MetricLevel::Light => 2,
            MetricLevel::Details => 3,
        }
    }
}

/// 远程诊断采集静态权限。
///
/// 这些权限只能由启动参数设置，避免 server 在运行时直接扩大采样范围。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct DiagnosticOptions {
    /// server 允许触发的最高进程采样级别。
    pub process_permission: RemoteMetricPermission,
    /// server 允许触发的最高 Socket 采样级别。
    pub socket_permission: RemoteMetricPermission,
}

impl Default for DiagnosticOptions {
    /// 默认允许远程 count，light/details 需要显式启动参数授权。
    fn default() -> Self {
        Self {
            process_permission: RemoteMetricPermission::Count,
            socket_permission: RemoteMetricPermission::Count,
        }
    }
}

impl DiagnosticOptions {
    /// 校验诊断权限选项。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// agent 服务启动状态。
#[derive(Debug, Clone)]
pub(crate) struct ServiceState {
    /// 最新采样缓存。
    pub latest_telemetry: LatestTelemetry,
    /// 是否允许发送第一包上报。
    pub first_report_ready: bool,
}
