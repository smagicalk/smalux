//! Service 运行参数和运行状态。

use super::{shell::RemoteShellOptions, task::RemoteTaskOptions};
use crate::telemetry::TelemetryState;

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

/// 远程诊断采集静态权限。
///
/// 这些开关只能由启动参数设置，避免 server 在运行时直接打开高成本明细采集。
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct DiagnosticOptions {
    /// 是否允许 server 触发进程 details 采集。
    pub allow_process_details: bool,
    /// 是否允许 server 触发 Socket details 采集。
    pub allow_socket_details: bool,
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
    pub store: TelemetryState,
    /// 是否允许发送第一包上报。
    pub first_report_ready: bool,
}
