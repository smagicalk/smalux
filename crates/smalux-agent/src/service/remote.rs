//! 远程能力模块。
//!
//! 这里集中放置由 server 触发的 agent 远程能力：交互式 shell、非交互任务和网络探测。
//! CLI-only 的能力开关仍由 `ServiceOptions` 持有，运行期限制来自动态配置。

pub(crate) mod job;
pub(crate) mod probe;
pub(crate) mod shell;
pub(crate) mod task;

/// 远程命令静态能力开关。
///
/// 这里统一控制所有“会在目标机上执行命令”的远程能力。当前包括：
/// - 交互式 remote shell
/// - 非交互 remote task
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RemoteCommandOptions {
    /// 是否允许远程命令能力运行；只能由 CLI 启动参数开启。
    pub enabled: bool,
}

impl Default for RemoteCommandOptions {
    /// 默认关闭远程命令能力。
    fn default() -> Self {
        Self { enabled: false }
    }
}

impl RemoteCommandOptions {
    /// 校验远程命令静态选项。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
