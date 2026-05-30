//! 远程交互式 shell 静态能力开关。
//!
//! 这里只定义 CLI-only 的能力开关。会话上限、超时和 shell 程序放在动态
//! `AgentConfig.remote_shell` 中，server patch 可以在能力已开启后调整。

/// 远程交互式 shell 运行选项。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RemoteShellOptions {
    /// 是否启用远程 shell；只能由 CLI 启动参数开启。
    pub enabled: bool,
}

impl Default for RemoteShellOptions {
    /// 默认关闭远程 shell。
    fn default() -> Self {
        Self { enabled: false }
    }
}

impl RemoteShellOptions {
    /// 校验远程 shell 静态选项。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! 远程 shell 选项测试。

    use super::*;

    /// 验证默认远程 shell 关闭。
    #[test]
    fn remote_shell_is_disabled_by_default() {
        let options = RemoteShellOptions::default();

        assert!(!options.enabled);
        options.validate().unwrap();
    }
}
