//! 会话内插件运行时快照的最小状态机。

use smalux_protocol::agent::v1::{AgentPluginAck, PluginRuntimeSnapshot};

/// 当前会话已经接受的插件运行时状态。
///
/// 此结构不保存 Secret，也不写磁盘；连接断开或 Agent 重启后必须重新接收 Server 快照。
#[derive(Debug, Default)]
pub struct PluginRuntimeState {
    applied_revision: u64,
    applied_snapshot: Option<PluginRuntimeSnapshot>,
}

/// 应用 Server 运行时快照后的结果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeSnapshotResult {
    /// 新快照已接受，调用方可以按其中的插件启动或更新 Worker。
    Applied,
    /// 同版本或更旧快照被安全忽略，避免延迟包回退运行时。
    IgnoredStale,
}

impl PluginRuntimeState {
    /// 返回当前会话已接受的最新版本；零表示尚未收到可用快照。
    pub fn applied_revision(&self) -> u64 {
        self.applied_revision
    }

    /// 先执行版本门控；Worker 成功初始化后调用 [`confirm_applied`](Self::confirm_applied)。
    pub fn validate_snapshot(
        &self,
        snapshot: &PluginRuntimeSnapshot,
    ) -> Result<RuntimeSnapshotResult, String> {
        if snapshot.revision == 0 {
            return Err("plugin runtime snapshot revision must be greater than zero".to_owned());
        }
        if snapshot.revision < self.applied_revision {
            return Ok(RuntimeSnapshotResult::IgnoredStale);
        }
        if snapshot.revision == self.applied_revision {
            if self.applied_snapshot.as_ref() == Some(snapshot) {
                return Ok(RuntimeSnapshotResult::IgnoredStale);
            }
            return Err("same plugin runtime revision contains different content".to_owned());
        }
        Ok(RuntimeSnapshotResult::Applied)
    }

    /// Worker 全部就绪后保存完整快照并创建发回 Server 的确认。
    pub fn confirm_snapshot(&mut self, snapshot: &PluginRuntimeSnapshot) -> AgentPluginAck {
        self.applied_revision = snapshot.revision;
        self.applied_snapshot = Some(snapshot.clone());
        AgentPluginAck {
            revision: snapshot.revision,
            accepted: true,
            error: None,
        }
    }

    /// 将配置或 Worker 初始化错误转换为不会泄漏 Secret 的拒绝确认。
    pub fn reject(&self, revision: u64, error: impl Into<String>) -> AgentPluginAck {
        AgentPluginAck {
            revision,
            accepted: false,
            error: Some(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PluginRuntimeState, RuntimeSnapshotResult};
    use smalux_protocol::agent::v1::PluginRuntimeSnapshot;

    #[test]
    fn snapshot_revision_only_advances_after_explicit_confirmation() {
        let mut state = PluginRuntimeState::default();
        let snapshot = PluginRuntimeSnapshot {
            revision: 4,
            plugins: Vec::new(),
        };
        assert_eq!(
            state.validate_snapshot(&snapshot).unwrap(),
            RuntimeSnapshotResult::Applied
        );
        assert_eq!(state.applied_revision(), 0);
        assert!(state.confirm_snapshot(&snapshot).accepted);
        assert_eq!(state.applied_revision(), 4);
        assert_eq!(
            state.validate_snapshot(&snapshot).unwrap(),
            RuntimeSnapshotResult::IgnoredStale
        );
    }

    #[test]
    fn same_revision_with_different_content_is_rejected() {
        let mut state = PluginRuntimeState::default();
        let first = PluginRuntimeSnapshot {
            revision: 4,
            plugins: Vec::new(),
        };
        state.confirm_snapshot(&first);
        let changed = PluginRuntimeSnapshot {
            revision: 4,
            plugins: vec![smalux_protocol::agent::v1::PluginRuntimeConfig {
                plugin_id: "plugin".to_owned(),
                version: "1.0.0".to_owned(),
                ..Default::default()
            }],
        };
        assert!(state.validate_snapshot(&changed).is_err());
    }
}
