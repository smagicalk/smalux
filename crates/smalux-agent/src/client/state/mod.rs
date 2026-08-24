//! Agent 长期认证状态与可替换的异步存储边界。
//!
//! 领域状态及其阶段转换位于 `model`，默认 JSON 持久化实现位于
//! `file_store`。这个模块只保留存储 trait 和稳定的对外导出。

use async_trait::async_trait;

mod file_store;
mod model;

pub use file_store::FileAgentStateStore;
pub use model::{PersistedAgentState, RegistrationStage};

/// Agent Client 使用的异步持久化边界；文件、SQLite 或其他数据库均可实现该接口。
#[async_trait]
pub trait AgentStateStore: Send + Sync {
    /// 加载最后一次完整保存的认证状态；首次启动时返回 `None`。
    async fn load(&self) -> anyhow::Result<Option<PersistedAgentState>>;
    /// 完整替换当前认证状态。
    async fn save(&self, state: &PersistedAgentState) -> anyhow::Result<()>;
    /// 删除主状态及可能的临时/备份文件。
    async fn clear(&self) -> anyhow::Result<()>;
}
