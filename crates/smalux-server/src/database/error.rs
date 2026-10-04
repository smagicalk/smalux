//! 数据库连接建立后的稳定错误边界。

use thiserror::Error;

use crate::config::DatabaseConfigError;

/// Server 数据库运行时错误。
///
/// 连接入口仍然只返回这一种错误：配置错误通过透明变体向上传递，
/// 已连接后的 SeaORM、迁移和领域数据错误则由其余变体表达。
#[derive(Debug, Error)]
pub enum DatabaseError {
    /// 数据库配置在建立连接前校验失败。
    #[error(transparent)]
    Config(#[from] DatabaseConfigError),
    /// SeaORM 或底层 SQLx 驱动返回的连接、查询或迁移错误。
    #[error("database operation failed: {0}")]
    SeaOrm(#[from] sea_orm::DbErr),
    /// 数据库中保存的 Server 密钥环结构不完整或不一致。
    #[error("invalid persisted Server keyring: {0}")]
    InvalidServerKeyring(String),
    /// Server 密钥环记录不存在。
    #[error("persisted Server keyring record is missing")]
    MissingServerKeyring,
    /// Server 密钥环写入时发现其他进程已经提交了更新。
    #[error("Server keyring revision conflict: expected {expected}, actual {actual:?}")]
    ServerKeyringRevisionConflict {
        /// 调用方读取到的 revision。
        expected: i64,
        /// 数据库当前 revision；数据库记录被删除时为 None。
        actual: Option<i64>,
    },
    /// revision 达到 i64 上限，无法再安全递增。
    #[error("Server keyring revision overflow")]
    ServerKeyringRevisionOverflow,
    /// 读取系统时间失败，无法比较注册 Token 或注册事务的过期时间。
    #[error("failed to read database clock: {0}")]
    Clock(#[from] std::time::SystemTimeError),
    /// 注册相关记录违反持久化不变量。
    #[error("invalid persisted Agent registration data: {0}")]
    InvalidAgentRegistration(String),
    /// 插件参数 Schema 与其内容地址或身份不一致。
    #[error("invalid persisted Plugin schema: {0}")]
    InvalidPluginSchema(String),
    /// 权威 Agent Job 目录、定义或存储内容无效。
    #[error("invalid persisted Agent Job catalog: {0}")]
    InvalidJobCatalog(String),
    /// 控制面提交的目录版本已经落后于数据库当前版本。
    #[error("{resource} revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict {
        /// 发生冲突的权威资源名称，例如 `Agent Job catalog`。
        resource: &'static str,
        /// 调用方声明的期望版本。
        expected: i64,
        /// 数据库当前版本。
        actual: i64,
    },
    /// Agent 上报的成功 TaskReport 缺少执行身份或结果。
    #[error("invalid Agent Task report: {0}")]
    InvalidTaskReport(String),
    /// 下发命令与 Agent 返回结果的关联不一致。
    #[error("invalid Agent Job command: {0}")]
    InvalidJobCommand(String),
    /// Agent Scheduler 事件的实例、序号或 Job 归属不一致。
    #[error("invalid Agent Job event: {0}")]
    InvalidJobEvent(String),
    /// 插件 runtime 的身份、Schema 或并发限制不完整。
    #[error("invalid Agent plugin runtime: {0}")]
    InvalidPluginRuntime(String),
    /// 已认证 Agent 上报的能力或插件清单无法安全持久化。
    #[error("invalid Agent capability or plugin inventory snapshot: {0}")]
    InvalidAgentSnapshot(String),
    /// 恢复或校验 Noise 身份时失败。
    #[error("Noise keyring operation failed: {0}")]
    Noise(#[from] smalux_protocol::noise::NoiseError),
}
