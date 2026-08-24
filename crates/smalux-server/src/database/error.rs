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
    /// 恢复或校验 Noise 身份时失败。
    #[error("Noise keyring operation failed: {0}")]
    Noise(#[from] smalux_protocol::noise::NoiseError),
}
