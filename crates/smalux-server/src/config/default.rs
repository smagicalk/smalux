//! Server 进程级配置的默认参数。
//!
//! 环境变量未设置时，`ServerConfig` 使用这些值。数据库连接池拥有独立的配置边界，
//! 其默认值继续放在 `database` 子模块中，避免混合不同职责的参数。

/// 同时保持的 Agent 长连接数量上限。
pub(crate) const DEFAULT_MAX_AGENT_SESSIONS: usize = 256;
/// 同时执行的首次注册流程数量上限。
pub(crate) const DEFAULT_MAX_REGISTRATION_SESSIONS: usize = 32;
/// 单个 gRPC protobuf 消息允许的最大字节数。
pub(crate) const DEFAULT_MAX_GRPC_MESSAGE_BYTES: usize = 1024 * 1024;
/// 收到关闭信号后等待长期会话退出的默认秒数。
pub(crate) const DEFAULT_SHUTDOWN_GRACE_SECONDS: u64 = 15;
