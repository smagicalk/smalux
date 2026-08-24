//! Agent 领域服务及其 gRPC 适配层。

/// 注册 Token 签发、首次注册事务和 IK 授权查询。
pub mod agent_registry;
/// 已认证 Agent 的权威远程 Job 目录边界。
pub mod job_catalog;
/// Server Noise 身份的加载、CAS 轮换和跨实例同步。
pub mod keyring_manager;
/// Plus 参数 Schema 的内容寻址存储和解析缓存。
pub mod plugin_schema_registry;
/// 当前进程中的实时 Agent Session 目录。
pub mod session_registry;
/// Agent gRPC 服务共享依赖。
pub mod state;
/// Tonic AgentTransport 服务及 Session 状态机。
pub mod transport;
