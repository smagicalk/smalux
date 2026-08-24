//! Server 本地管理 IPC 协议 DTO。

use serde::{Deserialize, Serialize};

/// 本地管理 IPC 的线协议版本。
///
/// 请求和响应都携带此值。发生不兼容变更时必须递增，Server 不会尝试猜测旧格式。
pub const CONTROL_PROTOCOL_VERSION: u32 = 1;
/// 单个管理请求或响应允许的最大 JSON 负载，防止本地客户端触发无界内存分配。
pub const MAX_CONTROL_FRAME_BYTES: usize = 1024 * 1024;

/// CLI 发给正在运行的 Server 的顶层消息。
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestEnvelope {
    /// 发送方使用的 [`CONTROL_PROTOCOL_VERSION`]。
    pub protocol_version: u32,
    /// 本次只执行一次的管理操作。
    pub request: ControlRequest,
}

/// Server 返回给 CLI 的顶层消息。
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    /// Server 使用的 [`CONTROL_PROTOCOL_VERSION`]。
    pub protocol_version: u32,
    /// 成功结果或脱敏后的结构化错误。
    pub response: ControlResponse,
}

/// 本地管理端点支持的全部操作。
///
/// 即使调用方绕过 Clap 直接构造请求，`AdminService` 仍会重新校验分页、过滤器和 ID。
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    /// 查询运行时间、连接数以及 Agent/Token 汇总。
    Status,
    /// 查询正在运行的脱敏配置；数据库 URL 和凭据不会返回。
    EffectiveConfig,
    /// 创建一次性 Agent 注册凭据。
    CreateRegistrationToken {
        /// Server 预先绑定的展示名称；省略时注册后默认使用 Agent ID。
        agent_name: Option<String>,
        /// 有效秒数；`None` 表示永久有效，`Some(0)` 非法。
        valid_for_seconds: Option<u64>,
    },
    /// 按 Token ID 游标分页查询公开元数据。
    ListRegistrationTokens {
        /// `active`、`used`、`revoked` 或动态计算的 `expired`。
        status: Option<String>,
        /// 展示名称的包含匹配。
        agent_name: Option<String>,
        /// 单页数量，服务端限定为 1..=500。
        limit: u32,
        /// 只返回 Token ID 字典序大于此值的记录。
        after: Option<String>,
    },
    /// 查询一个 Token 的公开元数据，不返回 PSK。
    GetRegistrationToken {
        /// 注册凭据中点号前的公开标识。
        token_id: String,
    },
    /// 幂等吊销一条尚未使用的注册 Token。
    RevokeRegistrationToken {
        /// 待吊销的公开 Token ID。
        token_id: String,
    },
    /// 按 Agent ID 游标分页查询持久化身份。
    ListAgents {
        /// `active` 或 `revoked`。
        status: Option<String>,
        /// 展示名称的包含匹配；名称不是身份键且允许重复。
        name: Option<String>,
        /// `Some(true)` 仅在线，`Some(false)` 仅离线，`None` 不过滤。
        online: Option<bool>,
        /// 单页数量，服务端限定为 1..=500。
        limit: u32,
        /// 只返回 Agent ID 字典序大于此值的记录。
        after: Option<String>,
    },
    /// 按稳定 Agent ID 查询身份。
    GetAgent {
        /// Server 生成并持久化的身份 ID。
        agent_id: String,
    },
    /// 只修改展示名称，不改变 Agent ID 或 Noise 身份。
    RenameAgent {
        /// 目标 Agent 的稳定 ID。
        agent_id: String,
        /// 新展示名称，允许与其他 Agent 重复。
        name: String,
    },
    /// 持久化吊销 Agent，并取消当前进程内属于它的活动 Session。
    RevokeAgent {
        /// 目标 Agent 的稳定 ID。
        agent_id: String,
    },
    /// 查询仅存在于当前进程内的实时 Session。
    ListSessions {
        /// 可选的稳定 Agent ID 过滤器。
        agent_id: Option<String>,
        /// `handshaking`、`registering` 或 `authenticated`。
        state: Option<String>,
    },
    /// 按临时 Session ID 查询连接状态。
    GetSession {
        /// Server 进程内递增的 ID，重启后不保证延续。
        session_id: u64,
    },
    /// 取消一个连接，但不吊销 Agent，因此对端仍可重连。
    DisconnectSession {
        /// Server 进程内的临时 Session ID。
        session_id: u64,
    },
    /// 查询密钥环 revision 和公开 key ID，不返回公私钥材料。
    KeyringStatus,
    /// 触发与 Ctrl+C 相同的优雅关闭令牌。
    Shutdown,
}

/// 管理操作的结构化结果。
///
/// 只有 [`ControlResponse::RegistrationTokenCreated`] 含一次性秘密，其余查询结果都可以
/// 安全地用于管理展示，但仍不应写入公开日志。
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ControlResponse {
    /// Server 汇总状态。
    Status(ServerStatus),
    /// 脱敏后的运行配置。
    EffectiveConfig(EffectiveConfig),
    /// 新签发的完整 Token；创建后无法再次查询 PSK。
    RegistrationTokenCreated(IssuedToken),
    /// Token 元数据列表。
    RegistrationTokens(Vec<RegistrationTokenView>),
    /// Token 元数据；`None` 表示不存在。
    RegistrationToken(Option<RegistrationTokenView>),
    /// 吊销后的 Token 元数据。
    RegistrationTokenRevoked(RegistrationTokenView),
    /// Agent 元数据列表。
    Agents(Vec<AgentView>),
    /// Agent 元数据；`None` 表示不存在。
    Agent(Option<AgentView>),
    /// 修改展示名称后的 Agent 元数据。
    AgentUpdated(AgentView),
    /// Agent 吊销结果及本次发出取消信号的 Session 数。
    AgentRevoked {
        /// 持久化后的 Agent 状态。
        agent: AgentView,
        /// 找到并取消的当前进程 Session 数量。
        disconnected_sessions: usize,
    },
    /// 当前进程内的 Session 列表。
    Sessions(Vec<SessionView>),
    /// Session 信息；`None` 表示它不存在或已经结束。
    Session(Option<SessionView>),
    /// 已向指定 Session 发出取消信号。
    SessionDisconnected {
        /// 被取消的进程内 Session ID。
        session_id: u64,
    },
    /// Server Noise 密钥环的脱敏状态。
    KeyringStatus(KeyringStatus),
    /// Server 已接受关闭请求；实际退出仍遵守优雅关闭期限。
    ShutdownAccepted,
    /// 可安全返回给本地调用方的失败信息。
    Error {
        /// 稳定、适合脚本判断的错误类别。
        code: String,
        /// 不包含数据库内部信息或密钥材料的说明。
        message: String,
    },
}

/// `status` 命令返回的运行状态快照。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerStatus {
    /// Server crate 版本。
    pub version: String,
    /// 当前进程运行毫秒数。
    pub uptime_ms: u64,
    /// `sqlite`、`postgres` 或 `mysql`，不含连接地址。
    pub database_backend: String,
    /// 当前登记在进程 Session 目录中的连接数，包含握手中连接。
    pub active_sessions: usize,
    /// 配置允许的并发 Agent gRPC 流上限。
    pub max_agent_sessions: usize,
    /// 配置允许的并发注册业务上限，不是 Agent 总数限制。
    pub max_registration_sessions: usize,
    /// 数据库中状态为 active 的 Agent 数。
    pub active_agents: u64,
    /// 数据库中已吊销的 Agent 数。
    pub revoked_agents: u64,
    /// 尚未使用且未过期的注册 Token 数。
    pub active_tokens: u64,
    /// 已成功完成注册的 Token 数。
    pub used_tokens: u64,
    /// 被管理员吊销的 Token 数。
    pub revoked_tokens: u64,
    /// 数据库状态仍为 active、但按当前时间已经过期的 Token 数。
    pub expired_tokens: u64,
    /// 当前内存密钥环对应的数据库 CAS revision。
    pub keyring_revision: i64,
    /// 当前可接受握手的 Server key 数量。
    pub active_server_keys: usize,
    /// 全局关闭令牌是否已被触发。
    pub shutting_down: bool,
}

/// `config show` 返回的脱敏运行配置。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EffectiveConfig {
    /// HTTP/gRPC 监听地址。
    pub listen_address: String,
    /// HTTP/gRPC 共用端口。
    pub listen_port: u16,
    /// 数据库后端类型。
    pub database_backend: String,
    /// 固定返回 `<redacted>`，用于明确原始 URL 被隐藏。
    pub database_url: String,
    /// 并发 Agent Session 上限。
    pub max_agent_sessions: usize,
    /// 并发注册阶段上限。
    pub max_registration_sessions: usize,
    /// 单条 protobuf 消息大小上限。
    pub max_grpc_message_bytes: usize,
}

/// 创建 Token 时唯一一次返回的完整注册凭据。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssuedToken {
    /// 可公开查询和吊销的 Token ID。
    pub token_id: String,
    /// `token_id.psk` 完整秘密，后续 list/show 均不会返回。
    pub credential: String,
    /// Unix epoch 微秒；`None` 表示永久有效。
    pub expires_at_unix_micros: Option<i64>,
}

/// 不含 PSK 的注册 Token 管理视图。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistrationTokenView {
    /// 可公开使用的 Token ID。
    pub token_id: String,
    /// Server 在签发时预绑定的展示名称。
    pub agent_name: Option<String>,
    /// `active`、`used`、`revoked` 或运行时计算的 `expired`。
    pub status: String,
    /// 创建时间，Unix epoch 微秒。
    pub created_at_unix_micros: i64,
    /// 最近持久化更新时间，Unix epoch 微秒。
    pub updated_at_unix_micros: i64,
    /// 过期时间，Unix epoch 微秒；`None` 表示永久有效。
    pub expires_at_unix_micros: Option<i64>,
    /// 首次成功消费时间，Unix epoch 微秒。
    pub used_at_unix_micros: Option<i64>,
}

/// Agent 持久化身份和实时在线状态的组合视图。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentView {
    /// 唯一且稳定的身份键；所有修改和吊销操作都使用它。
    pub agent_id: String,
    /// 可重复、可修改的展示名称。
    pub name: String,
    /// 持久化授权状态：`active` 或 `revoked`。
    pub status: String,
    /// 当前进程是否存在属于此 Agent 的已认证 Session。
    pub online: bool,
    /// 创建时间，Unix epoch 微秒。
    pub created_at_unix_micros: i64,
    /// 最近持久化更新时间，Unix epoch 微秒。
    pub updated_at_unix_micros: i64,
    /// 吊销时间，Unix epoch 微秒；活动 Agent 为 `None`。
    pub revoked_at_unix_micros: Option<i64>,
}

/// 一个正在运行的 gRPC Session 的非持久化诊断视图。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionView {
    /// 当前 Server 进程内递增的临时 ID。
    pub session_id: u64,
    /// 完成认证后才有值；握手和注册阶段为 `None`。
    pub agent_id: Option<String>,
    /// 已选择的 Noise 模式，例如 `xxpsk3` 或 `ik`。
    pub authentication_mode: Option<String>,
    /// `handshaking`、`registering` 或 `authenticated`。
    pub state: String,
    /// 接受 gRPC 流的时间，Unix epoch 微秒。
    pub connected_at_unix_micros: i64,
    /// 最近一次状态变化或业务消息活动时间，Unix epoch 微秒。
    pub last_activity_at_unix_micros: i64,
}

/// 不暴露公钥原文或私钥的 Server Noise 密钥环状态。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyringStatus {
    /// 数据库 CAS revision，用于判断不同 Server 实例是否已同步。
    pub revision: i64,
    /// 当前签名/响应身份的十六进制 key ID。
    pub current_key_id: String,
    /// 轮换准备阶段的新 key ID。
    pub next_key_id: Option<String>,
    /// 轮换提交后暂时保留的旧 key ID。
    pub previous_key_id: Option<String>,
    /// 当前轮换事务 ID；没有轮换时为 `None`。
    pub rotation_id: Option<String>,
}
