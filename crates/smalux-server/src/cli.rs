//! Server 命令行模型。
//!
//! 本模块只负责把不可信参数转换为强类型命令，不执行数据库、IPC 或网络操作。运行配置
//! 的最终优先级统一为“CLI > 环境变量 > 默认值”；管理子命令则连接本机控制端点。

use std::{env, path::PathBuf, time::Duration};

use clap::{Args, Parser, Subcommand, ValueEnum};

const CONTROL_ENDPOINT_ENV: &str = "SMALUX_SERVER_CONTROL_ENDPOINT";

/// Smalux Server 顶层命令。
#[derive(Parser)]
#[command(name = "smalux-server", version, about = "Smalux monitoring server")]
pub struct Cli {
    /// 本地管理 IPC 地址；该端点不会监听 TCP。
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        default_value_os_t = default_control_endpoint_value()
    )]
    pub control_endpoint: PathBuf,
    /// 单次本地管理请求的超时时间。
    #[arg(
        long,
        global = true,
        value_name = "DURATION",
        default_value = "3s",
        value_parser = parse_nonzero_duration
    )]
    pub request_timeout: Duration,
    /// 未指定子命令时保持历史行为，直接启动 Server。
    #[command(subcommand)]
    pub command: Option<CliCommand>,
}

impl Cli {
    /// 拆出全局 IPC 参数，并把省略的子命令解释为默认 `run`。
    pub fn command_or_default(self) -> (PathBuf, Duration, CliCommand) {
        (
            self.control_endpoint,
            self.request_timeout,
            self.command
                .unwrap_or_else(|| CliCommand::Run(Box::default())),
        )
    }
}

/// Server 的启动命令和本地管理命令集合。
#[derive(Subcommand)]
pub enum CliCommand {
    /// 启动 HTTP/gRPC Server 和本地管理端点。
    Run(Box<RunArgs>),
    /// 查询运行状态和资源使用量。
    Status(StatusArgs),
    /// 查看或校验 Server 配置。
    Config {
        /// 具体配置操作。
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// 管理一次性 Agent 注册 Token。
    RegistrationToken {
        /// 具体 Token 管理操作。
        #[command(subcommand)]
        command: RegistrationTokenCommand,
    },
    /// 管理已经注册的 Agent 身份。
    Agent {
        /// 具体 Agent 管理操作。
        #[command(subcommand)]
        command: AgentCommand,
    },
    /// 查看或断开当前进程中的 Agent Session。
    Session {
        /// 具体 Session 管理操作。
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// 查看 Server Noise 密钥环状态。
    Keyring {
        /// 具体密钥环只读操作。
        #[command(subcommand)]
        command: KeyringCommand,
    },
    /// 请求正在运行的 Server 优雅关闭。
    Shutdown(ConfirmationArgs),
}

/// `run` 的监听、容量和数据库覆盖参数。
#[derive(Args, Default)]
pub struct RunArgs {
    /// HTTP/gRPC 监听 IP；不建议直接监听未受防护的公网地址。
    #[arg(long, env = "SMALUX_SERVER_LISTEN_ADDRESS", value_name = "IP")]
    pub listen_address: Option<String>,
    /// HTTP、普通 API 和 gRPC 共用的监听端口。
    #[arg(long, env = "SMALUX_SERVER_LISTEN_PORT", value_name = "PORT")]
    pub listen_port: Option<u16>,
    /// 同时占用资源的 Agent gRPC 流上限。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub max_agent_sessions: Option<usize>,
    /// 同时执行 XXpsk3 注册业务的会话上限。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub max_registration_sessions: Option<usize>,
    /// 单条 gRPC protobuf 消息的最大字节数。
    #[arg(long, value_parser = parse_positive_usize, value_name = "BYTES")]
    pub max_grpc_message_bytes: Option<usize>,
    /// 收到关闭请求后等待长期 Session 退出的时间。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub shutdown_grace: Option<Duration>,
    /// SQLite、PostgreSQL 或 MySQL 连接 URL；URL 中不能嵌入凭据。
    #[arg(long, value_name = "URL")]
    pub database_url: Option<String>,
    /// 数据库用户名；SQLite 不接受用户名。
    #[arg(long, value_name = "NAME")]
    pub database_username: Option<String>,
    /// 从文件读取数据库密码，避免秘密出现在命令历史和进程列表中。
    #[arg(long, value_name = "PATH")]
    pub database_password_file: Option<PathBuf>,
    /// 数据库连接池最大连接数。
    #[arg(long, value_parser = parse_positive_u32, value_name = "COUNT")]
    pub database_max_connections: Option<u32>,
    /// 数据库连接池保持的最小连接数。
    #[arg(long, value_parser = parse_positive_u32, value_name = "COUNT")]
    pub database_min_connections: Option<u32>,
    /// 建立数据库连接的最长等待时间。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub database_connect_timeout: Option<Duration>,
    /// 从连接池获取连接的最长等待时间。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub database_acquire_timeout: Option<Duration>,
    /// 空闲数据库连接的回收时间。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub database_idle_timeout: Option<Duration>,
    /// 单个数据库连接的最大生命周期。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub database_max_lifetime: Option<Duration>,
    /// 是否让 SQLx 记录 SQL 语句日志；默认关闭。
    #[arg(long, value_name = "BOOL")]
    pub database_sqlx_logging: Option<bool>,
    /// 是否把 SQL 语句写入 tracing span；默认关闭。
    #[arg(long, value_name = "BOOL")]
    pub database_record_statements: Option<bool>,
    /// 后端专属标量选项，格式为 KEY=JSON_VALUE；可重复指定。
    #[arg(long, value_name = "KEY=VALUE")]
    pub database_option: Vec<String>,
}

/// `status` 的一次性查询或持续观察参数。
#[derive(Args)]
pub struct StatusArgs {
    /// 持续刷新状态，直到按下 Ctrl+C。
    #[arg(long)]
    pub watch: bool,
    /// `--watch` 模式的刷新间隔。
    #[arg(long, default_value = "2s", value_parser = parse_nonzero_duration)]
    pub interval: Duration,
    /// 状态响应的输出格式。
    #[command(flatten)]
    pub output: OutputArgs,
}

/// 配置检查和运行态脱敏查看命令。
#[derive(Subcommand)]
pub enum ConfigCommand {
    /// 通过本地 IPC 查看运行中的脱敏配置。
    Show(OutputArgs),
    /// 解析并校验启动配置，但不连接数据库或启动 Server。
    Check(Box<RunArgs>),
}

/// 注册 Token 的签发、查询和吊销命令。
#[derive(Subcommand)]
pub enum RegistrationTokenCommand {
    /// 签发只显示一次完整凭据的注册 Token。
    Create(TokenCreateArgs),
    /// 分页列出不含 PSK 的 Token 元数据。
    List(TokenListArgs),
    /// 按公开 Token ID 查看不含 PSK 的元数据。
    Show {
        /// 注册凭据中点号前的公开 Token ID。
        token_id: String,
        /// 查询响应的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
    /// 吊销一条尚未消费的 Token；重复吊销保持幂等。
    Revoke {
        /// 待吊销的公开 Token ID。
        token_id: String,
        /// 危险操作确认参数。
        #[command(flatten)]
        confirmation: ConfirmationArgs,
        /// 吊销结果的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// 创建注册 Token 的名称、有效期、凭据文件和输出参数。
#[derive(Args)]
pub struct TokenCreateArgs {
    /// Server 绑定到 Token 的 Agent 展示名称；允许和其他 Agent 重复。
    #[arg(long, value_name = "NAME")]
    pub agent_name: Option<String>,
    /// Token 有效期，支持 `30m`、`24h`、`7d`；默认 30 分钟。
    #[arg(
        long,
        default_value = "30m",
        value_parser = parse_nonzero_duration,
        conflicts_with = "no_expiry"
    )]
    pub expires_in: Duration,
    /// 显式创建永久 Token；不建议作为常规注册方式。
    #[arg(long, conflicts_with = "expires_in")]
    pub no_expiry: bool,
    /// 只把完整凭据写入一个必须尚不存在的新文件，不在控制台重复显示。
    #[arg(long, value_name = "PATH")]
    pub credential_file: Option<PathBuf>,
    /// 创建响应的输出格式；使用凭据文件时不会再次输出完整凭据。
    #[command(flatten)]
    pub output: OutputArgs,
}

impl TokenCreateArgs {
    /// 返回传给服务层的有效期；`--no-expiry` 映射为 `None`。
    pub fn validity(&self) -> Option<Duration> {
        (!self.no_expiry).then_some(self.expires_in)
    }
}

impl RunArgs {
    /// 在环境配置上应用 CLI 覆盖，并执行与正式启动相同的联合校验。
    pub(crate) fn resolve(self) -> anyhow::Result<crate::config::ServerConfig> {
        let mut config = crate::config::ServerConfig::from_env()?;
        if let Some(value) = self.listen_address {
            config.address = value;
        }
        if let Some(value) = self.listen_port {
            config.port = value;
        }
        if let Some(value) = self.max_agent_sessions {
            config.max_agent_sessions = value;
        }
        if let Some(value) = self.max_registration_sessions {
            config.max_registration_sessions = value;
        }
        if let Some(value) = self.max_grpc_message_bytes {
            config.max_grpc_message_bytes = value;
        }
        if let Some(value) = self.shutdown_grace {
            config.shutdown_grace_seconds = whole_seconds("--shutdown-grace", value)?;
        }
        if let Some(value) = self.database_url {
            config.database.set_url(value);
        }
        if let Some(value) = self.database_username {
            config.database.set_username(value);
        }
        if let Some(path) = self.database_password_file {
            let password = std::fs::read_to_string(&path).map_err(|error| {
                anyhow::anyhow!(
                    "failed to read database password file {}: {error}",
                    path.display()
                )
            })?;
            config
                .database
                .set_password(password.trim_end_matches(['\r', '\n']).to_owned());
        }
        if let Some(value) = self.database_max_connections {
            config.database.pool.max_connections = value;
        }
        if let Some(value) = self.database_min_connections {
            config.database.pool.min_connections = value;
        }
        if let Some(value) = self.database_connect_timeout {
            config.database.pool.connect_timeout_seconds =
                whole_seconds("--database-connect-timeout", value)?;
        }
        if let Some(value) = self.database_acquire_timeout {
            config.database.pool.acquire_timeout_seconds =
                Some(whole_seconds("--database-acquire-timeout", value)?);
        }
        if let Some(value) = self.database_idle_timeout {
            config.database.pool.idle_timeout_seconds =
                Some(whole_seconds("--database-idle-timeout", value)?);
        }
        if let Some(value) = self.database_max_lifetime {
            config.database.pool.max_lifetime_seconds =
                Some(whole_seconds("--database-max-lifetime", value)?);
        }
        if let Some(value) = self.database_sqlx_logging {
            config.database.pool.sqlx_logging = value;
        }
        if let Some(value) = self.database_record_statements {
            config.database.pool.record_stmt_in_spans = value;
        }
        for option in self.database_option {
            let (key, value) = option
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("database option must use KEY=VALUE"))?;
            anyhow::ensure!(
                !key.trim().is_empty(),
                "database option key must not be empty"
            );
            let value = serde_json::from_str(value)
                .unwrap_or_else(|_| serde_json::Value::String(value.to_owned()));
            anyhow::ensure!(
                !value.is_array() && !value.is_object(),
                "database option value must be a JSON scalar"
            );
            config.database.set_option(key.trim().to_owned(), value);
        }
        config.database.validate()?;
        Ok(config)
    }
}

/// Token 元数据分页过滤参数。
#[derive(Args)]
pub struct TokenListArgs {
    /// 按 active、used、revoked 或运行时计算的 expired 状态过滤。
    #[arg(long, value_enum)]
    pub status: Option<TokenStatusFilter>,
    /// 按 Agent 展示名称做包含匹配。
    #[arg(long, value_name = "NAME")]
    pub agent_name: Option<String>,
    /// 单页数量，默认 50，最大 500。
    #[arg(long, default_value_t = 50, value_parser = parse_page_limit)]
    pub limit: u32,
    /// 只返回 Token ID 字典序位于该值之后的记录。
    #[arg(long, value_name = "TOKEN_ID")]
    pub after: Option<String>,
    /// 查询响应的输出格式。
    #[command(flatten)]
    pub output: OutputArgs,
}

/// CLI 接受的 Token 状态过滤器。
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TokenStatusFilter {
    /// 尚未使用且未过期。
    Active,
    /// 已被一次成功注册消费。
    Used,
    /// 已由管理员吊销。
    Revoked,
    /// 状态仍为 active、但有效期已结束。
    Expired,
}

/// Agent 身份的查询、展示名称修改和吊销命令。
#[derive(Subcommand)]
pub enum AgentCommand {
    /// 分页查询 Agent 元数据和当前在线状态。
    List(AgentListArgs),
    /// 按稳定 Agent ID 查看详情。
    Show {
        /// Server 持久化的稳定 Agent ID。
        agent_id: String,
        /// 查询响应的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
    /// 修改展示名称；身份识别仍只使用 Agent ID。
    Rename {
        /// 目标 Agent 的稳定 ID。
        agent_id: String,
        /// 新展示名称，允许重复但必须满足协议长度限制。
        #[arg(long)]
        name: String,
        /// 修改结果的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
    /// 持久化吊销 Agent，并立即断开其活动 Session。
    Revoke {
        /// 目标 Agent 的稳定 ID。
        agent_id: String,
        /// 危险操作确认参数。
        #[command(flatten)]
        confirmation: ConfirmationArgs,
        /// 吊销结果的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Agent 元数据分页和在线状态过滤参数。
#[derive(Args)]
pub struct AgentListArgs {
    /// 按 active 或 revoked 授权状态过滤。
    #[arg(long, value_enum)]
    pub status: Option<AgentStatusFilter>,
    /// 按展示名称做包含匹配。
    #[arg(long)]
    pub name: Option<String>,
    /// 只返回当前进程中存在已认证 Session 的 Agent。
    #[arg(long, conflicts_with = "offline")]
    pub online: bool,
    /// 只返回当前进程中没有已认证 Session 的 Agent。
    #[arg(long, conflicts_with = "online")]
    pub offline: bool,
    /// 单页数量，默认 50，最大 500。
    #[arg(long, default_value_t = 50, value_parser = parse_page_limit)]
    pub limit: u32,
    /// 只返回 Agent ID 字典序位于该值之后的记录。
    #[arg(long, value_name = "AGENT_ID")]
    pub after: Option<String>,
    /// 查询响应的输出格式。
    #[command(flatten)]
    pub output: OutputArgs,
}

/// CLI 接受的持久化 Agent 授权状态。
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AgentStatusFilter {
    /// 当前允许建立 IK Session。
    Active,
    /// 已禁止后续授权。
    Revoked,
}

/// 当前进程 Session 的查询与定向断开命令。
#[derive(Subcommand)]
pub enum SessionCommand {
    /// 查看当前进程中的实时 Session。
    List(SessionListArgs),
    /// 按临时 Session ID 查看详情。
    Show {
        /// 当前 Server 进程内分配的临时 Session ID。
        session_id: u64,
        /// 查询响应的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
    /// 只断开当前连接，不吊销 Agent；Agent 可能自动重连。
    Disconnect {
        /// 当前 Server 进程内分配的临时 Session ID。
        session_id: u64,
        /// 危险操作确认参数。
        #[command(flatten)]
        confirmation: ConfirmationArgs,
        /// 断开结果的输出格式。
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Session 的身份和认证阶段过滤参数。
#[derive(Args)]
pub struct SessionListArgs {
    /// 只返回属于指定稳定 Agent ID 的 Session。
    #[arg(long)]
    pub agent_id: Option<String>,
    /// 按 handshaking、registering 或 authenticated 阶段过滤。
    #[arg(long, value_enum)]
    pub state: Option<SessionStateFilter>,
    /// 查询响应的输出格式。
    #[command(flatten)]
    pub output: OutputArgs,
}

/// CLI 接受的 Server 端 Session 阶段。
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SessionStateFilter {
    /// 已连接，尚在协商或执行 Noise 握手。
    Handshaking,
    /// 正在执行首次 XXpsk3 注册。
    Registering,
    /// 已完成 IK 身份认证。
    Authenticated,
}

/// Server Noise 密钥环的只读管理命令。
#[derive(Subcommand)]
pub enum KeyringCommand {
    /// 只展示 revision 和公开 key ID，不返回公钥原文或私钥。
    Status(OutputArgs),
}

/// 危险操作共享的非交互确认参数。
#[derive(Args)]
pub struct ConfirmationArgs {
    /// 跳过交互确认；非交互环境执行危险操作时必须指定。
    #[arg(long)]
    pub yes: bool,
}

/// 管理命令共享的输出格式参数。
#[derive(Args)]
pub struct OutputArgs {
    /// 输出适合人工阅读的文本或稳定的 JSON DTO。
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

/// 管理结果的控制台编码格式。
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutputFormat {
    /// 面向操作人员的紧凑键值文本。
    Table,
    /// 面向脚本的完整结构化 JSON。
    Json,
}

/// 从当前进程参数解析 Server CLI；语法错误由 Clap 负责显示并退出。
pub fn parse() -> Cli {
    Cli::parse()
}

/// 解析管理端点默认值：显式环境变量优先，否则使用平台本地安全端点。
pub(crate) fn default_control_endpoint_value() -> PathBuf {
    if let Some(value) = env::var_os(CONTROL_ENDPOINT_ENV).filter(|value| !value.is_empty()) {
        return PathBuf::from(value);
    }
    #[cfg(windows)]
    {
        PathBuf::from(r"\\.\pipe\smalux-server")
    }
    #[cfg(unix)]
    {
        smalux_core::config::data_dir()
            .expect("failed to resolve Server data directory")
            .join("server")
            .join("control.sock")
    }
}

/// 把时长转为配置使用的整秒，并拒绝亚秒截断。
fn whole_seconds(name: &str, duration: Duration) -> anyhow::Result<u64> {
    anyhow::ensure!(
        duration.subsec_nanos() == 0 && duration.as_secs() > 0,
        "{name} must be at least one whole second"
    );
    Ok(duration.as_secs())
}

/// 解析 `30m`、`24h` 等人类可读时长并拒绝零值。
fn parse_nonzero_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    (!duration.is_zero())
        .then_some(duration)
        .ok_or_else(|| "duration must be greater than zero".to_owned())
}

/// 解析平台宽度的正整数容量参数。
fn parse_positive_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())
        .and_then(|value| {
            (value > 0)
                .then_some(value)
                .ok_or_else(|| "value must be greater than zero".to_owned())
        })
}

/// 解析 `u32` 正整数参数。
fn parse_positive_u32(value: &str) -> Result<u32, String> {
    value
        .parse::<u32>()
        .map_err(|error| error.to_string())
        .and_then(|value| {
            (value > 0)
                .then_some(value)
                .ok_or_else(|| "value must be greater than zero".to_owned())
        })
}

/// 解析 1..=500 的分页大小，限制本地 IPC 响应规模。
fn parse_page_limit(value: &str) -> Result<u32, String> {
    let value = parse_positive_u32(value)?;
    (value <= 500)
        .then_some(value)
        .ok_or_else(|| "page limit must not exceed 500".to_owned())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, CliCommand, RegistrationTokenCommand};

    #[test]
    fn no_subcommand_defaults_to_run() {
        let cli = Cli::try_parse_from(["smalux-server"]).unwrap();
        let (_, _, command) = cli.command_or_default();
        assert!(matches!(command, CliCommand::Run(_)));
    }

    #[test]
    fn token_creation_defaults_to_thirty_minutes() {
        let cli = Cli::try_parse_from(["smalux-server", "registration-token", "create"]).unwrap();
        let Some(CliCommand::RegistrationToken { command }) = cli.command else {
            panic!("registration-token command")
        };
        let RegistrationTokenCommand::Create(args) = command else {
            panic!("create command")
        };
        assert_eq!(args.validity(), Some(std::time::Duration::from_secs(1_800)));
    }

    #[test]
    fn token_creation_accepts_days_and_permanent_mode() {
        let cli = Cli::try_parse_from([
            "smalux-server",
            "registration-token",
            "create",
            "--no-expiry",
        ])
        .unwrap();
        let Some(CliCommand::RegistrationToken { command }) = cli.command else {
            panic!("registration-token command")
        };
        let RegistrationTokenCommand::Create(args) = command else {
            panic!("create command")
        };
        assert_eq!(args.validity(), None);

        Cli::try_parse_from([
            "smalux-server",
            "registration-token",
            "create",
            "--expires-in",
            "7d",
        ])
        .unwrap();
    }

    #[test]
    fn page_limit_rejects_unbounded_ipc_responses() {
        assert!(
            Cli::try_parse_from(["smalux-server", "agent", "list", "--limit", "501",]).is_err()
        );
    }

    #[test]
    fn database_password_is_only_accepted_as_a_file() {
        assert!(
            Cli::try_parse_from(["smalux-server", "run", "--database-password", "secret",])
                .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "smalux-server",
                "run",
                "--database-password-file",
                "secret.txt",
            ])
            .is_ok()
        );
    }
}
