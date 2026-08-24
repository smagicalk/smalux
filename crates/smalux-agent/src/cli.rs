//! Agent 命令行解析、配置覆盖和本地查询命令模型。
//!
//! 本模块不启动网络、Scheduler 或 IPC。它只把不可信字符串转换为强类型参数，并按
//! “CLI > 环境变量 > 默认值”生成最终启动配置。包含 Token 的类型故意不派生 `Debug`。

use std::{env, num::NonZeroUsize, path::PathBuf, time::Duration};

use clap::{Args, Parser, Subcommand, ValueEnum};
use smalux_agent::{
    client::{RegistrationToken, SmaluxClientConfig},
    scheduler::SchedulerConfig,
    tasks::TaskSource,
};
use uuid::Uuid;

const CONTROL_ENDPOINT_ENV: &str = "SMALUX_CONTROL_ENDPOINT";
const TASK_REPORT_BUFFER_CAPACITY_ENV: &str = "SMALUX_TASK_REPORT_BUFFER_CAPACITY";
const JOB_RESULT_BUFFER_CAPACITY_ENV: &str = "SMALUX_JOB_RESULT_BUFFER_CAPACITY";
const SHUTDOWN_DRAIN_TIMEOUT_ENV: &str = "SMALUX_SHUTDOWN_DRAIN_TIMEOUT";
const OFFLINE_JOB_TIMEOUT_ENV: &str = "SMALUX_OFFLINE_JOB_TIMEOUT";
const SCHEDULER_GLOBAL_CONCURRENCY_ENV: &str = "SMALUX_SCHEDULER_GLOBAL_CONCURRENCY";
const SCHEDULER_GLOBAL_MAX_PENDING_ENV: &str = "SMALUX_SCHEDULER_GLOBAL_MAX_PENDING";
const SCHEDULER_DEFAULT_JOB_CONCURRENCY_ENV: &str = "SMALUX_SCHEDULER_DEFAULT_JOB_CONCURRENCY";
const SCHEDULER_DEFAULT_JOB_MAX_PENDING_ENV: &str = "SMALUX_SCHEDULER_DEFAULT_JOB_MAX_PENDING";
const SCHEDULER_MAX_JOBS_ENV: &str = "SMALUX_SCHEDULER_MAX_JOBS";
const SCHEDULER_SHUTDOWN_TIMEOUT_ENV: &str = "SMALUX_SCHEDULER_SHUTDOWN_TIMEOUT";
const PLUGIN_DIRECTORY_ENV: &str = "SMALUX_PLUGIN_DIRECTORY";
const PLUGIN_MAX_WORKERS_ENV: &str = "SMALUX_PLUGIN_MAX_WORKERS";
const PLUGIN_MAX_CONCURRENCY_ENV: &str = "SMALUX_PLUGIN_MAX_CONCURRENCY";
const PLUGIN_TASK_TIMEOUT_ENV: &str = "SMALUX_PLUGIN_TASK_TIMEOUT";
const PLUGIN_SHUTDOWN_TIMEOUT_ENV: &str = "SMALUX_PLUGIN_SHUTDOWN_TIMEOUT";
const PLUGIN_STARTUP_TIMEOUT_ENV: &str = "SMALUX_PLUGIN_STARTUP_TIMEOUT";
const PLUGIN_RESTART_MAX_ATTEMPTS_ENV: &str = "SMALUX_PLUGIN_RESTART_MAX_ATTEMPTS";
const PLUGIN_RESTART_WINDOW_ENV: &str = "SMALUX_PLUGIN_RESTART_WINDOW";
const DEFAULT_TASK_REPORT_BUFFER_CAPACITY: usize = 1_024;
const DEFAULT_JOB_RESULT_BUFFER_CAPACITY: usize = 1_024;
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_OFFLINE_JOB_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DEFAULT_PLUGIN_MAX_WORKERS: usize = 16;
const DEFAULT_PLUGIN_MAX_CONCURRENCY: usize = 4;
const DEFAULT_PLUGIN_TASK_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const DEFAULT_PLUGIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PLUGIN_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_PLUGIN_RESTART_MAX_ATTEMPTS: usize = 3;
const DEFAULT_PLUGIN_RESTART_WINDOW: Duration = Duration::from_secs(10 * 60);

/// Smalux Agent 顶层命令。
#[derive(Parser)]
#[command(name = "smalux-agent", version, about = "Smalux host monitoring Agent")]
pub struct Cli {
    /// 本地管理 IPC 地址。
    ///
    /// 默认值：Windows 为 `\\.\pipe\smalux-agent`；Linux/macOS 为
    /// `<data_dir>/agent/control.sock`。该端点不监听 TCP。
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        default_value_os_t = default_control_endpoint_value()
    )]
    pub control_endpoint: PathBuf,

    #[command(subcommand)]
    pub command: CliCommand,
}

/// Agent 支持的启动和只读管理命令。
#[derive(Subcommand)]
pub enum CliCommand {
    /// 启动常驻 Agent、连接 Server 并运行 Scheduler。
    Run(Box<RunArgs>),
    /// 查询正在运行的 Agent 整体状态。
    Status(StatusArgs),
    /// 查询正在运行的 Job 和本地远程 Job 策略。
    Jobs {
        #[command(subcommand)]
        command: JobsCommand,
    },
    /// 列出当前 Agent 二进制支持的 Task 能力。
    Tasks {
        #[command(subcommand)]
        command: TasksCommand,
    },
    /// 查询 Plus 插件；插件运行时未接入时会返回明确状态。
    Plugins {
        #[command(subcommand)]
        command: PluginsCommand,
    },
    /// 查询正在运行的 Agent 最终生效配置。
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// 离线读取 Agent 持久化身份摘要。
    Identity {
        #[command(subcommand)]
        command: IdentityCommand,
    },
}

/// `run` 的连接、心跳、状态文件和远程 Job 策略参数。
#[derive(Args)]
pub struct RunArgs {
    /// Server 的 HTTP(S) 基础地址，例如 `https://smalux.example.com`。
    #[arg(long, value_name = "URL")]
    pub server_endpoint: Option<String>,
    /// 反向代理暴露 gRPC 时使用的路径前缀，例如 `/api/v1/grpc`。
    #[arg(long, value_name = "PATH", conflicts_with = "no_grpc_prefix")]
    pub grpc_prefix: Option<String>,
    /// 不使用 gRPC 路径前缀，直接访问标准 service path。
    #[arg(long, conflicts_with = "grpc_prefix")]
    pub no_grpc_prefix: bool,
    /// 一次性注册 Token。它可能出现在命令历史和本机进程列表中。
    #[arg(long, value_name = "TOKEN", conflicts_with = "token_file")]
    pub token: Option<String>,
    /// 从 UTF-8 文件读取一次性注册 Token；只去除文件末尾的 CR/LF。
    #[arg(long, value_name = "PATH", conflicts_with = "token")]
    pub token_file: Option<PathBuf>,
    /// Agent 长期身份和 Server 公钥的本地状态文件。
    #[arg(long, value_name = "PATH")]
    pub state_file: Option<PathBuf>,
    /// 手工安装 Plus Worker 的根目录，布局为 `<plugin_id>/<version>/plugin.json`。
    #[arg(long, value_name = "PATH")]
    pub plugin_directory: Option<PathBuf>,
    /// Agent 本地允许同时运行的 Plus Worker 数量。
    #[arg(long, value_parser = parse_positive_usize)]
    pub plugin_max_workers: Option<usize>,
    /// Agent 本地允许的单 Worker 最大并发。
    #[arg(long, value_parser = parse_positive_usize)]
    pub plugin_max_concurrency: Option<usize>,
    /// Plus Task 的本地执行超时。
    #[arg(long, value_parser = parse_nonzero_duration)]
    pub plugin_task_timeout: Option<Duration>,
    /// 关闭旧 Worker 的最长等待时间。
    #[arg(long, value_parser = parse_nonzero_duration)]
    pub plugin_shutdown_timeout: Option<Duration>,
    /// Plus Worker 启动 Hello/Initialize 握手的最长等待时间。
    #[arg(long, value_parser = parse_nonzero_duration)]
    pub plugin_startup_timeout: Option<Duration>,
    /// Plus Worker 在失败窗口内允许的最大启动/运行失败次数。
    #[arg(long, value_parser = parse_positive_usize)]
    pub plugin_restart_max_attempts: Option<usize>,
    /// Plus Worker 失败次数统计窗口。
    #[arg(long, value_parser = parse_nonzero_duration)]
    pub plugin_restart_window: Option<Duration>,
    /// Noise 注册、认证和恢复握手的单次超时，例如 `5s`。
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub handshake_timeout: Option<Duration>,
    /// 已认证会话自动发送心跳的间隔，例如 `30s`。
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub heartbeat_interval: Option<Duration>,
    /// 未收到有效响应时判定会话失活的时间，必须大于心跳间隔。
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub heartbeat_timeout: Option<Duration>,
    /// 首次重连前的退避时间，例如 `1s`。
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub reconnect_initial_delay: Option<Duration>,
    /// 指数退避允许增长到的最大重连等待时间，例如 `30s`。
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub reconnect_max_delay: Option<Duration>,
    /// 断线期间最多在内存中保留的 TaskReport 数量；满载时丢弃最旧报告。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub task_report_buffer_capacity: Option<usize>,
    /// 断线期间最多保留的 JobCommandResult 数量；满载时丢弃最旧结果。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub job_result_buffer_capacity: Option<usize>,
    /// 关闭时等待内存报告发送完成的最长时间，例如 `5s`。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub shutdown_drain_timeout: Option<Duration>,
    /// 链路持续断开多久后清空远程 Job，并等待 Server 重连后重新下发。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub offline_job_timeout: Option<Duration>,
    /// 所有 Job 合计允许同时运行的最大 Task 数量。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub scheduler_global_concurrency: Option<usize>,
    /// Scheduler 全局 ReadyQueue 的 Pending 上限。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub scheduler_global_max_pending: Option<usize>,
    /// 未单独配置时，每个 Job 允许的并发 Task 数量。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub scheduler_default_job_concurrency: Option<usize>,
    /// 未单独配置时，每个 Job 的 Pending 上限。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub scheduler_default_job_max_pending: Option<usize>,
    /// Scheduler 可注册的最大 Job 数量。
    #[arg(long, value_parser = parse_positive_usize, value_name = "COUNT")]
    pub scheduler_max_jobs: Option<usize>,
    /// Scheduler 关闭时等待正在执行的 Task 退出的最长时间。
    #[arg(long, value_parser = parse_nonzero_duration, value_name = "DURATION")]
    pub scheduler_shutdown_timeout: Option<Duration>,
}

/// 已完成环境变量和 CLI 合并的运行输入。
pub struct RunConfiguration {
    pub client: SmaluxClientConfig,
    pub state_file: PathBuf,
    pub control_endpoint: PathBuf,
    pub policy_file: PathBuf,
    pub plugin_directory: PathBuf,
    pub plugin_max_workers: usize,
    pub plugin_max_concurrency: usize,
    pub plugin_task_timeout: Duration,
    pub plugin_shutdown_timeout: Duration,
    pub plugin_startup_timeout: Duration,
    pub plugin_restart_max_attempts: usize,
    pub plugin_restart_window: Duration,
    pub task_report_buffer_capacity: usize,
    pub job_result_buffer_capacity: usize,
    pub shutdown_drain_timeout: Duration,
    pub offline_job_timeout: Duration,
    pub scheduler: SchedulerConfig,
}

impl RunArgs {
    /// 在环境配置上应用 CLI 覆盖，并执行一次最终联合校验。
    pub fn resolve(self, control_endpoint: PathBuf) -> anyhow::Result<RunConfiguration> {
        let mut client = SmaluxClientConfig::from_env()?;
        if let Some(endpoint) = self.server_endpoint {
            client.endpoint = endpoint;
        }
        if self.no_grpc_prefix {
            client.set_grpc_prefix(None);
        } else if let Some(prefix) = self.grpc_prefix {
            client.set_grpc_prefix(Some(prefix));
        }
        if let Some(token) = resolve_cli_token(self.token, self.token_file)? {
            client.set_registration_token(Some(token));
        }
        if let Some(value) = self.handshake_timeout {
            client.handshake_timeout = value;
        }
        if let Some(value) = self.heartbeat_interval {
            client.heartbeat.interval = value;
        }
        if let Some(value) = self.heartbeat_timeout {
            client.heartbeat.timeout = value;
        }
        if let Some(value) = self.reconnect_initial_delay {
            client.reconnect.initial_delay = value;
        }
        if let Some(value) = self.reconnect_max_delay {
            client.reconnect.max_delay = value;
        }
        client.validate()?;

        let mut scheduler = SchedulerConfig::default();
        scheduler.global_concurrency = resolve_nonzero_usize(
            self.scheduler_global_concurrency,
            SCHEDULER_GLOBAL_CONCURRENCY_ENV,
            scheduler.global_concurrency,
        )?;
        scheduler.global_max_pending = resolve_usize(
            self.scheduler_global_max_pending,
            SCHEDULER_GLOBAL_MAX_PENDING_ENV,
            scheduler.global_max_pending,
        )?;
        scheduler.default_job_concurrency = resolve_nonzero_usize(
            self.scheduler_default_job_concurrency,
            SCHEDULER_DEFAULT_JOB_CONCURRENCY_ENV,
            scheduler.default_job_concurrency,
        )?;
        scheduler.default_job_max_pending = resolve_usize(
            self.scheduler_default_job_max_pending,
            SCHEDULER_DEFAULT_JOB_MAX_PENDING_ENV,
            scheduler.default_job_max_pending,
        )?;
        scheduler.max_jobs = resolve_usize(
            self.scheduler_max_jobs,
            SCHEDULER_MAX_JOBS_ENV,
            scheduler.max_jobs,
        )?;
        scheduler.shutdown_timeout = resolve_duration(
            self.scheduler_shutdown_timeout,
            SCHEDULER_SHUTDOWN_TIMEOUT_ENV,
            scheduler.shutdown_timeout,
        )?;

        let task_report_buffer_capacity = resolve_usize(
            self.task_report_buffer_capacity,
            TASK_REPORT_BUFFER_CAPACITY_ENV,
            DEFAULT_TASK_REPORT_BUFFER_CAPACITY,
        )?;
        let job_result_buffer_capacity = resolve_usize(
            self.job_result_buffer_capacity,
            JOB_RESULT_BUFFER_CAPACITY_ENV,
            DEFAULT_JOB_RESULT_BUFFER_CAPACITY,
        )?;
        let shutdown_drain_timeout = resolve_duration(
            self.shutdown_drain_timeout,
            SHUTDOWN_DRAIN_TIMEOUT_ENV,
            DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
        )?;
        let offline_job_timeout = resolve_duration(
            self.offline_job_timeout,
            OFFLINE_JOB_TIMEOUT_ENV,
            DEFAULT_OFFLINE_JOB_TIMEOUT,
        )?;

        let state_file = self.state_file.unwrap_or(default_state_file()?);
        let plugin_directory = resolve_path(
            self.plugin_directory,
            PLUGIN_DIRECTORY_ENV,
            default_plugin_directory()?,
        )?;
        let plugin_max_workers = resolve_usize(
            self.plugin_max_workers,
            PLUGIN_MAX_WORKERS_ENV,
            DEFAULT_PLUGIN_MAX_WORKERS,
        )?;
        let plugin_max_concurrency = resolve_usize(
            self.plugin_max_concurrency,
            PLUGIN_MAX_CONCURRENCY_ENV,
            DEFAULT_PLUGIN_MAX_CONCURRENCY,
        )?;
        let plugin_task_timeout = resolve_duration(
            self.plugin_task_timeout,
            PLUGIN_TASK_TIMEOUT_ENV,
            DEFAULT_PLUGIN_TASK_TIMEOUT,
        )?;
        let plugin_shutdown_timeout = resolve_duration(
            self.plugin_shutdown_timeout,
            PLUGIN_SHUTDOWN_TIMEOUT_ENV,
            DEFAULT_PLUGIN_SHUTDOWN_TIMEOUT,
        )?;
        let plugin_startup_timeout = resolve_duration(
            self.plugin_startup_timeout,
            PLUGIN_STARTUP_TIMEOUT_ENV,
            DEFAULT_PLUGIN_STARTUP_TIMEOUT,
        )?;
        let plugin_restart_max_attempts = resolve_usize(
            self.plugin_restart_max_attempts,
            PLUGIN_RESTART_MAX_ATTEMPTS_ENV,
            DEFAULT_PLUGIN_RESTART_MAX_ATTEMPTS,
        )?;
        let plugin_restart_window = resolve_duration(
            self.plugin_restart_window,
            PLUGIN_RESTART_WINDOW_ENV,
            DEFAULT_PLUGIN_RESTART_WINDOW,
        )?;
        Ok(RunConfiguration {
            client,
            state_file,
            control_endpoint,
            policy_file: default_policy_file()?,
            plugin_directory,
            plugin_max_workers,
            plugin_max_concurrency,
            plugin_task_timeout,
            plugin_shutdown_timeout,
            plugin_startup_timeout,
            plugin_restart_max_attempts,
            plugin_restart_window,
            task_report_buffer_capacity,
            job_result_buffer_capacity,
            shutdown_drain_timeout,
            offline_job_timeout,
            scheduler,
        })
    }
}

#[derive(Args)]
pub struct StatusArgs {
    /// 持续刷新状态，直到按下 Ctrl+C。
    #[arg(long)]
    pub watch: bool,
    /// `--watch` 模式的刷新间隔。
    #[arg(long, default_value = "2s", value_parser = parse_duration)]
    pub interval: Duration,
    /// 输出人类可读表格或稳定 JSON。
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Subcommand)]
pub enum JobsCommand {
    List(JobListArgs),
    Show {
        job_id: Uuid,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
    Policy {
        #[command(subcommand)]
        command: JobPolicyCommand,
    },
}

/// 只通过本地 IPC 修改的远程 Job 安全策略。
#[derive(Subcommand)]
pub enum JobPolicyCommand {
    Show(OutputArgs),
    AddTask {
        task_kind: String,
        #[command(flatten)]
        output: OutputArgs,
    },
    RemoveTask {
        task_kind: String,
        #[command(flatten)]
        output: OutputArgs,
    },
    DenyAll(OutputArgs),
    AllowAll(OutputArgs),
    /// Agent 无法启动时，离线备份损坏文件并重置为空策略。
    Repair {
        #[arg(long, required = true)]
        reset: bool,
    },
}

#[derive(Args)]
pub struct OutputArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Args)]
pub struct JobListArgs {
    /// 只展示处于指定生命周期状态的 Job。
    #[arg(long, value_enum)]
    pub state: Option<JobStateFilter>,
    /// 只展示当前至少有一次执行正在运行的 Job。
    #[arg(long)]
    pub running: bool,
    /// 只展示使用指定稳定 Task kind 的 Job。
    #[arg(long)]
    pub task_kind: Option<String>,
    /// 输出人类可读表格或稳定 JSON。
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum JobStateFilter {
    Enabled,
    Completed,
    Disabled,
}

#[derive(Subcommand)]
pub enum TasksCommand {
    List {
        #[arg(long, value_enum)]
        source: Option<TaskSourceArg>,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
    Show {
        task_kind: String,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub enum TaskSourceArg {
    Builtin,
    Plugin,
}

impl TaskSourceArg {
    pub fn matches(self, source: TaskSource) -> bool {
        matches!(
            (self, source),
            (Self::Builtin, TaskSource::Builtin) | (Self::Plugin, TaskSource::Plugin)
        )
    }
}

#[derive(Subcommand)]
pub enum PluginsCommand {
    List {
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
    Show {
        plugin_id: String,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[derive(Subcommand)]
pub enum ConfigCommand {
    Show {
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[derive(Subcommand)]
pub enum IdentityCommand {
    Show {
        #[arg(long)]
        state_file: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
}

/// 解析进程参数；Clap 会在格式错误时生成包含默认 IPC 说明的帮助。
pub fn parse() -> Cli {
    Cli::parse()
}

fn default_control_endpoint_value() -> PathBuf {
    if let Some(value) = env::var_os(CONTROL_ENDPOINT_ENV).filter(|value| !value.is_empty()) {
        return PathBuf::from(value);
    }
    default_control_endpoint().unwrap_or_else(|error| {
        panic!("failed to determine the default Agent control endpoint: {error}")
    })
}

pub fn default_state_file() -> anyhow::Result<PathBuf> {
    Ok(smalux_core::config::data_dir()?
        .join("agent")
        .join("connection-state.json"))
}

pub fn default_policy_file() -> anyhow::Result<PathBuf> {
    Ok(smalux_core::config::data_dir()?
        .join("agent")
        .join("job-policy.json"))
}

/// 手工安装的 Plus Worker 默认位于 Agent 数据目录下，不和身份、策略文件混放。
pub fn default_plugin_directory() -> anyhow::Result<PathBuf> {
    Ok(smalux_core::config::data_dir()?
        .join("agent")
        .join("plugins"))
}

fn default_control_endpoint() -> anyhow::Result<PathBuf> {
    #[cfg(windows)]
    {
        Ok(PathBuf::from(r"\\.\pipe\smalux-agent"))
    }
    #[cfg(unix)]
    {
        Ok(smalux_core::config::data_dir()?
            .join("agent")
            .join("control.sock"))
    }
}

fn resolve_cli_token(
    token: Option<String>,
    token_file: Option<PathBuf>,
) -> anyhow::Result<Option<RegistrationToken>> {
    let value = match (token, token_file) {
        (Some(value), None) => Some(value),
        (None, Some(path)) => {
            let value = std::fs::read_to_string(&path).map_err(|error| {
                anyhow::anyhow!(
                    "failed to read registration Token file {}: {error}",
                    path.display()
                )
            })?;
            Some(value.trim_end_matches(['\r', '\n']).to_owned())
        }
        (None, None) => None,
        (Some(_), Some(_)) => anyhow::bail!("--token and --token-file are mutually exclusive"),
    };
    value.map(RegistrationToken::new).transpose()
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

fn parse_nonzero_duration(value: &str) -> Result<Duration, String> {
    let duration = parse_duration(value)?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(duration)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    if value == 0 {
        return Err("value must be greater than zero".to_owned());
    }
    Ok(value)
}

fn resolve_nonzero_usize(
    cli_value: Option<usize>,
    env_name: &str,
    default: NonZeroUsize,
) -> anyhow::Result<NonZeroUsize> {
    NonZeroUsize::new(resolve_usize(cli_value, env_name, default.get())?)
        .ok_or_else(|| anyhow::anyhow!("{env_name} must be greater than zero"))
}

fn resolve_usize(
    cli_value: Option<usize>,
    env_name: &str,
    default: usize,
) -> anyhow::Result<usize> {
    let value = match cli_value {
        Some(value) => value,
        None => match env::var(env_name) {
            Ok(value) => value
                .parse::<usize>()
                .map_err(|error| anyhow::anyhow!("invalid {env_name} value `{value}`: {error}"))?,
            Err(env::VarError::NotPresent) => default,
            Err(error) => return Err(anyhow::anyhow!("failed to read {env_name}: {error}")),
        },
    };
    anyhow::ensure!(value > 0, "{env_name} must be greater than zero");
    Ok(value)
}

fn resolve_path(
    cli_value: Option<PathBuf>,
    env_name: &str,
    default: PathBuf,
) -> anyhow::Result<PathBuf> {
    match cli_value {
        Some(value) => Ok(value),
        None => match env::var_os(env_name) {
            Some(value) if !value.is_empty() => Ok(PathBuf::from(value)),
            Some(_) => anyhow::bail!("{env_name} must not be empty"),
            None => Ok(default),
        },
    }
}

fn resolve_duration(
    cli_value: Option<Duration>,
    env_name: &str,
    default: Duration,
) -> anyhow::Result<Duration> {
    let value = match cli_value {
        Some(value) => value,
        None => match env::var(env_name) {
            Ok(value) => humantime::parse_duration(&value)
                .map_err(|error| anyhow::anyhow!("invalid {env_name} value `{value}`: {error}"))?,
            Err(env::VarError::NotPresent) => default,
            Err(error) => return Err(anyhow::anyhow!("failed to read {env_name}: {error}")),
        },
    };
    anyhow::ensure!(!value.is_zero(), "{env_name} must be greater than zero");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::PathBuf, time::Duration};

    use clap::Parser;

    use super::{Cli, CliCommand, JobsCommand, default_control_endpoint_value, resolve_cli_token};

    #[test]
    fn run_parses_connection_arguments_without_static_policy() {
        let cli = Cli::try_parse_from([
            "smalux-agent",
            "run",
            "--server-endpoint",
            "http://127.0.0.1:12345",
            "--token",
            "temporary-token",
            "--heartbeat-interval",
            "30s",
        ])
        .unwrap();
        let CliCommand::Run(args) = cli.command else {
            panic!("run command")
        };
        assert_eq!(
            args.server_endpoint.as_deref(),
            Some("http://127.0.0.1:12345")
        );
    }

    #[test]
    fn run_resolves_report_shutdown_and_scheduler_limits() {
        let cli = Cli::try_parse_from([
            "smalux-agent",
            "run",
            "--task-report-buffer-capacity",
            "64",
            "--shutdown-drain-timeout",
            "3s",
            "--offline-job-timeout",
            "45m",
            "--scheduler-global-concurrency",
            "8",
            "--scheduler-global-max-pending",
            "512",
            "--scheduler-default-job-concurrency",
            "2",
            "--scheduler-default-job-max-pending",
            "32",
            "--scheduler-max-jobs",
            "128",
            "--scheduler-shutdown-timeout",
            "20s",
        ])
        .unwrap();
        let CliCommand::Run(args) = cli.command else {
            panic!("run command")
        };

        let configuration = args.resolve(cli.control_endpoint).unwrap();

        assert_eq!(configuration.task_report_buffer_capacity, 64);
        assert_eq!(configuration.shutdown_drain_timeout, Duration::from_secs(3));
        assert_eq!(
            configuration.offline_job_timeout,
            Duration::from_secs(45 * 60)
        );
        assert_eq!(configuration.scheduler.global_concurrency.get(), 8);
        assert_eq!(configuration.scheduler.global_max_pending, 512);
        assert_eq!(configuration.scheduler.default_job_concurrency.get(), 2);
        assert_eq!(configuration.scheduler.default_job_max_pending, 32);
        assert_eq!(configuration.scheduler.max_jobs, 128);
        assert_eq!(
            configuration.scheduler.shutdown_timeout,
            Duration::from_secs(20)
        );
    }

    #[test]
    fn run_resolves_local_plugin_limits() {
        let cli = Cli::try_parse_from([
            "smalux-agent",
            "run",
            "--plugin-max-workers",
            "3",
            "--plugin-max-concurrency",
            "2",
            "--plugin-task-timeout",
            "10s",
            "--plugin-shutdown-timeout",
            "1s",
            "--plugin-restart-max-attempts",
            "5",
            "--plugin-restart-window",
            "20m",
        ])
        .unwrap();
        let CliCommand::Run(args) = cli.command else {
            panic!("run command")
        };
        let config = args.resolve(default_control_endpoint_value()).unwrap();
        assert_eq!(config.plugin_max_workers, 3);
        assert_eq!(config.plugin_max_concurrency, 2);
        assert_eq!(config.plugin_task_timeout, Duration::from_secs(10));
        assert_eq!(config.plugin_shutdown_timeout, Duration::from_secs(1));
        assert_eq!(config.plugin_restart_max_attempts, 5);
        assert_eq!(config.plugin_restart_window, Duration::from_secs(20 * 60));
    }

    #[test]
    fn run_resolves_job_result_buffer_and_plugin_startup_timeout() {
        let cli = Cli::try_parse_from([
            "smalux-agent",
            "run",
            "--job-result-buffer-capacity",
            "12",
            "--plugin-startup-timeout",
            "7s",
        ])
        .unwrap();
        let CliCommand::Run(args) = cli.command else {
            panic!("run command")
        };
        let config = args.resolve(default_control_endpoint_value()).unwrap();
        assert_eq!(config.job_result_buffer_capacity, 12);
        assert_eq!(config.plugin_startup_timeout, Duration::from_secs(7));
    }

    #[test]
    fn token_sources_and_grpc_prefix_modes_are_mutually_exclusive() {
        assert!(
            Cli::try_parse_from([
                "smalux-agent",
                "run",
                "--token",
                "x",
                "--token-file",
                "token.txt"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "smalux-agent",
                "run",
                "--grpc-prefix",
                "/grpc",
                "--no-grpc-prefix"
            ])
            .is_err()
        );
    }

    #[test]
    fn removed_job_id_blacklist_argument_is_rejected() {
        assert!(
            Cli::try_parse_from([
                "smalux-agent",
                "run",
                "--deny-job-id",
                "550e8400-e29b-41d4-a716-446655440000",
            ])
            .is_err()
        );
    }

    #[test]
    fn jobs_policy_has_an_explicit_command_shape() {
        let cli = Cli::try_parse_from(["smalux-agent", "jobs", "policy", "show"]).unwrap();
        assert!(matches!(
            cli.command,
            CliCommand::Jobs {
                command: JobsCommand::Policy { .. }
            }
        ));
    }

    #[test]
    fn jobs_policy_mutations_have_explicit_subcommands() {
        let cli = Cli::try_parse_from([
            "smalux-agent",
            "jobs",
            "policy",
            "add-task",
            "smalux.collect.cpu.v1",
            "--output",
            "json",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            CliCommand::Jobs {
                command: JobsCommand::Policy {
                    command: super::JobPolicyCommand::AddTask { .. }
                }
            }
        ));
    }

    #[test]
    fn static_policy_arguments_are_rejected() {
        assert!(Cli::try_parse_from(["smalux-agent", "run", "--deny-remote-jobs"]).is_err());
        assert!(Cli::try_parse_from(["smalux-agent", "run", "--deny-task-kind", "x"]).is_err());
    }

    #[test]
    fn help_documents_the_platform_default_control_endpoint() {
        let error = Cli::try_parse_from(["smalux-agent", "--help"])
            .err()
            .expect("help exits through Clap");
        let help = error.to_string();
        assert!(help.contains("smalux-agent"));
        assert!(help.contains("control.sock") || help.contains(r"\\.\pipe\smalux-agent"));
    }

    #[test]
    fn control_endpoint_has_a_parsed_default_and_accepts_an_explicit_override() {
        let default_cli = Cli::try_parse_from(["smalux-agent", "status"]).unwrap();
        assert_eq!(
            default_cli.control_endpoint,
            default_control_endpoint_value()
        );

        let custom = PathBuf::from("custom-control-endpoint");
        let custom_cli = Cli::try_parse_from([
            "smalux-agent",
            "--control-endpoint",
            custom.to_str().unwrap(),
            "status",
        ])
        .unwrap();
        assert_eq!(custom_cli.control_endpoint, custom);
    }

    #[test]
    fn token_file_removes_only_trailing_line_endings() {
        let path =
            std::env::temp_dir().join(format!("smalux-agent-token-{}.txt", uuid::Uuid::new_v4()));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"token-id.secret\r\n").unwrap();
        drop(file);

        let token = resolve_cli_token(None, Some(path.clone()))
            .unwrap()
            .unwrap();
        assert_eq!(format!("{token:?}"), "RegistrationToken(<redacted>)");
        std::fs::remove_file(path).unwrap();
    }
}
