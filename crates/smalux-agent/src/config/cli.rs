//! Agent 启动参数解析。

use super::model::{
    AgentConfig, AgentConfigPatch, DiskConfigPatch, ExportAuthMode, ExportConfigPatch,
    ExportFormat, ExportWireMode, GroupConfigPatch, JobConfigPatch, JobsConfigPatch,
    NetworkConfigPatch, ProcessConfigPatch, PublicIpConfigPatch, RemoteProbeConfigPatch,
    RemoteShellConfigPatch, RemoteTaskConfigPatch, ReportConfigPatch, SocketConfigPatch,
};
use crate::service::ServiceOptions;
use clap::Parser;
use clap::builder::PossibleValue;
use smalux_core::model::info::MetricLevel;
use std::collections::BTreeMap;
use std::time::Duration;

/// 解析人类可读时间，例如 `1s`、`5m`、`24h`。
fn parse_duration(value: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(value)
}

/// 解析 `KEY=VALUE` 形式的额外 query 参数。
fn parse_query_param(value: &str) -> Result<CliQueryParam, String> {
    let Some((key, query_value)) = value.split_once('=') else {
        return Err("query parameter must be KEY=VALUE".to_string());
    };
    if key.trim().is_empty() {
        return Err("query parameter key cannot be empty".to_string());
    }

    Ok(CliQueryParam {
        key: key.to_string(),
        value: query_value.to_string(),
    })
}

/// CLI 额外 query 参数。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct CliQueryParam {
    /// query 参数名。
    key: String,
    /// query 参数值。
    value: String,
}

/// CLI 解析后的 agent 启动输入。
#[derive(Debug, Clone)]
pub(crate) struct AgentStartup {
    /// 动态运行配置。
    pub(crate) config: AgentConfig,
    /// CLI-only 服务静态选项。
    pub(crate) service_options: ServiceOptions,
}

/// smalux-agent 启动参数。
#[derive(Debug, Clone, Parser, Default)]
#[command(author, version, about = "smalux agent")]
pub(crate) struct CliArgs {
    /// agent 实例 ID。
    #[arg(short = 'i', long)]
    pub agent_id: Option<String>,
    /// 日志文件前缀，例如 `logs/smalux-agent.log`。
    #[arg(short = 'l', long)]
    pub log_file: Option<String>,
    /// 保留的滚动日志文件数，必须大于 0。
    #[arg(short = 'L', long)]
    pub log_retention_files: Option<usize>,
    /// 单个日志文件最大大小，单位 MB。
    #[arg(long)]
    pub log_max_size_mb: Option<u64>,

    /// server WebSocket 地址。
    #[arg(short = 's', long)]
    pub server_url: Option<String>,
    /// 导出数据编码格式：smalux_json/komari。
    #[arg(short = 'f', long = "format", value_enum)]
    pub format: Option<CliExportFormat>,
    /// Smalux 自有协议 wire 模式：binary_plain/secure_psk。
    #[arg(long = "wire-mode", value_enum)]
    pub wire_mode: Option<CliWireMode>,
    /// 是否要求 Smalux 自有协议必须启用安全通道。
    #[arg(long)]
    pub secure_required: Option<bool>,
    /// 认证 token。
    #[arg(short = 't', long)]
    pub token: Option<String>,
    /// 认证方式：none/query/bearer。
    #[arg(short = 'a', long, value_enum)]
    pub auth: Option<CliAuthMode>,
    /// query token 参数名。
    #[arg(short = 'k', long)]
    pub query_token_param: Option<String>,
    /// 额外 query 参数，可重复传入，例如 `--query agent_id=a1 --query region=local`。
    #[arg(short = 'q', long = "query", value_name = "KEY=VALUE", value_parser = parse_query_param)]
    pub query: Vec<CliQueryParam>,
    /// 跳过 TLS 证书校验。
    #[arg(short = 'u', long)]
    pub unsafe_cert: Option<bool>,
    /// WebSocket 心跳间隔，例如 `30s`。
    #[arg(short = 'H', long, value_parser = parse_duration)]
    pub heartbeat: Option<Duration>,
    /// WebSocket 断线或连接失败后的重连间隔，例如 `5s`。
    #[arg(short = 'r', long, value_parser = parse_duration)]
    pub reconnect_interval: Option<Duration>,

    /// 是否启用核心指标。
    #[arg(long)]
    pub core_enabled: Option<bool>,
    /// 核心指标采样间隔，例如 `1s`。
    #[arg(short = 'c', long, value_parser = parse_duration)]
    pub core_interval: Option<Duration>,

    /// 是否启用磁盘指标。
    #[arg(long)]
    pub disk_enabled: Option<bool>,
    /// 磁盘指标采样间隔，例如 `5s`。
    #[arg(short = 'd', long, value_parser = parse_duration)]
    pub disk_interval: Option<Duration>,
    /// 是否上报单磁盘明细。
    #[arg(long)]
    pub disk_per_device: Option<bool>,

    /// 是否启用网络指标。
    #[arg(long)]
    pub network_enabled: Option<bool>,
    /// 网络指标采样间隔，例如 `5s`。
    #[arg(short = 'n', long, value_parser = parse_duration)]
    pub network_interval: Option<Duration>,
    /// 是否上报单网卡明细。
    #[arg(long)]
    pub network_per_interface: Option<bool>,
    /// 只统计指定网卡；可重复传入，未传表示全部网卡。
    #[arg(short = 'I', long = "network-interface", value_name = "NAME")]
    pub network_interfaces: Vec<String>,
    /// 排除指定网卡；include 列表非空时忽略，可重复传入。
    #[arg(long = "network-exclude-interface", value_name = "NAME")]
    pub network_exclude_interfaces: Vec<String>,

    /// 是否启用进程汇总指标。
    #[arg(long)]
    pub processes_enabled: Option<bool>,
    /// 进程汇总采样间隔，例如 `60s`。
    #[arg(long, value_parser = parse_duration)]
    pub processes_interval: Option<Duration>,
    /// 进程采集级别：count/light/details。
    #[arg(long, value_enum)]
    pub processes_level: Option<CliMetricLevel>,
    /// 进程 light/details 返回条数上限。
    #[arg(long)]
    pub processes_limit: Option<usize>,
    /// 是否允许 server 触发进程 details 诊断采集。
    #[arg(long)]
    pub allow_process_details: Option<bool>,

    /// 是否启用 Socket 汇总指标。
    #[arg(long)]
    pub sockets_enabled: Option<bool>,
    /// Socket 汇总采样间隔，例如 `60s`。
    #[arg(long, value_parser = parse_duration)]
    pub sockets_interval: Option<Duration>,
    /// Socket 采集级别：count/light/details。
    #[arg(long, value_enum)]
    pub sockets_level: Option<CliMetricLevel>,
    /// Socket details 返回条数上限。
    #[arg(long)]
    pub sockets_limit: Option<usize>,
    /// 是否允许 server 触发 Socket details 诊断采集。
    #[arg(long)]
    pub allow_socket_details: Option<bool>,

    /// 是否启用公网 IP 采集。
    #[arg(long)]
    pub public_ip_enabled: Option<bool>,
    /// 第一包是否必须等待公网 IP。
    #[arg(long)]
    pub public_ip_required: Option<bool>,
    /// 是否优先使用网卡公网候选地址。
    #[arg(long)]
    pub public_ip_prefer_interface: Option<bool>,
    /// 是否校验网卡公网候选地址。
    #[arg(long)]
    pub public_ip_verify_interface: Option<bool>,
    /// 公网 IP 启动探测超时，例如 `3s`。
    #[arg(long, value_parser = parse_duration)]
    pub public_ip_startup_timeout: Option<Duration>,
    /// 公网 IP 失败重试间隔，例如 `30s`。
    #[arg(long, value_parser = parse_duration)]
    pub public_ip_retry_interval: Option<Duration>,
    /// 公网 IP 低频刷新间隔，例如 `24h`。
    #[arg(short = 'p', long, value_parser = parse_duration)]
    pub public_ip_refresh_interval: Option<Duration>,
    /// 公网 IP 外部服务最大并发。
    #[arg(long)]
    pub public_ip_max_concurrency: Option<usize>,

    /// 是否启用上报。
    #[arg(long)]
    pub report_enabled: Option<bool>,
    /// 上报间隔，例如 `5s`。
    #[arg(short = 'R', long, value_parser = parse_duration)]
    pub report_interval: Option<Duration>,
    /// 是否启用业务级心跳。
    #[arg(long)]
    pub report_heartbeat_enabled: Option<bool>,
    /// 业务级心跳间隔，例如 `30s`。
    #[arg(long, value_parser = parse_duration)]
    pub report_heartbeat_interval: Option<Duration>,
    /// 是否启用 delta 增量上报。
    #[arg(long)]
    pub report_delta_enabled: Option<bool>,
    /// 启用 delta 后，强制定期发送完整 snapshot 的间隔，例如 `5m`。
    #[arg(long, value_parser = parse_duration)]
    pub report_snapshot_interval: Option<Duration>,
    /// server 强制 snapshot 的最小响应间隔，例如 `10s`。
    #[arg(long, value_parser = parse_duration)]
    pub report_force_snapshot_min_interval: Option<Duration>,

    /// 是否启用实时上报导出 job。
    #[arg(long)]
    pub realtime_report_enabled: Option<bool>,
    /// 实时上报导出 job 间隔；未传时 `--report-interval` 会同时作为兼容别名。
    #[arg(long, value_parser = parse_duration)]
    pub realtime_report_interval: Option<Duration>,
    /// 实时上报导出 job 是否在第一份 report ready 后立即运行。
    #[arg(long)]
    pub realtime_report_run_on_start: Option<bool>,
    /// 是否启用 basic info 导出 job。
    #[arg(long)]
    pub basic_info_enabled: Option<bool>,
    /// basic info 导出 job 间隔，例如 `5m`。
    #[arg(long, value_parser = parse_duration)]
    pub basic_info_interval: Option<Duration>,
    /// basic info 导出 job 是否在第一份 report ready 后立即运行。
    #[arg(long)]
    pub basic_info_run_on_start: Option<bool>,

    /// 是否启用远程交互式 shell；只能启动时设置，server patch 不能修改。
    #[arg(short = 'S', long)]
    pub remote_shell_enabled: Option<bool>,
    /// 最大远程 shell 并发会话数。
    #[arg(short = 'M', long)]
    pub remote_shell_max_sessions: Option<usize>,
    /// 远程 shell 空闲超时，例如 `10m`。
    #[arg(long, value_parser = parse_duration)]
    pub remote_shell_idle_timeout: Option<Duration>,
    /// 远程 shell 单会话最长运行时间，例如 `1h`。
    #[arg(long, value_parser = parse_duration)]
    pub remote_shell_session_timeout: Option<Duration>,
    /// 自定义远程 shell 程序；未传时按平台选择默认 shell。
    #[arg(short = 'P', long)]
    pub remote_shell_program: Option<String>,

    /// 是否启用远程非交互任务；只能启动时设置，server patch 不能修改。
    #[arg(short = 'T', long)]
    pub remote_task_enabled: Option<bool>,
    /// 最大远程任务并发数。
    #[arg(short = 'C', long)]
    pub remote_task_max_concurrent: Option<usize>,
    /// 远程任务默认最大运行时间，例如 `30s`。
    #[arg(long, value_parser = parse_duration)]
    pub remote_task_timeout: Option<Duration>,
    /// 远程任务 stdout 最大回传字节数。
    #[arg(long)]
    pub remote_task_max_stdout_bytes: Option<usize>,
    /// 远程任务 stderr 最大回传字节数。
    #[arg(long)]
    pub remote_task_max_stderr_bytes: Option<usize>,

    /// 是否启用远程网络探测；server 运行时也可动态开启或关闭。
    #[arg(long)]
    pub remote_probe_enabled: Option<bool>,
    /// 远程网络探测单次超时，例如 `3s`。
    #[arg(long, value_parser = parse_duration)]
    pub remote_probe_timeout: Option<Duration>,
    /// 任意两个远程探测启动之间的最小间隔，例如 `500ms`。
    #[arg(long, value_parser = parse_duration)]
    pub remote_probe_global_min_interval: Option<Duration>,
    /// 同一目标重复远程探测的最小间隔，例如 `10s`。
    #[arg(long, value_parser = parse_duration)]
    pub remote_probe_target_min_interval: Option<Duration>,
}

impl CliArgs {
    /// 从启动参数构造完整启动输入。
    pub(crate) fn into_startup(self) -> anyhow::Result<AgentStartup> {
        let service_options = self.to_service_options()?;
        let config = self.into_config()?;

        Ok(AgentStartup {
            config,
            service_options,
        })
    }

    /// 从启动参数构造初始配置。
    pub(crate) fn into_config(self) -> anyhow::Result<AgentConfig> {
        let mut config = AgentConfig::default();
        self.apply_startup_only_config(&mut config);
        let patch = self.into_patch();
        patch.apply_to(&mut config);
        super::manager::validate_config(&config)?;
        Ok(config)
    }

    /// 应用只能在启动阶段生效的配置。
    fn apply_startup_only_config(&self, config: &mut AgentConfig) {
        if let Some(log_file) = self.log_file.clone() {
            config.log_file = log_file;
        }
        if let Some(log_retention_files) = self.log_retention_files {
            config.log_retention_files = log_retention_files;
        }
        if let Some(log_max_size_mb) = self.log_max_size_mb {
            config.log_max_size_mb = log_max_size_mb;
        }
    }

    /// 从启动参数构造 CLI-only 服务静态选项。
    pub(crate) fn to_service_options(&self) -> anyhow::Result<ServiceOptions> {
        let mut options = ServiceOptions::default();
        if let Some(enabled) = self.remote_shell_enabled {
            options.remote_shell.enabled = enabled;
        }
        if let Some(allow) = self.allow_process_details {
            options.diagnostics.allow_process_details = allow;
        }
        if let Some(allow) = self.allow_socket_details {
            options.diagnostics.allow_socket_details = allow;
        }
        if let Some(enabled) = self.remote_task_enabled {
            options.remote_task.enabled = enabled;
        }

        options.validate()?;
        Ok(options)
    }

    /// 将启动参数转换为配置 patch。
    pub(crate) fn into_patch(self) -> AgentConfigPatch {
        let realtime_report_interval = self.realtime_report_interval.or(self.report_interval);
        AgentConfigPatch {
            agent_id: self.agent_id,
            core: (self.core_enabled.is_some() || self.core_interval.is_some()).then_some(
                GroupConfigPatch {
                    enabled: self.core_enabled,
                    interval: self.core_interval,
                },
            ),
            disk: (self.disk_enabled.is_some()
                || self.disk_interval.is_some()
                || self.disk_per_device.is_some())
            .then_some(DiskConfigPatch {
                enabled: self.disk_enabled,
                interval: self.disk_interval,
                include_per_device: self.disk_per_device,
            }),
            network: (self.network_enabled.is_some()
                || self.network_interval.is_some()
                || self.network_per_interface.is_some()
                || !self.network_interfaces.is_empty()
                || !self.network_exclude_interfaces.is_empty())
            .then_some(NetworkConfigPatch {
                enabled: self.network_enabled,
                interval: self.network_interval,
                include_per_interface: self.network_per_interface,
                include_interfaces: optional_non_empty_vec(self.network_interfaces),
                exclude_interfaces: optional_non_empty_vec(self.network_exclude_interfaces),
            }),
            processes: (self.processes_enabled.is_some()
                || self.processes_interval.is_some()
                || self.processes_level.is_some()
                || self.processes_limit.is_some())
            .then_some(ProcessConfigPatch {
                enabled: self.processes_enabled,
                interval: self.processes_interval,
                level: self.processes_level.map(Into::into),
                limit: self.processes_limit,
            }),
            sockets: (self.sockets_enabled.is_some()
                || self.sockets_interval.is_some()
                || self.sockets_level.is_some()
                || self.sockets_limit.is_some())
            .then_some(SocketConfigPatch {
                enabled: self.sockets_enabled,
                interval: self.sockets_interval,
                level: self.sockets_level.map(Into::into),
                limit: self.sockets_limit,
            }),
            public_ip: (self.public_ip_enabled.is_some()
                || self.public_ip_required.is_some()
                || self.public_ip_prefer_interface.is_some()
                || self.public_ip_verify_interface.is_some()
                || self.public_ip_startup_timeout.is_some()
                || self.public_ip_retry_interval.is_some()
                || self.public_ip_refresh_interval.is_some()
                || self.public_ip_max_concurrency.is_some())
            .then_some(PublicIpConfigPatch {
                enabled: self.public_ip_enabled,
                required_for_first_report: self.public_ip_required,
                prefer_interface_candidate: self.public_ip_prefer_interface,
                verify_interface_candidate: self.public_ip_verify_interface,
                startup_timeout: self.public_ip_startup_timeout,
                retry_interval: self.public_ip_retry_interval,
                refresh_interval: self.public_ip_refresh_interval,
                max_concurrency: self.public_ip_max_concurrency,
            }),
            report: (self.report_enabled.is_some()
                || self.report_interval.is_some()
                || self.report_heartbeat_enabled.is_some()
                || self.report_heartbeat_interval.is_some()
                || self.report_delta_enabled.is_some()
                || self.report_snapshot_interval.is_some()
                || self.report_force_snapshot_min_interval.is_some())
            .then_some(ReportConfigPatch {
                enabled: self.report_enabled,
                interval: self.report_interval,
                heartbeat_enabled: self.report_heartbeat_enabled,
                heartbeat_interval: self.report_heartbeat_interval,
                delta_enabled: self.report_delta_enabled,
                snapshot_interval: self.report_snapshot_interval,
                force_snapshot_min_interval: self.report_force_snapshot_min_interval,
            }),
            jobs: (self.realtime_report_enabled.is_some()
                || realtime_report_interval.is_some()
                || self.realtime_report_run_on_start.is_some()
                || self.basic_info_enabled.is_some()
                || self.basic_info_interval.is_some()
                || self.basic_info_run_on_start.is_some())
            .then_some(JobsConfigPatch {
                realtime_report: (self.realtime_report_enabled.is_some()
                    || realtime_report_interval.is_some()
                    || self.realtime_report_run_on_start.is_some())
                .then_some(JobConfigPatch {
                    enabled: self.realtime_report_enabled,
                    interval: realtime_report_interval,
                    run_on_start: self.realtime_report_run_on_start,
                }),
                basic_info: (self.basic_info_enabled.is_some()
                    || self.basic_info_interval.is_some()
                    || self.basic_info_run_on_start.is_some())
                .then_some(JobConfigPatch {
                    enabled: self.basic_info_enabled,
                    interval: self.basic_info_interval,
                    run_on_start: self.basic_info_run_on_start,
                }),
            }),
            remote_shell: (self.remote_shell_max_sessions.is_some()
                || self.remote_shell_idle_timeout.is_some()
                || self.remote_shell_session_timeout.is_some()
                || self.remote_shell_program.is_some())
            .then_some(RemoteShellConfigPatch {
                max_sessions: self.remote_shell_max_sessions,
                idle_timeout: self.remote_shell_idle_timeout,
                session_timeout: self.remote_shell_session_timeout,
                program: self.remote_shell_program.map(Some),
            }),
            remote_task: (self.remote_task_max_concurrent.is_some()
                || self.remote_task_timeout.is_some()
                || self.remote_task_max_stdout_bytes.is_some()
                || self.remote_task_max_stderr_bytes.is_some())
            .then_some(RemoteTaskConfigPatch {
                max_concurrent: self.remote_task_max_concurrent,
                timeout: self.remote_task_timeout,
                max_stdout_bytes: self.remote_task_max_stdout_bytes,
                max_stderr_bytes: self.remote_task_max_stderr_bytes,
            }),
            remote_probe: (self.remote_probe_enabled.is_some()
                || self.remote_probe_timeout.is_some()
                || self.remote_probe_global_min_interval.is_some()
                || self.remote_probe_target_min_interval.is_some())
            .then_some(RemoteProbeConfigPatch {
                enabled: self.remote_probe_enabled,
                timeout: self.remote_probe_timeout,
                global_min_interval: self.remote_probe_global_min_interval,
                target_min_interval: self.remote_probe_target_min_interval,
            }),
            export: (self.server_url.is_some()
                || self.format.is_some()
                || self.wire_mode.is_some()
                || self.secure_required.is_some()
                || self.token.is_some()
                || self.auth.is_some()
                || self.query_token_param.is_some()
                || !self.query.is_empty()
                || self.unsafe_cert.is_some()
                || self.heartbeat.is_some()
                || self.reconnect_interval.is_some())
            .then_some(ExportConfigPatch {
                server_url: self.server_url,
                format: self.format.map(Into::into),
                wire_mode: self.wire_mode.map(Into::into),
                secure_required: self.secure_required,
                token: self.token,
                auth_mode: self.auth.map(Into::into),
                query_token_param: self.query_token_param,
                query: query_params_to_map(self.query),
                unsafe_cert: self.unsafe_cert,
                heartbeat: self.heartbeat,
                reconnect_interval: self.reconnect_interval,
            }),
        }
    }
}

/// 空列表表示未覆盖配置，非空列表表示整体替换当前配置。
fn optional_non_empty_vec(values: Vec<String>) -> Option<Vec<String>> {
    (!values.is_empty()).then_some(values)
}

/// 把 CLI query 参数转换为有序 map，重复 key 使用最后一次传入的值。
fn query_params_to_map(query: Vec<CliQueryParam>) -> Option<BTreeMap<String, String>> {
    if query.is_empty() {
        return None;
    }

    Some(
        query
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect(),
    )
}

/// CLI 导出数据编码格式。
#[derive(Debug, Clone, Copy)]
pub(crate) enum CliExportFormat {
    /// Smalux 默认 JSON frame。
    SmaluxJson,
    /// Komari 兼容格式。
    Komari,
}

impl clap::ValueEnum for CliExportFormat {
    /// 当前可用的导出格式。
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::SmaluxJson, Self::Komari]
    }

    /// 使用配置里的 snake_case 名称，避免 CLI 和 JSON patch 命名不一致。
    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::SmaluxJson => Some(PossibleValue::new("smalux_json")),
            Self::Komari => Some(PossibleValue::new("komari")),
        }
    }
}

impl From<CliExportFormat> for ExportFormat {
    /// 转换为运行时导出格式。
    fn from(value: CliExportFormat) -> Self {
        match value {
            CliExportFormat::SmaluxJson => Self::SmaluxJson,
            CliExportFormat::Komari => Self::Komari,
        }
    }
}

/// CLI Smalux 自有协议 wire 模式。
#[derive(Debug, Clone, Copy)]
pub(crate) enum CliWireMode {
    /// 二进制明文 JSON bytes。
    BinaryPlain,
    /// Noise PSK 安全通道。
    SecurePsk,
}

impl clap::ValueEnum for CliWireMode {
    /// 当前可用的 wire 模式。
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::BinaryPlain, Self::SecurePsk]
    }

    /// 使用配置里的 snake_case 名称，避免 CLI 和 JSON patch 命名不一致。
    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::BinaryPlain => Some(PossibleValue::new("binary_plain")),
            Self::SecurePsk => Some(PossibleValue::new("secure_psk")),
        }
    }
}

impl From<CliWireMode> for ExportWireMode {
    /// 转换为运行时 wire 模式。
    fn from(value: CliWireMode) -> Self {
        match value {
            CliWireMode::BinaryPlain => Self::BinaryPlain,
            CliWireMode::SecurePsk => Self::SecurePsk,
        }
    }
}

/// CLI 指标采集级别。
#[derive(Debug, Clone, Copy)]
pub(crate) enum CliMetricLevel {
    /// 只采集总数。
    Count,
    /// 采集轻量信息。
    Light,
    /// 采集完整明细。
    Details,
}

impl clap::ValueEnum for CliMetricLevel {
    /// 当前可用的采集级别。
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Count, Self::Light, Self::Details]
    }

    /// 使用配置里的 snake_case 名称。
    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::Count => Some(PossibleValue::new("count")),
            Self::Light => Some(PossibleValue::new("light")),
            Self::Details => Some(PossibleValue::new("details")),
        }
    }
}

impl From<CliMetricLevel> for MetricLevel {
    /// 转换为运行时采集级别。
    fn from(value: CliMetricLevel) -> Self {
        match value {
            CliMetricLevel::Count => Self::Count,
            CliMetricLevel::Light => Self::Light,
            CliMetricLevel::Details => Self::Details,
        }
    }
}

/// CLI 认证方式。
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum CliAuthMode {
    /// 不发送认证信息。
    None,
    /// 使用 query token。
    Query,
    /// 使用 Authorization Bearer token。
    Bearer,
}

impl From<CliAuthMode> for ExportAuthMode {
    /// 转换为运行时认证方式。
    fn from(value: CliAuthMode) -> Self {
        match value {
            CliAuthMode::None => Self::None,
            CliAuthMode::Query => Self::Query,
            CliAuthMode::Bearer => Self::Bearer,
        }
    }
}

#[cfg(test)]
mod tests {
    //! 启动参数解析测试。

    use super::*;

    /// 验证启动参数可以覆盖默认配置。
    #[test]
    fn cli_args_override_default_config() {
        let args = CliArgs {
            agent_id: Some("agent-1".to_string()),
            log_file: Some("logs/test-agent.log".to_string()),
            log_retention_files: Some(9),
            log_max_size_mb: Some(32),
            server_url: Some("ws://127.0.0.1:9000/ws".to_string()),
            format: Some(CliExportFormat::SmaluxJson),
            core_interval: Some(Duration::from_secs(2)),
            disk_interval: Some(Duration::from_secs(10)),
            network_interval: Some(Duration::from_secs(12)),
            network_interfaces: vec!["Ethernet".to_string(), "Wi-Fi".to_string()],
            network_exclude_interfaces: vec!["Loopback".to_string()],
            processes_interval: Some(Duration::from_secs(61)),
            processes_level: Some(CliMetricLevel::Light),
            processes_limit: Some(25),
            sockets_interval: Some(Duration::from_secs(62)),
            sockets_level: Some(CliMetricLevel::Details),
            sockets_limit: Some(100),
            report_interval: Some(Duration::from_secs(3)),
            realtime_report_interval: Some(Duration::from_secs(4)),
            basic_info_interval: Some(Duration::from_secs(60)),
            report_heartbeat_enabled: Some(true),
            report_heartbeat_interval: Some(Duration::from_secs(30)),
            report_delta_enabled: Some(true),
            report_snapshot_interval: Some(Duration::from_secs(120)),
            report_force_snapshot_min_interval: Some(Duration::from_secs(8)),
            reconnect_interval: Some(Duration::from_secs(7)),
            query: vec![CliQueryParam {
                key: "agent_id".to_string(),
                value: "agent-1".to_string(),
            }],
            ..CliArgs::default()
        };

        let config = args.into_config().unwrap();

        assert_eq!(config.agent_id, "agent-1");
        assert_eq!(config.log_file, "logs/test-agent.log");
        assert_eq!(config.log_retention_files, 9);
        assert_eq!(config.log_max_size_mb, 32);
        assert_eq!(config.export.server_url, "ws://127.0.0.1:9000/ws");
        assert_eq!(config.export.format, ExportFormat::SmaluxJson);
        assert_eq!(config.core.interval, Duration::from_secs(2));
        assert_eq!(config.disk.interval, Duration::from_secs(10));
        assert_eq!(config.network.interval, Duration::from_secs(12));
        assert_eq!(config.network.include_interfaces, ["Ethernet", "Wi-Fi"]);
        assert_eq!(config.network.exclude_interfaces, ["Loopback"]);
        assert_eq!(config.processes.interval, Duration::from_secs(61));
        assert_eq!(config.processes.level, MetricLevel::Light);
        assert_eq!(config.processes.limit, 25);
        assert_eq!(config.sockets.interval, Duration::from_secs(62));
        assert_eq!(config.sockets.level, MetricLevel::Details);
        assert_eq!(config.sockets.limit, 100);
        assert_eq!(config.report.interval, Duration::from_secs(3));
        assert_eq!(config.jobs.realtime_report.interval, Duration::from_secs(4));
        assert_eq!(config.jobs.basic_info.interval, Duration::from_secs(60));
        assert!(config.report.heartbeat_enabled);
        assert_eq!(config.report.heartbeat_interval, Duration::from_secs(30));
        assert!(config.report.delta_enabled);
        assert_eq!(config.report.snapshot_interval, Duration::from_secs(120));
        assert_eq!(
            config.report.force_snapshot_min_interval,
            Duration::from_secs(8)
        );
        assert_eq!(config.export.reconnect_interval, Duration::from_secs(7));
        assert_eq!(
            config.export.query.get("agent_id").map(String::as_str),
            Some("agent-1")
        );
    }

    /// 验证常用短参数可以解析并覆盖默认配置。
    #[test]
    fn cli_short_args_override_default_config() {
        let args = CliArgs::try_parse_from([
            "smalux-agent",
            "-i",
            "agent-short",
            "-l",
            "logs/short.log",
            "-L",
            "5",
            "--log-max-size-mb",
            "16",
            "-s",
            "ws://127.0.0.1:9001/ws",
            "-f",
            "smalux_json",
            "-a",
            "bearer",
            "-t",
            "short-token",
            "-k",
            "access_token",
            "-q",
            "region=local",
            "-u",
            "true",
            "-H",
            "15s",
            "-r",
            "8s",
            "-c",
            "2s",
            "-d",
            "11s",
            "-n",
            "13s",
            "-I",
            "Ethernet",
            "-I",
            "Wi-Fi",
            "--network-exclude-interface",
            "Loopback",
            "--processes-interval",
            "61s",
            "--processes-level",
            "light",
            "--processes-limit",
            "25",
            "--sockets-interval",
            "62s",
            "--sockets-enabled",
            "true",
            "--sockets-level",
            "details",
            "--sockets-limit",
            "100",
            "-p",
            "12h",
            "-R",
            "4s",
            "--basic-info-interval",
            "90s",
            "--basic-info-enabled",
            "false",
            "--report-heartbeat-enabled",
            "true",
            "--report-heartbeat-interval",
            "30s",
            "--report-delta-enabled",
            "true",
            "--report-snapshot-interval",
            "2m",
        ])
        .unwrap();

        let config = args.into_config().unwrap();

        assert_eq!(config.agent_id, "agent-short");
        assert_eq!(config.log_file, "logs/short.log");
        assert_eq!(config.log_retention_files, 5);
        assert_eq!(config.log_max_size_mb, 16);
        assert_eq!(config.export.server_url, "ws://127.0.0.1:9001/ws");
        assert_eq!(config.export.format, ExportFormat::SmaluxJson);
        assert_eq!(config.export.auth_mode, ExportAuthMode::Bearer);
        assert_eq!(config.export.token.as_deref(), Some("short-token"));
        assert_eq!(config.export.query_token_param.as_str(), "access_token");
        assert_eq!(
            config.export.query.get("region").map(String::as_str),
            Some("local")
        );
        assert!(config.export.unsafe_cert);
        assert_eq!(config.export.heartbeat, Duration::from_secs(15));
        assert_eq!(config.export.reconnect_interval, Duration::from_secs(8));
        assert_eq!(config.core.interval, Duration::from_secs(2));
        assert_eq!(config.disk.interval, Duration::from_secs(11));
        assert_eq!(config.network.interval, Duration::from_secs(13));
        assert_eq!(config.network.include_interfaces, ["Ethernet", "Wi-Fi"]);
        assert_eq!(config.network.exclude_interfaces, ["Loopback"]);
        assert_eq!(config.processes.interval, Duration::from_secs(61));
        assert_eq!(config.processes.level, MetricLevel::Light);
        assert_eq!(config.processes.limit, 25);
        assert!(config.sockets.enabled);
        assert_eq!(config.sockets.interval, Duration::from_secs(62));
        assert_eq!(config.sockets.level, MetricLevel::Details);
        assert_eq!(config.sockets.limit, 100);
        assert_eq!(
            config.public_ip.refresh_interval,
            Duration::from_secs(12 * 60 * 60)
        );
        assert_eq!(config.report.interval, Duration::from_secs(4));
        assert_eq!(config.jobs.realtime_report.interval, Duration::from_secs(4));
        assert!(!config.jobs.basic_info.enabled);
        assert_eq!(config.jobs.basic_info.interval, Duration::from_secs(90));
        assert!(config.report.heartbeat_enabled);
        assert_eq!(config.report.heartbeat_interval, Duration::from_secs(30));
        assert!(config.report.delta_enabled);
        assert_eq!(config.report.snapshot_interval, Duration::from_secs(120));
    }

    /// 验证 Komari 格式可以通过短参数解析。
    #[test]
    fn cli_short_format_accepts_komari() {
        let args = CliArgs::try_parse_from([
            "smalux-agent",
            "-s",
            "wss://example.com/api/clients/report",
            "-f",
            "komari",
            "-a",
            "query",
            "-t",
            "secret-token",
        ])
        .unwrap();

        let config = args.into_config().unwrap();

        assert_eq!(config.export.format, ExportFormat::Komari);
        assert_eq!(config.export.auth_mode, ExportAuthMode::Query);
        assert_eq!(config.export.token.as_deref(), Some("secret-token"));
    }

    /// 验证远程能力默认关闭，且未传运行限制时不进入动态配置 patch。
    #[test]
    fn remote_capabilities_are_disabled_by_default() {
        let args = CliArgs::default();

        let options = args.to_service_options().unwrap();
        let patch = args.into_patch();

        assert!(!options.remote_shell.enabled);
        assert!(!options.diagnostics.allow_process_details);
        assert!(!options.diagnostics.allow_socket_details);
        assert!(!options.remote_task.enabled);
        assert_eq!(patch, AgentConfigPatch::default());
    }

    /// 验证日志参数只在启动阶段生效，不会进入 server 动态 patch。
    #[test]
    fn log_args_do_not_build_dynamic_patch() {
        let args = CliArgs {
            log_file: Some("logs/startup-only.log".to_string()),
            log_retention_files: Some(7),
            log_max_size_mb: Some(48),
            ..CliArgs::default()
        };

        let config = args.clone().into_config().unwrap();
        let patch = args.into_patch();

        assert_eq!(config.log_file, "logs/startup-only.log");
        assert_eq!(config.log_retention_files, 7);
        assert_eq!(config.log_max_size_mb, 48);
        assert_eq!(patch, AgentConfigPatch::default());
    }

    /// 验证远程能力 CLI 参数会拆分为静态开关和动态运行限制。
    #[test]
    fn remote_capability_args_build_service_options() {
        let args = CliArgs::try_parse_from([
            "smalux-agent",
            "-S",
            "true",
            "-M",
            "2",
            "--remote-shell-idle-timeout",
            "5m",
            "--remote-shell-session-timeout",
            "30m",
            "-P",
            "powershell.exe",
            "--allow-process-details",
            "true",
            "--allow-socket-details",
            "true",
            "-T",
            "true",
            "-C",
            "3",
            "--remote-task-timeout",
            "45s",
            "--remote-task-max-stdout-bytes",
            "1024",
            "--remote-task-max-stderr-bytes",
            "2048",
            "--remote-probe-enabled",
            "true",
            "--remote-probe-timeout",
            "2s",
            "--remote-probe-global-min-interval",
            "750ms",
            "--remote-probe-target-min-interval",
            "12s",
        ])
        .unwrap();

        let options = args.to_service_options().unwrap();
        let patch = args.into_patch();
        let remote_shell = patch.remote_shell.unwrap();
        let remote_task = patch.remote_task.unwrap();
        let remote_probe = patch.remote_probe.unwrap();

        assert!(options.remote_shell.enabled);
        assert_eq!(
            remote_shell.program.as_ref().unwrap().as_deref(),
            Some("powershell.exe")
        );
        assert_eq!(remote_shell.max_sessions, Some(2));
        assert_eq!(remote_shell.idle_timeout, Some(Duration::from_secs(5 * 60)));
        assert_eq!(
            remote_shell.session_timeout,
            Some(Duration::from_secs(30 * 60))
        );
        assert!(options.diagnostics.allow_process_details);
        assert!(options.diagnostics.allow_socket_details);
        assert!(options.remote_task.enabled);
        assert_eq!(remote_task.max_concurrent, Some(3));
        assert_eq!(remote_task.timeout, Some(Duration::from_secs(45)));
        assert_eq!(remote_task.max_stdout_bytes, Some(1024));
        assert_eq!(remote_task.max_stderr_bytes, Some(2048));
        assert_eq!(remote_probe.enabled, Some(true));
        assert_eq!(remote_probe.timeout, Some(Duration::from_secs(2)));
        assert_eq!(
            remote_probe.global_min_interval,
            Some(Duration::from_millis(750))
        );
        assert_eq!(
            remote_probe.target_min_interval,
            Some(Duration::from_secs(12))
        );
    }

    /// 验证远程能力的无效动态运行限制会被启动期拒绝。
    #[test]
    fn invalid_remote_capability_args_are_rejected() {
        let shell_error = CliArgs {
            remote_shell_max_sessions: Some(0),
            ..CliArgs::default()
        }
        .into_config()
        .unwrap_err();
        let task_error = CliArgs {
            remote_task_max_concurrent: Some(0),
            ..CliArgs::default()
        }
        .into_config()
        .unwrap_err();

        assert!(
            shell_error
                .to_string()
                .contains("remote_shell.max_sessions")
        );
        assert!(
            task_error
                .to_string()
                .contains("remote_task.max_concurrent")
        );
    }
}
