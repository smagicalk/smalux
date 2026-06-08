//! CLI 参数定义和原始解析器。

use super::value::{
    CliAuthMode, CliExportFormat, CliMetricLevel, CliRemoteMetricPermission, CliWireMode,
};
use clap::Parser;
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
    pub(crate) key: String,
    /// query 参数值。
    pub(crate) value: String,
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
    /// 是否允许 trace 日志打印截断后的实际 payload，可能包含敏感数据。
    #[arg(long)]
    pub log_payload: Option<bool>,
    /// 实际 payload 日志预览最大原始字节数。
    #[arg(long)]
    pub log_payload_max_bytes: Option<usize>,

    /// server 根地址，例如 `https://example.com`；adapter 会派生具体 endpoint。
    #[arg(short = 's', long = "server")]
    pub base_url: Option<String>,
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
    /// 允许 server 触发的最高进程采样级别：none/count/light/details。
    #[arg(long = "allow-process-level", value_enum)]
    pub allow_process_level: Option<CliRemoteMetricPermission>,

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
    /// 允许 server 触发的最高 Socket 采样级别：none/count/light/details。
    #[arg(long = "allow-socket-level", value_enum)]
    pub allow_socket_level: Option<CliRemoteMetricPermission>,

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
    /// 公网 IP 外部探测超时，例如 `3s`。
    #[arg(long, value_parser = parse_duration)]
    pub public_ip_lookup_timeout: Option<Duration>,
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

    /// 是否启用实时 report 出站。
    #[arg(long)]
    pub realtime_report_enabled: Option<bool>,
    /// 第一份 report ready 后是否立即发送实时 report。
    #[arg(long)]
    pub realtime_report_send_on_start: Option<bool>,
    /// 是否启用 basic info 出站事件。
    #[arg(long)]
    pub basic_info_enabled: Option<bool>,
    /// basic info 刷新事件生成间隔，例如 `5m`。
    #[arg(long, value_parser = parse_duration)]
    pub basic_info_refresh_interval: Option<Duration>,
    /// 第一份 telemetry ready 后是否立即发送 basic info。
    #[arg(long)]
    pub basic_info_send_on_start: Option<bool>,

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
