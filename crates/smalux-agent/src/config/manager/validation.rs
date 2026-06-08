//! 动态配置校验规则。

use crate::config::model::{
    AgentConfig, AgentConfigPatch, ExportAuthMode, ExportFormat, ExportWireMode,
};
use crate::export::parse_export_base_url;
use smalux_core::model::info::MetricLevel;
use smalux_core::utils::validate::{ensure_interval_at_least, ensure_non_empty};
use smalux_protocol::secure::parse_secure_token;
use std::time::Duration;

/// 最小配置间隔，避免 server 下发过小间隔导致 agent 忙等。
const MIN_INTERVAL: Duration = Duration::from_millis(100);
/// light 级诊断采样的最小定时间隔。
pub(crate) const MIN_LIGHT_INTERVAL: Duration = Duration::from_secs(1);
/// details 级诊断采样的最小定时间隔。
pub(crate) const MIN_DETAILS_INTERVAL: Duration = Duration::from_secs(10);
/// 进程 light/details 最大返回条数。
pub(crate) const MAX_PROCESSES_LIMIT: usize = 500;
/// Socket details 最大返回条数。
pub(crate) const MAX_SOCKETS_LIMIT: usize = 2_000;
/// Komari WebSocket 模式最大建议上报间隔，避免长时间没有数据帧被服务端断开。
const KOMARI_MAX_WEBSOCKET_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// 防止 server patch 在运行中关闭启动时已经要求的安全通道。
pub(super) fn ensure_secure_required_not_downgraded(
    current: &AgentConfig,
    patch: &AgentConfigPatch,
) -> anyhow::Result<()> {
    if !current.export.secure_required {
        return Ok(());
    }

    if matches!(
        patch
            .export
            .as_ref()
            .and_then(|export| export.secure_required),
        Some(false)
    ) {
        anyhow::bail!("export.secure_required cannot be disabled by server patch once enabled");
    }

    Ok(())
}

/// 校验 agent 配置。
pub(crate) fn validate_config(config: &AgentConfig) -> anyhow::Result<()> {
    ensure_non_empty("agent_id", &config.agent_id)?;
    ensure_non_empty("log_file", &config.log_file)?;
    if config.log_retention_files == 0 {
        anyhow::bail!("log_retention_files must be greater than 0");
    }
    if config.log_max_size_mb == 0 {
        anyhow::bail!("log_max_size_mb must be greater than 0");
    }
    if config.log_payload_max_bytes == 0 {
        anyhow::bail!("log_payload_max_bytes must be greater than 0");
    }
    ensure_config_interval("core.interval", config.core.interval)?;
    ensure_config_interval("disk.interval", config.disk.interval)?;
    ensure_config_interval("network.interval", config.network.interval)?;
    ensure_config_interval("processes.interval", config.processes.interval)?;
    ensure_config_interval("sockets.interval", config.sockets.interval)?;
    ensure_config_interval("public_ip.lookup_timeout", config.public_ip.lookup_timeout)?;
    ensure_config_interval("public_ip.retry_interval", config.public_ip.retry_interval)?;
    ensure_config_interval(
        "public_ip.refresh_interval",
        config.public_ip.refresh_interval,
    )?;
    ensure_config_interval("report.interval", config.report.interval)?;
    ensure_config_interval(
        "report.heartbeat_interval",
        config.report.heartbeat_interval,
    )?;
    ensure_config_interval("report.snapshot_interval", config.report.snapshot_interval)?;
    ensure_config_interval(
        "report.force_snapshot_min_interval",
        config.report.force_snapshot_min_interval,
    )?;
    ensure_config_interval(
        "outbound.basic_info.refresh_interval",
        config.outbound.basic_info.refresh_interval,
    )?;
    config.remote_shell.validate()?;
    config.remote_task.validate()?;
    config.remote_probe.validate()?;
    ensure_optional_export_heartbeat("export.heartbeat", config.export.heartbeat)?;
    ensure_config_interval(
        "export.reconnect_interval",
        config.export.reconnect_interval,
    )?;

    if config.public_ip.max_concurrency == 0 {
        anyhow::bail!("public_ip.max_concurrency must be greater than 0");
    }
    validate_process_sampling_options(
        config.processes.level,
        config.processes.limit,
        Some(config.processes.interval),
    )?;
    validate_socket_sampling_options(
        config.sockets.level,
        config.sockets.limit,
        Some(config.sockets.interval),
    )?;

    ensure_non_empty("export.base_url", &config.export.base_url)?;
    parse_export_base_url(&config.export.base_url)?;
    if let Some(token) = &config.export.token {
        ensure_non_empty("export.token", token)?;
    }
    if matches!(
        config.export.auth_mode,
        ExportAuthMode::Query | ExportAuthMode::Bearer
    ) && config
        .export
        .token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
        .is_none()
    {
        anyhow::bail!("export.token is required when export.auth_mode is not none");
    }
    ensure_non_empty("export.query_token_param", &config.export.query_token_param)?;
    for key in config.export.query.keys() {
        ensure_non_empty("export.query key", key)?;
    }
    for interface in &config.network.include_interfaces {
        ensure_non_empty("network.include_interfaces", interface)?;
    }
    for interface in &config.network.exclude_interfaces {
        ensure_non_empty("network.exclude_interfaces", interface)?;
    }
    validate_export_format_constraints(config)?;

    Ok(())
}

/// 校验进程采样级别、返回上限和可选定时间隔。
pub(crate) fn validate_process_sampling_options(
    level: MetricLevel,
    limit: usize,
    interval: Option<Duration>,
) -> anyhow::Result<()> {
    ensure_sampling_limit("processes.limit", limit, MAX_PROCESSES_LIMIT)?;
    if let Some(interval) = interval {
        ensure_level_interval("processes.interval", level, interval)?;
    }
    Ok(())
}

/// 校验 Socket 采样级别、返回上限和可选定时间隔。
pub(crate) fn validate_socket_sampling_options(
    level: MetricLevel,
    limit: usize,
    interval: Option<Duration>,
) -> anyhow::Result<()> {
    ensure_sampling_limit("sockets.limit", limit, MAX_SOCKETS_LIMIT)?;
    if let Some(interval) = interval {
        ensure_level_interval("sockets.interval", level, interval)?;
    }
    Ok(())
}

/// 校验间隔非零且不低于最小间隔。
fn ensure_config_interval(name: &str, value: Duration) -> anyhow::Result<()> {
    ensure_interval_at_least(name, value, MIN_INTERVAL)
}

/// 校验 WebSocket ping 心跳间隔；0 是显式禁用，其它值仍要避免过高频率。
fn ensure_optional_export_heartbeat(name: &str, value: Duration) -> anyhow::Result<()> {
    if value.is_zero() {
        return Ok(());
    }

    ensure_config_interval(name, value)
}

/// 校验返回条数上限在安全范围内。
fn ensure_sampling_limit(name: &str, value: usize, max: usize) -> anyhow::Result<()> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    if value > max {
        anyhow::bail!("{name} must be at most {max}");
    }
    Ok(())
}

/// 按采样级别校验最小间隔，避免高成本诊断被过高频率调度。
fn ensure_level_interval(name: &str, level: MetricLevel, interval: Duration) -> anyhow::Result<()> {
    match level {
        MetricLevel::Count => Ok(()),
        MetricLevel::Light => ensure_interval_at_least(name, interval, MIN_LIGHT_INTERVAL),
        MetricLevel::Details => ensure_interval_at_least(name, interval, MIN_DETAILS_INTERVAL),
    }
}

/// 校验特定导出格式的约束。
fn validate_export_format_constraints(config: &AgentConfig) -> anyhow::Result<()> {
    if config.export.secure_required && !matches!(config.export.format, ExportFormat::SmaluxJson) {
        anyhow::bail!("export.format must be smalux_json when export.secure_required is true");
    }

    match config.export.format {
        ExportFormat::SmaluxJson => validate_smalux_json_config(config),
        ExportFormat::Komari => validate_komari_config(config),
    }
}

/// 校验 Smalux 自有协议格式约束。
fn validate_smalux_json_config(config: &AgentConfig) -> anyhow::Result<()> {
    if config.export.secure_required
        && !matches!(config.export.wire_mode, ExportWireMode::SecurePsk)
    {
        anyhow::bail!("export.wire_mode must be secure_psk when export.secure_required is true");
    }
    if matches!(config.export.wire_mode, ExportWireMode::SecurePsk)
        && config
            .export
            .token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .is_none()
    {
        anyhow::bail!("export.token is required when export.wire_mode is secure_psk");
    }
    if matches!(config.export.wire_mode, ExportWireMode::SecurePsk)
        && !matches!(config.export.auth_mode, ExportAuthMode::None)
    {
        anyhow::bail!(
            "export.auth_mode must be none when export.wire_mode is secure_psk; token is used only for PSK derivation"
        );
    }
    if matches!(config.export.wire_mode, ExportWireMode::SecurePsk) {
        let token = config.export.token.as_deref().ok_or_else(|| {
            anyhow::anyhow!("export.token is required when export.wire_mode is secure_psk")
        })?;
        parse_secure_token(token)?;
    }

    Ok(())
}

/// 校验 Komari 兼容格式约束。
fn validate_komari_config(config: &AgentConfig) -> anyhow::Result<()> {
    if config.report.delta_enabled {
        anyhow::bail!("report.delta_enabled must be false when export.format is komari");
    }
    if config.report.heartbeat_enabled {
        anyhow::bail!("report.heartbeat_enabled must be false when export.format is komari");
    }
    if matches!(config.export.auth_mode, ExportAuthMode::Bearer) {
        anyhow::bail!("export.auth_mode=bearer is not supported when export.format is komari");
    }
    if config.report.interval > KOMARI_MAX_WEBSOCKET_REPORT_INTERVAL {
        anyhow::bail!(
            "report.interval must be at most 10s when export.format is komari over websocket"
        );
    }
    if !komari_query_token_configured(config) {
        anyhow::bail!("komari export requires query token in export.token or export.query");
    }

    Ok(())
}

/// 判断 Komari query token 是否已配置。
fn komari_query_token_configured(config: &AgentConfig) -> bool {
    if matches!(config.export.auth_mode, ExportAuthMode::Query)
        && config
            .export
            .token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .is_some()
    {
        return true;
    }

    if config
        .export
        .query
        .get(&config.export.query_token_param)
        .is_some_and(|token| !token.trim().is_empty())
    {
        return true;
    }

    false
}
