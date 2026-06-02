//! Agent 动态配置管理。

use super::model::{AgentConfig, AgentConfigPatch, ExportAuthMode, ExportFormat, ExportWireMode};
use crate::export::security::parse_secure_token;
use smalux_core::model::info::MetricLevel;
use smalux_core::utils::validate::{ensure_interval_at_least, ensure_non_empty};
use std::time::Duration;
use tokio::sync::watch;

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

/// 动态配置管理器。
#[derive(Debug, Clone)]
pub(crate) struct ConfigManager {
    sender: watch::Sender<AgentConfig>,
}

impl ConfigManager {
    /// 创建动态配置管理器。
    pub(crate) fn new(initial_config: AgentConfig) -> anyhow::Result<Self> {
        validate_config(&initial_config)?;
        let (sender, _receiver) = watch::channel(initial_config);
        Ok(Self { sender })
    }

    /// 订阅配置变更。
    pub(crate) fn subscribe(&self) -> watch::Receiver<AgentConfig> {
        self.sender.subscribe()
    }

    /// 获取当前配置。
    pub(crate) fn current(&self) -> AgentConfig {
        self.sender.borrow().clone()
    }

    /// 应用 server 下发的配置 patch。
    pub(crate) fn apply_patch(&self, patch: AgentConfigPatch) -> anyhow::Result<AgentConfig> {
        let current = self.current();
        ensure_secure_required_not_downgraded(&current, &patch)?;
        let mut next = current.clone();
        patch.apply_to(&mut next);

        if next == current {
            tracing::debug!("service config patch ignored; no changes");
            return Ok(current);
        }

        validate_config(&next)?;
        self.sender.send_replace(next.clone());
        tracing::info!("service config updated");
        Ok(next)
    }
}

/// 防止 server patch 在运行中关闭启动时已经要求的安全通道。
fn ensure_secure_required_not_downgraded(
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
    ensure_config_interval("core.interval", config.core.interval)?;
    ensure_config_interval("disk.interval", config.disk.interval)?;
    ensure_config_interval("network.interval", config.network.interval)?;
    ensure_config_interval("processes.interval", config.processes.interval)?;
    ensure_config_interval("sockets.interval", config.sockets.interval)?;
    ensure_config_interval(
        "public_ip.startup_timeout",
        config.public_ip.startup_timeout,
    )?;
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
        "jobs.realtime_report.interval",
        config.jobs.realtime_report.interval,
    )?;
    ensure_config_interval("jobs.basic_info.interval", config.jobs.basic_info.interval)?;
    config.remote_shell.validate()?;
    config.remote_task.validate()?;
    config.remote_probe.validate()?;
    ensure_config_interval("export.heartbeat", config.export.heartbeat)?;
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

    ensure_non_empty("export.server_url", &config.export.server_url)?;
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
    for (key, _value) in &config.export.query {
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
    if komari_uses_websocket_report(&config.export.server_url)
        && (config.report.interval > KOMARI_MAX_WEBSOCKET_REPORT_INTERVAL
            || config.jobs.realtime_report.interval > KOMARI_MAX_WEBSOCKET_REPORT_INTERVAL)
    {
        anyhow::bail!(
            "report.interval and jobs.realtime_report.interval must be at most 10s when export.format is komari over websocket"
        );
    }
    if !komari_query_token_configured(config) {
        anyhow::bail!(
            "komari export requires query token in export.token, export.query, or server_url"
        );
    }

    Ok(())
}

/// 判断 Komari report 是否会走 WebSocket。
///
/// `https://host` 这类官方基础 endpoint 会在 Komari adapter 中派生为 WebSocket
/// report；显式传入 HTTPS report endpoint 时也会规范化为 WebSocket report。
fn komari_uses_websocket_report(server_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(server_url) else {
        return false;
    };

    match url.scheme() {
        "ws" | "wss" | "http" | "https" => true,
        _ => false,
    }
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
        .contains_key(&config.export.query_token_param)
    {
        return true;
    }

    reqwest::Url::parse(&config.export.server_url)
        .ok()
        .map(|url| {
            url.query_pairs().any(|(key, value)| {
                key == config.export.query_token_param && !value.trim().is_empty()
            })
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    //! 动态配置管理测试。

    use super::*;
    use crate::config::model::{
        AgentConfigPatch, DiskConfigPatch, ExportConfigPatch, GroupConfigPatch, NetworkConfigPatch,
        ProcessConfigPatch, PublicIpConfigPatch, RemoteProbeConfigPatch, RemoteShellConfigPatch,
        RemoteTaskConfigPatch, SocketConfigPatch,
    };
    use base64::Engine;

    /// 构造一份合法的 Komari 测试配置。
    fn valid_komari_config() -> AgentConfig {
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.export.server_url = "wss://example.com/api/clients/report".to_string();
        config.export.auth_mode = ExportAuthMode::Query;
        config.export.token = Some("secret-token".to_string());
        config
    }

    /// 构造测试用 secure_psk token。
    fn secure_test_token(byte: u8) -> String {
        format!(
            "smx1.agent-key.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([byte; 32])
        )
    }

    /// 构造一份要求 secure_psk 的合法配置。
    fn valid_secure_config() -> AgentConfig {
        let mut config = AgentConfig::default();
        config.export.wire_mode = ExportWireMode::SecurePsk;
        config.export.secure_required = true;
        config.export.auth_mode = ExportAuthMode::None;
        config.export.token = Some(secure_test_token(1));
        config
    }

    /// 验证运行时 patch 可以覆盖当前配置并通知订阅者。
    #[test]
    fn apply_patch_updates_config() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();
        let mut receiver = manager.subscribe();

        let patch = AgentConfigPatch {
            core: Some(GroupConfigPatch {
                interval: Some(Duration::from_secs(2)),
                ..GroupConfigPatch::default()
            }),
            public_ip: Some(PublicIpConfigPatch {
                max_concurrency: Some(4),
                ..PublicIpConfigPatch::default()
            }),
            ..AgentConfigPatch::default()
        };

        let updated = manager.apply_patch(patch).unwrap();
        receiver.borrow_and_update();

        assert_eq!(updated.core.interval, Duration::from_secs(2));
        assert_eq!(updated.public_ip.max_concurrency, 4);
        assert_eq!(receiver.borrow().core.interval, Duration::from_secs(2));
    }

    /// 验证没有订阅者时仍然能更新配置，后续订阅者可以拿到最新值。
    #[test]
    fn apply_patch_without_subscribers_keeps_latest_config() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                core: Some(GroupConfigPatch {
                    interval: Some(Duration::from_secs(3)),
                    ..GroupConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap();
        let receiver = manager.subscribe();

        assert_eq!(updated.core.interval, Duration::from_secs(3));
        assert_eq!(receiver.borrow().core.interval, Duration::from_secs(3));
    }

    /// 验证无变化 patch 不会通知订阅者。
    #[test]
    fn apply_patch_skips_notification_when_config_is_unchanged() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();
        let receiver = manager.subscribe();
        let current = manager.current();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                agent_id: Some(current.agent_id.clone()),
                ..AgentConfigPatch::default()
            })
            .unwrap();

        assert_eq!(updated, current);
        assert!(!receiver.has_changed().unwrap());
    }

    /// 验证过小间隔会被拒绝。
    #[test]
    fn validate_rejects_too_small_interval() {
        let mut config = AgentConfig::default();
        config.core.interval = Duration::from_millis(1);

        assert!(validate_config(&config).is_err());
    }

    /// 验证业务心跳间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_report_heartbeat_interval() {
        let mut config = AgentConfig::default();
        config.report.heartbeat_interval = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.heartbeat_interval"));
    }

    /// 验证完整快照间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_report_snapshot_interval() {
        let mut config = AgentConfig::default();
        config.report.snapshot_interval = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.snapshot_interval"));
    }

    /// 验证强制快照最小间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_force_snapshot_min_interval() {
        let mut config = AgentConfig::default();
        config.report.force_snapshot_min_interval = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("report.force_snapshot_min_interval")
        );
    }

    /// 验证实时上报 job 间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_realtime_report_job_interval() {
        let mut config = AgentConfig::default();
        config.jobs.realtime_report.interval = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("jobs.realtime_report.interval"));
    }

    /// 验证 basic info job 间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_basic_info_job_interval() {
        let mut config = AgentConfig::default();
        config.jobs.basic_info.interval = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("jobs.basic_info.interval"));
    }

    /// 验证 server patch 可以动态调整远程 shell 运行限制。
    #[test]
    fn apply_patch_updates_remote_shell_runtime_limits() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                remote_shell: Some(RemoteShellConfigPatch {
                    max_sessions: Some(2),
                    idle_timeout: Some(Duration::from_secs(5 * 60)),
                    session_timeout: Some(Duration::from_secs(30 * 60)),
                    program: Some(Some("powershell.exe".to_string())),
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap();

        assert_eq!(updated.remote_shell.max_sessions, 2);
        assert_eq!(
            updated.remote_shell.idle_timeout,
            Duration::from_secs(5 * 60)
        );
        assert_eq!(
            updated.remote_shell.session_timeout,
            Duration::from_secs(30 * 60)
        );
        assert_eq!(
            updated.remote_shell.program.as_deref(),
            Some("powershell.exe")
        );
    }

    /// 验证 server patch 可以清空自定义 shell 程序。
    #[test]
    fn apply_patch_can_clear_remote_shell_program() {
        let mut config = AgentConfig::default();
        config.remote_shell.program = Some("powershell.exe".to_string());
        let manager = ConfigManager::new(config).unwrap();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                remote_shell: Some(RemoteShellConfigPatch {
                    program: Some(None),
                    ..RemoteShellConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap();

        assert_eq!(updated.remote_shell.program, None);
    }

    /// 验证 server patch 可以动态调整远程任务运行限制。
    #[test]
    fn apply_patch_updates_remote_task_runtime_limits() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                remote_task: Some(RemoteTaskConfigPatch {
                    max_concurrent: Some(3),
                    timeout: Some(Duration::from_secs(45)),
                    max_stdout_bytes: Some(1024),
                    max_stderr_bytes: Some(2048),
                    ..RemoteTaskConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap();

        assert_eq!(updated.remote_task.max_concurrent, 3);
        assert_eq!(updated.remote_task.timeout, Duration::from_secs(45));
        assert_eq!(updated.remote_task.max_stdout_bytes, 1024);
        assert_eq!(updated.remote_task.max_stderr_bytes, 2048);
    }

    /// 验证 server patch 可以动态调整远程探测运行限制。
    #[test]
    fn apply_patch_updates_remote_probe_runtime_limits() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                remote_probe: Some(RemoteProbeConfigPatch {
                    enabled: Some(true),
                    timeout: Some(Duration::from_secs(2)),
                    global_min_interval: Some(Duration::from_millis(750)),
                    target_min_interval: Some(Duration::from_secs(12)),
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap();

        assert!(updated.remote_probe.enabled);
        assert_eq!(updated.remote_probe.timeout, Duration::from_secs(2));
        assert_eq!(
            updated.remote_probe.global_min_interval,
            Duration::from_millis(750)
        );
        assert_eq!(
            updated.remote_probe.target_min_interval,
            Duration::from_secs(12)
        );
    }

    /// 验证过高频远程探测配置会被拒绝。
    #[test]
    fn validate_rejects_too_small_remote_probe_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                remote_probe: Some(RemoteProbeConfigPatch {
                    global_min_interval: Some(Duration::from_millis(1)),
                    ..RemoteProbeConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("remote_probe.global_min_interval")
        );
    }

    /// 验证磁盘采样间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_disk_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                disk: Some(DiskConfigPatch {
                    interval: Some(Duration::from_millis(1)),
                    ..DiskConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("disk.interval"));
    }

    /// 验证网络采样间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_network_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                network: Some(NetworkConfigPatch {
                    interval: Some(Duration::from_millis(1)),
                    ..NetworkConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("network.interval"));
    }

    /// 验证进程汇总采样间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_processes_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                processes: Some(ProcessConfigPatch {
                    interval: Some(Duration::from_millis(1)),
                    ..ProcessConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("processes.interval"));
    }

    /// 验证 Socket 汇总采样间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_sockets_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                sockets: Some(SocketConfigPatch {
                    interval: Some(Duration::from_millis(1)),
                    ..SocketConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("sockets.interval"));
    }

    /// 验证进程 light/details 返回上限必须大于 0。
    #[test]
    fn validate_rejects_zero_processes_limit() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                processes: Some(ProcessConfigPatch {
                    limit: Some(0),
                    ..ProcessConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("processes.limit"));
    }

    /// 验证 Socket details 返回上限必须大于 0。
    #[test]
    fn validate_rejects_zero_sockets_limit() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                sockets: Some(SocketConfigPatch {
                    limit: Some(0),
                    ..SocketConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("sockets.limit"));
    }

    /// 验证进程返回上限不能超过保护值。
    #[test]
    fn validate_rejects_processes_limit_above_cap() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                processes: Some(ProcessConfigPatch {
                    limit: Some(MAX_PROCESSES_LIMIT + 1),
                    ..ProcessConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("processes.limit"));
    }

    /// 验证 Socket 返回上限不能超过保护值。
    #[test]
    fn validate_rejects_sockets_limit_above_cap() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                sockets: Some(SocketConfigPatch {
                    limit: Some(MAX_SOCKETS_LIMIT + 1),
                    ..SocketConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("sockets.limit"));
    }

    /// 验证 light 级诊断采集不能配置过高频率。
    #[test]
    fn validate_rejects_too_small_light_processes_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                processes: Some(ProcessConfigPatch {
                    level: Some(MetricLevel::Light),
                    interval: Some(MIN_LIGHT_INTERVAL - Duration::from_millis(1)),
                    ..ProcessConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("processes.interval"));
    }

    /// 验证 details 级诊断采集不能配置过高频率。
    #[test]
    fn validate_rejects_too_small_details_sockets_interval() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                sockets: Some(SocketConfigPatch {
                    level: Some(MetricLevel::Details),
                    interval: Some(MIN_DETAILS_INTERVAL - Duration::from_millis(1)),
                    ..SocketConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("sockets.interval"));
    }

    /// 验证动态 patch 会清理网络网卡过滤中的空白名称。
    #[test]
    fn apply_patch_normalizes_blank_network_interface_name() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();

        let updated = manager
            .apply_patch(AgentConfigPatch {
                network: Some(NetworkConfigPatch {
                    include_interfaces: Some(vec![
                        " Ethernet ".to_string(),
                        " ".to_string(),
                        "Ethernet".to_string(),
                    ]),
                    exclude_interfaces: Some(vec![
                        " Loopback ".to_string(),
                        "".to_string(),
                        "Loopback".to_string(),
                    ]),
                    ..NetworkConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap();

        assert_eq!(updated.network.include_interfaces, ["Ethernet"]);
        assert_eq!(updated.network.exclude_interfaces, ["Loopback"]);
    }

    /// 验证手工构造的非法网络网卡过滤仍会被拒绝。
    #[test]
    fn validate_rejects_blank_network_interface_name() {
        let mut config = AgentConfig::default();
        config.network.exclude_interfaces = vec![" ".to_string()];

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("network.exclude_interfaces"));
    }

    /// 验证日志滚动保留数量必须大于 0。
    #[test]
    fn validate_rejects_zero_log_retention_files() {
        let mut config = AgentConfig::default();
        config.log_retention_files = 0;

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("log_retention_files must be greater than 0")
        );
    }

    /// 验证日志大小滚动阈值必须大于 0。
    #[test]
    fn validate_rejects_zero_log_max_size_mb() {
        let mut config = AgentConfig::default();
        config.log_max_size_mb = 0;

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("log_max_size_mb must be greater than 0")
        );
    }

    /// 验证需要 token 的认证模式会 fail-fast。
    #[test]
    fn validate_rejects_token_auth_without_token() {
        let error = ConfigManager::new(AgentConfig::default())
            .unwrap()
            .apply_patch(AgentConfigPatch {
                export: Some(ExportConfigPatch {
                    auth_mode: Some(ExportAuthMode::Bearer),
                    ..ExportConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(error.to_string().contains("export.token is required"));
    }

    /// 验证 secure_psk 不允许同时把 token 放到 URL 或 Authorization。
    #[test]
    fn validate_rejects_secure_psk_with_transport_token_auth() {
        let mut config = AgentConfig::default();
        config.export.wire_mode = ExportWireMode::SecurePsk;
        config.export.token = Some(secure_test_token(1));
        config.export.auth_mode = ExportAuthMode::Query;

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("auth_mode must be none"));
    }

    /// 验证 secure_required 一旦启用，server patch 不能降级关闭。
    #[test]
    fn apply_patch_rejects_disabling_secure_required_once_enabled() {
        let manager = ConfigManager::new(valid_secure_config()).unwrap();

        let error = manager
            .apply_patch(AgentConfigPatch {
                export: Some(ExportConfigPatch {
                    wire_mode: Some(ExportWireMode::BinaryPlain),
                    secure_required: Some(false),
                    ..ExportConfigPatch::default()
                }),
                ..AgentConfigPatch::default()
            })
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("secure_required cannot be disabled")
        );
        assert!(manager.current().export.secure_required);
        assert_eq!(
            manager.current().export.wire_mode,
            ExportWireMode::SecurePsk
        );
    }

    /// 验证 secure_required 不能搭配第三方兼容格式。
    #[test]
    fn validate_rejects_secure_required_with_komari_format() {
        let mut config = valid_komari_config();
        config.export.secure_required = true;

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("export.format must be smalux_json")
        );
    }

    /// 验证 secure_psk token 格式会在配置阶段校验。
    #[test]
    fn validate_rejects_invalid_secure_psk_token() {
        let mut config = AgentConfig::default();
        config.export.wire_mode = ExportWireMode::SecurePsk;
        config.export.token = Some("bad-token".to_string());

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("smx1"));
    }

    /// 验证 Komari 不支持 delta 上报。
    #[test]
    fn validate_rejects_komari_with_delta_enabled() {
        let mut config = valid_komari_config();
        config.report.delta_enabled = true;

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.delta_enabled"));
    }

    /// 验证 Komari 不支持业务级 heartbeat。
    #[test]
    fn validate_rejects_komari_with_business_heartbeat_enabled() {
        let mut config = valid_komari_config();
        config.report.heartbeat_enabled = true;

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.heartbeat_enabled"));
    }

    /// 验证 Komari 不支持 Bearer 认证。
    #[test]
    fn validate_rejects_komari_with_bearer_auth() {
        let mut config = valid_komari_config();
        config.export.auth_mode = ExportAuthMode::Bearer;

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("export.auth_mode=bearer"));
    }

    /// 验证 Komari WebSocket 上报间隔不能超过兼容上限。
    #[test]
    fn validate_rejects_komari_websocket_report_interval_above_limit() {
        let mut config = valid_komari_config();
        config.report.interval = Duration::from_secs(11);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.interval"));
    }

    /// 验证 Komari WebSocket 上报 job 间隔不能超过兼容上限。
    #[test]
    fn validate_rejects_komari_websocket_job_interval_above_limit() {
        let mut config = valid_komari_config();
        config.jobs.realtime_report.interval = Duration::from_secs(11);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("jobs.realtime_report.interval"));
    }

    /// 验证 Komari 官方风格 HTTPS 基础 endpoint 也按 WebSocket 上报间隔限制校验。
    #[test]
    fn validate_rejects_komari_base_endpoint_interval_above_websocket_limit() {
        let mut config = valid_komari_config();
        config.export.server_url = "https://example.com".to_string();
        config.report.interval = Duration::from_secs(11);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.interval"));
    }

    /// 验证显式 HTTPS report endpoint 仍按 WebSocket report 间隔限制校验。
    #[test]
    fn validate_rejects_komari_https_report_endpoint_interval_above_websocket_limit() {
        let mut config = valid_komari_config();
        config.export.server_url = "https://example.com/api/clients/report".to_string();
        config.report.interval = Duration::from_secs(60);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.interval"));
    }

    /// 验证 Komari token 可以直接放在 URL query 中。
    #[test]
    fn validate_accepts_komari_token_from_server_url() {
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.export.server_url =
            "wss://example.com/api/clients/report?token=from-url".to_string();

        validate_config(&config).unwrap();
    }

    /// 验证 Komari token 可以放在 export.query 中。
    #[test]
    fn validate_accepts_komari_token_from_export_query() {
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.export.server_url = "wss://example.com/api/clients/report".to_string();
        config
            .export
            .query
            .insert("token".to_string(), "from-query".to_string());

        validate_config(&config).unwrap();
    }
}
