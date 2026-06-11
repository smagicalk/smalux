//! Agent 动态配置管理。

mod validation;

use super::model::{AgentConfig, AgentConfigPatch};
use tokio::sync::watch;
use validation::ensure_secure_required_not_downgraded;

#[cfg(test)]
use validation::{
    MAX_PROCESSES_LIMIT, MAX_SOCKETS_LIMIT, MIN_DETAILS_INTERVAL, MIN_LIGHT_INTERVAL,
};
pub(crate) use validation::{
    validate_config, validate_process_sampling_options, validate_socket_sampling_options,
};

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

#[cfg(test)]
mod tests {
    //! 动态配置管理测试。

    use super::*;
    use crate::config::model::{
        AgentConfigPatch, DiskConfigPatch, ExportAuthMode, ExportConfigPatch, ExportFormat,
        ExportWireMode, GroupConfigPatch, NetworkConfigPatch, ProcessConfigPatch,
        PublicIpConfigPatch, RemoteProbeConfigPatch, RemoteShellConfigPatch, RemoteTaskConfigPatch,
        SocketConfigPatch,
    };
    use base64::Engine;
    use smalux_core::model::info::MetricLevel;
    use std::time::Duration;

    /// 构造一份合法的 Komari 测试配置。
    fn valid_komari_config() -> AgentConfig {
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.export.base_url = "https://example.com".to_string();
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
                core: Some(GroupConfigPatch {
                    interval: Some(current.core.interval),
                    ..GroupConfigPatch::default()
                }),
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

    /// 验证 WebSocket ping 心跳可以显式设置为 0 来禁用。
    #[test]
    fn validate_accepts_zero_export_heartbeat() {
        let mut config = AgentConfig::default();
        config.export.heartbeat = Duration::ZERO;

        validate_config(&config).unwrap();
    }

    /// 验证非零 WebSocket ping 心跳仍然不能过小。
    #[test]
    fn validate_rejects_too_small_export_heartbeat() {
        let mut config = AgentConfig::default();
        config.export.heartbeat = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("export.heartbeat"));
    }

    /// 验证 basic info delivery 间隔过小会被拒绝。
    #[test]
    fn validate_rejects_too_small_basic_info_refresh_interval() {
        let mut config = AgentConfig::default();
        config.outbound.basic_info.refresh_interval = Duration::from_millis(1);

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("outbound.basic_info.refresh_interval")
        );
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
    fn apply_patch_updates_remote_probe_execution_limits() {
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
        let config = AgentConfig {
            log_retention_files: 0,
            ..AgentConfig::default()
        };

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
        let config = AgentConfig {
            log_max_size_mb: 0,
            ..AgentConfig::default()
        };

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("log_max_size_mb must be greater than 0")
        );
    }

    /// 验证 payload 日志预览长度必须大于 0。
    #[test]
    fn validate_rejects_zero_log_payload_max_bytes() {
        let config = AgentConfig {
            log_payload_max_bytes: 0,
            ..AgentConfig::default()
        };

        let error = validate_config(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("log_payload_max_bytes must be greater than 0")
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

    /// 验证 Komari base URL 也按 WebSocket 上报间隔限制校验。
    #[test]
    fn validate_rejects_komari_base_endpoint_interval_above_websocket_limit() {
        let mut config = valid_komari_config();
        config.report.interval = Duration::from_secs(11);

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("report.interval"));
    }

    /// 验证 base_url 不能包含 path。
    #[test]
    fn validate_rejects_export_base_url_with_path() {
        let mut config = valid_komari_config();
        config.export.base_url = "https://example.com/api/clients/report".to_string();

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("export.base_url"));
        assert!(error.to_string().contains("path"));
    }

    /// 验证 base_url 不能包含 query。
    #[test]
    fn validate_rejects_export_base_url_with_query() {
        let mut config = valid_komari_config();
        config.export.base_url = "https://example.com?token=from-url".to_string();

        let error = validate_config(&config).unwrap_err();

        assert!(error.to_string().contains("export.base_url"));
        assert!(error.to_string().contains("query"));
    }

    /// 验证 Komari token 可以放在 export.query 中。
    #[test]
    fn validate_accepts_komari_token_from_export_query() {
        let mut config = AgentConfig::default();
        config.export.format = ExportFormat::Komari;
        config.export.base_url = "https://example.com".to_string();
        config
            .export
            .query
            .insert("token".to_string(), "from-query".to_string());

        validate_config(&config).unwrap();
    }
}
