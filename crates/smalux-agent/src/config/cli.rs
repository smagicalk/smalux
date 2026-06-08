//! Agent 启动参数解析。

mod args;
mod startup;
mod value;

pub(crate) use args::CliArgs;

#[cfg(test)]
mod tests {
    //! 启动参数解析测试。

    use super::args::CliQueryParam;
    use super::value::{CliExportFormat, CliMetricLevel};
    use super::*;
    use crate::config::model::{AgentConfigPatch, ExportAuthMode, ExportFormat};
    use crate::service::RemoteMetricPermission;
    use clap::Parser;
    use smalux_core::model::info::MetricLevel;
    use std::time::Duration;

    /// 验证启动参数可以覆盖默认配置。
    #[test]
    fn cli_args_override_default_config() {
        let args = CliArgs {
            agent_id: Some("agent-1".to_string()),
            log_file: Some("logs/test-agent.log".to_string()),
            log_retention_files: Some(9),
            log_max_size_mb: Some(32),
            log_payload: Some(true),
            log_payload_max_bytes: Some(2048),
            base_url: Some("http://127.0.0.1:9000".to_string()),
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
            basic_info_refresh_interval: Some(Duration::from_secs(60)),
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
        assert!(config.log_payload);
        assert_eq!(config.log_payload_max_bytes, 2048);
        assert_eq!(config.export.base_url, "http://127.0.0.1:9000");
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
        assert_eq!(
            config.outbound.basic_info.refresh_interval,
            Duration::from_secs(60)
        );
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
            "--log-payload",
            "true",
            "--log-payload-max-bytes",
            "1024",
            "-s",
            "http://127.0.0.1:9001",
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
            "--basic-info-refresh-interval",
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
        assert!(config.log_payload);
        assert_eq!(config.log_payload_max_bytes, 1024);
        assert_eq!(config.export.base_url, "http://127.0.0.1:9001");
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
        assert!(!config.outbound.basic_info.enabled);
        assert_eq!(
            config.outbound.basic_info.refresh_interval,
            Duration::from_secs(90)
        );
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
            "https://example.com",
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
        assert_eq!(
            options.diagnostics.process_permission,
            RemoteMetricPermission::Count
        );
        assert_eq!(
            options.diagnostics.socket_permission,
            RemoteMetricPermission::Count
        );
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
            log_payload: Some(true),
            log_payload_max_bytes: Some(512),
            ..CliArgs::default()
        };

        let config = args.clone().into_config().unwrap();
        let patch = args.into_patch();

        assert_eq!(config.log_file, "logs/startup-only.log");
        assert_eq!(config.log_retention_files, 7);
        assert_eq!(config.log_max_size_mb, 48);
        assert!(config.log_payload);
        assert_eq!(config.log_payload_max_bytes, 512);
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
            "--allow-process-level",
            "details",
            "--allow-socket-level",
            "light",
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
        assert_eq!(
            options.diagnostics.process_permission,
            RemoteMetricPermission::Details
        );
        assert_eq!(
            options.diagnostics.socket_permission,
            RemoteMetricPermission::Light
        );
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
