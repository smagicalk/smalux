//! smalux-agent 二进制入口。
//!
//! 这里只负责启动期的最小装配：注册日志、解析启动参数、初始化动态配置，
//! 后续采集循环和导出流程应放到 `service` / `telemetry` 模块里。

use clap::Parser;
use config::{CliArgs, ConfigManager};

mod collect;
mod config;
mod export;
mod service;
mod telemetry;

/// agent 主入口。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let startup = CliArgs::parse().into_startup()?;
    let initial_config = startup.config;
    let service_options = startup.service_options;

    smalux_core::log::init_tracing(
        &initial_config.log_file,
        initial_config.log_retention_files,
        initial_config.log_max_size_mb,
    )?;
    let config_manager = ConfigManager::new(initial_config)?;
    let current_config = config_manager.current();

    tracing::info!(
        agent_id = %current_config.agent_id,
        log_file = %current_config.log_file,
        log_retention_files = current_config.log_retention_files,
        log_max_size_mb = current_config.log_max_size_mb,
        log_payload = current_config.log_payload,
        log_payload_max_bytes = current_config.log_payload_max_bytes,
        base_url = %current_config.export.base_url,
        core_interval_ms = current_config.core.interval.as_millis(),
        disk_interval_ms = current_config.disk.interval.as_millis(),
        network_interval_ms = current_config.network.interval.as_millis(),
        report_interval_ms = current_config.report.interval.as_millis(),
        basic_info_refresh_interval_ms = current_config
            .outbound
            .basic_info
            .refresh_interval
            .as_millis(),
        remote_command_enabled = service_options.remote_command_enabled(),
        remote_shell_max_sessions = current_config.remote_shell.max_sessions,
        remote_task_max_concurrent = current_config.remote_task.max_concurrent,
        "smalux-agent starting"
    );
    if current_config.log_payload {
        tracing::warn!(
            log_payload_max_bytes = current_config.log_payload_max_bytes,
            "payload logging is enabled; logs may contain telemetry payloads and remote command output"
        );
    }
    service::run(config_manager, service_options).await
}
