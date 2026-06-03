//! CLI 启动输入到运行时配置的转换。

use super::super::model::{
    AgentConfig, AgentConfigPatch, DiskConfigPatch, ExportConfigPatch, GroupConfigPatch,
    JobConfigPatch, JobsConfigPatch, NetworkConfigPatch, ProcessConfigPatch, PublicIpConfigPatch,
    RemoteProbeConfigPatch, RemoteShellConfigPatch, RemoteTaskConfigPatch, ReportConfigPatch,
    SocketConfigPatch,
};
use super::args::{CliArgs, CliQueryParam};
use crate::service::ServiceOptions;
use std::collections::BTreeMap;

/// CLI 解析后的 agent 启动输入。
#[derive(Debug, Clone)]
pub(crate) struct AgentStartup {
    /// 动态运行配置。
    pub(crate) config: AgentConfig,
    /// CLI-only 服务静态选项。
    pub(crate) service_options: ServiceOptions,
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
        super::super::manager::validate_config(&config)?;
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
