//! Agent 运行时配置模型入口。
//!
//! 这里负责组织配置模型子模块并重导出类型，具体字段和 patch 逻辑拆到独立文件。

mod agent;
mod disk;
mod export;
mod group;
mod job;
mod network;
mod process;
mod public_ip;
mod remote;
mod report;
mod socket;

#[allow(unused_imports)]
pub(crate) use agent::{AgentConfig, AgentConfigPatch};
#[allow(unused_imports)]
pub(crate) use disk::{DiskConfig, DiskConfigPatch};
#[allow(unused_imports)]
pub(crate) use export::{
    ExportAuthMode, ExportConfig, ExportConfigPatch, ExportFormat, ExportWireMode,
};
#[allow(unused_imports)]
pub(crate) use group::GroupConfigPatch;
#[allow(unused_imports)]
pub(crate) use job::{JobConfig, JobConfigPatch, JobsConfig, JobsConfigPatch};
#[allow(unused_imports)]
pub(crate) use network::{NetworkConfig, NetworkConfigPatch};
#[allow(unused_imports)]
pub(crate) use process::{ProcessConfig, ProcessConfigPatch};
#[allow(unused_imports)]
pub(crate) use public_ip::{PublicIpConfig, PublicIpConfigPatch};
#[allow(unused_imports)]
pub(crate) use remote::{
    RemoteProbeConfig, RemoteProbeConfigPatch, RemoteShellConfig, RemoteShellConfigPatch,
    RemoteTaskConfig, RemoteTaskConfigPatch,
};
#[allow(unused_imports)]
pub(crate) use report::ReportConfigPatch;
#[allow(unused_imports)]
pub(crate) use socket::{SocketConfig, SocketConfigPatch};

#[cfg(test)]
mod tests {
    //! 配置模型和 patch 序列化测试。

    use super::*;
    use std::time::Duration;

    /// 验证 server 下发 JSON patch 可以使用人类可读时间。
    #[test]
    fn config_patch_deserializes_human_duration() {
        let patch: AgentConfigPatch = serde_json::from_str(
            r#"{
                "core": { "interval": "2s" },
                "disk": { "interval": "10s" },
                "network": { "interval": "11s" },
                "processes": { "interval": "60s", "level": "light", "limit": 25 },
                "sockets": { "interval": "60s", "level": "details", "limit": 100 },
                "public_ip": { "refresh_interval": "12h" },
                "report": { "interval": "5s" },
                "jobs": {
                    "realtime_report": { "interval": "5s" },
                    "basic_info": { "interval": "5m" }
                },
                "remote_shell": {
                    "max_sessions": 2,
                    "idle_timeout": "5m",
                    "session_timeout": "30m",
                    "program": "powershell.exe"
                },
                "remote_task": { "max_concurrent": 3 },
                "remote_probe": {
                    "enabled": true,
                    "timeout": "2s",
                    "global_min_interval": "500ms",
                    "target_min_interval": "10s"
                },
                "export": {
                    "format": "smalux_json",
                    "reconnect_interval": "15s",
                    "query": { "agent_id": "agent-1" }
                }
            }"#,
        )
        .unwrap();

        assert_eq!(patch.core.unwrap().interval, Some(Duration::from_secs(2)));
        assert_eq!(patch.disk.unwrap().interval, Some(Duration::from_secs(10)));
        assert_eq!(
            patch.network.unwrap().interval,
            Some(Duration::from_secs(11))
        );
        let processes = patch.processes.unwrap();
        let sockets = patch.sockets.unwrap();
        assert_eq!(processes.interval, Some(Duration::from_secs(60)));
        assert_eq!(
            processes.level,
            Some(smalux_core::model::info::MetricLevel::Light)
        );
        assert_eq!(processes.limit, Some(25));
        assert_eq!(sockets.interval, Some(Duration::from_secs(60)));
        assert_eq!(
            sockets.level,
            Some(smalux_core::model::info::MetricLevel::Details)
        );
        assert_eq!(sockets.limit, Some(100));
        assert_eq!(
            patch.public_ip.unwrap().refresh_interval,
            Some(Duration::from_secs(12 * 60 * 60))
        );
        assert_eq!(patch.report.unwrap().interval, Some(Duration::from_secs(5)));
        assert_eq!(
            patch
                .jobs
                .as_ref()
                .unwrap()
                .realtime_report
                .unwrap()
                .interval,
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            patch.jobs.as_ref().unwrap().basic_info.unwrap().interval,
            Some(Duration::from_secs(5 * 60))
        );
        let remote_shell = patch.remote_shell.unwrap();
        assert_eq!(remote_shell.max_sessions, Some(2));
        assert_eq!(remote_shell.idle_timeout, Some(Duration::from_secs(5 * 60)));
        assert_eq!(
            remote_shell.session_timeout,
            Some(Duration::from_secs(30 * 60))
        );
        assert_eq!(
            remote_shell.program.as_ref().unwrap().as_deref(),
            Some("powershell.exe")
        );
        assert_eq!(patch.remote_task.unwrap().max_concurrent, Some(3));
        let remote_probe = patch.remote_probe.unwrap();
        assert_eq!(remote_probe.enabled, Some(true));
        assert_eq!(remote_probe.timeout, Some(Duration::from_secs(2)));
        assert_eq!(
            remote_probe.global_min_interval,
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            remote_probe.target_min_interval,
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            patch.export.as_ref().unwrap().reconnect_interval,
            Some(Duration::from_secs(15))
        );
        assert_eq!(
            patch.export.as_ref().unwrap().format,
            Some(ExportFormat::SmaluxJson)
        );
        assert_eq!(
            patch
                .export
                .unwrap()
                .query
                .unwrap()
                .get("agent_id")
                .map(String::as_str),
            Some("agent-1")
        );
    }

    /// 验证 server patch 中的日志字段不会修改启动期日志配置。
    #[test]
    fn config_patch_ignores_startup_only_log_fields() {
        let patch: AgentConfigPatch = serde_json::from_str(
            r#"{
                "log_file": "logs/server.log",
                "log_retention_files": 1,
                "log_max_size_mb": 1
            }"#,
        )
        .unwrap();
        let mut config = AgentConfig::default();
        let log_file = config.log_file.clone();
        let log_retention_files = config.log_retention_files;
        let log_max_size_mb = config.log_max_size_mb;

        patch.apply_to(&mut config);

        assert_eq!(config.log_file, log_file);
        assert_eq!(config.log_retention_files, log_retention_files);
        assert_eq!(config.log_max_size_mb, log_max_size_mb);
    }
}
