//! Agent 运行时配置模型入口。
//!
//! 这里负责组织配置模型子模块并重导出类型，具体字段和 patch 逻辑拆到独立文件。

mod agent;
mod disk;
mod export;
mod group;
mod network;
mod outbound;
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
pub(crate) use network::{NetworkConfig, NetworkConfigPatch};
#[allow(unused_imports)]
pub(crate) use outbound::{
    BasicInfoOutputConfig, BasicInfoOutputConfigPatch, OutboundConfig, OutboundConfigPatch,
    RealtimeReportOutputConfig, RealtimeReportOutputConfigPatch,
};
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
pub(crate) use report::{ReportConfig, ReportConfigPatch};
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
                "outbound": {
                    "realtime_report": { "enabled": true, "send_on_start": false },
                    "basic_info": { "refresh_interval": "5m" }
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
                    "base_url": "https://example.com",
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
        let realtime_report = patch.outbound.as_ref().unwrap().realtime_report.unwrap();
        assert_eq!(realtime_report.enabled, Some(true));
        assert_eq!(realtime_report.send_on_start, Some(false));
        assert_eq!(
            patch
                .outbound
                .as_ref()
                .unwrap()
                .basic_info
                .unwrap()
                .refresh_interval,
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
            patch.export.as_ref().unwrap().base_url.as_deref(),
            Some("https://example.com")
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

    /// 验证 server patch 拒绝启动期日志字段，避免无效字段被静默忽略。
    #[test]
    fn config_patch_rejects_startup_only_log_fields() {
        let error = serde_json::from_str::<AgentConfigPatch>(
            r#"{
                "log_file": "logs/server.log",
                "log_retention_files": 1,
                "log_max_size_mb": 1,
                "log_payload": true,
                "log_payload_max_bytes": 1024
            }"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains("log_file"));
    }

    /// 验证 realtime report 没有 refresh_interval 字段，server 下发时会直接拒绝。
    #[test]
    fn config_patch_rejects_unknown_realtime_report_field() {
        let error = serde_json::from_str::<AgentConfigPatch>(
            r#"{
                "outbound": {
                    "realtime_report": { "refresh_interval": "5s" }
                }
            }"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains("refresh_interval"));
    }
}
