//! Agent 总配置模型。

use super::super::defaults::{
    DEFAULT_CORE_INTERVAL, DEFAULT_LOG_FILE, DEFAULT_LOG_MAX_SIZE_MB, DEFAULT_LOG_PAYLOAD,
    DEFAULT_LOG_PAYLOAD_MAX_BYTES, DEFAULT_LOG_RETENTION_FILES, DEFAULT_PROCESSES_INTERVAL,
    DEFAULT_PROCESSES_LIMIT, DEFAULT_SOCKETS_INTERVAL, DEFAULT_SOCKETS_LIMIT,
};
use super::disk::{DiskConfig, DiskConfigPatch};
use super::export::{ExportConfig, ExportConfigPatch};
use super::group::{GroupConfig, GroupConfigPatch};
use super::network::{NetworkConfig, NetworkConfigPatch};
use super::outbound::{OutboundConfig, OutboundConfigPatch};
use super::process::{ProcessConfig, ProcessConfigPatch};
use super::public_ip::{PublicIpConfig, PublicIpConfigPatch};
use super::remote::{
    RemoteProbeConfig, RemoteProbeConfigPatch, RemoteShellConfig, RemoteShellConfigPatch,
    RemoteTaskConfig, RemoteTaskConfigPatch,
};
use super::report::{ReportConfig, ReportConfigPatch};
use super::socket::{SocketConfig, SocketConfigPatch};
use serde::{Deserialize, Serialize};
use smalux_core::model::info::MetricLevel;

/// agent 采集和上报配置。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct AgentConfig {
    /// agent 实例 ID。
    pub agent_id: String,
    /// 日志文件前缀，启动时用于初始化 tracing。
    pub log_file: String,
    /// 保留的滚动日志文件数，必须大于 0。
    pub log_retention_files: usize,
    /// 单个日志文件最大大小，单位 MB。
    pub log_max_size_mb: u64,
    /// 是否允许 trace 日志打印截断后的实际 payload。
    pub log_payload: bool,
    /// 实际 payload 日志预览最大原始字节数。
    pub log_payload_max_bytes: usize,
    /// 核心指标配置。
    pub core: GroupConfig,
    /// 磁盘指标配置。
    pub disk: DiskConfig,
    /// 网络指标配置。
    pub network: NetworkConfig,
    /// 进程汇总指标配置。
    pub processes: ProcessConfig,
    /// Socket 汇总指标配置。
    pub sockets: SocketConfig,
    /// 公网 IP 配置。
    pub public_ip: PublicIpConfig,
    /// 上报配置。
    pub report: ReportConfig,
    /// 出站业务事件配置。
    pub outbound: OutboundConfig,
    /// 远程 shell 运行限制。
    pub remote_shell: RemoteShellConfig,
    /// 远程任务运行限制。
    pub remote_task: RemoteTaskConfig,
    /// 远程网络探测运行限制。
    pub remote_probe: RemoteProbeConfig,
    /// 导出连接配置。
    pub export: ExportConfig,
}

impl Default for AgentConfig {
    /// 默认启用基础监控 agent 能力。
    fn default() -> Self {
        Self {
            agent_id: default_agent_id(),
            log_file: DEFAULT_LOG_FILE.to_string(),
            log_retention_files: DEFAULT_LOG_RETENTION_FILES,
            log_max_size_mb: DEFAULT_LOG_MAX_SIZE_MB,
            log_payload: DEFAULT_LOG_PAYLOAD,
            log_payload_max_bytes: DEFAULT_LOG_PAYLOAD_MAX_BYTES,
            core: GroupConfig::new(true, DEFAULT_CORE_INTERVAL),
            disk: DiskConfig::default(),
            network: NetworkConfig::default(),
            processes: ProcessConfig::new(
                true,
                DEFAULT_PROCESSES_INTERVAL,
                MetricLevel::Count,
                DEFAULT_PROCESSES_LIMIT,
            ),
            sockets: SocketConfig::new(
                true,
                DEFAULT_SOCKETS_INTERVAL,
                MetricLevel::Count,
                DEFAULT_SOCKETS_LIMIT,
            ),
            public_ip: PublicIpConfig::default(),
            report: ReportConfig::default(),
            outbound: OutboundConfig::default(),
            remote_shell: RemoteShellConfig::default(),
            remote_task: RemoteTaskConfig::default(),
            remote_probe: RemoteProbeConfig::default(),
            export: ExportConfig::default(),
        }
    }
}

/// server 运行期配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentConfigPatch {
    /// 核心指标配置 patch。
    pub core: Option<GroupConfigPatch>,
    /// 磁盘指标配置 patch。
    pub disk: Option<DiskConfigPatch>,
    /// 网络指标配置 patch。
    pub network: Option<NetworkConfigPatch>,
    /// 进程汇总指标配置 patch。
    pub processes: Option<ProcessConfigPatch>,
    /// Socket 汇总指标配置 patch。
    pub sockets: Option<SocketConfigPatch>,
    /// 公网 IP 配置 patch。
    pub public_ip: Option<PublicIpConfigPatch>,
    /// 上报配置 patch。
    pub report: Option<ReportConfigPatch>,
    /// 出站业务事件配置 patch。
    pub outbound: Option<OutboundConfigPatch>,
    /// 远程 shell 运行限制 patch。
    pub remote_shell: Option<RemoteShellConfigPatch>,
    /// 远程任务运行限制 patch。
    pub remote_task: Option<RemoteTaskConfigPatch>,
    /// 远程网络探测运行限制 patch。
    pub remote_probe: Option<RemoteProbeConfigPatch>,
    /// 导出连接配置 patch。
    pub export: Option<ExportConfigPatch>,
}

impl AgentConfigPatch {
    /// 应用 server 运行期配置 patch。
    pub(crate) fn apply_to(&self, config: &mut AgentConfig) {
        if let Some(core) = &self.core {
            core.apply_to(&mut config.core);
        }
        if let Some(disk) = &self.disk {
            disk.apply_to(&mut config.disk);
        }
        if let Some(network) = &self.network {
            network.apply_to(&mut config.network);
        }
        if let Some(processes) = &self.processes {
            processes.apply_to(&mut config.processes);
        }
        if let Some(sockets) = &self.sockets {
            sockets.apply_to(&mut config.sockets);
        }
        if let Some(public_ip) = &self.public_ip {
            public_ip.apply_to(&mut config.public_ip);
        }
        if let Some(report) = &self.report {
            report.apply_to(&mut config.report);
        }
        if let Some(outbound) = &self.outbound {
            outbound.apply_to(&mut config.outbound);
        }
        if let Some(remote_shell) = &self.remote_shell {
            remote_shell.apply_to(&mut config.remote_shell);
        }
        if let Some(remote_task) = &self.remote_task {
            remote_task.apply_to(&mut config.remote_task);
        }
        if let Some(remote_probe) = &self.remote_probe {
            remote_probe.apply_to(&mut config.remote_probe);
        }
        if let Some(export) = &self.export {
            export.apply_to(&mut config.export);
        }
    }
}

/// 创建默认 agent ID。
fn default_agent_id() -> String {
    resolve_agent_id(std::env::var("SMALUX_AGENT_ID").ok())
}

/// 根据环境变量解析 agent ID，缺失时生成临时 UUID。
fn resolve_agent_id(env_agent_id: Option<String>) -> String {
    env_agent_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

#[cfg(test)]
mod tests {
    //! Agent 总配置模型测试。

    use super::*;

    /// 验证环境变量中的 agent ID 优先于自动生成值。
    #[test]
    fn resolve_agent_id_uses_non_blank_env_value() {
        assert_eq!(
            resolve_agent_id(Some(" agent-from-env ".to_string())),
            "agent-from-env"
        );
    }

    /// 验证缺失环境变量时生成 UUID。
    #[test]
    fn resolve_agent_id_generates_uuid_when_env_missing() {
        let agent_id = resolve_agent_id(None);

        assert!(uuid::Uuid::parse_str(&agent_id).is_ok());
    }

    /// 验证空白环境变量不会生成无效 agent ID。
    #[test]
    fn resolve_agent_id_generates_uuid_when_env_blank() {
        let agent_id = resolve_agent_id(Some("  ".to_string()));

        assert!(uuid::Uuid::parse_str(&agent_id).is_ok());
    }
}
