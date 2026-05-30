//! Agent telemetry 最新状态。
//!
//! 该状态不是发送队列，而是每个采样组的最新快照。它用于构建完整 snapshot、
//! 计算 delta，以及给兼容协议读取当前状态。

use crate::collect::{
    CoreSample, DiskSample, NetworkSample, ProcessSample, SocketSample, unix_timestamp_secs,
};
use smalux_core::model::info::{
    AGENT_REPORT_SCHEMA_VERSION, AgentReport, IdentityInfo, PublicIpStatus, ReportMeta, Stamped,
    SystemInfo,
};

/// 采样组缓存状态。
#[derive(Debug, Clone)]
pub(crate) enum GroupState<T> {
    /// 用户未启用该采样组。
    Disabled,
    /// 已启用但还没有采样结果。
    Pending,
    /// 已有可上报数据。
    Ready(T),
}

impl<T> Default for GroupState<T> {
    /// 默认处于待采样状态。
    fn default() -> Self {
        Self::Pending
    }
}

impl<T> GroupState<T> {
    /// 返回 ready 数据引用。
    pub(crate) fn ready(&self) -> Option<&T> {
        match self {
            Self::Ready(value) => Some(value),
            Self::Disabled | Self::Pending => None,
        }
    }

    /// 判断采样组是否已经满足上报前置条件。
    pub(crate) fn ready_or_disabled(&self) -> bool {
        matches!(self, Self::Ready(_) | Self::Disabled)
    }

    /// 根据配置开关切换采样组状态。
    pub(crate) fn configure_enabled(&mut self, enabled: bool) {
        if enabled {
            if matches!(self, Self::Disabled) {
                *self = Self::Pending;
            }
        } else {
            *self = Self::Disabled;
        }
    }
}

/// agent 最新 telemetry 状态。
#[derive(Debug, Clone, Default)]
pub(crate) struct TelemetryState {
    /// 身份信息。
    pub identity: GroupState<IdentityInfo>,
    /// 静态系统信息。
    pub system: GroupState<SystemInfo>,
    /// 核心指标。
    pub core: GroupState<CoreSample>,
    /// 磁盘指标。
    pub disk: GroupState<DiskSample>,
    /// 网络指标。
    pub network: GroupState<NetworkSample>,
    /// 进程汇总指标。
    pub processes: GroupState<ProcessSample>,
    /// Socket 汇总指标。
    pub sockets: GroupState<SocketSample>,
}

impl TelemetryState {
    /// 写入身份信息。
    pub(crate) fn set_identity(&mut self, identity: IdentityInfo) {
        self.identity = GroupState::Ready(identity);
    }

    /// 写入静态系统信息。
    pub(crate) fn set_system(&mut self, system: SystemInfo) {
        self.system = GroupState::Ready(system);
    }

    /// 写入核心指标。
    pub(crate) fn set_core(&mut self, core: CoreSample) {
        self.core = GroupState::Ready(core);
    }

    /// 写入磁盘指标。
    pub(crate) fn set_disk(&mut self, disk: DiskSample) {
        self.disk = GroupState::Ready(disk);
    }

    /// 写入网络指标。
    pub(crate) fn set_network(&mut self, network: NetworkSample) {
        self.network = GroupState::Ready(network);
    }

    /// 写入进程汇总指标。
    pub(crate) fn set_processes(&mut self, processes: ProcessSample) {
        self.processes = GroupState::Ready(processes);
    }

    /// 写入 Socket 汇总指标。
    pub(crate) fn set_sockets(&mut self, sockets: SocketSample) {
        self.sockets = GroupState::Ready(sockets);
    }

    /// 根据配置开关标记可选采样组，避免禁用组阻塞第一包上报。
    pub(crate) fn configure_metric_groups(
        &mut self,
        core_enabled: bool,
        disk_enabled: bool,
        network_enabled: bool,
        processes_enabled: bool,
        sockets_enabled: bool,
    ) {
        self.core.configure_enabled(core_enabled);
        self.disk.configure_enabled(disk_enabled);
        self.network.configure_enabled(network_enabled);
        self.processes.configure_enabled(processes_enabled);
        self.sockets.configure_enabled(sockets_enabled);
    }

    /// 判断第一包所需数据是否全部就绪。
    pub(crate) fn first_report_ready(&self) -> bool {
        self.identity.ready().is_some()
            && self.system.ready().is_some()
            && self.core.ready_or_disabled()
            && self.disk.ready_or_disabled()
            && self.network.ready_or_disabled()
            && self.processes.ready_or_disabled()
            && self.sockets.ready_or_disabled()
    }

    /// 判断公网 IP 是否已经成功获取。
    pub(crate) fn public_ip_ready(&self) -> bool {
        matches!(
            self.identity
                .ready()
                .map(|identity| &identity.public_ip.status),
            Some(PublicIpStatus::Ready)
        )
    }

    /// 组装完整监控上报数据。
    pub(crate) fn build_report(&self, agent_version: &str) -> anyhow::Result<AgentReport> {
        let identity = self
            .identity
            .ready()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Identity is not ready"))?;
        let system = self
            .system
            .ready()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("System info is not ready"))?;
        let core = optional_group(&self.core, "Core metrics")?;
        let disk = optional_group(&self.disk, "Disk metrics")?;
        let network = optional_group(&self.network, "Network metrics")?;
        let processes = optional_group(&self.processes, "Process metrics")?;
        let sockets = optional_group(&self.sockets, "Socket metrics")?;

        Ok(AgentReport {
            meta: ReportMeta {
                schema_version: AGENT_REPORT_SCHEMA_VERSION,
                agent_version: agent_version.to_string(),
                report_at: unix_timestamp_secs(),
            },
            identity,
            system,
            core: core.map(|core| Stamped {
                sampled_at: core.sampled_at,
                value: core.value,
            }),
            disk: disk.map(|disk| Stamped {
                sampled_at: disk.sampled_at,
                value: disk.value,
            }),
            network: network.map(|network| Stamped {
                sampled_at: network.sampled_at,
                value: network.value,
            }),
            processes: processes.map(|processes| Stamped {
                sampled_at: processes.sampled_at,
                value: processes.value,
            }),
            sockets: sockets.map(|sockets| Stamped {
                sampled_at: sockets.sampled_at,
                value: sockets.value,
            }),
        })
    }
}

/// 返回可选采样组结果，禁用组映射为 `None`。
fn optional_group<T: Clone>(state: &GroupState<T>, name: &str) -> anyhow::Result<Option<T>> {
    match state {
        GroupState::Ready(value) => Ok(Some(value.clone())),
        GroupState::Disabled => Ok(None),
        GroupState::Pending => anyhow::bail!("{name} are not ready"),
    }
}

#[cfg(test)]
mod tests {
    //! Telemetry 状态测试。

    use super::*;
    use smalux_core::model::info::{
        CoreInfo, DiskInfo, NetworkInfo, ProcessInfo, PublicIpInfo, PublicIpSource, SocketInfo,
    };
    use std::net::{IpAddr, Ipv4Addr};

    /// 构造测试身份信息。
    fn test_identity() -> IdentityInfo {
        IdentityInfo {
            agent_id: "agent-test".to_string(),
            hostname: "host-test".to_string(),
            public_ip: PublicIpInfo::ready(
                IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
                PublicIpSource::ExternalHttp,
                1,
                Some(1),
            ),
            local_ips: vec![],
        }
    }

    /// 验证缺少必需组时不会构造上报。
    #[test]
    fn build_report_requires_all_required_groups() {
        let state = TelemetryState::default();

        assert!(!state.first_report_ready());
        assert!(state.build_report("0.1.0").is_err());
    }

    /// 验证缓存齐全时可以构造上报。
    #[test]
    fn build_report_returns_agent_report() {
        let mut state = TelemetryState::default();
        state.set_identity(test_identity());
        state.set_system(SystemInfo::default());
        state.set_core(CoreSample {
            sampled_at: 1,
            value: CoreInfo::default(),
        });
        state.set_disk(DiskSample {
            sampled_at: 2,
            value: DiskInfo::default(),
        });
        state.set_network(NetworkSample {
            sampled_at: 3,
            value: NetworkInfo::default(),
        });
        state.set_processes(ProcessSample {
            sampled_at: 4,
            value: ProcessInfo::ready(10),
        });
        state.set_sockets(SocketSample {
            sampled_at: 5,
            value: SocketInfo::ready(
                3,
                1,
                smalux_core::model::info::SocketSource::SocketTable,
                smalux_core::model::info::SocketAccuracy::SocketTable,
            ),
        });

        let report = state.build_report("0.1.0").unwrap();

        assert!(state.first_report_ready());
        assert_eq!(report.meta.schema_version, AGENT_REPORT_SCHEMA_VERSION);
        assert_eq!(report.identity.agent_id, "agent-test");
        assert_eq!(report.core.unwrap().sampled_at, 1);
        assert_eq!(report.disk.unwrap().sampled_at, 2);
        assert_eq!(report.network.unwrap().sampled_at, 3);
        assert_eq!(report.processes.unwrap().sampled_at, 4);
        assert_eq!(report.sockets.unwrap().sampled_at, 5);
    }

    /// 验证禁用的采样组不会阻塞上报，也不会出现在 payload 中。
    #[test]
    fn build_report_omits_disabled_metric_groups() {
        let mut state = TelemetryState::default();
        state.configure_metric_groups(true, false, false, false, false);
        state.set_identity(test_identity());
        state.set_system(SystemInfo::default());
        state.set_core(CoreSample {
            sampled_at: 1,
            value: CoreInfo::default(),
        });

        let report = state.build_report("0.1.0").unwrap();

        assert!(state.first_report_ready());
        assert!(report.core.is_some());
        assert!(report.disk.is_none());
        assert!(report.network.is_none());
        assert!(report.processes.is_none());
        assert!(report.sockets.is_none());
    }

    /// 验证公网 IP 失败状态不会阻塞普通第一包上报。
    #[test]
    fn build_report_allows_failed_public_ip_status() {
        let mut identity = test_identity();
        identity.public_ip = PublicIpInfo::failed("temporary failure".to_string(), 10);
        let mut state = TelemetryState::default();
        state.configure_metric_groups(false, false, false, false, false);
        state.set_identity(identity);
        state.set_system(SystemInfo::default());

        let report = state.build_report("0.1.0").unwrap();

        assert!(state.first_report_ready());
        assert!(!state.public_ip_ready());
        assert_eq!(report.identity.public_ip.status, PublicIpStatus::Failed);
    }

    /// 验证公网 IP ready 判断只接受真实获取成功状态。
    #[test]
    fn public_ip_ready_requires_ready_status() {
        let mut state = TelemetryState::default();
        state.set_identity(test_identity());
        assert!(state.public_ip_ready());

        let mut identity = test_identity();
        identity.public_ip = PublicIpInfo::failed("temporary failure".to_string(), 10);
        state.set_identity(identity);
        assert!(!state.public_ip_ready());
    }
}
