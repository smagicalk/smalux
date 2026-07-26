//! 完整本机指标周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::{HostMetricsCollector, SystemSnapshot},
};

use super::{
    DiskIoTaskConfig, LocalIpTaskConfig, MetricSample, NetworkIoTaskConfig,
    blocking::CollectState,
    selection::{filter_disk, filter_local_ip, filter_network},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// System Task 对组合快照中各资源的筛选配置。
pub struct SystemTaskConfig {
    /// 磁盘容量与 IO 的筛选配置。
    pub disk_io: DiskIoTaskConfig,
    /// 网络流量的网卡筛选配置。
    pub network_io: NetworkIoTaskConfig,
    /// 本地地址的网卡筛选配置。
    pub local_ip: LocalIpTaskConfig,
}

/// 使用独立组合 collector 生成完整本机快照的调度任务。
pub struct SystemTask {
    state: CollectState<HostMetricsCollector>,
    config: SystemTaskConfig,
}

impl SystemTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.system.v1";

    /// 使用默认资源选择创建拥有独立组合采样状态的 Task。
    pub fn new() -> Self {
        Self::with_config(SystemTaskConfig::default())
    }

    /// 使用显式资源筛选配置创建 Task。
    pub fn with_config(config: SystemTaskConfig) -> Self {
        Self {
            state: CollectState::new(HostMetricsCollector::new()),
            config,
        }
    }

    /// 返回当前生效的组合资源配置。
    pub fn config(&self) -> &SystemTaskConfig {
        &self.config
    }
}

impl Default for SystemTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ValueTask for SystemTask {
    type Output = MetricSample<SystemSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let mut output = self
            .state
            .collect(context, HostMetricsCollector::collect)
            .await?;
        output.sampled_at_ms = output.snapshot.sampled_at_ms;
        output.sample_interval_ms = output.snapshot.sample_interval_ms;
        filter_disk(&mut output.snapshot.disk_io, &self.config.disk_io.disks);
        filter_network(
            &mut output.snapshot.network_io,
            &self.config.network_io.interfaces,
        );
        filter_local_ip(&mut output.snapshot.ip, &self.config.local_ip.interfaces);
        Ok(output)
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn cancellation_mode(&self) -> crate::scheduler::TaskCancellationMode {
        crate::scheduler::TaskCancellationMode::NonCancellable
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        scheduler::ValueTask,
        tasks::collect::{
            CpuTask, DiskIoTaskConfig, DiskSelection, InterfaceSelection, LocalIpTaskConfig,
            NetworkIoTaskConfig, context,
        },
    };

    use super::{SystemTask, SystemTaskConfig};

    #[tokio::test]
    async fn system_task_reuses_snapshot_metadata_and_isolates_cpu_state() {
        let system_task = SystemTask::new();
        let cpu_task = CpuTask::new();

        let system = system_task.run(context()).await.unwrap();
        let cpu = cpu_task.run(context()).await.unwrap();

        assert_eq!(system_task.kind(), SystemTask::KIND);
        assert_eq!(system.sampled_at_ms, system.snapshot.sampled_at_ms);
        assert_eq!(
            system.sample_interval_ms,
            system.snapshot.sample_interval_ms
        );
        assert!(!cpu.snapshot.warmed_up);
    }

    #[tokio::test]
    async fn configured_system_task_applies_nested_resource_selections() {
        let missing_interface = InterfaceSelection {
            include: vec!["smalux-missing-interface".to_owned()],
            exclude: Vec::new(),
        };
        let task = SystemTask::with_config(SystemTaskConfig {
            disk_io: DiskIoTaskConfig {
                disks: DiskSelection {
                    include_names: vec!["smalux-missing-disk".to_owned()],
                    ..DiskSelection::default()
                },
            },
            network_io: NetworkIoTaskConfig {
                interfaces: missing_interface.clone(),
            },
            local_ip: LocalIpTaskConfig {
                interfaces: missing_interface,
            },
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.config().disk_io.disks.include_names.len(), 1);
        assert!(output.snapshot.disk_io.devices.is_empty());
        assert!(output.snapshot.network_io.interfaces.is_empty());
        assert!(output.snapshot.ip.local.is_empty());
    }
}
