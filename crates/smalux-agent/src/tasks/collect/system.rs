//! 完整本机指标周期采集任务。

use async_trait::async_trait;
pub use smalux_protocol::agent::v1::SystemTaskConfig;
use smalux_protocol::agent::v1::{SampleMetadata, SystemSnapshot, TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::{HostMetricsCollector, SystemSnapshot as CollectedSystemSnapshot},
};

use super::{
    blocking::CollectState,
    process::into_proto_snapshot as into_proto_process_snapshot,
    selection::{filter_disk, filter_local_ip, filter_network},
    socket::into_proto_snapshot as into_proto_socket_snapshot,
};

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
impl ReportingTask for SystemTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let mut output = self
            .state
            .collect(context, HostMetricsCollector::collect)
            .await?;
        output.sampled_at_ms = output.snapshot.sampled_at_ms;
        output.sample_interval_ms = output.snapshot.sample_interval_ms;
        if let Some(selection) = self
            .config
            .disk_io
            .as_ref()
            .and_then(|config| config.disks.as_ref())
        {
            filter_disk(&mut output.snapshot.disk_io, selection);
        }
        if let Some(selection) = self
            .config
            .network_io
            .as_ref()
            .and_then(|config| config.interfaces.as_ref())
        {
            filter_network(&mut output.snapshot.network_io, selection);
        }
        if let Some(selection) = self
            .config
            .local_ip
            .as_ref()
            .and_then(|config| config.interfaces.as_ref())
        {
            filter_local_ip(&mut output.snapshot.ip, selection);
        }
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::System(Box::new(into_proto_snapshot(
                output.snapshot,
            )))),
        })
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn cancellation_mode(&self) -> crate::scheduler::TaskCancellationMode {
        crate::scheduler::TaskCancellationMode::NonCancellable
    }
}

fn into_proto_snapshot(snapshot: CollectedSystemSnapshot) -> SystemSnapshot {
    SystemSnapshot {
        host: Some(snapshot.host),
        cpu: Some(snapshot.cpu),
        memory: Some(snapshot.memory),
        load: Some(snapshot.load),
        disk_io: Some(snapshot.disk_io),
        network_io: Some(snapshot.network_io),
        ip: Some(snapshot.ip),
        sockets: Some(into_proto_socket_snapshot(snapshot.sockets)),
        processes: Some(into_proto_process_snapshot(snapshot.processes)),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        scheduler::ReportingTask,
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
        let sample = system.sample.expect("system sample metadata is required");
        assert!(sample.sampled_at_ms > 0);
        let Some(smalux_protocol::agent::v1::task_result::Result::System(system)) = system.result
        else {
            panic!("system task must return TaskResult.system");
        };
        assert!(system.cpu.is_some());
        let Some(smalux_protocol::agent::v1::task_result::Result::Cpu(cpu)) = cpu.result else {
            panic!("CPU task must return TaskResult.cpu");
        };
        assert!(!cpu.warmed_up);
    }

    #[tokio::test]
    async fn configured_system_task_applies_nested_resource_selections() {
        let missing_interface = InterfaceSelection {
            include: vec!["smalux-missing-interface".to_owned()],
            exclude: Vec::new(),
        };
        let task = SystemTask::with_config(SystemTaskConfig {
            disk_io: Some(DiskIoTaskConfig {
                disks: Some(DiskSelection {
                    include_names: vec!["smalux-missing-disk".to_owned()],
                    ..DiskSelection::default()
                }),
            }),
            network_io: Some(NetworkIoTaskConfig {
                interfaces: Some(missing_interface.clone()),
            }),
            local_ip: Some(LocalIpTaskConfig {
                interfaces: Some(missing_interface),
            }),
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(
            task.config()
                .disk_io
                .as_ref()
                .expect("disk config is configured")
                .disks
                .as_ref()
                .expect("disk selection is configured")
                .include_names
                .len(),
            1
        );
        let Some(smalux_protocol::agent::v1::task_result::Result::System(snapshot)) = output.result
        else {
            panic!("system task must return TaskResult.system");
        };
        assert!(snapshot.disk_io.expect("disk snapshot").devices.is_empty());
        assert!(
            snapshot
                .network_io
                .expect("network snapshot")
                .interfaces
                .is_empty()
        );
        assert!(snapshot.ip.expect("IP snapshot").local.is_empty());
    }
}
