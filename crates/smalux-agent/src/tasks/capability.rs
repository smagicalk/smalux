//! Agent 可执行 Task 的稳定能力目录。
//!
//! 本模块是 Proto Task 分支、本地实现名称、CLI 列表和远程 Job 策略之间的唯一映射。
//! 增加 Task 时必须先在这里登记，避免各层分别维护字符串而产生绕过或显示不一致。

use serde::Serialize;
use smalux_protocol::agent::v1::{
    AgentCapabilitySnapshot, ProbeProtocol, TaskDefinition, task_definition,
};

const CAPABILITY_REVISION: u64 = 1;

use super::collect::{
    CpuTask, DiskIoTask, HostTask, LoadTask, LocalIpTask, MemoryTask, NetworkIoTask, ProbeTask,
    ProcessTask, PublicIpTask, SocketTask, SystemTask,
};

/// Task 实现来源；插件运行时接入后会出现 `Plugin` 条目。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskSource {
    /// 编译进 Agent 的固定实现。
    Builtin,
    /// 由受信任 Plus 插件提供的实现。
    Plugin,
}

/// 可供 Job 引用和本地策略匹配的一项 Task 能力。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct TaskCapability {
    /// 跨版本稳定的完整标识，也是 `jobs policy add-task` 接受的值。
    pub kind: &'static str,
    /// 只用于终端展示和搜索的短名称，不参与策略判断。
    pub short_name: &'static str,
    /// 当前能力来自 Agent 内置代码还是动态插件。
    pub source: TaskSource,
}

const BUILTIN_TASKS: &[TaskCapability] = &[
    builtin(SystemTask::KIND, "system"),
    builtin(CpuTask::KIND, "cpu"),
    builtin(MemoryTask::KIND, "memory"),
    builtin(LoadTask::KIND, "load"),
    builtin(HostTask::KIND, "host"),
    builtin(DiskIoTask::KIND, "disk_io"),
    builtin(NetworkIoTask::KIND, "network_io"),
    builtin(LocalIpTask::KIND, "local_ip"),
    builtin(PublicIpTask::KIND, "public_ip"),
    builtin(ProcessTask::KIND, "process"),
    builtin(SocketTask::KIND, "socket"),
    builtin(ProbeTask::KIND, "probe"),
];

const fn builtin(kind: &'static str, short_name: &'static str) -> TaskCapability {
    TaskCapability {
        kind,
        short_name,
        source: TaskSource::Builtin,
    }
}

/// 返回当前二进制内置的全部 Task 能力。
pub fn builtin_task_capabilities() -> &'static [TaskCapability] {
    BUILTIN_TASKS
}

/// 构造发送给 Server 的当前二进制完整能力快照。
///
/// 列表在发送前排序，确保相同构建在每次连接和不同平台上产生完全一致的内容。
pub fn agent_capability_snapshot() -> AgentCapabilitySnapshot {
    let mut task_kinds = BUILTIN_TASKS
        .iter()
        .map(|capability| capability.kind.to_owned())
        .collect::<Vec<_>>();
    task_kinds.sort_unstable();

    AgentCapabilitySnapshot {
        revision: CAPABILITY_REVISION,
        agent_version: env!("CARGO_PKG_VERSION").to_owned(),
        task_kinds,
        probe_protocols: vec![
            ProbeProtocol::IcmpEcho as i32,
            ProbeProtocol::TcpConnect as i32,
            ProbeProtocol::Http as i32,
            ProbeProtocol::UdpRequest as i32,
        ],
    }
}

/// 按完整稳定标识或展示短名称查找能力。
pub fn find_task_capability(value: &str) -> Option<&'static TaskCapability> {
    BUILTIN_TASKS
        .iter()
        .find(|capability| capability.kind == value || capability.short_name == value)
}

/// 从解码后的 Proto TaskDefinition 返回对应的稳定 Task 标识。
///
/// `None` 表示 Proto oneof 为空，而不是未知插件；未知字段会由 Prost 在解码时忽略。
pub fn task_kind(definition: &TaskDefinition) -> Option<String> {
    use task_definition::Task;

    Some(match definition.task.as_ref()? {
        Task::System(_) => SystemTask::KIND.to_owned(),
        Task::Cpu(_) => CpuTask::KIND.to_owned(),
        Task::Memory(_) => MemoryTask::KIND.to_owned(),
        Task::Load(_) => LoadTask::KIND.to_owned(),
        Task::Host(_) => HostTask::KIND.to_owned(),
        Task::DiskIo(_) => DiskIoTask::KIND.to_owned(),
        Task::NetworkIo(_) => NetworkIoTask::KIND.to_owned(),
        Task::LocalIp(_) => LocalIpTask::KIND.to_owned(),
        Task::PublicIp(_) => PublicIpTask::KIND.to_owned(),
        Task::Process(_) => ProcessTask::KIND.to_owned(),
        Task::Socket(_) => SocketTask::KIND.to_owned(),
        Task::Probe(_) => ProbeTask::KIND.to_owned(),
        // 插件 Task 的完整 kind 来自已校验的运行时配置。
        Task::Plugin(config) if !config.task_kind.is_empty() => config.task_kind.clone(),
        Task::Plugin(_) => return None,
    })
}
