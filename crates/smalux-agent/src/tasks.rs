//! Agent 具体任务入口。

mod capability;
pub mod collect;

pub use capability::{
    TaskCapability, TaskSource, agent_capability_snapshot, builtin_task_capabilities,
    find_task_capability, task_kind,
};

#[cfg(test)]
mod capability_contract_tests {
    use super::{agent_capability_snapshot, builtin_task_capabilities, task_kind};
    use smalux_protocol::agent::v1::{
        CpuTaskConfig, ProbeProtocol, TaskDefinition, task_definition,
    };

    #[test]
    fn capability_registry_resolves_proto_task_to_listed_stable_kind() {
        let definition = TaskDefinition {
            task: Some(task_definition::Task::Cpu(CpuTaskConfig {})),
        };
        let kind = task_kind(&definition).expect("CPU task kind");

        assert_eq!(kind, "smalux.collect.cpu.v1");
        assert!(
            builtin_task_capabilities()
                .iter()
                .any(|capability| capability.kind == kind)
        );
    }

    #[test]
    fn capability_registry_contains_unique_stable_kinds_for_every_builtin_branch() {
        let capabilities = builtin_task_capabilities();
        assert_eq!(capabilities.len(), 12);
        let unique = capabilities
            .iter()
            .map(|capability| capability.kind)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), capabilities.len());
        assert!(capabilities.iter().all(|capability| {
            capability.kind.starts_with("smalux.") && capability.kind.ends_with(".v1")
        }));
    }

    #[test]
    fn protocol_capability_snapshot_is_stable_sorted_and_complete() {
        let snapshot = agent_capability_snapshot();
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.agent_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(snapshot.task_kinds.len(), builtin_task_capabilities().len());
        assert!(snapshot.task_kinds.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            snapshot.probe_protocols,
            vec![
                ProbeProtocol::IcmpEcho as i32,
                ProbeProtocol::TcpConnect as i32,
                ProbeProtocol::Http as i32,
                ProbeProtocol::UdpRequest as i32,
            ]
        );
    }
}
