//! Job 与 Task protobuf 公共模型的行为测试。

use prost::Message;
use smalux_protocol::agent::v1::{
    CollectionMode, CpuTaskConfig, JobCommand, JobCommandResult, JobCommandStatus, JobDefinition,
    ProcessDetails, ProcessEntry, ProcessRanking, ProcessSelection, ProcessSnapshot,
    ProcessTaskConfig, TaskDefinition, TaskResult, UpsertJob, job_command, task_definition,
    task_result,
};

#[test]
fn job_definition_round_trips_a_typed_cpu_task() {
    let definition = JobDefinition {
        job_id: vec![7; 16],
        revision: 1,
        enabled: true,
        trigger: None,
        options: None,
        task: Some(TaskDefinition {
            task: Some(task_definition::Task::Cpu(CpuTaskConfig {})),
        }),
    };

    let decoded = JobDefinition::decode(definition.encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded, definition);
}

#[test]
fn process_config_and_result_round_trip_detailed_fields() {
    let config = ProcessTaskConfig {
        mode: CollectionMode::Detailed as i32,
        selection: Some(ProcessSelection {
            include_pids: vec![42],
            include_names: vec!["worker".to_owned()],
            exclude_names: Vec::new(),
        }),
        ranking: ProcessRanking::Memory as i32,
        max_entries: Some(8),
    };
    let definition = TaskDefinition {
        task: Some(task_definition::Task::Process(config)),
    };
    assert_eq!(
        TaskDefinition::decode(definition.encode_to_vec().as_slice()).unwrap(),
        definition
    );

    let result = TaskResult {
        sample: None,
        result: Some(task_result::Result::Process(ProcessSnapshot {
            mode: CollectionMode::Detailed as i32,
            total_processes: 1,
            matched_processes: 1,
            states: Vec::new(),
            entries: vec![ProcessEntry {
                pid: 42,
                parent_pid: None,
                name: "worker".to_owned(),
                state: 0,
                started_at_seconds: 10,
                details: Some(ProcessDetails {
                    cpu_usage_percent: 12.5,
                    memory_bytes: 1024,
                    virtual_memory_bytes: 2048,
                    read_bytes: 1,
                    written_bytes: 2,
                    total_read_bytes: 3,
                    total_written_bytes: 4,
                    executable: Some("worker.exe".to_owned()),
                    command: vec!["worker".to_owned()],
                }),
            }],
            truncated: false,
            cpu_warmed_up: true,
        })),
    };
    assert_eq!(
        TaskResult::decode(result.encode_to_vec().as_slice()).unwrap(),
        result
    );
}

#[test]
fn upsert_command_preserves_catalog_and_command_identity() {
    let definition = JobDefinition {
        job_id: vec![9; 16],
        revision: 3,
        enabled: true,
        trigger: None,
        options: None,
        task: Some(TaskDefinition {
            task: Some(task_definition::Task::Cpu(CpuTaskConfig {})),
        }),
    };
    let command = JobCommand {
        command_id: vec![5; 16],
        action: Some(job_command::Action::Upsert(Box::new(UpsertJob {
            catalog_revision: 11,
            job: Some(definition),
        }))),
    };

    let decoded = JobCommand::decode(command.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, command);

    let result = JobCommandResult {
        command_id: command.command_id,
        status: JobCommandStatus::Applied as i32,
        catalog_revision: 11,
        job: None,
        error: None,
    };
    assert_eq!(result.status(), JobCommandStatus::Applied);
}
