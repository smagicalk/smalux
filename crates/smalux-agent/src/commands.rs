//! 非常驻 CLI 子命令的执行和终端格式化。
//!
//! 这里不创建 Client 或 Scheduler。在线查询只连接本地管理 IPC；Task 能力和身份摘要
//! 使用本地只读来源，因此 Server 不可达时仍可用于诊断启动参数。

use std::path::PathBuf;

use serde::Serialize;
use smalux_agent::{
    client::{AgentStateStore, FileAgentStateStore},
    management::{
        self, ControlRequest, ControlResponse, JobPolicyMutation, JobSnapshot, StatusSnapshot,
    },
    remote_jobs::RemoteJobPolicyManager,
    tasks::{builtin_task_capabilities, find_task_capability},
};

use crate::cli::{
    self, CliCommand, ConfigCommand, IdentityCommand, JobListArgs, JobPolicyCommand,
    JobStateFilter, JobsCommand, OutputFormat, PluginsCommand, StatusArgs, TasksCommand,
};

pub async fn execute(control_endpoint: PathBuf, command: CliCommand) -> anyhow::Result<()> {
    match command {
        CliCommand::Run(_) => unreachable!("run is handled by the process entrypoint"),
        CliCommand::Status(args) => status(control_endpoint, args).await,
        CliCommand::Jobs { command } => jobs(control_endpoint, command).await,
        CliCommand::Tasks { command } => tasks(command),
        CliCommand::Plugins { command } => plugins(control_endpoint, command).await,
        CliCommand::Config { command } => config(control_endpoint, command).await,
        CliCommand::Identity { command } => identity(command).await,
    }
}

async fn status(endpoint: PathBuf, args: StatusArgs) -> anyhow::Result<()> {
    loop {
        let response = query(&endpoint, ControlRequest::Status).await?;
        let ControlResponse::Status(status) = response else {
            return unexpected(response);
        };
        print_status(&status, args.output)?;
        if !args.watch {
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(args.interval) => {}
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
        }
    }
}

async fn jobs(endpoint: PathBuf, command: JobsCommand) -> anyhow::Result<()> {
    match command {
        JobsCommand::List(args) => {
            let response = query(&endpoint, ControlRequest::ListJobs).await?;
            let ControlResponse::Jobs(mut jobs) = response else {
                return unexpected(response);
            };
            filter_jobs(&mut jobs, &args);
            print_jobs(&jobs, args.output)
        }
        JobsCommand::Show { job_id, output } => {
            let response = query(&endpoint, ControlRequest::GetJob { job_id }).await?;
            let ControlResponse::Job(job) = response else {
                return unexpected(response);
            };
            let job = job.ok_or_else(|| anyhow::anyhow!("Job `{job_id}` was not found"))?;
            print_value(&job, output)
        }
        JobsCommand::Policy { command } => job_policy(endpoint, command).await,
    }
}

async fn job_policy(endpoint: PathBuf, command: JobPolicyCommand) -> anyhow::Result<()> {
    if let JobPolicyCommand::Repair { reset: true } = command {
        anyhow::ensure!(
            !management::endpoint_is_active(&endpoint).await,
            "Agent is running; stop it before repairing the policy file"
        );
        let path = cli::default_policy_file()?;
        let backup = RemoteJobPolicyManager::repair_file(&path).await?;
        println!("reset Agent Job policy at {}", path.display());
        if let Some(backup) = backup {
            println!("backup: {}", backup.display());
        }
        return Ok(());
    }

    let (request, output) = match command {
        JobPolicyCommand::Show(args) => (ControlRequest::JobPolicy, args.output),
        JobPolicyCommand::AddTask { task_kind, output } => {
            anyhow::ensure!(
                find_task_capability(&task_kind)
                    .is_some_and(|capability| capability.kind == task_kind),
                "unknown Task kind `{task_kind}`; run `smalux-agent tasks list` for valid values"
            );
            (
                ControlRequest::UpdateJobPolicy {
                    change: JobPolicyMutation::AddTask(task_kind),
                },
                output.output,
            )
        }
        JobPolicyCommand::RemoveTask { task_kind, output } => (
            ControlRequest::UpdateJobPolicy {
                change: JobPolicyMutation::RemoveTask(task_kind),
            },
            output.output,
        ),
        JobPolicyCommand::DenyAll(args) => (
            ControlRequest::UpdateJobPolicy {
                change: JobPolicyMutation::DenyAll,
            },
            args.output,
        ),
        JobPolicyCommand::AllowAll(args) => (
            ControlRequest::UpdateJobPolicy {
                change: JobPolicyMutation::AllowAll,
            },
            args.output,
        ),
        JobPolicyCommand::Repair { reset: false } => unreachable!("Clap requires --reset"),
        JobPolicyCommand::Repair { reset: true } => unreachable!("handled above"),
    };
    match query(&endpoint, request).await? {
        ControlResponse::JobPolicy(policy) => print_value(&policy, output),
        ControlResponse::JobPolicyUpdated {
            policy,
            affected_jobs,
        } => print_value(
            &serde_json::json!({ "policy": policy, "affected_jobs": affected_jobs }),
            output,
        ),
        response => unexpected(response),
    }
}

fn tasks(command: TasksCommand) -> anyhow::Result<()> {
    match command {
        TasksCommand::List { source, output } => {
            let capabilities = builtin_task_capabilities()
                .iter()
                .filter(|capability| source.is_none_or(|source| source.matches(capability.source)))
                .collect::<Vec<_>>();
            if matches!(output, OutputFormat::Json) {
                println!("{}", serde_json::to_string_pretty(&capabilities)?);
            } else {
                println!("{:<36} {:<12} SOURCE", "TASK KIND", "SHORT NAME");
                for capability in capabilities {
                    println!(
                        "{:<36} {:<12} {:?}",
                        capability.kind, capability.short_name, capability.source
                    );
                }
            }
            Ok(())
        }
        TasksCommand::Show { task_kind, output } => {
            let capability = find_task_capability(&task_kind)
                .ok_or_else(|| anyhow::anyhow!("Task kind `{task_kind}` is not available"))?;
            print_value(capability, output)
        }
    }
}

async fn plugins(endpoint: PathBuf, command: PluginsCommand) -> anyhow::Result<()> {
    let (request, output) = match command {
        PluginsCommand::List { output } => (ControlRequest::ListPlugins, output),
        PluginsCommand::Show { plugin_id, output } => {
            (ControlRequest::GetPlugin { plugin_id }, output)
        }
    };
    let response = query(&endpoint, request).await?;
    match response {
        ControlResponse::Plugins(plugins) => print_value(&plugins, output),
        ControlResponse::Plugin(plugin) => print_value(&plugin, output),
        ControlResponse::PluginsUnavailable { message } => {
            if matches!(output, OutputFormat::Json) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "status": "unavailable",
                        "message": message,
                    }))?
                );
            } else {
                println!("{message}");
            }
            Ok(())
        }
        response => unexpected(response),
    }
}

async fn config(endpoint: PathBuf, command: ConfigCommand) -> anyhow::Result<()> {
    let ConfigCommand::Show { output } = command;
    let response = query(&endpoint, ControlRequest::EffectiveConfig).await?;
    let ControlResponse::EffectiveConfig(config) = response else {
        return unexpected(response);
    };
    print_value(&config, output)
}

async fn identity(command: IdentityCommand) -> anyhow::Result<()> {
    let IdentityCommand::Show { state_file, output } = command;
    let state_file = state_file.unwrap_or(cli::default_state_file()?);
    let store = FileAgentStateStore::new(&state_file);
    let state = store.load().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "Agent identity state does not exist at {}",
            state_file.display()
        )
    })?;
    #[derive(Serialize)]
    struct IdentitySummary {
        state_file: PathBuf,
        agent_id: Option<String>,
        registration_stage: String,
        agent_public_key_id: String,
        server_public_key_ids: Vec<String>,
    }
    let summary = IdentitySummary {
        state_file,
        agent_id: state.agent_id().map(str::to_owned),
        registration_stage: match state.stage() {
            smalux_agent::client::RegistrationStage::IdentityPrepared => "identity_prepared",
            smalux_agent::client::RegistrationStage::RegistrationPending => "registration_pending",
            smalux_agent::client::RegistrationStage::Registered => "registered",
        }
        .to_owned(),
        agent_public_key_id: format!("{:?}", state.identity().public_key().key_id()),
        server_public_key_ids: state
            .server_public_keys()
            .iter()
            .map(|key| format!("{:?}", key.key_id()))
            .collect(),
    };
    print_value(&summary, output)
}

async fn query(
    endpoint: &std::path::Path,
    request: ControlRequest,
) -> anyhow::Result<ControlResponse> {
    let response = management::request(endpoint, request)
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "could not query running Agent at {}: {error:#}",
                endpoint.display()
            )
        })?;
    if let ControlResponse::Error { code, message } = &response {
        anyhow::bail!("Agent control request failed ({code}): {message}");
    }
    Ok(response)
}

fn filter_jobs(jobs: &mut Vec<JobSnapshot>, args: &JobListArgs) {
    jobs.retain(|job| {
        let state_matches = match args.state {
            None => true,
            Some(JobStateFilter::Enabled) => job.state == "enabled",
            Some(JobStateFilter::Completed) => job.state == "completed",
            Some(JobStateFilter::Disabled) => job.state == "disabled",
        };
        state_matches
            && (!args.running || job.running_count > 0)
            && args
                .task_kind
                .as_ref()
                .is_none_or(|kind| &job.task_kind == kind)
    });
}

fn print_status(status: &StatusSnapshot, output: OutputFormat) -> anyhow::Result<()> {
    if matches!(output, OutputFormat::Json) {
        return print_value(status, output);
    }
    println!(
        "Agent ID:           {}",
        status.agent_id.as_deref().unwrap_or("not_registered")
    );
    println!(
        "Registration:       {}",
        status.registration_stage.as_deref().unwrap_or("none")
    );
    println!("Connection:         {}", status.connection_status);
    println!(
        "Authentication:     {}",
        status.authentication_mode.as_deref().unwrap_or("none")
    );
    println!("Scheduler:          {}", status.scheduler_status);
    println!(
        "Jobs:               {} (running {}, pending {})",
        status.job_count, status.running_jobs, status.pending_runs
    );
    println!(
        "Job results:        {} pending, {} dropped",
        status.pending_job_results, status.dropped_job_results
    );
    println!(
        "Heartbeat RTT:      {}",
        status
            .heartbeat_rtt_ms
            .map(|value| format!("{value} ms"))
            .unwrap_or_else(|| "n/a".to_owned())
    );
    println!("Plugin subsystem:   {}", status.plugin_subsystem);
    println!("Uptime:             {} ms", status.uptime_ms);
    if let Some(reason) = &status.last_disconnect_reason {
        println!("Last disconnect:    {reason}");
    }
    Ok(())
}

fn print_jobs(jobs: &[JobSnapshot], output: OutputFormat) -> anyhow::Result<()> {
    if matches!(output, OutputFormat::Json) {
        return print_value(jobs, output);
    }
    println!(
        "{:<36} {:<8} {:<34} {:<10} {:>7} {:>7}",
        "JOB ID", "SOURCE", "TASK KIND", "STATE", "RUNNING", "PENDING"
    );
    for job in jobs {
        println!(
            "{:<36} {:<8} {:<34} {:<10} {:>7} {:>7}",
            job.job_id, job.source, job.task_kind, job.state, job.running_count, job.pending_count
        );
    }
    Ok(())
}

fn print_value<T: Serialize + ?Sized>(value: &T, output: OutputFormat) -> anyhow::Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(value)?),
        OutputFormat::Table => println!("{}", serde_json::to_string_pretty(value)?),
    }
    Ok(())
}

fn unexpected<T>(response: ControlResponse) -> anyhow::Result<T> {
    anyhow::bail!("Agent returned an unexpected control response: {response:?}")
}
