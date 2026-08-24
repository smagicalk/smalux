//! 已认证 Agent 的长期业务循环。
//!
//! Server 先查询 Agent 的本地 Job 策略和插件 inventory。插件运行时快照得到 Agent
//! 确认后，才向 Provider 请求可下发目录，避免 Job 先于对应 Worker 到达。

use smalux_protocol::{
    agent::v1::{
        AgentCapabilityQuery, AgentCapabilitySnapshot, AgentCapabilitySync, AgentJobPolicyAck,
        AgentJobPolicyQuery, AgentJobPolicySnapshot, AgentJobPolicySync, AgentPluginInventory,
        AgentPluginQuery, AgentPluginSync, JobCommand, PluginPauseAck, PluginRuntimeSnapshot,
        ProbeProtocol, ReplaceAllJobs, SecureError, SecureErrorCode, SecureMessage,
        agent_capability_sync, agent_job_policy_sync, agent_plugin_sync, job_command,
        secure_message,
    },
    tonic_transport::{SessionEvent, TonicNoiseSession, TransportError},
};

use super::super::AgentTransportService;

const MAX_DENIED_TASK_KINDS: usize = 1024;
const MAX_TASK_KIND_BYTES: usize = 256;
const MAX_CAPABILITY_TASK_KINDS: usize = 4096;
const MAX_AGENT_VERSION_BYTES: usize = 128;
const MAX_INSTALLED_PLUGINS: usize = 1024;
const MAX_PLUGIN_ID_BYTES: usize = 256;
const MAX_PLUGIN_VERSION_BYTES: usize = 64;

impl AgentTransportService {
    pub(super) async fn run_business_session(
        &self,
        session_id: u64,
        agent_id: &str,
        session: &mut TonicNoiseSession,
    ) -> Result<(), TransportError> {
        session
            .send_agent_job_policy(AgentJobPolicySync {
                body: Some(agent_job_policy_sync::Body::Query(AgentJobPolicyQuery {})),
            })
            .await?;
        session
            .send_agent_capability(AgentCapabilitySync {
                body: Some(agent_capability_sync::Body::Query(AgentCapabilityQuery {})),
            })
            .await?;
        session
            .send_agent_plugin(AgentPluginSync {
                body: Some(agent_plugin_sync::Body::Query(AgentPluginQuery {})),
            })
            .await?;
        let mut accepted_policy: Option<AgentJobPolicySnapshot> = None;
        let mut accepted_capability: Option<AgentCapabilitySnapshot> = None;
        let mut sent_plugin_runtime: Option<PluginRuntimeSnapshot> = None;
        let mut plugin_runtime_ready = false;
        let mut accepted_plugin_inventory: Option<AgentPluginInventory> = None;
        let mut pending_schema_hashes: Vec<Vec<u8>> = Vec::new();
        let mut last_sent_catalog: Option<ReplaceAllJobs> = None;

        while let Some(event) = session.receive_event().await? {
            self.state.sessions.touch(session_id).await;
            match event {
                SessionEvent::AgentJobPolicy(message) => {
                    let Some(agent_job_policy_sync::Body::Snapshot(snapshot)) = message.body else {
                        send_invalid_message(
                            session,
                            "Server requires an Agent Job policy snapshot",
                        )
                        .await?;
                        continue;
                    };
                    // 延迟到达的旧快照不改变 Server 视图；重发当前 ACK 让 Agent 尽快收敛。
                    if let Some(current) = accepted_policy.as_ref()
                        && snapshot.revision < current.revision
                    {
                        session
                            .send_agent_job_policy(policy_acknowledgement(current.revision))
                            .await?;
                        continue;
                    }
                    if let Err(reason) =
                        validate_policy_snapshot(&snapshot, accepted_policy.as_ref())
                    {
                        tracing::warn!(session_id, %reason, "Agent Job policy snapshot was rejected");
                        send_invalid_message(session, reason).await?;
                        continue;
                    }
                    let unchanged = accepted_policy.as_ref() == Some(&snapshot);
                    accepted_policy = Some(snapshot.clone());
                    session
                        .send_agent_job_policy(policy_acknowledgement(snapshot.revision))
                        .await?;
                    tracing::info!(
                        session_id,
                        revision = snapshot.revision,
                        deny_all = snapshot.deny_all,
                        denied_task_kinds = snapshot.denied_task_kinds.len(),
                        "accepted Agent Job policy snapshot"
                    );
                    if !unchanged && plugin_runtime_ready && accepted_capability.is_some() {
                        self.send_job_catalog(
                            session_id,
                            agent_id,
                            session,
                            &snapshot,
                            &mut last_sent_catalog,
                        )
                        .await?;
                    }
                }
                SessionEvent::AgentCapability(message) => {
                    let Some(agent_capability_sync::Body::Snapshot(snapshot)) = message.body else {
                        send_invalid_message(
                            session,
                            "Server requires an Agent capability snapshot",
                        )
                        .await?;
                        continue;
                    };
                    if let Err(reason) = validate_capability_snapshot(&snapshot) {
                        tracing::warn!(session_id, %reason, "Agent capability snapshot was rejected");
                        send_invalid_message(session, reason).await?;
                        continue;
                    }
                    tracing::info!(
                        session_id,
                        revision = snapshot.revision,
                        agent_version = %snapshot.agent_version,
                        task_kinds = snapshot.task_kinds.len(),
                        probe_protocols = snapshot.probe_protocols.len(),
                        "accepted Agent capability snapshot"
                    );
                    accepted_capability = Some(snapshot);
                    if plugin_runtime_ready && let Some(policy) = accepted_policy.as_ref() {
                        self.send_job_catalog(
                            session_id,
                            agent_id,
                            session,
                            policy,
                            &mut last_sent_catalog,
                        )
                        .await?;
                    }
                }
                SessionEvent::AgentPlugin(message) => match message.body {
                    Some(agent_plugin_sync::Body::Inventory(inventory)) => {
                        if let Err(reason) = validate_plugin_inventory(&inventory) {
                            tracing::warn!(session_id, %reason, "Agent plugin inventory was rejected");
                            send_invalid_message(session, reason).await?;
                            continue;
                        }
                        accepted_plugin_inventory = Some(inventory.clone());
                        pending_schema_hashes = match self
                            .state
                            .plugin_schemas
                            .missing_for_inventory(&inventory)
                            .await
                        {
                            Ok(missing) => missing,
                            Err(error) => {
                                tracing::warn!(session_id, %error, "Agent plugin Schema inventory was rejected");
                                send_invalid_message(
                                    session,
                                    "Agent plugin Schema inventory is invalid",
                                )
                                .await?;
                                continue;
                            }
                        };
                        sent_plugin_runtime = None;
                        plugin_runtime_ready = false;
                        if pending_schema_hashes.is_empty() {
                            self.send_plugin_runtime_snapshot(
                                session_id,
                                agent_id,
                                session,
                                &inventory,
                                &mut sent_plugin_runtime,
                                &mut plugin_runtime_ready,
                            )
                            .await?;
                        } else {
                            session
                                .send_agent_plugin(AgentPluginSync {
                                    body: Some(agent_plugin_sync::Body::SchemaQuery(
                                        smalux_protocol::agent::v1::PluginSchemaQuery {
                                            schema_hashes: pending_schema_hashes.clone(),
                                        },
                                    )),
                                })
                                .await?;
                            tracing::info!(
                                session_id,
                                inventory_revision = inventory.revision,
                                missing_schemas = pending_schema_hashes.len(),
                                "requested missing Agent plugin Schemas"
                            );
                        }
                    }
                    Some(agent_plugin_sync::Body::SchemaResponse(response)) => {
                        let Some(inventory) = accepted_plugin_inventory.as_ref() else {
                            send_invalid_message(
                                session,
                                "Agent plugin Schema response arrived before inventory",
                            )
                            .await?;
                            continue;
                        };
                        let Some(index) = pending_schema_hashes
                            .iter()
                            .position(|hash| hash.as_slice() == response.schema_hash.as_slice())
                        else {
                            send_invalid_message(
                                session,
                                "Agent plugin Schema response was not requested",
                            )
                            .await?;
                            continue;
                        };
                        if let Err(error) = self
                            .state
                            .plugin_schemas
                            .store_response(
                                inventory,
                                &response.schema_hash,
                                &response.schema_bundle,
                            )
                            .await
                        {
                            tracing::warn!(session_id, %error, "Agent plugin Schema response was rejected");
                            send_invalid_message(
                                session,
                                "Agent plugin Schema response is invalid",
                            )
                            .await?;
                            continue;
                        }
                        pending_schema_hashes.swap_remove(index);
                        tracing::info!(
                            session_id,
                            remaining_schemas = pending_schema_hashes.len(),
                            "stored Agent plugin Schema"
                        );
                        if pending_schema_hashes.is_empty() {
                            self.send_plugin_runtime_snapshot(
                                session_id,
                                agent_id,
                                session,
                                inventory,
                                &mut sent_plugin_runtime,
                                &mut plugin_runtime_ready,
                            )
                            .await?;
                        }
                    }
                    Some(agent_plugin_sync::Body::Acknowledgement(ack)) => {
                        let Some(sent) = sent_plugin_runtime.as_ref() else {
                            send_invalid_message(
                                session,
                                "Agent plugin acknowledgement arrived before runtime snapshot",
                            )
                            .await?;
                            continue;
                        };
                        if ack.revision != sent.revision {
                            send_invalid_message(
                                session,
                                "Agent plugin acknowledgement has an unexpected revision",
                            )
                            .await?;
                            continue;
                        }
                        if !ack.accepted {
                            tracing::warn!(
                                session_id,
                                revision = ack.revision,
                                error = ?ack.error,
                                "Agent rejected plugin runtime snapshot; Job catalog remains blocked"
                            );
                            continue;
                        }
                        plugin_runtime_ready = true;
                        tracing::info!(
                            session_id,
                            revision = ack.revision,
                            "Agent plugin runtime is ready"
                        );
                        if accepted_capability.is_some()
                            && let Some(policy) = accepted_policy.as_ref()
                        {
                            self.send_job_catalog(
                                session_id,
                                agent_id,
                                session,
                                policy,
                                &mut last_sent_catalog,
                            )
                            .await?;
                        }
                    }
                    Some(agent_plugin_sync::Body::PauseNotice(notice)) => {
                        let valid_runtime = sent_plugin_runtime.as_ref().is_some_and(|snapshot| {
                            snapshot.revision == notice.runtime_revision
                                && snapshot.plugins.iter().any(|plugin| {
                                    plugin.plugin_id == notice.plugin_id
                                        && plugin.version == notice.plugin_version
                                })
                        });
                        if !valid_runtime {
                            session
                                .send_agent_plugin(AgentPluginSync {
                                    body: Some(agent_plugin_sync::Body::PauseAcknowledgement(
                                        PluginPauseAck {
                                            plugin_id: notice.plugin_id,
                                            plugin_version: notice.plugin_version,
                                            runtime_revision: notice.runtime_revision,
                                            accepted: false,
                                            error: Some(
                                                "pause notice does not match the active runtime snapshot"
                                                    .to_owned(),
                                            ),
                                        },
                                    )),
                                })
                                .await?;
                            continue;
                        }
                        let ack = match self
                            .state
                            .plugin_pauses
                            .record(agent_id, notice.clone())
                            .await
                        {
                            Ok(()) => {
                                tracing::warn!(
                                    session_id,
                                    plugin_id = %notice.plugin_id,
                                    plugin_version = %notice.plugin_version,
                                    failure_count = notice.failure_count,
                                    "Agent paused Plus Worker; Server will stop sending its Jobs"
                                );
                                if let Some(catalog) = last_sent_catalog.as_ref() {
                                    let filtered = self
                                        .state
                                        .plugin_pauses
                                        .filter_catalog(agent_id, catalog.clone())
                                        .await;
                                    session
                                        .send_job_command(JobCommand {
                                            command_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                                            action: Some(job_command::Action::ReplaceAll(
                                                filtered.clone(),
                                            )),
                                        })
                                        .await?;
                                    last_sent_catalog = Some(filtered);
                                }
                                PluginPauseAck {
                                    plugin_id: notice.plugin_id,
                                    plugin_version: notice.plugin_version,
                                    runtime_revision: notice.runtime_revision,
                                    accepted: true,
                                    error: None,
                                }
                            }
                            Err(error) => PluginPauseAck {
                                plugin_id: notice.plugin_id,
                                plugin_version: notice.plugin_version,
                                runtime_revision: notice.runtime_revision,
                                accepted: false,
                                error: Some(error.to_string()),
                            },
                        };
                        session
                            .send_agent_plugin(AgentPluginSync {
                                body: Some(agent_plugin_sync::Body::PauseAcknowledgement(ack)),
                            })
                            .await?;
                    }
                    Some(agent_plugin_sync::Body::PauseAcknowledgement(_)) => {
                        send_invalid_message(
                            session,
                            "Server does not accept a Plus pause acknowledgement",
                        )
                        .await?;
                    }
                    _ => {
                        send_invalid_message(
                            session,
                            "Server requires an Agent plugin inventory, Schema response, acknowledgement or pause notice",
                        )
                        .await?;
                    }
                },
                SessionEvent::JobCommandResult(result) => {
                    tracing::debug!(
                        session_id,
                        command_id_len = result.command_id.len(),
                        "received Agent Job command result"
                    );
                }
                SessionEvent::TaskReport(report) => {
                    tracing::debug!(
                        session_id,
                        job_id_len = report.job_id.len(),
                        "received Agent Task report"
                    );
                }
                other => {
                    tracing::warn!(
                        session_id,
                        ?other,
                        "Agent sent an unexpected business message"
                    );
                    send_invalid_message(session, "unexpected Agent business message").await?;
                }
            }
        }
        if accepted_capability.is_none() {
            tracing::warn!(
                session_id,
                "Agent disconnected before sending a capability snapshot"
            );
        }
        tracing::info!(session_id, "Agent encrypted business session disconnected");
        Ok(())
    }

    /// 在策略和插件 Worker 都已确认后，读取并下发权威远程 Job 目录。
    async fn send_job_catalog(
        &self,
        session_id: u64,
        agent_id: &str,
        session: &mut TonicNoiseSession,
        policy: &AgentJobPolicySnapshot,
        last_sent_catalog: &mut Option<ReplaceAllJobs>,
    ) -> Result<(), TransportError> {
        match self.state.job_catalog.load_catalog(agent_id, policy).await {
            Ok(Some(catalog)) => {
                let catalog = self
                    .state
                    .plugin_pauses
                    .filter_catalog(agent_id, catalog)
                    .await;
                session
                    .send_job_command(JobCommand {
                        command_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                        action: Some(job_command::Action::ReplaceAll(catalog.clone())),
                    })
                    .await?;
                *last_sent_catalog = Some(catalog);
            }
            Ok(None) => {
                tracing::debug!(session_id, "Agent Job catalog Provider returned no update")
            }
            Err(error) => tracing::error!(session_id, %error, "failed to load Agent Job catalog"),
        }
        Ok(())
    }

    async fn send_plugin_runtime_snapshot(
        &self,
        session_id: u64,
        agent_id: &str,
        session: &mut TonicNoiseSession,
        inventory: &AgentPluginInventory,
        sent_plugin_runtime: &mut Option<PluginRuntimeSnapshot>,
        plugin_runtime_ready: &mut bool,
    ) -> Result<(), TransportError> {
        match self
            .state
            .plugin_runtime
            .load_runtime(agent_id, inventory)
            .await
        {
            Ok(snapshot) => {
                let revision = snapshot.revision;
                self.state
                    .plugin_pauses
                    .acknowledge_new_runtime(agent_id, &snapshot)
                    .await;
                session
                    .send_agent_plugin(AgentPluginSync {
                        body: Some(agent_plugin_sync::Body::Snapshot(snapshot.clone())),
                    })
                    .await?;
                *sent_plugin_runtime = Some(snapshot);
                *plugin_runtime_ready = false;
                tracing::info!(
                    session_id,
                    inventory_revision = inventory.revision,
                    runtime_revision = revision,
                    plugins = inventory.plugins.len(),
                    "sent Agent plugin runtime snapshot"
                );
            }
            Err(error) => {
                tracing::error!(session_id, %error, "failed to load Agent plugin runtime")
            }
        }
        Ok(())
    }
}

fn validate_capability_snapshot(snapshot: &AgentCapabilitySnapshot) -> Result<(), &'static str> {
    if snapshot.revision == 0 {
        return Err("Agent capability revision must be greater than zero");
    }
    if snapshot.agent_version.is_empty() || snapshot.agent_version.len() > MAX_AGENT_VERSION_BYTES {
        return Err("Agent capability contains an invalid Agent version");
    }
    if snapshot.task_kinds.is_empty() || snapshot.task_kinds.len() > MAX_CAPABILITY_TASK_KINDS {
        return Err("Agent capability contains an invalid Task kind count");
    }
    if snapshot
        .task_kinds
        .iter()
        .any(|kind| kind.is_empty() || kind.len() > MAX_TASK_KIND_BYTES)
        || snapshot
            .task_kinds
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err("Agent capability Task kinds must be valid, sorted and unique");
    }
    if snapshot.probe_protocols.is_empty()
        || snapshot
            .probe_protocols
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || snapshot.probe_protocols.iter().any(|value| {
            ProbeProtocol::try_from(*value)
                .map(|protocol| protocol == ProbeProtocol::Unspecified)
                .unwrap_or(true)
        })
    {
        return Err("Agent capability Probe protocols must be valid, sorted and unique");
    }
    Ok(())
}

fn validate_plugin_inventory(snapshot: &AgentPluginInventory) -> Result<(), &'static str> {
    if snapshot.revision == 0 || snapshot.plugins.len() > MAX_INSTALLED_PLUGINS {
        return Err("Agent plugin inventory has an invalid revision or plugin count");
    }
    if snapshot.plugins.iter().any(|plugin| {
        plugin.plugin_id.is_empty()
            || plugin.plugin_id.len() > MAX_PLUGIN_ID_BYTES
            || plugin.version.is_empty()
            || plugin.version.len() > MAX_PLUGIN_VERSION_BYTES
            || plugin.schema_hash.len() != 32
            || plugin.schema_format_version == 0
            || plugin.task_kinds.is_empty()
            || plugin
                .task_kinds
                .iter()
                .any(|kind| kind.is_empty() || kind.len() > MAX_TASK_KIND_BYTES)
            || plugin.task_kinds.windows(2).any(|pair| pair[0] >= pair[1])
    }) {
        return Err("Agent plugin inventory contains an invalid plugin entry");
    }
    if snapshot.plugins.windows(2).any(|pair| {
        (pair[0].plugin_id.as_str(), pair[0].version.as_str())
            >= (pair[1].plugin_id.as_str(), pair[1].version.as_str())
    }) {
        return Err("Agent plugin inventory entries must be sorted and unique");
    }
    Ok(())
}

fn policy_acknowledgement(revision: u64) -> AgentJobPolicySync {
    AgentJobPolicySync {
        body: Some(agent_job_policy_sync::Body::Acknowledgement(
            AgentJobPolicyAck { revision },
        )),
    }
}

fn validate_policy_snapshot(
    snapshot: &AgentJobPolicySnapshot,
    current: Option<&AgentJobPolicySnapshot>,
) -> Result<(), &'static str> {
    if snapshot.denied_task_kinds.len() > MAX_DENIED_TASK_KINDS {
        return Err("Agent Job policy contains too many Task kinds");
    }
    if snapshot
        .denied_task_kinds
        .iter()
        .any(|kind| kind.is_empty() || kind.len() > MAX_TASK_KIND_BYTES)
    {
        return Err("Agent Job policy contains an invalid Task kind");
    }
    if snapshot
        .denied_task_kinds
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err("Agent Job policy Task kinds must be sorted and unique");
    }
    if let Some(current) = current {
        if snapshot.revision < current.revision {
            return Err("Agent Job policy revision cannot move backwards");
        }
        if snapshot.revision == current.revision && snapshot != current {
            return Err("Agent Job policy content changed without a new revision");
        }
    }
    Ok(())
}

async fn send_invalid_message(
    session: &mut TonicNoiseSession,
    message: impl Into<String>,
) -> Result<(), TransportError> {
    session
        .send(SecureMessage {
            body: Some(secure_message::Body::Error(SecureError {
                code: SecureErrorCode::InvalidMessage as i32,
                message: message.into(),
            })),
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::{
        validate_capability_snapshot, validate_plugin_inventory, validate_policy_snapshot,
    };
    use smalux_protocol::agent::v1::{
        AgentCapabilitySnapshot, AgentJobPolicySnapshot, AgentPluginInventory,
        PluginInventoryEntry, ProbeProtocol,
    };

    #[test]
    fn rejects_changed_content_at_the_same_revision() {
        let current = AgentJobPolicySnapshot {
            revision: 7,
            deny_all: false,
            denied_task_kinds: vec![],
        };
        let changed = AgentJobPolicySnapshot {
            revision: 7,
            deny_all: true,
            denied_task_kinds: vec![],
        };
        assert!(validate_policy_snapshot(&changed, Some(&current)).is_err());
    }

    #[test]
    fn rejects_unsorted_or_duplicate_task_kinds() {
        let snapshot = AgentJobPolicySnapshot {
            revision: 1,
            deny_all: false,
            denied_task_kinds: vec!["b".into(), "a".into()],
        };
        assert!(validate_policy_snapshot(&snapshot, None).is_err());
    }

    #[test]
    fn accepts_an_identical_replay_at_the_same_revision() {
        let snapshot = AgentJobPolicySnapshot {
            revision: 3,
            deny_all: false,
            denied_task_kinds: vec!["smalux.collect.cpu.v1".into()],
        };
        assert!(validate_policy_snapshot(&snapshot, Some(&snapshot)).is_ok());
    }

    fn valid_capability() -> AgentCapabilitySnapshot {
        AgentCapabilitySnapshot {
            revision: 1,
            agent_version: "0.1.0".into(),
            task_kinds: vec![
                "smalux.collect.cpu.v1".into(),
                "smalux.collect.memory.v1".into(),
            ],
            probe_protocols: vec![
                ProbeProtocol::IcmpEcho as i32,
                ProbeProtocol::TcpConnect as i32,
            ],
        }
    }

    #[test]
    fn accepts_a_sorted_capability_snapshot() {
        assert!(validate_capability_snapshot(&valid_capability()).is_ok());
    }

    #[test]
    fn rejects_unsorted_capabilities_and_unknown_probe_protocols() {
        let mut unsorted = valid_capability();
        unsorted.task_kinds.reverse();
        assert!(validate_capability_snapshot(&unsorted).is_err());

        let mut unknown_protocol = valid_capability();
        unknown_protocol.probe_protocols.push(999);
        assert!(validate_capability_snapshot(&unknown_protocol).is_err());
    }

    #[test]
    fn rejects_zero_capability_revision() {
        let mut snapshot = valid_capability();
        snapshot.revision = 0;
        assert!(validate_capability_snapshot(&snapshot).is_err());
    }

    #[test]
    fn plugin_inventory_requires_sorted_unique_entries_and_task_kinds() {
        let valid = AgentPluginInventory {
            revision: 1,
            plugins: vec![
                PluginInventoryEntry {
                    plugin_id: "smalux.plus.alpha".into(),
                    version: "1.0.0".into(),
                    task_kinds: vec!["smalux.plus.alpha.v1".into()],
                    schema_hash: vec![1; 32],
                    schema_format_version: 1,
                },
                PluginInventoryEntry {
                    plugin_id: "smalux.plus.echo".into(),
                    version: "1.0.0".into(),
                    task_kinds: vec!["smalux.plus.echo.v1".into()],
                    schema_hash: vec![2; 32],
                    schema_format_version: 1,
                },
            ],
        };
        assert!(validate_plugin_inventory(&valid).is_ok());

        let mut duplicate = valid;
        duplicate.plugins.push(duplicate.plugins[1].clone());
        assert!(validate_plugin_inventory(&duplicate).is_err());
    }
}
