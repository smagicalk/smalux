//! 已认证 Agent 的长期业务循环。
//!
//! Server 向 Agent 查询本地 Job 策略；Agent 在认证连接后主动上报能力和插件 inventory。
//! 插件运行时快照得到 Agent 确认后，Server 才向 Provider 请求可下发目录，避免 Job 先于对应 Worker 到达。

use smalux_protocol::{
    agent::v1::{
        AgentCapabilityQuery, AgentCapabilitySnapshot, AgentCapabilitySync, AgentJobPolicyAck,
        AgentJobPolicyQuery, AgentJobPolicySnapshot, AgentJobPolicySync, AgentPluginInventory,
        AgentPluginSync, AgentReconcileSummary, JobCommand, JobCommandStatus, PluginPauseAck,
        PluginRuntimeSnapshot, ProbeProtocol, ReplaceAllJobs, SecureError, SecureErrorCode,
        SecureMessage, agent_capability_sync, agent_job_policy_sync, agent_plugin_sync,
        job_command, secure_message, task_definition,
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

/// 一次权威目录下发需要的已确认会话快照。
struct JobCatalogRequest<'a> {
    agent_id: &'a str,
    policy: &'a AgentJobPolicySnapshot,
    capability: &'a AgentCapabilitySnapshot,
    inventory: &'a AgentPluginInventory,
    runtime: &'a PluginRuntimeSnapshot,
    /// Agent 最近一次上报的摘要；缺失时必须完整下发。
    reconcile: Option<&'a AgentReconcileSummary>,
    /// `true` 表示 Agent 明确请求恢复，即使摘要相同也要发送完整快照。
    force: bool,
}

/// 一次插件 runtime 下发所需的已确认 Agent 会话资料。
struct RuntimeSnapshotRequest<'a> {
    /// Server 端临时 Session ID，仅用于日志和 SessionRegistry。
    session_id: u64,
    /// 认证后的稳定 Agent ID，用于读取权威 runtime。
    agent_id: &'a str,
    /// Agent 当前安装的插件清单，用于匹配版本和 Schema hash。
    inventory: &'a AgentPluginInventory,
    /// Agent 最近一次上报的 runtime 摘要；缺失时必须发送完整快照。
    reconcile: Option<&'a AgentReconcileSummary>,
    /// 新进程实例或摘要缺失时强制发送，不能仅凭相同 revision 跳过。
    force: bool,
}

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
        // Agent 在认证连接建立时主动发送完整 inventory，避免重复查询导致重复 SchemaResponse。
        let mut accepted_policy: Option<AgentJobPolicySnapshot> = None;
        let mut accepted_capability: Option<AgentCapabilitySnapshot> = None;
        let mut sent_plugin_runtime: Option<PluginRuntimeSnapshot> = None;
        let mut plugin_runtime_ready = false;
        let mut accepted_plugin_inventory: Option<AgentPluginInventory> = None;
        let mut pending_schema_hashes: Vec<Vec<u8>> = Vec::new();
        let mut last_sent_catalog: Option<ReplaceAllJobs> = None;
        let mut reconcile_summary: Option<AgentReconcileSummary> = None;
        // 在收到合法摘要前按未知状态处理；新进程实例也必须完整同步一次。
        let mut force_reconcile = true;
        // 记录已经为哪个 Agent 目录版本发出过恢复命令，抑制重复结果造成的命令风暴。
        let mut resync_requested_revision: Option<u64> = None;
        let mut catalog_changes = self
            .state
            .sessions
            .subscribe_catalog_changes(session_id)
            .await
            .ok_or_else(|| {
                TransportError::Protocol("Agent Session is not registered".to_owned())
            })?;
        let mut runtime_changes = self
            .state
            .sessions
            .subscribe_runtime_changes(session_id)
            .await
            .ok_or_else(|| {
                TransportError::Protocol("Agent Session is not registered".to_owned())
            })?;

        loop {
            let event = tokio::select! {
                event = session.receive_event() => event?,
                changed = catalog_changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.state.sessions.touch(session_id).await;
                    resync_requested_revision = None;
                    if plugin_runtime_ready
                        && let (Some(policy), Some(capability), Some(inventory)) = (
                            accepted_policy.as_ref(),
                            accepted_capability.as_ref(),
                            accepted_plugin_inventory.as_ref(),
                        )
                    {
                        self.send_job_catalog(
                            session_id,
                            session,
                            JobCatalogRequest {
                                agent_id,
                                policy,
                                capability,
                                inventory,
                                runtime: sent_plugin_runtime
                                    .as_ref()
                                    .expect("runtime acknowledged after snapshot"),
                                reconcile: reconcile_summary.as_ref(),
                                force: force_reconcile,
                            },
                            &mut last_sent_catalog,
                        )
                        .await?;
                        // 首次完整同步已经完成；后续目录变化可以重新使用摘要判断，
                        // 不会因为同一连接内的策略 ACK 再次强制发送整份快照。
                        force_reconcile = false;
                    }
                    continue;
                },
                changed = runtime_changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.state.sessions.touch(session_id).await;
                    if let Some(inventory) = accepted_plugin_inventory.as_ref() {
                        // runtime 变化必须先得到 Agent 的 ACK，再重新计算可下发 Job，
                        // 这样新 Plugin Job 不会先于对应 Worker 配置进入 Scheduler。
                        sent_plugin_runtime = None;
                        plugin_runtime_ready = false;
                        self.send_plugin_runtime_snapshot(
                            session,
                            RuntimeSnapshotRequest {
                                session_id,
                                agent_id,
                                inventory,
                                reconcile: reconcile_summary.as_ref(),
                                force: force_reconcile,
                            },
                            &mut sent_plugin_runtime,
                            &mut plugin_runtime_ready,
                        ).await?;
                    }
                    continue;
                }
            };
            let Some(event) = event else {
                break;
            };
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
                    if !unchanged {
                        // 策略变化会改变 Server 过滤后的有效目录，旧对账摘要不能继续复用。
                        reconcile_summary = None;
                    }
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
                    if !unchanged
                        && plugin_runtime_ready
                        && let (Some(capability), Some(inventory)) = (
                            accepted_capability.as_ref(),
                            accepted_plugin_inventory.as_ref(),
                        )
                    {
                        self.send_job_catalog(
                            session_id,
                            session,
                            JobCatalogRequest {
                                agent_id,
                                policy: &snapshot,
                                capability,
                                inventory,
                                runtime: sent_plugin_runtime
                                    .as_ref()
                                    .expect("runtime acknowledged after snapshot"),
                                reconcile: reconcile_summary.as_ref(),
                                force: force_reconcile,
                            },
                            &mut last_sent_catalog,
                        )
                        .await?;
                    }
                }
                SessionEvent::ReconcileSummary(summary) => {
                    if let Err(reason) = validate_reconcile_summary(&summary) {
                        tracing::warn!(session_id, %reason, "Agent reconcile summary was rejected");
                        // 摘要只是性能优化提示；非法摘要按“未知状态”处理，继续完整同步。
                        reconcile_summary = None;
                        continue;
                    }
                    let new_instance = self
                        .state
                        .sessions
                        .observe_reconcile_instance(agent_id, &summary)
                        .await;
                    force_reconcile = new_instance;
                    tracing::debug!(
                        session_id,
                        instance_id_len = summary.instance_id.len(),
                        catalog_revision = summary.catalog_revision,
                        runtime_revision = summary.runtime_revision,
                        new_instance,
                        "accepted Agent reconcile summary"
                    );
                    reconcile_summary = Some(summary);
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
                    if let Err(error) = self
                        .state
                        .database
                        .save_agent_capability(agent_id, &snapshot)
                        .await
                    {
                        tracing::error!(session_id, %error, "failed to persist Agent capability snapshot");
                        return Err(TransportError::Protocol(
                            "failed to persist Agent capability snapshot".to_owned(),
                        ));
                    }
                    accepted_capability = Some(snapshot);
                    if plugin_runtime_ready
                        && let (Some(policy), Some(inventory)) =
                            (accepted_policy.as_ref(), accepted_plugin_inventory.as_ref())
                    {
                        self.send_job_catalog(
                            session_id,
                            session,
                            JobCatalogRequest {
                                agent_id,
                                policy,
                                capability: accepted_capability
                                    .as_ref()
                                    .expect("capability was set"),
                                inventory,
                                runtime: sent_plugin_runtime
                                    .as_ref()
                                    .expect("runtime acknowledged after snapshot"),
                                reconcile: reconcile_summary.as_ref(),
                                force: force_reconcile,
                            },
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
                        if let Err(error) = self
                            .state
                            .database
                            .save_agent_plugin_inventory(agent_id, &inventory)
                            .await
                        {
                            tracing::error!(session_id, %error, "failed to persist Agent plugin inventory");
                            return Err(TransportError::Protocol(
                                "failed to persist Agent plugin inventory".to_owned(),
                            ));
                        }
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
                                session,
                                RuntimeSnapshotRequest {
                                    session_id,
                                    agent_id,
                                    inventory: &inventory,
                                    reconcile: reconcile_summary.as_ref(),
                                    force: force_reconcile,
                                },
                                &mut sent_plugin_runtime,
                                &mut plugin_runtime_ready,
                            )
                            .await?;
                            if plugin_runtime_ready
                                && let (Some(policy), Some(capability)) =
                                    (accepted_policy.as_ref(), accepted_capability.as_ref())
                            {
                                self.send_job_catalog(
                                    session_id,
                                    session,
                                    JobCatalogRequest {
                                        agent_id,
                                        policy,
                                        capability,
                                        inventory: &inventory,
                                        runtime: sent_plugin_runtime
                                            .as_ref()
                                            .expect("runtime ready after snapshot"),
                                        reconcile: reconcile_summary.as_ref(),
                                        force: force_reconcile,
                                    },
                                    &mut last_sent_catalog,
                                )
                                .await?;
                                force_reconcile = false;
                            }
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
                                session,
                                RuntimeSnapshotRequest {
                                    session_id,
                                    agent_id,
                                    inventory,
                                    reconcile: reconcile_summary.as_ref(),
                                    force: force_reconcile,
                                },
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
                        if let (Some(policy), Some(capability), Some(inventory)) = (
                            accepted_policy.as_ref(),
                            accepted_capability.as_ref(),
                            accepted_plugin_inventory.as_ref(),
                        ) {
                            self.send_job_catalog(
                                session_id,
                                session,
                                JobCatalogRequest {
                                    agent_id,
                                    policy,
                                    capability,
                                    inventory,
                                    runtime: sent,
                                    reconcile: reconcile_summary.as_ref(),
                                    force: force_reconcile,
                                },
                                &mut last_sent_catalog,
                            )
                            .await?;
                            // runtime ACK 和目录快照都完成后，当前连接已经收敛；后续
                            // 目录/策略变化只按 revision + digest 判断是否需要发送。
                            force_reconcile = false;
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
                                    let command = JobCommand {
                                        command_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                                        action: Some(job_command::Action::ReplaceAll(
                                            filtered.clone(),
                                        )),
                                    };
                                    self.state
                                        .database
                                        .record_job_command(agent_id, &command)
                                        .await
                                        .map_err(|error| {
                                            TransportError::Protocol(error.to_string())
                                        })?;
                                    session.send_job_command(command).await?;
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
                    if let Err(error) = self
                        .state
                        .database
                        .complete_job_command(agent_id, &result)
                        .await
                    {
                        tracing::warn!(session_id, %error, "Agent Job command result was rejected");
                    }
                    tracing::debug!(
                        session_id,
                        command_id_len = result.command_id.len(),
                        status = result.status,
                        "received Agent Job command result"
                    );
                    if result.status == JobCommandStatus::Applied as i32 {
                        resync_requested_revision = None;
                    } else if result.status == JobCommandStatus::ResyncRequired as i32
                        && resync_requested_revision != Some(result.catalog_revision)
                    {
                        tracing::warn!(
                            session_id,
                            catalog_revision = result.catalog_revision,
                            "Agent requested a full Job catalog resynchronization"
                        );
                        if plugin_runtime_ready
                            && let (Some(policy), Some(capability), Some(inventory), Some(runtime)) = (
                                accepted_policy.as_ref(),
                                accepted_capability.as_ref(),
                                accepted_plugin_inventory.as_ref(),
                                sent_plugin_runtime.as_ref(),
                            )
                        {
                            let sent = self
                                .send_job_catalog(
                                    session_id,
                                    session,
                                    JobCatalogRequest {
                                        agent_id,
                                        policy,
                                        capability,
                                        inventory,
                                        runtime,
                                        reconcile: reconcile_summary.as_ref(),
                                        force: true,
                                    },
                                    &mut last_sent_catalog,
                                )
                                .await?;
                            if sent {
                                resync_requested_revision = Some(result.catalog_revision);
                            }
                        }
                    }
                }
                SessionEvent::TaskReport(report) => {
                    if let Err(error) = self
                        .state
                        .database
                        .append_task_report(agent_id, &report)
                        .await
                    {
                        tracing::error!(session_id, %error, "failed to persist Agent Task report");
                        return Err(TransportError::Protocol(
                            "failed to persist Agent Task report".to_owned(),
                        ));
                    }
                    tracing::debug!(
                        session_id,
                        job_id_len = report.job_id.len(),
                        "received Agent Task report"
                    );
                }
                SessionEvent::JobEvent(event) => {
                    if let Err(error) = self.state.database.append_job_event(agent_id, &event).await
                    {
                        tracing::error!(session_id, %error, "failed to persist Agent Job event");
                        return Err(TransportError::Protocol(
                            "failed to persist Agent Job event".to_owned(),
                        ));
                    }
                    tracing::debug!(
                        session_id,
                        sequence = event.sequence,
                        kind = event.kind,
                        job_id_len = event.job_id.len(),
                        "received Agent Job event"
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
        session: &mut TonicNoiseSession,
        request: JobCatalogRequest<'_>,
        last_sent_catalog: &mut Option<ReplaceAllJobs>,
    ) -> Result<bool, TransportError> {
        match self
            .state
            .job_catalog
            .load_catalog(
                request.agent_id,
                request.policy,
                request.capability,
                request.inventory,
            )
            .await
        {
            Ok(Some(catalog)) => {
                let catalog = self
                    .state
                    .plugin_pauses
                    .filter_catalog(request.agent_id, catalog)
                    .await;
                let catalog = filter_catalog_for_runtime(catalog, request.runtime);
                let digest = smalux_protocol::reconciliation::catalog_digest(&catalog);
                // watch 信号可能合并或重复唤醒；如果当前会话已经发送过完全相同的
                // 快照，直接复用结果。显式 resync(force=true) 仍允许重放。
                if !request.force && last_sent_catalog.as_ref() == Some(&catalog) {
                    tracing::debug!(
                        session_id,
                        catalog_revision = catalog.catalog_revision,
                        "Agent Job catalog was already sent in this session"
                    );
                    return Ok(false);
                }
                if !request.force
                    && request.reconcile.is_some_and(|summary| {
                        summary.catalog_revision == catalog.catalog_revision
                            && summary.catalog_digest.as_slice() == digest.as_slice()
                    })
                {
                    tracing::debug!(
                        session_id,
                        catalog_revision = catalog.catalog_revision,
                        "Agent Job catalog is already synchronized"
                    );
                    *last_sent_catalog = Some(catalog);
                    return Ok(false);
                }
                let command = JobCommand {
                    command_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                    action: Some(job_command::Action::ReplaceAll(catalog.clone())),
                };
                self.state
                    .database
                    .record_job_command(request.agent_id, &command)
                    .await
                    .map_err(|error| TransportError::Protocol(error.to_string()))?;
                session.send_job_command(command).await?;
                *last_sent_catalog = Some(catalog);
                Ok(true)
            }
            Ok(None) => {
                tracing::debug!(session_id, "Agent Job catalog Provider returned no update");
                Ok(false)
            }
            Err(error) => {
                tracing::error!(session_id, %error, "failed to load Agent Job catalog");
                Ok(false)
            }
        }
    }

    async fn send_plugin_runtime_snapshot(
        &self,
        session: &mut TonicNoiseSession,
        request: RuntimeSnapshotRequest<'_>,
        sent_plugin_runtime: &mut Option<PluginRuntimeSnapshot>,
        plugin_runtime_ready: &mut bool,
    ) -> Result<(), TransportError> {
        match self
            .state
            .plugin_runtime
            .load_runtime(request.agent_id, request.inventory)
            .await
        {
            Ok(snapshot) => {
                let revision = snapshot.revision;
                let digest = smalux_protocol::reconciliation::runtime_digest(&snapshot);
                self.state
                    .plugin_pauses
                    .acknowledge_new_runtime(request.agent_id, &snapshot)
                    .await;
                let already_synchronized = request.reconcile.is_some_and(|summary| {
                    !request.force
                        && summary.runtime_revision == revision
                        && summary.runtime_digest.as_slice() == digest.as_slice()
                });
                *sent_plugin_runtime = Some(snapshot.clone());
                *plugin_runtime_ready = already_synchronized;
                if already_synchronized {
                    tracing::debug!(
                        request.session_id,
                        inventory_revision = request.inventory.revision,
                        runtime_revision = revision,
                        "Agent plugin runtime is already synchronized"
                    );
                } else {
                    session
                        .send_agent_plugin(AgentPluginSync {
                            body: Some(agent_plugin_sync::Body::Snapshot(snapshot)),
                        })
                        .await?;
                    tracing::info!(
                        request.session_id,
                        inventory_revision = request.inventory.revision,
                        runtime_revision = revision,
                        plugins = request.inventory.plugins.len(),
                        "sent Agent plugin runtime snapshot"
                    );
                }
            }
            Err(error) => {
                tracing::error!(session_id = request.session_id, %error, "failed to load Agent plugin runtime")
            }
        }
        Ok(())
    }
}

/// 已安装并出现在 inventory 中不代表 Worker 已经接受 runtime；仅保留已 ACK snapshot
/// 明确启用的 Plus Job，固定采集任务不受影响。
fn filter_catalog_for_runtime(
    mut catalog: ReplaceAllJobs,
    runtime: &PluginRuntimeSnapshot,
) -> ReplaceAllJobs {
    catalog.jobs.retain(|job| {
        let Some(task_definition) = job.task.as_ref() else {
            return true;
        };
        let Some(task_definition::Task::Plugin(plugin)) = task_definition.task.as_ref() else {
            return true;
        };
        runtime.plugins.iter().any(|configured| {
            configured.plugin_id == plugin.plugin_id && configured.version == plugin.plugin_version
        })
    });
    catalog
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

/// 校验 Agent 对账摘要的固定长度和 revision/摘要组合。
///
/// 摘要属于性能提示而不是授权材料；非法摘要不会让已认证连接失败，而是由调用方
/// 清空本地摘要并回退到完整同步。
fn validate_reconcile_summary(snapshot: &AgentReconcileSummary) -> Result<(), &'static str> {
    if snapshot.instance_id.len() != 16 {
        return Err("Agent reconcile instance id must contain 16 bytes");
    }
    if !smalux_protocol::reconciliation::valid_digest(&snapshot.catalog_digest)
        || !smalux_protocol::reconciliation::valid_digest(&snapshot.runtime_digest)
    {
        return Err("Agent reconcile digest must be empty or 32 bytes");
    }
    if (snapshot.catalog_revision == 0) != snapshot.catalog_digest.is_empty() {
        return Err("catalog reconcile revision and digest do not match");
    }
    if (snapshot.runtime_revision == 0) != snapshot.runtime_digest.is_empty() {
        return Err("runtime reconcile revision and digest do not match");
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
        validate_reconcile_summary,
    };
    use smalux_protocol::agent::v1::{
        AgentCapabilitySnapshot, AgentJobPolicySnapshot, AgentPluginInventory,
        AgentReconcileSummary, PluginInventoryEntry, ProbeProtocol,
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
    fn reconcile_summary_requires_fixed_instance_and_matching_digest() {
        let mut summary = AgentReconcileSummary {
            instance_id: vec![1; 16],
            catalog_revision: 2,
            catalog_digest: vec![2; 32],
            runtime_revision: 0,
            runtime_digest: Vec::new(),
        };
        assert!(validate_reconcile_summary(&summary).is_ok());

        summary.instance_id = vec![1; 15];
        assert!(validate_reconcile_summary(&summary).is_err());
        summary.instance_id = vec![1; 16];
        summary.catalog_digest = vec![2; 31];
        assert!(validate_reconcile_summary(&summary).is_err());
        summary.catalog_digest = Vec::new();
        assert!(validate_reconcile_summary(&summary).is_err());
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
