use std::sync::Arc;

use crate::state::AppState;
use axum::Router;
use axum::http::header::HeaderName;
use tower::ServiceBuilder;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};

/// 装配正式 Server 的全部顶层路由。
pub(crate) fn build_app_router(app_state: AppState) -> anyhow::Result<Router> {
    tracing::debug!("building top-level server router");
    let agent_routes = crate::controller::agent::router(Arc::clone(&app_state.agent))?;
    // tonic 生成的 gRPC router 本身不提取 Axum State；用空状态把它转换成与其他
    // 子路由相同的 Router<AppState> 类型，真正的 AppState 仍由顶层 Router 统一提供。
    let agent_routes: Router<AppState> = agent_routes.with_state(());
    let frontend_routes = crate::controller::frontend::router();
    let request_id_header = HeaderName::from_static("x-request-id");
    tracing::debug!(header = %request_id_header, "installing common Server request ID propagation");
    Ok(Router::new()
        .merge(crate::web_auth::router())
        .merge(frontend_routes)
        .merge(agent_routes)
        .layer(
            ServiceBuilder::new()
                // 网关已经提供 request ID 时保留它；没有时由 Server 生成 UUID。
                .layer(SetRequestIdLayer::new(
                    request_id_header.clone(),
                    MakeRequestUuid,
                ))
                // 把 request ID 返回给调用方，便于关联 Server 日志与网关日志。
                .layer(PropagateRequestIdLayer::new(request_id_header)),
        )
        .with_state(app_state))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{
        Router,
        body::Body,
        body::to_bytes,
        http::{Request, StatusCode},
    };
    use smalux_agent::client::{
        AgentStateStore, AuthenticationMode, FileAgentStateStore, PersistedAgentState,
        RegistrationToken, SmaluxClient, SmaluxClientConfig, SmaluxClientEvent,
    };
    use smalux_agent::plugins::PluginRuntimeState;
    use smalux_agent::remote_jobs::RemoteJobController;
    use smalux_agent::scheduler::{SchedulerConfig, SchedulerRuntime, TaskReportSink};
    use smalux_protocol::agent::v1::{
        AgentCapabilitySync, AgentJobPolicySync, AgentPluginInventory, AgentPluginSync,
        AgentReconcileSummary, JobDefinition, TaskDefinition, agent_capability_sync,
        agent_job_policy_sync, agent_plugin_sync, task_definition,
    };
    use smalux_protocol::tonic_transport::SessionEvent;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tower::ServiceExt;
    use uuid::Uuid;

    use crate::config::RuntimeConfig;
    use crate::database::{DatabaseConfig, ServerDatabase};
    use crate::state::AppState;

    use super::build_app_router;

    fn test_config() -> RuntimeConfig {
        RuntimeConfig {
            address: "127.0.0.1".to_owned(),
            port: 0,
            max_agent_sessions: crate::config::DEFAULT_MAX_AGENT_SESSIONS,
            max_registration_sessions: crate::config::DEFAULT_MAX_REGISTRATION_SESSIONS,
            max_grpc_message_bytes: crate::config::DEFAULT_MAX_GRPC_MESSAGE_BYTES,
            web_enabled: false,
            web_origin: None,
            web_development: false,
            web_absolute_ttl_seconds: 86_400,
            web_idle_ttl_seconds: 1_800,
            web_login_limit: 30,
            web_metrics_bindings: "[]".to_owned(),
            web_metrics_stale_seconds: 60,
        }
    }

    async fn test_router() -> Router {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("test database should connect");
        let app_state = AppState::build(test_config(), database)
            .await
            .expect("test app state should build");
        build_app_router(app_state).expect("server router should build")
    }

    #[tokio::test]
    async fn top_level_router_assigns_and_propagates_request_id() {
        let router = test_router().await;
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router should return a response");

        assert!(response.headers().contains_key("x-request-id"));
    }

    #[tokio::test]
    async fn top_level_router_preserves_incoming_request_id() {
        let router = test_router().await;
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/missing")
                    .header("x-request-id", "request-from-gateway")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router should return a response");

        assert_eq!(
            response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("request-from-gateway")
        );
    }

    #[tokio::test]
    async fn frontend_health_returns_the_server_date() {
        let router = test_router().await;

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router should return a response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("health response body should be readable");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("health response should be JSON");
        let date = payload["date"]
            .as_str()
            .expect("health response should contain a date string");
        assert_eq!(date.len(), 10);
        assert_eq!(&date[4..5], "-");
        assert_eq!(&date[7..8], "-");
        assert!(payload.get("database").is_none());
    }

    #[tokio::test]
    async fn official_agent_client_registers_then_reconnects_with_saved_identity() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("test database should connect");
        let app_state = AppState::build(test_config(), database)
            .await
            .expect("test app state should build");
        let issued = app_state
            .agent
            .agent_registry
            .create_registration_token(Some("agent-e2e".to_owned()), Some(Duration::from_secs(60)))
            .await
            .expect("test registration Token should be issued");
        let credential = issued.expose_credential().to_owned();
        let router = build_app_router(app_state).expect("server router should build");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral listener should bind");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test Server should run");
        });

        let directory = std::env::temp_dir().join(format!("smalux-e2e-{}", Uuid::new_v4()));
        let store = Arc::new(FileAgentStateStore::new(directory.join("identity.json")));
        let mut registration_config = SmaluxClientConfig::new(format!("http://{address}"))
            .expect("Client config should be valid");
        registration_config.set_registration_token(Some(
            RegistrationToken::new(credential).expect("issued Token should be valid"),
        ));
        let mut registration_client = SmaluxClient::new(registration_config, store.clone());

        tokio::time::timeout(Duration::from_secs(10), registration_client.connect())
            .await
            .expect("registration should not time out")
            .expect("registration should succeed");
        let event = tokio::time::timeout(Duration::from_secs(2), registration_client.next_event())
            .await
            .expect("connected event should arrive")
            .expect("event receive should succeed")
            .expect("connected event should exist");
        assert!(matches!(
            event,
            SmaluxClientEvent::Connected {
                mode: AuthenticationMode::RegistrationXxPsk3
            }
        ));
        registration_client
            .disconnect()
            .await
            .expect("registered Client should disconnect");
        assert!(matches!(
            store.load().await.expect("saved state should load"),
            Some(PersistedAgentState::Registered { .. })
        ));

        // 第二次连接不配置 Token；Client 必须仅依赖落盘的 Agent 密钥和 Server 公钥选择 IK。
        let reconnect_config = SmaluxClientConfig::new(format!("http://{address}"))
            .expect("reconnect config should be valid");
        let mut reconnect_client = SmaluxClient::new(reconnect_config, store.clone());
        tokio::time::timeout(Duration::from_secs(10), reconnect_client.connect())
            .await
            .expect("IK reconnect should not time out")
            .expect("IK reconnect should succeed");
        let event = tokio::time::timeout(Duration::from_secs(2), reconnect_client.next_event())
            .await
            .expect("reconnected event should arrive")
            .expect("event receive should succeed")
            .expect("reconnected event should exist");
        assert!(matches!(
            event,
            SmaluxClientEvent::Connected {
                mode: AuthenticationMode::ReconnectIk
            }
        ));
        reconnect_client
            .disconnect()
            .await
            .expect("reconnected Client should disconnect");

        server.abort();
        let _ = server.await;
        store.clear().await.expect("test state should clear");
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn encrypted_agent_executes_server_job_and_persists_report() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("test database should connect");
        let app_state = AppState::build(test_config(), database)
            .await
            .expect("test app state should build");
        let database = Arc::clone(&app_state.database);
        let control_plane = Arc::clone(&app_state.agent.control_plane);
        let issued = app_state
            .agent
            .agent_registry
            .create_registration_token(
                Some("agent-job-e2e".to_owned()),
                Some(Duration::from_secs(60)),
            )
            .await
            .expect("test registration Token should be issued");
        let credential = issued.expose_credential().to_owned();
        let router = build_app_router(app_state).expect("server router should build");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral listener should bind");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test Server should run");
        });

        let directory = std::env::temp_dir().join(format!("smalux-job-e2e-{}", Uuid::new_v4()));
        let store = Arc::new(FileAgentStateStore::new(directory.join("identity.json")));
        let mut client_config = SmaluxClientConfig::new(format!("http://{address}"))
            .expect("Client config should be valid");
        client_config.set_registration_token(Some(
            RegistrationToken::new(credential).expect("issued Token should be valid"),
        ));
        let mut client = SmaluxClient::new(client_config, store.clone());
        tokio::time::timeout(Duration::from_secs(10), client.connect())
            .await
            .expect("registration should not time out")
            .expect("registration should succeed");
        let client_handle = client
            .handle()
            .expect("connected Client should expose a handle");
        let agent_id = store
            .load()
            .await
            .expect("saved state should load")
            .and_then(|state| state.agent_id().map(str::to_owned))
            .expect("registration should save Agent ID");

        let scheduler_runtime = SchedulerRuntime::start(SchedulerConfig::default())
            .expect("test Scheduler should start");
        let (report_sender, mut report_receiver) = mpsc::channel(8);
        let report_sink: Arc<dyn TaskReportSink> = Arc::new(move |report| {
            let report_sender = report_sender.clone();
            async move {
                report_sender.send(report).await.map_err(|_| {
                    smalux_agent::scheduler::CallbackError::Transient(anyhow::anyhow!(
                        "test report receiver closed"
                    ))
                })
            }
        });
        let remote_jobs = RemoteJobController::new(scheduler_runtime.scheduler(), report_sink);
        let mut plugin_runtime = PluginRuntimeState::default();
        let process_instance_id = [7; 16];
        let job_id = Uuid::new_v4();
        let job = JobDefinition {
            job_id: job_id.as_bytes().to_vec(),
            revision: 1,
            enabled: true,
            trigger: Some(smalux_protocol::agent::v1::JobTrigger {
                timeout: None,
                misfire: Some(smalux_protocol::agent::v1::MisfirePolicy {
                    behavior: smalux_protocol::agent::v1::MisfireBehavior::Skip as i32,
                    max_runs: 0,
                }),
                schedule: Some(smalux_protocol::agent::v1::job_trigger::Schedule::Interval(
                    smalux_protocol::agent::v1::IntervalSchedule {
                        every: Some(prost_types::Duration {
                            seconds: 0,
                            nanos: 100_000_000,
                        }),
                        start_at: None,
                    },
                )),
            }),
            task: Some(TaskDefinition {
                task: Some(task_definition::Task::Cpu(Default::default())),
            }),
            ..Default::default()
        };
        let mut sent_agent_snapshots = false;
        let mut job_command_received = false;
        let mut report = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline && report.is_none() {
            tokio::select! {
                event = tokio::time::timeout(Duration::from_secs(10), client.next_event()) => {
                    let event = event
                        .expect("Agent event should arrive before the test step timeout")
                        .expect("Agent Client event should be readable")
                        .expect("Agent Client event stream should remain open");
                    match event {
                        SmaluxClientEvent::Connected { .. } if !sent_agent_snapshots => {
                            sent_agent_snapshots = true;
                            client_handle
                                .send_reconcile_summary(AgentReconcileSummary {
                                    instance_id: process_instance_id.to_vec(),
                                    ..Default::default()
                                })
                                .await
                                .expect("reconcile summary should be sent");
                            client_handle
                                .send_agent_job_policy(AgentJobPolicySync {
                                    body: Some(agent_job_policy_sync::Body::Snapshot(
                                        smalux_agent::remote_jobs::RemoteJobPolicy::default()
                                            .snapshot()
                                            .to_protocol_message()
                                            .body
                                            .and_then(|body| match body {
                                                agent_job_policy_sync::Body::Snapshot(snapshot) => Some(snapshot),
                                                _ => None,
                                            })
                                            .expect("policy snapshot should exist"),
                                    )),
                                })
                                .await
                                .expect("policy snapshot should be sent");
                            client_handle
                                .send_agent_capability(AgentCapabilitySync {
                                    body: Some(agent_capability_sync::Body::Snapshot(
                                        smalux_agent::tasks::agent_capability_snapshot(),
                                    )),
                                })
                                .await
                                .expect("capability snapshot should be sent");
                            client_handle
                                .send_agent_plugin(AgentPluginSync {
                                    body: Some(agent_plugin_sync::Body::Inventory(
                                        AgentPluginInventory {
                                            revision: 1,
                                            plugins: Vec::new(),
                                        },
                                    )),
                                })
                                .await
                                .expect("plugin inventory should be sent");
                            control_plane
                                .replace_catalog(&agent_id, vec![job.clone()])
                                .await
                                .expect("server Job catalog should be committed");
                        }
                        SmaluxClientEvent::Session(SessionEvent::AgentJobPolicy(message)) => {
                            if matches!(message.body, Some(agent_job_policy_sync::Body::Query(_))) {
                                client_handle
                                    .send_agent_job_policy(AgentJobPolicySync {
                                        body: Some(agent_job_policy_sync::Body::Snapshot(
                                            smalux_agent::remote_jobs::RemoteJobPolicy::default()
                                                .snapshot()
                                                .to_protocol_message()
                                                .body
                                                .and_then(|body| match body {
                                                    agent_job_policy_sync::Body::Snapshot(snapshot) => Some(snapshot),
                                                    _ => None,
                                                })
                                                .expect("policy snapshot should exist"),
                                        )),
                                    })
                                    .await
                                    .expect("policy query response should be sent");
                            }
                        }
                        SmaluxClientEvent::Session(SessionEvent::AgentCapability(message)) => {
                            if matches!(message.body, Some(agent_capability_sync::Body::Query(_))) {
                                client_handle
                                    .send_agent_capability(AgentCapabilitySync {
                                        body: Some(agent_capability_sync::Body::Snapshot(
                                            smalux_agent::tasks::agent_capability_snapshot(),
                                        )),
                                    })
                                    .await
                                    .expect("capability query response should be sent");
                            }
                        }
                        SmaluxClientEvent::Session(SessionEvent::AgentPlugin(message)) => {
                            if let Some(agent_plugin_sync::Body::Query(_)) = message.body {
                                client_handle
                                    .send_agent_plugin(AgentPluginSync {
                                        body: Some(agent_plugin_sync::Body::Inventory(
                                            AgentPluginInventory {
                                                revision: 1,
                                                plugins: Vec::new(),
                                            },
                                        )),
                                    })
                                    .await
                                    .expect("plugin query response should be sent");
                            } else if let Some(agent_plugin_sync::Body::Snapshot(snapshot)) = message.body {
                                let ack = plugin_runtime.confirm_snapshot(&snapshot);
                                client_handle
                                    .send_agent_plugin(AgentPluginSync {
                                        body: Some(agent_plugin_sync::Body::Acknowledgement(ack)),
                                    })
                                    .await
                                    .expect("plugin runtime acknowledgement should be sent");
                            }
                        }
                        SmaluxClientEvent::Session(SessionEvent::JobCommand(command)) => {
                            job_command_received = true;
                            let result = remote_jobs.apply_command(command).await;
                            client_handle
                                .send_job_command_result(result)
                                .await
                                .expect("Job command result should be sent");
                        }
                        SmaluxClientEvent::Disconnected { reason, .. } => {
                            panic!("Agent disconnected during Job execution: {reason}");
                        }
                        SmaluxClientEvent::Fatal(error) => panic!("Agent Client failed: {error}"),
                        _ => {}
                    }
                }
                next_report = report_receiver.recv() => {
                    report = next_report;
                }
            }
        }
        assert!(
            job_command_received,
            "Server should send the committed Job catalog"
        );
        let report = report.expect("Agent Scheduler should produce a TaskReport");
        assert_eq!(report.job_id, job_id.as_bytes());
        assert_eq!(report.job_revision, 1);
        client_handle
            .send_task_report(report)
            .await
            .expect("TaskReport should be sent to Server");

        let persisted = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let reports = database
                    .list_task_reports(Some(&agent_id), 10)
                    .await
                    .expect("TaskReport query should succeed");
                if !reports.is_empty() {
                    return reports;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Server should persist the TaskReport");
        assert_eq!(persisted.len(), 1);

        client.disconnect().await.expect("Client should disconnect");
        scheduler_runtime
            .shutdown()
            .await
            .expect("test Scheduler should shut down");
        server.abort();
        let _ = server.await;
        store.clear().await.expect("test state should clear");
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
