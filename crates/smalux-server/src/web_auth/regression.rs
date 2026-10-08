use super::*;
use crate::{config::ServerConfig, database::DatabaseConfig};
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use sea_orm::ConnectionTrait;
use tower::ServiceExt;

const ORIGIN: &str = "http://127.0.0.1:43127";
const PASSWORD: &str = "test-password-only-123";

async fn setup(limit: usize) -> (Router, Arc<ServerDatabase>) {
    let db = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
        .await
        .unwrap();
    let config = ServerConfig {
        web_enabled: true,
        web_origin: Some(ORIGIN.into()),
        web_development: true,
        web_login_limit: limit,
        ..Default::default()
    };
    let state = AppState::build(config.runtime_config(), db).await.unwrap();
    bootstrap(state.database.clone(), "Admin", PASSWORD)
        .await
        .unwrap();
    let db = state.database.clone();
    (crate::route::build_app_router(state).unwrap(), db)
}
async fn call(
    app: &Router,
    method: &str,
    path: &str,
    body: &str,
    cookie: Option<&str>,
    token: Option<&str>,
    origin: &str,
) -> (StatusCode, HeaderMap, serde_json::Value) {
    let mut r = Request::builder()
        .method(method)
        .uri(path)
        .header("origin", origin)
        .header("x-smalux-client", "web")
        .header("content-type", "application/json");
    if let Some(cookie) = cookie {
        r = r.header("cookie", cookie);
    }
    if let Some(token) = token {
        r = r.header("x-csrf-token", token);
    }
    let res = app
        .clone()
        .oneshot(r.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = to_bytes(res.into_body(), 65536).await.unwrap();
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| serde_json::json!({"nonJson":true}));
    (status, headers, value)
}
async fn signed_in(app: &Router) -> (String, String) {
    let (status, headers, value) = call(
        app,
        "POST",
        "/api/v1/auth/login",
        &serde_json::json!({"username":"ADMIN","password":PASSWORD}).to_string(),
        None,
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["role"], "admin");
    assert!(value.get("passwordHash").is_none());
    let cookie = headers["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    (cookie, value["csrfToken"].as_str().unwrap().to_owned())
}

#[tokio::test]
async fn agent_directory_rpc_exposes_real_cursor_pages_and_not_found() {
    use sea_orm::{ActiveValue::Set, EntityTrait};

    let (app, db) = setup(30).await;
    let now = 1_700_000_000_000_000_i64;
    for (agent_id, name, status) in [
        ("agent-a", "Agent A", "active"),
        ("agent-b", "Agent B", "revoked"),
    ] {
        crate::database::entity::agent::Entity::insert(
            crate::database::entity::agent::ActiveModel {
                agent_id: Set(agent_id.to_owned()),
                name: Set(name.to_owned()),
                public_key: Set(vec![agent_id.as_bytes()[agent_id.len() - 1]; 32]),
                status: Set(status.to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                revoked_at: Set((status == "revoked").then_some(now)),
            },
        )
        .exec(db.connection())
        .await
        .unwrap();
    }
    let (cookie, csrf) = signed_in(&app).await;
    let (_, _, features) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"features","method":"session.info","params":{}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    for method in [
        "agent.list",
        "agent.get",
        "job.list",
        "job.get",
        "report.list",
        "event.list",
    ] {
        assert!(
            features["result"]["rpcMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["method"] == method && entry["state"] == "available")
        );
    }
    let (status, _, first) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"agents-1","method":"agent.list","params":{"limit":1}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["result"]["items"][0]["agentId"], "agent-a");
    assert_eq!(first["result"]["items"][0]["displayName"], "Agent A");
    assert_eq!(first["result"]["items"][0]["authorizationStatus"], "active");
    assert_eq!(first["result"]["items"][0]["online"], false);
    assert_eq!(first["result"]["hasMore"], true);
    assert_eq!(first["result"]["nextCursor"], "agent-a");

    let (status, _, second) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"agents-2","method":"agent.list","params":{"limit":1,"after":"agent-a"}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["result"]["items"][0]["agentId"], "agent-b");
    assert_eq!(
        second["result"]["items"][0]["authorizationStatus"],
        "revoked"
    );
    assert_eq!(second["result"]["hasMore"], false);
    assert_eq!(second["result"]["nextCursor"], serde_json::Value::Null);

    let (status, _, detail) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"missing","method":"agent.get","params":{"agentId":"missing-agent"}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["error"]["data"]["kind"], "NOT_FOUND");
}
#[tokio::test]
async fn report_and_event_list_rpc_uses_operator_scope_and_bounded_queries() {
    use sea_orm::{ActiveValue::Set, ConnectionTrait, EntityTrait};

    let (app, db) = setup(30).await;
    let now = 1_700_000_000_000_000_i64;
    crate::database::entity::agent::Entity::insert(crate::database::entity::agent::ActiveModel {
        agent_id: Set("agent-a".to_owned()),
        name: Set("Agent A".to_owned()),
        public_key: Set(vec![3; 32]),
        status: Set("active".to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
        revoked_at: Set(None),
    })
    .exec(db.connection())
    .await
    .unwrap();
    use prost_types::Timestamp;
    use smalux_protocol::agent::v1::{
        CpuSnapshot, JobDefinition, JobEvent, JobEventKind, TaskDefinition, TaskReport, TaskResult,
        job_trigger, task_definition, task_result,
    };
    use uuid::Uuid;

    let job_id = Uuid::new_v4();
    let definition = JobDefinition {
        job_id: job_id.as_bytes().to_vec(),
        revision: 1,
        enabled: true,
        trigger: Some(smalux_protocol::agent::v1::JobTrigger {
            schedule: Some(job_trigger::Schedule::Interval(
                smalux_protocol::agent::v1::IntervalSchedule {
                    every: Some(prost_types::Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            )),
            misfire: Some(smalux_protocol::agent::v1::MisfirePolicy {
                behavior: smalux_protocol::agent::v1::MisfireBehavior::Skip as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
        task: Some(TaskDefinition {
            task: Some(task_definition::Task::Cpu(Default::default())),
        }),
        ..Default::default()
    };
    db.replace_agent_job_catalog("agent-a", vec![definition.clone()])
        .await
        .unwrap();
    let report = TaskReport {
        job_id: job_id.as_bytes().to_vec(),
        job_revision: 1,
        run_id: Uuid::new_v4().as_bytes().to_vec(),
        attempt: 1,
        started_at: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        result: Some(TaskResult {
            result: Some(task_result::Result::Cpu(CpuSnapshot::default())),
            ..Default::default()
        }),
        ..Default::default()
    };
    db.append_task_report("agent-a", &report).await.unwrap();
    db.append_job_event(
        "agent-a",
        &JobEvent {
            instance_id: vec![9; 16],
            sequence: 1,
            kind: JobEventKind::SchedulerStarted as i32,
            message: "scheduler started".to_owned(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (cookie, csrf) = signed_in(&app).await;

    for method in ["report.list", "event.list"] {
        let (status, _, response) = call(
            &app,
            "POST",
            "/api/v1/rpc",
            &serde_json::json!({
                "jsonrpc":"2.0",
                "id":method,
                "method":method,
                "params":{"agentId":"agent-a","limit":10}
            })
            .to_string(),
            Some(&cookie),
            Some(&csrf),
            ORIGIN,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["result"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(response["result"]["hasMore"], false);
        assert_eq!(response["result"]["nextCursor"], serde_json::Value::Null);
        assert!(response["result"]["items"][0].get("payload").is_none());
        if method == "report.list" {
            assert_eq!(response["result"]["items"][0]["jobId"], job_id.to_string());
            assert_eq!(response["result"]["items"][0]["resultKind"], "cpu");
            assert_eq!(response["result"]["items"][0]["jobRevision"], "1");
        } else {
            assert_eq!(
                response["result"]["items"][0]["kind"],
                "JOB_EVENT_KIND_SCHEDULER_STARTED"
            );
            assert_eq!(response["result"]["items"][0]["gapDetected"], false);
        }
    }

    let (status, _, catalog) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"jobs","method":"job.list","params":{"agentId":"agent-a"}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(catalog["result"]["catalogRevision"], "1");
    assert_eq!(catalog["result"]["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(catalog["result"]["jobs"][0]["jobId"], job_id.to_string());
    assert_eq!(
        catalog["result"]["jobs"][0]["taskKind"],
        "smalux.collect.cpu.v1"
    );
    let (status, _, job_detail) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &serde_json::json!({
            "jsonrpc":"2.0",
            "id":"job-detail",
            "method":"job.get",
            "params":{"agentId":"agent-a","jobId":job_id.to_string()}
        })
        .to_string(),
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(job_detail["result"]["job"]["jobId"], job_id.to_string());
    assert_eq!(
        job_detail["result"]["job"]["taskKind"],
        "smalux.collect.cpu.v1"
    );
    assert!(job_detail["result"]["definitionHex"].is_string());

    db.connection()
        .execute_unprepared("UPDATE web_users SET role = 'viewer' WHERE username = 'admin'")
        .await
        .unwrap();
    let (_, _, forbidden) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"viewer-report","method":"report.list","params":{"agentId":"agent-a"}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(forbidden["error"]["data"]["kind"], "FORBIDDEN");

    let (_, _, invalid_range) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"bad-range","method":"report.list","params":{"agentId":"agent-a","fromMs":1}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(invalid_range["error"]["code"], -32602);
}

#[tokio::test]
async fn job_catalog_replace_is_cas_idempotent_and_tracks_operation() {
    use sea_orm::{ActiveValue::Set, EntityTrait};
    use smalux_protocol::agent::v1::{JobDefinition, TaskDefinition, job_trigger, task_definition};
    use uuid::Uuid;

    let (app, db) = setup(30).await;
    let now = 1_700_000_000_000_000_i64;
    crate::database::entity::agent::Entity::insert(crate::database::entity::agent::ActiveModel {
        agent_id: Set("agent-a".to_owned()),
        name: Set("Agent A".to_owned()),
        public_key: Set(vec![3; 32]),
        status: Set("active".to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
        revoked_at: Set(None),
    })
    .exec(db.connection())
    .await
    .unwrap();
    let (cookie, csrf) = signed_in(&app).await;
    let job_id = Uuid::new_v4();
    let definition = JobDefinition {
        job_id: job_id.as_bytes().to_vec(),
        revision: 1,
        enabled: true,
        trigger: Some(smalux_protocol::agent::v1::JobTrigger {
            schedule: Some(job_trigger::Schedule::Interval(
                smalux_protocol::agent::v1::IntervalSchedule {
                    every: Some(prost_types::Duration {
                        seconds: 5,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            )),
            misfire: Some(smalux_protocol::agent::v1::MisfirePolicy {
                behavior: smalux_protocol::agent::v1::MisfireBehavior::Skip as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
        task: Some(TaskDefinition {
            task: Some(task_definition::Task::Cpu(Default::default())),
        }),
        ..Default::default()
    };
    let definitions_hex = vec![hex(&definition.encode_to_vec())];
    let client_mutation_id = Uuid::new_v4().to_string();
    let request = |id: &str, defs: Vec<String>| {
        serde_json::json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":"job.catalog.replace",
            "params":{
                "agentId":"agent-a",
                "expectedCatalogRevision":"0",
                "clientMutationId":client_mutation_id,
                "definitionsHex":defs
            }
        })
        .to_string()
    };
    let (_, _, csrf_denied) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &request("without-csrf", definitions_hex.clone()),
        Some(&cookie),
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(csrf_denied["error"]["data"]["kind"], "FORBIDDEN");

    let (_, _, stale_revision) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &serde_json::json!({
            "jsonrpc":"2.0",
            "id":"stale-write",
            "method":"job.catalog.replace",
            "params":{
                "agentId":"agent-a",
                "expectedCatalogRevision":"5",
                "clientMutationId":Uuid::new_v4().to_string(),
                "definitionsHex":definitions_hex.clone()
            }
        })
        .to_string(),
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(
        stale_revision["error"]["data"]["kind"], "REVISION_CONFLICT",
        "{stale_revision}"
    );
    assert_eq!(
        stale_revision["error"]["data"]["details"]["currentRevision"],
        "0"
    );

    let (status, _, first) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &request("write-1", definitions_hex.clone()),
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(
        first["result"]["operation"]["state"], "waiting_agent",
        "{first}"
    );
    assert_eq!(first["result"]["catalogRevision"], "1");
    let operation_id = first["result"]["operation"]["operationId"]
        .as_str()
        .unwrap()
        .to_owned();

    let (_, _, replay) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &request("write-retry", definitions_hex),
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(replay["result"]["operation"]["operationId"], operation_id);
    assert_eq!(replay["result"]["catalogRevision"], "1");

    let (_, _, conflict) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &request("write-conflict", Vec::new()),
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(conflict["error"]["data"]["kind"], "IDEMPOTENCY_CONFLICT");

    let (_, _, operation) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        &serde_json::json!({
            "jsonrpc":"2.0","id":"operation","method":"operation.get",
            "params":{"operationId":operation_id}
        })
        .to_string(),
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(operation["result"]["state"], "waiting_agent", "{operation}");
    assert!(operation["result"].get("requestHash").is_none());

    let (_, _, page) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"operations","method":"operation.list","params":{"agentId":"agent-a","limit":10}}"#,
        Some(&cookie),
        Some(&csrf),
        ORIGIN,
    )
    .await;
    assert_eq!(page["result"]["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn real_login_restore_rpc_and_logout_obey_contract() {
    let (app, db) = setup(30).await;
    let (cookie, csrf) = signed_in(&app).await;
    let (status, headers, session) = call(
        &app,
        "GET",
        "/api/v1/auth/session",
        "",
        Some(&cookie),
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(session["username"], "admin");
    assert_eq!(session["csrfToken"], csrf);
    let (status, _, v) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":"a","method":"session.info","params":{}}"#,
        Some(&cookie),
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(v["result"]["rpcMethods"].is_array());
    assert!(
        v["result"]["features"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["state"] == "available")
    );
    let (_, _, meta) = call(&app, "GET", "/api/v1/meta", "", None, None, ORIGIN).await;
    assert_eq!(meta["apiVersion"], "v1");
    assert!(meta["designContractVersion"].is_string());
    assert_eq!(meta["transports"][0], "http-json-rpc");
    assert!(meta["health"].is_string());
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/v1/auth/logout",
            "{}",
            Some(&cookie),
            Some("wrong"),
            ORIGIN
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/v1/auth/logout",
            "{}",
            Some(&cookie),
            Some(&csrf),
            ORIGIN
        )
        .await
        .2["revoked"],
        true
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/v1/auth/session",
            "",
            Some(&cookie),
            None,
            ORIGIN
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/v1/auth/logout",
            "{}",
            Some(&cookie),
            None,
            ORIGIN
        )
        .await
        .2["revoked"],
        false
    );
    let row = db
        .connection()
        .query_one_raw(params(&db, "SELECT secret_hash FROM web_sessions", vec![]))
        .await
        .unwrap()
        .unwrap();
    let stored: String = row.try_get("", "secret_hash").unwrap();
    assert_eq!(stored.len(), 64);
    assert!(!cookie.contains(&stored));
}

#[tokio::test]
async fn rejects_protocol_origin_and_body_errors_without_disclosing_credentials() {
    let (app, _) = setup(30).await;
    let (cookie, csrf) = signed_in(&app).await;
    for (request, code) in [
        ("{", -32700),
        ("[]", -32600),
        (r#"{"id":1,"method":"session.info","params":{}}"#, -32600),
        (
            r#"{"jsonrpc":"2.0","id":1,"method":"session.info","params":{"x":1}}"#,
            -32602,
        ),
        (
            r#"{"jsonrpc":"2.0","id":1,"method":"not.real","params":{}}"#,
            -32601,
        ),
    ] {
        let (_, _, v) = call(
            &app,
            "POST",
            "/api/v1/rpc",
            request,
            Some(&cookie),
            Some(&csrf),
            ORIGIN,
        )
        .await;
        assert_eq!(v["error"]["code"], code, "{request}: {v}");
    }
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/v1/auth/login",
            "{}",
            None,
            None,
            "http://evil.test"
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, headers, value) = call(
        &app,
        "POST",
        "/api/v1/auth/login",
        &"x".repeat(BODY_LIMIT + 1),
        None,
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(value["error"]["kind"], "PAYLOAD_TOO_LARGE");
    let (_, _, unknown) = call(
        &app,
        "POST",
        "/api/v1/auth/login",
        r#"{"username":"unknown","password":"wrong-password-123"}"#,
        None,
        None,
        ORIGIN,
    )
    .await;
    let (_, _, wrong) = call(
        &app,
        "POST",
        "/api/v1/auth/login",
        r#"{"username":"admin","password":"wrong-password-123"}"#,
        None,
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(unknown, wrong);
    assert_eq!(wrong["error"]["kind"], "UNAUTHENTICATED");
    let r = Request::builder()
        .method("POST")
        .uri("/api/v1/auth/login")
        .header("origin", ORIGIN)
        .header("x-smalux-client", "web")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        app.oneshot(r).await.unwrap().status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
}

#[tokio::test]
async fn expiry_disabled_user_and_rate_limit_are_enforced() {
    let (app, db) = setup(2).await;
    let (cookie, _) = signed_in(&app).await;
    db.connection()
        .execute_raw(params(
            &db,
            "UPDATE web_users SET enabled=?",
            vec![false.into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/v1/auth/session",
            "",
            Some(&cookie),
            None,
            ORIGIN
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    db.connection()
        .execute_raw(params(
            &db,
            "UPDATE web_users SET enabled=?",
            vec![true.into()],
        ))
        .await
        .unwrap();
    db.connection()
        .execute_raw(params(
            &db,
            "UPDATE web_sessions SET idle_expires_at=?",
            vec![0_i64.into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/v1/auth/session",
            "",
            Some(&cookie),
            None,
            ORIGIN
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let (second, _) = signed_in(&app).await;
    db.connection()
        .execute_raw(params(
            &db,
            "UPDATE web_sessions SET expires_at=?",
            vec![0_i64.into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/v1/auth/session",
            "",
            Some(&second),
            None,
            ORIGIN
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let r = call(
        &app,
        "POST",
        "/api/v1/auth/login",
        r#"{"username":"admin","password":"wrong-password-123"}"#,
        None,
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(r.0, StatusCode::TOO_MANY_REQUESTS);
    assert!(r.1.contains_key("retry-after"));
}

#[tokio::test]
async fn bootstrap_is_unique_and_database_survives_reopen() {
    let dir = std::env::temp_dir().join(format!("smalux-auth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.join("auth.db").to_string_lossy().replace('\\', "/")
    );
    let config = DatabaseConfig::new(url);
    let db = Arc::new(ServerDatabase::connect(config.clone()).await.unwrap());
    let (a, b) = tokio::join!(
        bootstrap(db.clone(), "admin1", PASSWORD),
        bootstrap(db.clone(), "admin2", PASSWORD)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(bootstrap(db.clone(), "admin3", PASSWORD).await.is_err());
    let reopened = ServerDatabase::connect(config).await.unwrap();
    let row = reopened
        .connection()
        .query_one_raw(params(
            &reopened,
            "SELECT COUNT(*) AS n FROM web_users",
            vec![],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 1);
    reopened.connection().clone().close().await.unwrap();
    db.connection().clone().close().await.unwrap();
    drop(db);
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn deployment_config_rejects_unsafe_origins_and_limits() {
    let base = ServerConfig {
        web_enabled: true,
        web_origin: Some(ORIGIN.into()),
        web_development: true,
        ..Default::default()
    };
    assert!(base.validate_web().is_ok());
    for origin in [
        "http://example.com",
        "http://127.0.0.1:43127/",
        "http://127.0.0.1/path",
        "http://user@127.0.0.1",
        "http://*.example.com",
    ] {
        let mut c = base.clone();
        c.web_origin = Some(origin.into());
        assert!(c.validate_web().is_err());
    }
    let mut c = base.clone();
    c.address = "0.0.0.0".into();
    assert!(c.validate_web().is_err());
    let mut c = base.clone();
    c.web_absolute_ttl_seconds = u64::MAX;
    assert!(c.validate_web().is_err());
    let mut c = base.clone();
    c.web_idle_ttl_seconds = 0;
    assert!(c.validate_web().is_err());
    let mut c = base.clone();
    c.web_login_limit = 0;
    assert!(c.validate_web().is_err());
    let mut c = base;
    c.web_development = false;
    c.web_origin = Some("https://console.example.com".into());
    assert!(c.validate_web().is_ok());
}

#[tokio::test]
async fn disabled_routes_and_database_failures_do_not_report_success() {
    let db = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
        .await
        .unwrap();
    let state = AppState::build(ServerConfig::default().runtime_config(), db)
        .await
        .unwrap();
    let app = crate::route::build_app_router(state).unwrap();
    assert_eq!(
        call(&app, "GET", "/api/v1/auth/session", "", None, None, ORIGIN)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (app, db) = setup(30).await;
    let (cookie, csrf) = signed_in(&app).await;
    db.connection()
        .execute_raw(params(&db, "DROP TABLE web_auth_events", vec![]))
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/v1/auth/logout",
            "{}",
            Some(&cookie),
            Some(&csrf),
            ORIGIN
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/v1/auth/session",
            "",
            Some(&cookie),
            None,
            ORIGIN
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn upgrade_preserves_existing_data_and_cleanup_preserves_current_session() {
    use sea_orm_migration::MigratorTrait;
    let conn = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    crate::database::migration::Migrator::up(&conn, Some(1))
        .await
        .unwrap();
    conn.execute_raw(sea_orm::Statement::from_string(
        sea_orm::DbBackend::Sqlite,
        "CREATE TABLE preserved_fixture(value TEXT NOT NULL)".to_owned(),
    ))
    .await
    .unwrap();
    conn.execute_raw(sea_orm::Statement::from_string(
        sea_orm::DbBackend::Sqlite,
        "INSERT INTO preserved_fixture VALUES('keep')".to_owned(),
    ))
    .await
    .unwrap();
    crate::database::migration::Migrator::up(&conn, None)
        .await
        .unwrap();
    let row = conn
        .query_one_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "SELECT value FROM preserved_fixture".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "value").unwrap(), "keep");
    let (app, db) = setup(30).await;
    let (cookie, _) = signed_in(&app).await;
    let old = now_ms() - 40 * 24 * 60 * 60 * 1000;
    db.connection()
        .execute_raw(params(
            &db,
            "UPDATE web_auth_events SET created_at=?",
            vec![old.into()],
        ))
        .await
        .unwrap();
    store::cleanup(&db).await.unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/v1/auth/session",
            "",
            Some(&cookie),
            None,
            ORIGIN
        )
        .await
        .0,
        StatusCode::OK
    );
    let row = db
        .connection()
        .query_one_raw(params(
            &db,
            "SELECT COUNT(*) AS n FROM web_auth_events",
            vec![],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}

#[tokio::test]
async fn shared_startup_rejects_unsafe_web_before_io() {
    let config = ServerConfig {
        address: "0.0.0.0".into(),
        web_enabled: true,
        web_origin: Some(ORIGIN.into()),
        web_development: true,
        database: DatabaseConfig::new("invalid://must-not-connect"),
        ..Default::default()
    };
    let error = crate::bootstrap::run_server(config, std::path::PathBuf::from("must-not-create"))
        .await
        .expect_err("unsafe listener must be rejected before database or IPC I/O");
    assert!(error.to_string().contains("loopback listener"));
}

#[tokio::test]
async fn metric_snapshot_does_not_renew_idle_session_even_when_scope_is_unavailable() {
    let (app, db) = setup(30).await;
    let (cookie, _) = signed_in(&app).await;
    let seen = now_ms() - 1000;
    let idle = now_ms() + 60_000;
    db.connection()
        .execute_raw(store::params(
            &db,
            "UPDATE web_sessions SET last_seen_at=?,idle_expires_at=?",
            vec![seen.into(), idle.into()],
        ))
        .await
        .unwrap();
    let (status, _, value) = call(
        &app,
        "POST",
        "/api/v1/rpc",
        r#"{"jsonrpc":"2.0","id":1,"method":"metrics.latest","params":{"agentIds":["agent-a"]}}"#,
        Some(&cookie),
        None,
        ORIGIN,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["error"]["data"]["kind"], "FORBIDDEN");
    let row = db
        .connection()
        .query_one_raw(store::params(
            &db,
            "SELECT last_seen_at,idle_expires_at FROM web_sessions",
            vec![],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "last_seen_at").unwrap(), seen);
    assert_eq!(row.try_get::<i64>("", "idle_expires_at").unwrap(), idle);
}
