use super::store::params;
use super::*;
use crate::{
    config::{DatabaseConfig, ServerConfig},
    database::entity::agent,
};
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use futures_util::{SinkExt, StreamExt};
use sea_orm::{ConnectionTrait, EntityTrait, Set};
use smalux_protocol::agent::v1::{
    CpuSnapshot, JobDefinition, MemorySnapshot, TaskDefinition, TaskReport, TaskResult,
    task_definition, task_result,
};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tower::ServiceExt;
const ORIGIN: &str = "http://127.0.0.1:5173";
const IO_TIMEOUT: Duration = Duration::from_secs(5);
async fn server() -> (
    String,
    String,
    String,
    AppState,
    tokio::task::JoinHandle<()>,
) {
    let db = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
        .await
        .unwrap();
    let config = ServerConfig {
        web_enabled: true,
        web_development: true,
        web_origin: Some(ORIGIN.into()),
        ..Default::default()
    };
    let state = AppState::build(config.runtime_config(), db).await.unwrap();
    bootstrap(state.database.clone(), "ws-admin", "ws-test-password-only")
        .await
        .unwrap();
    let router = crate::route::build_app_router(state.clone()).unwrap();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("origin", ORIGIN)
                .header("x-smalux-client", "web")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"ws-admin","password":"ws-test-password-only"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/api/v1/ws", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (
        url,
        cookie,
        value["csrfToken"].as_str().unwrap().to_owned(),
        state,
        handle,
    )
}
fn request(
    url: &str,
    cookie: Option<&str>,
    origin: &str,
) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut r = url.into_client_request().unwrap();
    r.headers_mut().insert("origin", origin.parse().unwrap());
    if let Some(c) = cookie {
        r.headers_mut().insert("cookie", c.parse().unwrap());
    }
    r
}
async fn metrics_server() -> (
    String,
    String,
    AppState,
    tokio::task::JoinHandle<()>,
    uuid::Uuid,
    uuid::Uuid,
) {
    let cpu_id = uuid::Uuid::new_v4();
    let memory_id = uuid::Uuid::new_v4();
    let bindings=serde_json::json!([{"agentId":"agent-a","cpuJobId":cpu_id.to_string(),"memoryJobId":memory_id.to_string()}]).to_string();
    let db = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
        .await
        .unwrap();
    let config = ServerConfig {
        web_enabled: true,
        web_development: true,
        web_origin: Some(ORIGIN.into()),
        web_metrics_bindings: bindings,
        ..Default::default()
    };
    let state = AppState::build(config.runtime_config(), db).await.unwrap();
    let now = now_ms();
    agent::Entity::insert(agent::ActiveModel {
        agent_id: Set("agent-a".into()),
        name: Set("Agent A".into()),
        public_key: Set(vec![7; 32]),
        status: Set("active".into()),
        created_at: Set(now),
        updated_at: Set(now),
        revoked_at: Set(None),
    })
    .exec(state.database.connection())
    .await
    .unwrap();
    state
        .database
        .replace_agent_job_catalog(
            "agent-a",
            vec![job(cpu_id, 1, true), job(memory_id, 1, false)],
        )
        .await
        .unwrap();
    append_metric_report(
        &state.database,
        cpu_id,
        memory_id,
        1,
        1,
        41.5,
        Some((4096, 16384)),
        sample_time(),
    )
    .await;
    bootstrap(state.database.clone(), "ws-admin", "ws-test-password-only")
        .await
        .unwrap();
    let router = crate::route::build_app_router(state.clone()).unwrap();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("origin", ORIGIN)
                .header("x-smalux-client", "web")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"ws-admin","password":"ws-test-password-only"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/api/v1/ws", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, cookie, state, handle, cpu_id, memory_id)
}
fn job(id: uuid::Uuid, revision: u64, cpu: bool) -> JobDefinition {
    JobDefinition {
        job_id: id.as_bytes().to_vec(),
        revision,
        enabled: true,
        task: Some(TaskDefinition {
            task: Some(if cpu {
                task_definition::Task::Cpu(Default::default())
            } else {
                task_definition::Task::Memory(Default::default())
            }),
        }),
        ..Default::default()
    }
}
fn sample_time() -> prost_types::Timestamp {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    prost_types::Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}
async fn append_metric_report(
    db: &ServerDatabase,
    cpu_id: uuid::Uuid,
    memory_id: uuid::Uuid,
    revision: u64,
    attempt: u32,
    cpu: f32,
    memory: Option<(u64, u64)>,
    started_at: prost_types::Timestamp,
) {
    db.append_task_report(
        "agent-a",
        &TaskReport {
            job_id: cpu_id.as_bytes().to_vec(),
            job_revision: revision,
            run_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            attempt,
            started_at: Some(started_at.clone()),
            result: Some(TaskResult {
                result: Some(task_result::Result::Cpu(CpuSnapshot {
                    warmed_up: true,
                    logical_cpu_count: 8,
                    global_usage_percent: cpu,
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    if let Some((used, total)) = memory {
        db.append_task_report(
            "agent-a",
            &TaskReport {
                job_id: memory_id.as_bytes().to_vec(),
                job_revision: revision,
                run_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                attempt,
                started_at: Some(started_at),
                result: Some(TaskResult {
                    result: Some(task_result::Result::Memory(MemorySnapshot {
                        used_bytes: used,
                        total_bytes: total,
                        usage_percent: used as f64 * 100.0 / total as f64,
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
}
async fn ws_json<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let text = tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => break text.to_string(),
                Some(Ok(Message::Ping(data))) => {
                    let _ = socket.send(Message::Pong(data)).await;
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("websocket read failed: {e}"),
                None => panic!("websocket closed before a reply"),
            }
        }
    })
    .await
    .expect("websocket reply timeout");
    serde_json::from_str(&text).unwrap()
}
async fn subscribe<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    id: u64,
    since: Option<&str>,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut params = json!({"topic":"metrics","agentIds":["agent-a"],"metrics":["cpu","memory"]});
    if let Some(cursor) = since {
        params["sinceCursor"] = json!(cursor);
    }
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":id,"method":"stream.subscribe","params":params})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    ws_json(socket).await
}
async fn http_latest(state: &AppState, cookie: &str) -> Value {
    let response=crate::route::build_app_router(state.clone()).unwrap().oneshot(Request::builder().method("POST").uri("/api/v1/rpc").header("origin",ORIGIN).header("x-smalux-client","web").header("content-type","application/json").header("cookie",cookie).body(Body::from(r#"{"jsonrpc":"2.0","id":88,"method":"metrics.latest","params":{"agentIds":["agent-a"],"metrics":["cpu","memory"]}}"#)).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 8192).await.unwrap();
    serde_json::from_slice::<Value>(&body).unwrap()["result"][0].clone()
}
fn cookie_secret_bytes(cookie: &str) -> Vec<u8> {
    let value = cookie.split_once('=').unwrap().1;
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
        .collect()
}
fn millis_timestamp(ms: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
    }
}
async fn login_metrics(state: &AppState) -> String {
    let response = crate::route::build_app_router(state.clone())
        .unwrap()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("origin", ORIGIN)
                .header("x-smalux-client", "web")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"ws-admin","password":"ws-test-password-only"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}
async fn no_text_for<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>, duration: Duration)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let result = tokio::time::timeout(duration, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Ping(data))) => {
                    let _ = socket.send(Message::Pong(data)).await;
                }
                Some(Ok(Message::Text(text))) => break Some(text.to_string()),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break None,
            }
        }
    })
    .await;
    assert!(
        result.is_err(),
        "unexpected websocket message after unsubscribe or stale report: {result:?}"
    );
}
async fn closed<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let result = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            match socket.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                _ => {}
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "websocket should close after access is revoked"
    );
}

#[tokio::test]
async fn real_ws_handshake_ping_and_logout_close() {
    let (url, cookie, csrf, state, handle) = server().await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .expect("authenticated websocket should upgrade");
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE web_sessions SET last_seen_at=?",
            vec![(now_ms() - 1000).into()],
        ))
        .await
        .unwrap();
    let secret = cookie_secret_bytes(&cookie);
    let digest = hex(&hash(&secret));
    let row = state
        .database
        .connection()
        .query_one_raw(params(
            &state.database,
            "SELECT last_seen_at FROM web_sessions WHERE secret_hash=?",
            vec![digest.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    let last_seen: i64 = row.try_get("", "last_seen_at").unwrap();
    socket
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":1,"method":"stream.ping","params":{}}"#.into(),
        ))
        .await
        .unwrap();
    let value = ws_json(&mut socket).await;
    assert!(value["result"]["serverTimeMs"].is_number());
    let row = state
        .database
        .connection()
        .query_one_raw(params(
            &state.database,
            "SELECT last_seen_at FROM web_sessions WHERE secret_hash=?",
            vec![digest.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.try_get::<i64>("", "last_seen_at").unwrap(),
        last_seen,
        "stream.ping must not touch session last_seen_at"
    );
    let app = crate::route::build_app_router(state.clone()).unwrap();
    let logout = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/logout")
                .header("origin", ORIGIN)
                .header("content-type", "application/json")
                .header("x-smalux-client", "web")
                .header("x-csrf-token", csrf)
                .header("cookie", cookie)
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::OK);
    let msg = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .expect("logout closes WS promptly");
    assert!(matches!(
        msg,
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    handle.abort();
    state.shutdown.cancel();
}
#[tokio::test]
async fn real_ws_rejects_missing_session_and_untrusted_origin() {
    let (url, cookie, _, state, handle) = server().await;
    for (c, o, code) in [
        (None, ORIGIN, 401),
        (Some(cookie.as_str()), "http://evil.test", 403),
    ] {
        let err = tokio_tungstenite::connect_async(request(&url, c, o))
            .await
            .unwrap_err();
        match err {
            tokio_tungstenite::tungstenite::Error::Http(r) => assert_eq!(r.status().as_u16(), code),
            _ => panic!("expected HTTP rejection"),
        }
    }
    handle.abort();
    state.shutdown.cancel();
}
#[tokio::test]
async fn real_ws_metrics_snapshot_updates_revision_and_resync() {
    let (url, cookie, state, handle, cpu_id, memory_id) = metrics_server().await;
    let http = http_latest(&state, &cookie).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .unwrap();
    let ack = subscribe(&mut socket, 1, None).await;
    assert_eq!(ack["id"], 1);
    assert_eq!(ack["result"]["sequence"], "0");
    assert_eq!(
        ack["result"]["snapshot"][0], http,
        "HTTP and WS first snapshots share the same real CPU/memory report projection"
    );
    assert_eq!(http["cpu"]["value"]["cpuUsagePercent"], 41.5);
    assert_eq!(http["cpu"]["value"]["logicalCores"], 8);
    assert_eq!(http["memory"]["value"]["usedBytes"], 4096);
    assert_eq!(http["memory"]["value"]["totalBytes"], 16384);
    assert_eq!(http["memory"]["value"]["usedPercent"], 25.0);

    append_metric_report(
        &state.database,
        cpu_id,
        memory_id,
        1,
        2,
        63.25,
        Some((8192, 16384)),
        sample_time(),
    )
    .await;
    let mut sequence = 0;
    wait_metrics(&mut socket, 63.25, 8192, &mut sequence).await;

    // A report received later but sampled earlier must not replace the newer measurement.
    let late = millis_timestamp(now_ms() - 10_000);
    append_metric_report(
        &state.database,
        cpu_id,
        memory_id,
        1,
        3,
        3.0,
        Some((1, 16384)),
        late,
    )
    .await;
    no_text_for(&mut socket, Duration::from_secs(3)).await;

    state
        .database
        .replace_agent_job_catalog(
            "agent-a",
            vec![job(cpu_id, 2, true), job(memory_id, 2, false)],
        )
        .await
        .unwrap();
    append_metric_report(
        &state.database,
        cpu_id,
        memory_id,
        2,
        1,
        22.0,
        Some((2048, 16384)),
        sample_time(),
    )
    .await;
    let update = wait_metrics(&mut socket, 22.0, 2048, &mut sequence).await;
    assert_eq!(
        update["params"]["data"]["items"][0]["cpu"]["sourceJobRevision"],
        "2"
    );
    append_metric_report(
        &state.database,
        cpu_id,
        memory_id,
        1,
        4,
        99.0,
        Some((16000, 16384)),
        sample_time(),
    )
    .await;
    no_text_for(&mut socket, Duration::from_secs(3)).await;

    let sub_id = ack["result"]["subscriptionId"].as_str().unwrap();
    let epoch = ack["result"]["streamEpoch"].as_str().unwrap();
    socket.send(Message::Text(json!({"jsonrpc":"2.0","id":2,"method":"stream.unsubscribe","params":{"subscriptionId":sub_id,"streamEpoch":epoch}}).to_string().into())).await.unwrap();
    let unsub = ws_json(&mut socket).await;
    assert_eq!(unsub["result"]["unsubscribed"], true);
    append_metric_report(
        &state.database,
        cpu_id,
        memory_id,
        2,
        2,
        78.0,
        Some((4096, 16384)),
        sample_time(),
    )
    .await;
    no_text_for(&mut socket, Duration::from_secs(3)).await;

    let old_epoch = epoch.to_owned();
    socket.close(None).await.unwrap();
    drop(socket);
    let (mut reconnect, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .unwrap();
    let ack = subscribe(&mut reconnect, 3, Some(&format!("{old_epoch}:1"))).await;
    assert_ne!(ack["result"]["streamEpoch"], old_epoch);
    assert_eq!(ack["result"]["sequence"], "0");
    let resync = ws_json(&mut reconnect).await;
    assert_eq!(resync["params"]["kind"], "resyncRequired");
    assert_eq!(resync["params"]["sequence"], "1");
    assert_eq!(resync["params"]["data"]["reason"], "epochChanged");

    reconnect.send(Message::Text(r#"{"jsonrpc":"2.0","id":4,"method":"stream.subscribe","params":{"topic":"metrics","agentIds":["unknown-agent"]}}"#.into())).await.unwrap();
    let denied = ws_json(&mut reconnect).await;
    assert_eq!(denied["error"]["data"]["kind"], "FORBIDDEN");
    reconnect.close(None).await.unwrap();
    drop(reconnect);
    handle.abort();
    state.shutdown.cancel();
}

#[tokio::test]
async fn real_ws_subscription_limits_duplicate_ids_and_non_stream_methods() {
    let (url, cookie, state, handle, _, _) = metrics_server().await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .unwrap();
    let mut ids = Vec::new();
    for id in 1..=16 {
        let ack = subscribe(&mut socket, id, None).await;
        assert!(ack["result"]["subscriptionId"].is_string());
        ids.push((
            ack["result"]["subscriptionId"].as_str().unwrap().to_owned(),
            ack["result"]["streamEpoch"].as_str().unwrap().to_owned(),
        ));
    }
    socket.send(Message::Text(r#"{"jsonrpc":"2.0","id":99,"method":"stream.subscribe","params":{"topic":"metrics","agentIds":["agent-a"]}}"#.into())).await.unwrap();
    assert_eq!(
        ws_json(&mut socket).await["error"]["data"]["kind"],
        "RATE_LIMITED"
    );
    socket
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":98,"method":"session.info","params":{}}"#.into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ws_json(&mut socket).await["error"]["data"]["kind"],
        "NOT_FOUND"
    );
    socket
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":97,"method":"stream.ping","params":{}}"#.into(),
        ))
        .await
        .unwrap();
    assert!(ws_json(&mut socket).await["result"]["serverTimeMs"].is_number());
    socket
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":97,"method":"stream.ping","params":{}}"#.into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ws_json(&mut socket).await["error"]["message"],
        "Repeated request id"
    );
    for (index, (subscription_id, stream_epoch)) in ids.iter().enumerate() {
        socket.send(Message::Text(json!({"jsonrpc":"2.0","id":100+index,"method":"stream.unsubscribe","params":{"subscriptionId":subscription_id,"streamEpoch":stream_epoch}}).to_string().into())).await.unwrap();
        assert_eq!(ws_json(&mut socket).await["result"]["unsubscribed"], true);
    }
    socket.close(None).await.unwrap();
    drop(socket);
    handle.abort();
    state.shutdown.cancel();
}

#[tokio::test]
async fn real_ws_persisted_authorization_changes_and_shutdown_close_stream() {
    let (url, first_cookie, _, state, handle) = server().await;
    let (mut disabled, _) =
        tokio_tungstenite::connect_async(request(&url, Some(&first_cookie), ORIGIN))
            .await
            .unwrap();
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE web_users SET enabled=?",
            vec![false.into()],
        ))
        .await
        .unwrap();
    closed(&mut disabled).await;
    drop(disabled);
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE web_users SET enabled=?",
            vec![true.into()],
        ))
        .await
        .unwrap();

    let revoked_cookie = login_metrics(&state).await;
    let (mut revoked, _) =
        tokio_tungstenite::connect_async(request(&url, Some(&revoked_cookie), ORIGIN))
            .await
            .unwrap();
    let revoked_hash = hex(&hash(&cookie_secret_bytes(&revoked_cookie)));
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE web_sessions SET revoked_at=? WHERE secret_hash=?",
            vec![now_ms().into(), revoked_hash.into()],
        ))
        .await
        .unwrap();
    closed(&mut revoked).await;
    drop(revoked);

    let expired_cookie = login_metrics(&state).await;
    let (mut expired, _) =
        tokio_tungstenite::connect_async(request(&url, Some(&expired_cookie), ORIGIN))
            .await
            .unwrap();
    let expired_hash = hex(&hash(&cookie_secret_bytes(&expired_cookie)));
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE web_sessions SET expires_at=? WHERE secret_hash=?",
            vec![0_i64.into(), expired_hash.into()],
        ))
        .await
        .unwrap();
    closed(&mut expired).await;
    drop(expired);
    handle.abort();
    state.shutdown.cancel();

    let (url, cookie, state, handle, _, _) = metrics_server().await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .unwrap();
    let ack = subscribe(&mut socket, 201, None).await;
    assert!(ack["result"]["subscriptionId"].is_string());
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE agents SET status=?,revoked_at=? WHERE agent_id=?",
            vec!["revoked".into(), now_ms().into(), "agent-a".into()],
        ))
        .await
        .unwrap();
    closed(&mut socket).await;
    drop(socket);

    let cookie = login_metrics(&state).await;
    let (mut shutdown, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .unwrap();
    state.shutdown.cancel();
    closed(&mut shutdown).await;
    drop(shutdown);
    handle.abort();
}

// CPU and memory reports commit independently. Intermediate snapshots are legitimate,
// but sequence gaps, invalid values, and failure to reach the committed state are not.
async fn wait_metrics<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    cpu: f64,
    used: u64,
    sequence: &mut u64,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            let update = ws_json(socket).await;
            assert_eq!(update["method"], "stream.notification", "{update}");
            assert_eq!(update["params"]["kind"], "metrics.update", "{update}");
            let next = update["params"]["sequence"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            assert_eq!(next, *sequence + 1, "sequence gap: {update}");
            *sequence = next;
            let item = &update["params"]["data"]["items"][0];
            for name in ["cpu", "memory"] {
                assert_ne!(item[name]["quality"]["state"], "unavailable", "{update}");
            }
            if item["cpu"]["value"]["cpuUsagePercent"] == cpu
                && item["memory"]["value"]["usedBytes"] == used
            {
                assert_eq!(item["cpu"]["quality"]["state"], "valid");
                assert_eq!(item["memory"]["quality"]["state"], "valid");
                return update;
            }
        }
    })
    .await
    .expect("committed CPU/memory values must arrive within the push deadline")
}

#[tokio::test]
async fn real_ws_idle_deadline_closes_even_after_ping() {
    let (url, cookie, _, state, handle) = server().await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, Some(&cookie), ORIGIN))
        .await
        .unwrap();
    state
        .database
        .connection()
        .execute_raw(params(
            &state.database,
            "UPDATE web_sessions SET idle_expires_at=?",
            vec![(now_ms() + 400).into()],
        ))
        .await
        .unwrap();
    socket
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":1,"method":"stream.ping","params":{}}"#.into(),
        ))
        .await
        .unwrap();
    assert!(ws_json(&mut socket).await["result"]["serverTimeMs"].is_number());
    closed(&mut socket).await;
    handle.abort();
    state.shutdown.cancel();
}
