//! Same-origin browser authentication; see web_auth/SPEC.md for the security boundary.
#[cfg(test)]
mod regression;
mod store;
mod ws;
#[cfg(test)]
mod ws_tests;
#[cfg(test)]
use store::params;

use crate::{
    database::{
        DatabaseError, JobEventCursor, ServerDatabase, TaskReportCursor, WebJobOperationDraft,
        WebJobOperationRecord,
    },
    management::{AgentView, ControlRequest, ControlResponse},
    state::AppState,
};
use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State, rejection::BytesRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use prost::Message;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use smalux_protocol::agent::v1::JobEventKind;
use smalux_protocol::agent::v1::{JobDefinition, task_definition};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use uuid::Uuid;

const COOKIE: &str = "smalux_session";
const BODY_LIMIT: usize = 8192;
const RPC_BODY_LIMIT: usize = 4 * 1024 * 1024;
const CSRF_DOMAIN: &[u8] = b"smalux-web-csrf-v1\0";
const MAX_WEB_JOB_CATALOG_ENTRIES: usize = 1024;
const MAX_WEB_JOB_CATALOG_BYTES: usize = 1024 * 1024;

const MAX_JOB_CATALOG_JSON_BYTES: usize = 4 * 1024 * 1024;
#[derive(Clone)]
pub(crate) struct WebAuthConfig {
    pub enabled: bool,
    pub origin: String,
    pub absolute_ms: i64,
    pub idle_ms: i64,
    pub login_limit: usize,
    pub secure: bool,
}
#[derive(Clone)]
pub(crate) struct WebAuth {
    db: Arc<ServerDatabase>,
    config: WebAuthConfig,
    limiter: Arc<Mutex<Vec<Instant>>>,
    hashes: Arc<tokio::sync::Semaphore>,
    dummy_hash: String,
    metrics: crate::web_metrics::MetricsConfig,
    ws_hub: ws::WsHub,
}
impl WebAuth {
    pub(crate) async fn new(
        db: Arc<ServerDatabase>,
        config: WebAuthConfig,
        metrics: crate::web_metrics::MetricsConfig,
    ) -> anyhow::Result<Self> {
        let dummy_hash = if config.enabled {
            let mut random = [0; 32];
            getrandom::fill(&mut random)
                .map_err(|_| anyhow::anyhow!("random source unavailable"))?;
            let password = SecretString::from(hex(&random));
            tokio::task::spawn_blocking(move || password_hash(password))
                .await?
                .map_err(anyhow::Error::msg)?
        } else {
            String::new()
        };
        if config.enabled {
            store::cleanup(&db).await?;
        }
        Ok(Self {
            db,
            config,
            limiter: Arc::new(Mutex::new(Vec::new())),
            hashes: Arc::new(tokio::sync::Semaphore::new(2)),
            dummy_hash,
            metrics,
            ws_hub: ws::WsHub::default(),
        })
    }
    fn limit(&self) -> Result<(), Response> {
        let now = Instant::now();
        let mut attempts = self.limiter.lock().map_err(|_| internal())?;
        attempts.retain(|t| now.duration_since(*t) < Duration::from_secs(60));
        if attempts.len() >= self.config.login_limit {
            return Err(rate_limited("60"));
        }
        attempts.push(now);
        Ok(())
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    Admin,
    Operator,
    Viewer,
}
impl Role {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "admin" => Some(Self::Admin),
            "operator" => Some(Self::Operator),
            "viewer" => Some(Self::Viewer),
            _ => None,
        }
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionView {
    user_id: String,
    username: String,
    role: Role,
    csrf_token: String,
    expires_at_ms: i64,
    idle_expires_at_ms: i64,
    permissions: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Login {
    username: String,
    password: SecretString,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentListParams {
    status: Option<String>,
    name: Option<String>,
    online: Option<bool>,
    limit: Option<u32>,
    after: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentGetParams {
    agent_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobListParams {
    agent_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobGetParams {
    agent_id: String,
    job_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobCatalogReplaceParams {
    agent_id: String,
    expected_catalog_revision: String,
    client_mutation_id: String,
    definitions_hex: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OperationGetParams {
    operation_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OperationListParams {
    agent_id: String,
    limit: Option<u32>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentSummary {
    agent_id: String,
    display_name: String,
    authorization_status: String,
    online: bool,
    created_at_ms: i64,
    updated_at_ms: i64,
    revoked_at_ms: Option<i64>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentPage {
    items: Vec<AgentSummary>,
    next_cursor: Option<String>,
    has_more: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReportListParams {
    agent_id: String,
    job_id: Option<String>,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    cursor: Option<String>,
    limit: Option<u32>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventListParams {
    agent_id: String,
    job_id: Option<String>,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    cursor: Option<String>,
    limit: Option<u32>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReportSummary {
    report_id: String,
    agent_id: String,
    job_id: Option<String>,
    job_revision: String,
    run_id: Option<String>,
    attempt: u32,
    scheduled_at_ms: Option<i64>,
    started_at_ms: Option<i64>,
    result_kind: String,
    received_at_ms: i64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventSummary {
    event_id: String,
    agent_id: String,
    job_id: Option<String>,
    revision: String,
    run_id: Option<String>,
    attempt: u32,
    emitted_at_ms: i64,
    kind: String,
    message: String,
    will_retry: bool,
    gap_detected: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReportPage {
    items: Vec<ReportSummary>,
    next_cursor: Option<String>,
    has_more: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventPage {
    items: Vec<EventSummary>,
    next_cursor: Option<String>,
    has_more: bool,
}
fn agent_summary(agent: AgentView) -> AgentSummary {
    AgentSummary {
        agent_id: agent.agent_id,
        display_name: agent.name,
        authorization_status: agent.status,
        online: agent.online,
        created_at_ms: agent.created_at_unix_micros / 1000,
        updated_at_ms: agent.updated_at_unix_micros / 1000,
        revoked_at_ms: agent.revoked_at_unix_micros.map(|value| value / 1000),
    }
}
fn parse_list_cursor(value: &str) -> Option<(i64, &str)> {
    let (timestamp, id) = value.split_once('|')?;
    let timestamp = timestamp.parse::<i64>().ok()?;
    (!id.is_empty() && id.len() <= 256).then_some((timestamp, id))
}
fn cursor_value(timestamp_micros: i64, id: &str) -> String {
    format!("{timestamp_micros}|{id}")
}
fn validate_window(from_ms: Option<i64>, to_ms: Option<i64>) -> bool {
    match (from_ms, to_ms) {
        (None, None) => true,
        (Some(from), Some(to)) => from >= 0 && from < to,
        _ => false,
    }
}
fn parse_job_id(value: Option<&str>) -> Option<Option<Vec<u8>>> {
    value
        .map(|value| {
            Uuid::parse_str(value)
                .ok()
                .filter(|id| id.to_string() == value)
                .map(|id| id.as_bytes().to_vec())
        })
        .map_or(Some(None), |value| value.map(Some))
}
async fn current_session(auth: &WebAuth, secret: &[u8]) -> Result<store::SessionView, Response> {
    store::session(
        &auth.db,
        secret,
        false,
        auth.config.idle_ms,
        auth.config.absolute_ms,
    )
    .await
    .map_err(|_| internal())?
    .ok_or_else(unauthenticated)
}
fn role_allowed(method: &str, role: Role) -> bool {
    match method {
        "job.list"
        | "job.get"
        | "report.list"
        | "event.list"
        | "job.catalog.replace"
        | "operation.get"
        | "operation.list" => {
            matches!(role, Role::Admin | Role::Operator)
        }
        _ => true,
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MetaView {
    api_version: &'static str,
    design_contract_version: &'static str,
    transports: Vec<&'static str>,
    health: &'static str,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FeatureCatalog {
    features: Vec<FeatureState>,
    rpc_methods: Vec<MethodState>,
}
#[derive(Serialize)]
struct FeatureState {
    name: &'static str,
    state: &'static str,
    reason: Option<&'static str>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MethodState {
    method: &'static str,
    state: &'static str,
    permission: &'static str,
    input_schema_version: &'static str,
}
const PLANNED_METHODS: &[&str] = &[
    "user.list",
    "user.create",
    "user.update",
    "user.disable",
    "agent.updateMetadata",
    "enrollment.create",
    "enrollment.list",
    "enrollment.get",
    "enrollment.revoke",
    "job.create",
    "job.update",
    "job.disable",
];
fn catalog(role: Role) -> FeatureCatalog {
    let mut methods = vec![MethodState {
        method: "session.info",
        state: "available",
        permission: "session.info",
        input_schema_version: "v1",
    }];
    methods.push(MethodState {
        method: "metrics.latest",
        state: "available",
        permission: "metrics.latest",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "agent.list",
        state: "available",
        permission: "agent.list",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "agent.get",
        state: "available",
        permission: "agent.get",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "report.list",
        state: "available",
        permission: "report.list",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "event.list",
        state: "available",
        permission: "event.list",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "job.list",
        state: "available",
        permission: "job.list",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "job.get",
        state: "available",
        permission: "job.get",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "job.catalog.replace",
        state: "available",
        permission: "job.catalog.replace",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "operation.get",
        state: "available",
        permission: "operation.get",
        input_schema_version: "v1",
    });
    methods.push(MethodState {
        method: "operation.list",
        state: "available",
        permission: "operation.list",
        input_schema_version: "v1",
    });
    methods.extend(PLANNED_METHODS.iter().map(|method| MethodState {
        method,
        state: "planned",
        permission: "unavailable",
        input_schema_version: "v1",
    }));
    FeatureCatalog {
        features: vec![
            FeatureState {
                name: "web-auth",
                state: "available",
                reason: None,
            },
            FeatureState {
                name: "agent-management",
                state: "planned",
                reason: Some(
                    "Agent directory reads are available; lifecycle, metadata and full resource ACL writes remain planned",
                ),
            },
            FeatureState {
                name: "agent-directory",
                state: "available",
                reason: Some("read-only Agent list and detail queries"),
            },
            FeatureState {
                name: "job-catalog-read",
                state: "available",
                reason: Some(
                    "read-only Job catalog summaries; no Job mutations or runtime apply operation",
                ),
            },
            FeatureState {
                name: "job-catalog-write",
                state: "available",
                reason: Some(
                    "full-catalog CAS replacement with durable idempotency and asynchronous operation tracking",
                ),
            },
            FeatureState {
                name: "report-event-summary",
                state: "available",
                reason: Some(
                    "operator/admin summaries; raw payloads are not exposed by list methods",
                ),
            },
            FeatureState {
                name: "websocket",
                state: "available",
                reason: Some("metrics topic: CPU and memory; configured Agent/Job bindings only"),
            },
            FeatureState {
                name: "metrics.latest",
                state: "available",
                reason: Some("CPU and memory only"),
            },
            FeatureState {
                name: "user-management",
                state: "planned",
                reason: Some("Not implemented"),
            },
            FeatureState {
                name: "mfa",
                state: "unsupported",
                reason: Some("Not implemented"),
            },
        ],
        rpc_methods: methods
            .into_iter()
            .filter(|method| method.state != "available" || role_allowed(method.method, role))
            .collect(),
    }
}
fn error(status: StatusCode, kind: &'static str, message: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"error":{"kind":kind,"message":message}})),
    )
        .into_response()
}
fn internal() -> Response {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "Request failed",
    )
}
fn unauthenticated() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "UNAUTHENTICATED",
        "Authentication required",
    )
}
fn forbidden() -> Response {
    error(StatusCode::FORBIDDEN, "FORBIDDEN", "Request is not allowed")
}
fn no_store<T: Serialize>(body: T) -> Response {
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        Json(body),
    )
        .into_response()
}
fn rate_limited(after: &'static str) -> Response {
    let mut r = error(
        StatusCode::TOO_MANY_REQUESTS,
        "RATE_LIMITED",
        "Too many login attempts",
    );
    r.headers_mut()
        .insert("retry-after", HeaderValue::from_static(after));
    r
}
fn body_bytes(body: Result<Bytes, BytesRejection>) -> Result<Bytes, Response> {
    body.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "PAYLOAD_TOO_LARGE",
                "Request body too large",
            )
        } else {
            error(
                StatusCode::BAD_REQUEST,
                "VALIDATION_FAILED",
                "Cannot read request body",
            )
        }
    })
}
fn json_input<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, Response> {
    serde_json::from_slice(body).map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "VALIDATION_FAILED",
            "Invalid JSON request",
        )
    })
}
fn read_gate(headers: &HeaderMap, c: &WebAuthConfig) -> Result<(), Response> {
    if !c.enabled {
        return Err(error(StatusCode::NOT_FOUND, "NOT_FOUND", "Not found"));
    }
    if headers
        .get(header::ORIGIN)
        .is_some_and(|v| v.to_str().ok() != Some(c.origin.as_str()))
    {
        return Err(forbidden());
    }
    if headers
        .get("sec-fetch-site")
        .is_some_and(|v| !matches!(v.to_str().ok(), Some("same-origin" | "none")))
    {
        return Err(forbidden());
    }
    Ok(())
}
fn write_gate(headers: &HeaderMap, c: &WebAuthConfig) -> Result<(), Response> {
    read_gate(headers, c)?;
    if headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) != Some(c.origin.as_str())
        || headers.get("x-smalux-client").and_then(|v| v.to_str().ok()) != Some("web")
    {
        return Err(forbidden());
    }
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return Err(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "VALIDATION_FAILED",
            "Content-Type must be application/json",
        ));
    }
    Ok(())
}
fn now_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
fn hash(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn csrf(secret: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(CSRF_DOMAIN);
    h.update(secret);
    hex(&h.finalize())
}
fn cookie_secret(headers: &HeaderMap) -> Option<Vec<u8>> {
    let text = headers.get(header::COOKIE)?.to_str().ok()?;
    let mut matches = text
        .split(';')
        .filter_map(|p| p.trim().split_once('='))
        .filter(|(k, _)| *k == COOKIE);
    let value = matches.next()?.1;
    if matches.next().is_some() || value.len() != 64 {
        return None;
    }
    let bytes = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(value.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<_>>>()?;
    if hex(&bytes) != value {
        return None;
    }
    Some(bytes)
}
fn cookie(secret: Option<&[u8]>, secure: bool) -> HeaderValue {
    let mut value = format!(
        "{COOKIE}={}; Path=/api/v1; HttpOnly; SameSite=Strict",
        secret.map(hex).unwrap_or_default()
    );
    if secure {
        value.push_str("; Secure")
    };
    if secret.is_none() {
        value.push_str("; Max-Age=0")
    }
    HeaderValue::from_str(&value).expect("generated cookie is ASCII")
}
fn user_name(s: &str) -> Option<String> {
    if !(3..=64).contains(&s.len())
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        None
    } else {
        Some(s.to_ascii_lowercase())
    }
}
fn valid_password(s: &str) -> bool {
    (12..=128).contains(&s.chars().count()) && s.len() <= 512
}
fn password_hash(password: SecretString) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.expose_secret().as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|_| "Password hashing failed".into())
}

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/session", get(session))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/meta", get(meta))
        .route("/api/v1/rpc", post(rpc))
        .route("/api/v1/ws", get(ws::upgrade))
        .layer(DefaultBodyLimit::max(RPC_BODY_LIMIT))
}
async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let a = &state.web_auth;
    if let Err(r) = write_gate(&headers, &a.config) {
        return r;
    }
    if let Err(r) = a.limit() {
        return r;
    }
    let body = match body_bytes(body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.len() > BODY_LIMIT {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "PAYLOAD_TOO_LARGE",
            "Request body too large",
        );
    }
    let req: Login = match json_input(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let username = user_name(&req.username);
    if !valid_password(req.password.expose_secret()) || username.is_none() {
        return error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "Invalid username or password",
        );
    }
    let permit = match a.hashes.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return rate_limited("1"),
    };
    let user = match store::user(&a.db, username.as_deref().unwrap_or_default()).await {
        Ok(v) => v,
        Err(_) => return internal(),
    };
    let phc = user
        .as_ref()
        .map(|u| u.password_hash.clone())
        .unwrap_or_else(|| a.dummy_hash.clone());
    let ok = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        PasswordHash::new(&phc).ok().is_some_and(|h| {
            Argon2::default()
                .verify_password(req.password.expose_secret().as_bytes(), &h)
                .is_ok()
        })
    })
    .await;
    let Ok(ok) = ok else { return internal() };
    if store::cleanup(&a.db).await.is_err() {
        return internal();
    }
    let user = match user {
        Some(u) if ok && u.enabled => u,
        _ => {
            if store::denied(&a.db).await.is_err() {
                return internal();
            }
            return error(
                StatusCode::UNAUTHORIZED,
                "UNAUTHENTICATED",
                "Invalid username or password",
            );
        }
    };
    let mut secret = [0; 32];
    if getrandom::fill(&mut secret).is_err() {
        return internal();
    }
    match store::create_session(
        &a.db,
        &user,
        &secret,
        a.config.absolute_ms,
        a.config.idle_ms,
    )
    .await
    {
        Ok(session) => {
            let mut r = no_store(session);
            r.headers_mut()
                .insert(header::SET_COOKIE, cookie(Some(&secret), a.config.secure));
            r
        }
        Err(_) => internal(),
    }
}
async fn session(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let a = &state.web_auth;
    if let Err(r) = read_gate(&headers, &a.config) {
        return r;
    }
    let Some(secret) = cookie_secret(&headers) else {
        return unauthenticated();
    };
    match store::session(&a.db, &secret, true, a.config.idle_ms, a.config.absolute_ms).await {
        Ok(Some(v)) => no_store(v),
        Ok(None) => unauthenticated(),
        Err(_) => internal(),
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobOperationView {
    operation_id: String,
    agent_id: String,
    action: String,
    state: String,
    expected_catalog_revision: String,
    target_catalog_revision: String,
    target_job_id: Option<String>,
    command_id: Option<String>,
    reason: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    sent_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobOperationResult {
    job_ids: Vec<String>,
    operation: JobOperationView,
    catalog_revision: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobOperationPage {
    items: Vec<JobOperationView>,
}
fn operation_view(operation: WebJobOperationRecord) -> JobOperationView {
    JobOperationView {
        operation_id: operation.operation_id,
        agent_id: operation.agent_id,
        action: operation.action,
        state: operation.state,
        expected_catalog_revision: operation.expected_catalog_revision.to_string(),
        target_catalog_revision: operation.target_catalog_revision.to_string(),
        target_job_id: operation.target_job_id,
        command_id: operation.command_id,
        reason: operation.reason,
        created_at_ms: operation.created_at / 1000,
        updated_at_ms: operation.updated_at / 1000,
        sent_at_ms: operation.sent_at.map(|value| value / 1000),
        completed_at_ms: operation.completed_at.map(|value| value / 1000),
    }
}
fn canonical_uuid(value: &str) -> Option<String> {
    Uuid::parse_str(value)
        .ok()
        .filter(|uuid| uuid.to_string() == value)
        .map(|uuid| uuid.to_string())
}
fn decode_job_definitions(values: Vec<String>) -> Result<Vec<JobDefinition>, &'static str> {
    if values.len() > MAX_WEB_JOB_CATALOG_ENTRIES {
        return Err("Job catalog has too many entries");
    }
    let mut total_bytes = 0usize;
    let mut definitions = Vec::with_capacity(values.len());

    for encoded in values {
        if encoded.is_empty() || encoded.len() > 512 * 1024 || encoded.len() % 2 != 0 {
            return Err("JobDefinition hex is invalid or too large");
        }
        let mut bytes = Vec::with_capacity(encoded.len() / 2);
        for pair in encoded.as_bytes().chunks_exact(2) {
            let high = (pair[0] as char)
                .to_digit(16)
                .ok_or("JobDefinition hex is invalid")?;
            let low = (pair[1] as char)
                .to_digit(16)
                .ok_or("JobDefinition hex is invalid")?;
            bytes.push(((high << 4) | low) as u8);
        }
        total_bytes = total_bytes
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_WEB_JOB_CATALOG_BYTES)
            .ok_or("Job catalog exceeds its byte limit")?;
        let definition = JobDefinition::decode(bytes.as_slice())
            .map_err(|_| "JobDefinition Protobuf is invalid")?;
        if Uuid::from_slice(&definition.job_id).is_err() || definition.revision == 0 {
            return Err("JobDefinition id or revision is invalid");
        }
        let trigger = definition
            .trigger
            .as_ref()
            .ok_or("Job trigger is required")?;
        if trigger.schedule.is_none() || trigger.misfire.is_none() {
            return Err("Job schedule and misfire policy are required");
        }
        let behavior = smalux_protocol::agent::v1::MisfireBehavior::try_from(
            trigger
                .misfire
                .as_ref()
                .map(|value| value.behavior)
                .unwrap_or_default(),
        )
        .map_err(|_| "Job misfire behavior is unknown")?;
        if behavior == smalux_protocol::agent::v1::MisfireBehavior::Unspecified {
            return Err("Job misfire behavior is required");
        }
        job_task_kind(&definition).ok_or("Job task type is invalid")?;

        definitions.push(definition);
    }
    Ok(definitions)
}
fn request_sha256(value: &Value) -> Result<String, ()> {
    let bytes = serde_json::to_vec(value).map_err(|_| ())?;
    Ok(hex(&Sha256::digest(bytes)))
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobSummary {
    job_id: String,
    revision: String,
    enabled: bool,
    task_kind: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobCatalogView {
    agent_id: String,
    catalog_revision: String,
    jobs: Vec<JobSummary>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobDetail {
    agent_id: String,
    catalog_revision: String,
    job: JobSummary,
}
fn job_task_kind(definition: &JobDefinition) -> Option<String> {
    let kind = match definition.task.as_ref()?.task.as_ref()? {
        task_definition::Task::System(_) => "smalux.collect.system.v1",
        task_definition::Task::Cpu(_) => "smalux.collect.cpu.v1",
        task_definition::Task::Memory(_) => "smalux.collect.memory.v1",
        task_definition::Task::Load(_) => "smalux.collect.load.v1",
        task_definition::Task::Host(_) => "smalux.collect.host.v1",
        task_definition::Task::DiskIo(_) => "smalux.collect.disk_io.v1",
        task_definition::Task::NetworkIo(_) => "smalux.collect.network_io.v1",
        task_definition::Task::LocalIp(_) => "smalux.collect.local_ip.v1",
        task_definition::Task::PublicIp(_) => "smalux.collect.public_ip.v1",
        task_definition::Task::Process(_) => "smalux.collect.process.v1",
        task_definition::Task::Socket(_) => "smalux.collect.socket.v1",
        task_definition::Task::Probe(_) => "smalux.probe.network.v1",
        task_definition::Task::Plugin(plugin) if !plugin.task_kind.is_empty() => {
            return Some(plugin.task_kind.clone());
        }
        task_definition::Task::Plugin(_) => return None,
    };
    Some(kind.to_owned())
}
fn job_summary(bytes: &[u8]) -> Result<JobSummary, ()> {
    let definition = JobDefinition::decode(bytes).map_err(|_| ())?;
    let job_id = Uuid::from_slice(&definition.job_id).map_err(|_| ())?;
    let task_kind = job_task_kind(&definition).ok_or(())?;
    Ok(JobSummary {
        job_id: job_id.to_string(),
        revision: definition.revision.to_string(),
        enabled: definition.enabled,
        task_kind,
    })
}
fn job_catalog_view(
    agent_id: String,
    catalog_revision: u64,
    definitions: Vec<Vec<u8>>,
) -> Result<JobCatalogView, ()> {
    let jobs = definitions
        .into_iter()
        .map(|bytes| job_summary(&bytes))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(JobCatalogView {
        agent_id,
        catalog_revision: catalog_revision.to_string(),
        jobs,
    })
}
async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let a = &state.web_auth;
    if let Err(r) = write_gate(&headers, &a.config) {
        return r;
    }
    let body = match body_bytes(body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.len() > BODY_LIMIT {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "PAYLOAD_TOO_LARGE",
            "Request body too large",
        );
    }
    if let Err(r) = json_input::<Empty>(&body) {
        return r;
    }
    let mut revoked = false;
    if let Some(secret) = cookie_secret(&headers) {
        match store::session(
            &a.db,
            &secret,
            false,
            a.config.idle_ms,
            a.config.absolute_ms,
        )
        .await
        {
            Ok(Some(s)) => {
                let supplied = headers
                    .get("x-csrf-token")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if s.csrf_token
                    .as_bytes()
                    .ct_eq(supplied.as_bytes())
                    .unwrap_u8()
                    != 1
                {
                    return forbidden();
                }
                revoked = match store::revoke(&a.db, &secret, &s.user_id).await {
                    Ok(v) => v,
                    Err(_) => return internal(),
                };
                a.ws_hub.revoke(&secret);
            }
            Ok(None) => {}
            Err(_) => return internal(),
        }
    }
    let mut r = no_store(json!({"revoked":revoked}));
    r.headers_mut()
        .insert(header::SET_COOKIE, cookie(None, a.config.secure));
    r
}
async fn meta(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = read_gate(&headers, &state.web_auth.config) {
        return r;
    }
    no_store(MetaView {
        api_version: "v1",
        design_contract_version: "1.0.0",
        transports: vec!["http-json-rpc", "websocket"],
        health: "ok",
    })
}
fn rpc_error(id: Value, code: i32, kind: &str, message: &str) -> Response {
    no_store(
        json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message,"data":{"kind":kind}}}),
    )
}
fn rpc_error_data(id: Value, code: i32, kind: &str, message: &str, data: Value) -> Response {
    no_store(json!({
        "jsonrpc":"2.0",
        "id":id,
        "error":{"code":code,"message":message,"data":{"kind":kind,"details":data}}
    }))
}
fn job_mutation_error(id: Value, error: DatabaseError) -> Response {
    match error {
        DatabaseError::RevisionConflict {
            expected, actual, ..
        } => rpc_error_data(
            id,
            -32009,
            "REVISION_CONFLICT",
            "Job catalog revision changed",
            json!({"expectedRevision":expected.to_string(),"currentRevision":actual.to_string()}),
        ),
        DatabaseError::WebJobIdempotencyConflict => rpc_error(
            id,
            -32010,
            "IDEMPOTENCY_CONFLICT",
            "Mutation id was reused with different content",
        ),
        DatabaseError::InvalidJobCatalog(_) | DatabaseError::InvalidJobCommand(_) => {
            rpc_error(id, -32003, "VALIDATION_FAILED", "Job catalog is invalid")
        }
        _ => rpc_error(id, -32018, "INTERNAL_ERROR", "Job catalog mutation failed"),
    }
}
fn rpc_auth_error(id: Value, response: Response) -> Response {
    match response.status() {
        StatusCode::UNAUTHORIZED => {
            rpc_error(id, -32001, "UNAUTHENTICATED", "Authentication required")
        }
        StatusCode::FORBIDDEN => rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed"),
        _ => rpc_error(id, -32018, "INTERNAL_ERROR", "Request failed"),
    }
}
async fn rpc(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let a = &state.web_auth;
    if let Err(r) = write_gate(&headers, &a.config) {
        return r;
    }
    let body = match body_bytes(body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.len() > MAX_JOB_CATALOG_JSON_BYTES {
        return rpc_error(
            Value::Null,
            -32014,
            "PAYLOAD_TOO_LARGE",
            "RPC request body is too large",
        );
    }
    let Some(secret) = cookie_secret(&headers) else {
        return unauthenticated();
    };
    if let Err(response) = current_session(&a, &secret).await {
        return rpc_auth_error(Value::Null, response);
    }
    let req: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return rpc_error(Value::Null, -32700, "VALIDATION_FAILED", "Parse error"),
    };
    let Some(obj) = req.as_object() else {
        return rpc_error(Value::Null, -32600, "VALIDATION_FAILED", "Invalid request");
    };
    let id = obj.get("id").cloned().unwrap_or(Value::Null);
    let valid_id = id.is_string()
        || id.is_null()
        || id
            .as_i64()
            .is_some_and(|n| n.unsigned_abs() <= 9_007_199_254_740_991);
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !obj.contains_key("id")
        || !valid_id
        || obj.get("method").and_then(Value::as_str).is_none()
        || obj
            .keys()
            .any(|k| !["jsonrpc", "id", "method", "params"].contains(&k.as_str()))
    {
        return rpc_error(Value::Null, -32600, "VALIDATION_FAILED", "Invalid request");
    }
    let method = obj["method"].as_str().unwrap_or_default();
    let mut current_session = match current_session(&a, &secret).await {
        Ok(session) => session,
        Err(response) => return rpc_auth_error(id.clone(), response),
    };
    let read_only = matches!(
        method,
        "metrics.latest"
            | "session.info"
            | "agent.list"
            | "agent.get"
            | "job.list"
            | "job.get"
            | "report.list"
            | "event.list"
            | "operation.get"
            | "operation.list"
    );
    if !read_only {
        if !role_allowed(method, current_session.role) {
            return rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed");
        }
        let supplied = headers
            .get("x-csrf-token")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !current_session
            .csrf_token
            .as_bytes()
            .ct_eq(supplied.as_bytes())
            .unwrap_u8()
            .eq(&1)
        {
            return rpc_error(id, -32002, "FORBIDDEN", "CSRF validation failed");
        }
    }
    if method != "metrics.latest" {
        current_session = match store::session(
            &a.db,
            &secret,
            true,
            a.config.idle_ms,
            a.config.absolute_ms,
        )
        .await
        {
            Ok(Some(session)) => session,
            Ok(None) => return unauthenticated(),
            Err(_) => return internal(),
        };
    }
    if method == "metrics.latest" {
        let params: crate::web_metrics::LatestParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(v) => v,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        match tokio::time::timeout(
            Duration::from_secs(5),
            crate::web_metrics::latest(&a.db, &a.metrics, &params),
        )
        .await
        {
            Ok(Ok(result)) => return no_store(json!({"jsonrpc":"2.0","id":id,"result":result})),
            Ok(Err(e)) => {
                let (code, kind, message) = metric_problem(e);
                return rpc_error(id, code, kind, message);
            }
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Metric query timed out"),
        }
    }
    if method == "agent.list" {
        let params: AgentListParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(value) => value,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        let limit = params.limit.unwrap_or(50);
        if !(1..=100).contains(&limit)
            || params
                .status
                .as_deref()
                .is_some_and(|status| !matches!(status, "active" | "revoked"))
            || params.name.as_ref().is_some_and(|name| name.len() > 128)
            || params
                .after
                .as_ref()
                .is_some_and(|cursor| cursor.len() > 64)
        {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params");
        }
        let response = state
            .management
            .handle(ControlRequest::ListAgents {
                status: params.status,
                name: params.name,
                online: params.online,
                limit: limit + 1,
                after: params.after,
            })
            .await;
        let ControlResponse::Agents(agents) = response else {
            return rpc_error(id, -32018, "INTERNAL_ERROR", "Agent query failed");
        };
        let has_more = agents.len() > limit as usize;
        let items = agents
            .into_iter()
            .take(limit as usize)
            .map(agent_summary)
            .collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| items.last().map(|agent| agent.agent_id.clone()))
            .flatten();
        return no_store(json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":AgentPage { items, next_cursor, has_more }
        }));
    }
    if method == "agent.get" {
        let params: AgentGetParams = match serde_json::from_value::<AgentGetParams>(
            obj.get("params").cloned().unwrap_or(Value::Null),
        ) {
            Ok(value) if !value.agent_id.is_empty() && value.agent_id.len() <= 64 => value,
            _ => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
        };
        let response = state
            .management
            .handle(ControlRequest::GetAgent {
                agent_id: params.agent_id,
            })
            .await;
        return match response {
            ControlResponse::Agent(Some(agent)) => no_store(json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":agent_summary(agent)
            })),
            ControlResponse::Agent(None) => rpc_error(id, -32004, "NOT_FOUND", "Agent not found"),
            ControlResponse::Error { code, .. } if code == "not_found" => {
                rpc_error(id, -32004, "NOT_FOUND", "Agent not found")
            }
            _ => rpc_error(id, -32018, "INTERNAL_ERROR", "Agent query failed"),
        };
    }
    if method == "job.catalog.replace" {
        let params: JobCatalogReplaceParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(value) => value,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        if params.agent_id.is_empty() || params.agent_id.len() > 64 {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid Agent id");
        }
        let Some(client_mutation_id) = canonical_uuid(&params.client_mutation_id) else {
            return rpc_error(
                id,
                -32602,
                "VALIDATION_FAILED",
                "clientMutationId must be a UUID",
            );
        };
        let Some(expected_revision) =
            params
                .expected_catalog_revision
                .parse::<u64>()
                .ok()
                .filter(|value| {
                    value.to_string() == params.expected_catalog_revision
                        && *value <= i64::MAX as u64
                })
        else {
            return rpc_error(
                id,
                -32602,
                "VALIDATION_FAILED",
                "Invalid expectedCatalogRevision",
            );
        };
        let mut jobs = match decode_job_definitions(params.definitions_hex) {
            Ok(value) => value,
            Err(message) => return rpc_error(id, -32602, "VALIDATION_FAILED", message),
        };
        jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        let canonical_definitions = jobs
            .iter()
            .map(|job| hex(&job.encode_to_vec()))
            .collect::<Vec<_>>();
        let Some(target_revision) = expected_revision.checked_add(1) else {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Catalog revision overflow");
        };
        let canonical_request = json!({
            "agentId":params.agent_id,
            "expectedCatalogRevision":expected_revision.to_string(),
            "definitionsHex":canonical_definitions,
        });
        let request_hash = match request_sha256(&canonical_request) {
            Ok(value) => value,
            Err(_) => {
                return rpc_error(id, -32018, "INTERNAL_ERROR", "Request normalization failed");
            }
        };
        let actor_user_id = current_session.user_id.clone();
        match a.db.find_agent(&params.agent_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return rpc_error(id, -32004, "NOT_FOUND", "Agent not found"),
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Agent query failed"),
        }
        match a
            .db
            .get_web_job_operation_by_key(
                &actor_user_id,
                "job.catalog.replace",
                &client_mutation_id,
            )
            .await
        {
            Ok(Some(existing)) if existing.request_hash == request_hash => {
                let Ok(result) = serde_json::from_str::<Value>(&existing.response_json) else {
                    return rpc_error(
                        id,
                        -32018,
                        "INTERNAL_ERROR",
                        "Stored mutation response is invalid",
                    );
                };
                return no_store(json!({"jsonrpc":"2.0","id":id,"result":result}));
            }
            Ok(Some(_)) => {
                return rpc_error(
                    id,
                    -32010,
                    "IDEMPOTENCY_CONFLICT",
                    "Mutation id was reused with different content",
                );
            }
            Ok(None) => {}
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Mutation lookup failed"),
        }
        let operation_id = Uuid::new_v4().to_string();
        let created_at_ms = now_ms();
        let initial_result = JobOperationResult {
            job_ids: jobs
                .iter()
                .filter_map(|job| {
                    Uuid::from_slice(&job.job_id)
                        .ok()
                        .map(|value| value.to_string())
                })
                .collect(),
            operation: JobOperationView {
                operation_id: operation_id.clone(),
                agent_id: params.agent_id.clone(),
                action: "catalog.replace".to_owned(),
                state: "waiting_agent".to_owned(),
                expected_catalog_revision: expected_revision.to_string(),
                target_catalog_revision: target_revision.to_string(),
                target_job_id: None,
                command_id: None,
                reason: None,
                created_at_ms,
                updated_at_ms: created_at_ms,
                sent_at_ms: None,
                completed_at_ms: None,
            },
            catalog_revision: target_revision.to_string(),
        };
        let response_json = match serde_json::to_string(&initial_result) {
            Ok(value) => value,
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Response encoding failed"),
        };
        let draft = WebJobOperationDraft {
            operation_id,
            actor_user_id,
            method: "job.catalog.replace".to_owned(),
            client_mutation_id,
            request_hash,
            agent_id: params.agent_id.clone(),
            action: "catalog.replace".to_owned(),
            target_job_id: None,
            response_json,
        };
        let mutation = match a
            .db
            .replace_agent_job_catalog_for_web(&params.agent_id, jobs, expected_revision, draft)
            .await
        {
            Ok(mutation) => mutation,
            Err(error) => return job_mutation_error(id, error),
        };
        if mutation.catalog.is_some() && !mutation.replayed {
            state
                .agent
                .sessions
                .notify_catalog_changed(&params.agent_id)
                .await;
        }
        let result = match serde_json::from_str::<Value>(&mutation.operation.response_json) {
            Ok(value) => value,
            Err(_) => {
                return rpc_error(
                    id,
                    -32018,
                    "INTERNAL_ERROR",
                    "Stored mutation response is invalid",
                );
            }
        };
        return no_store(json!({"jsonrpc":"2.0","id":id,"result":result}));
    }
    if method == "operation.get" {
        let params: OperationGetParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(value) => value,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        let Some(operation_id) = canonical_uuid(&params.operation_id) else {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid operation id");
        };
        if !role_allowed("operation.get", current_session.role) {
            return rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed");
        }
        return match a.db.get_web_job_operation(&operation_id).await {
            Ok(Some(operation)) => {
                if current_session.role != Role::Admin
                    && operation.actor_user_id != current_session.user_id
                {
                    return rpc_error(id, -32004, "NOT_FOUND", "Operation not found");
                }
                no_store(json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "result":operation_view(operation)
                }))
            }
            Ok(None) => rpc_error(id, -32004, "NOT_FOUND", "Operation not found"),
            Err(_) => rpc_error(id, -32018, "INTERNAL_ERROR", "Operation query failed"),
        };
    }
    if method == "operation.list" {
        let params: OperationListParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(value) => value,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        let limit = params.limit.unwrap_or(50);
        if params.agent_id.is_empty() || params.agent_id.len() > 64 || !(1..=100).contains(&limit) {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid operation filters");
        }
        if !role_allowed("operation.list", current_session.role) {
            return rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed");
        }
        match a.db.find_agent(&params.agent_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return rpc_error(id, -32004, "NOT_FOUND", "Agent not found"),
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Operation query failed"),
        }
        return match a
            .db
            .list_recent_web_job_operations(
                &params.agent_id,
                (current_session.role != Role::Admin).then_some(current_session.user_id.as_str()),
                limit as usize,
            )
            .await
        {
            Ok(operations) => no_store(json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":JobOperationPage {
                    items:operations.into_iter().map(operation_view).collect()
                }
            })),
            Err(_) => rpc_error(id, -32018, "INTERNAL_ERROR", "Operation query failed"),
        };
    }
    if method == "job.list" || method == "job.get" {
        let is_detail = method == "job.get";
        let parsed = if is_detail {
            serde_json::from_value::<JobGetParams>(
                obj.get("params").cloned().unwrap_or(Value::Null),
            )
            .map(|params| (params.agent_id, Some(params.job_id)))
        } else {
            serde_json::from_value::<JobListParams>(
                obj.get("params").cloned().unwrap_or(Value::Null),
            )
            .map(|params| (params.agent_id, None))
        };
        let (agent_id, requested_job_id) = match parsed {
            Ok((agent_id, job_id)) if !agent_id.is_empty() && agent_id.len() <= 64 => {
                (agent_id, job_id)
            }
            _ => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
        };
        let requested_job_id = match requested_job_id {
            Some(job_id) => match Uuid::parse_str(&job_id) {
                Ok(parsed) if parsed.to_string() == job_id => Some(job_id),
                _ => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid Job id"),
            },
            None => None,
        };
        if !matches!(current_session.role, Role::Admin | Role::Operator) {
            return rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed");
        }
        match a.db.find_agent(&agent_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return rpc_error(id, -32004, "NOT_FOUND", "Agent not found"),
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Job query failed"),
        }
        let response = state
            .management
            .handle(ControlRequest::GetAgentJobCatalog {
                agent_id: agent_id.clone(),
            })
            .await;
        let catalog = match response {
            ControlResponse::AgentJobCatalog(Some(catalog)) => catalog,
            ControlResponse::AgentJobCatalog(None) => crate::management::AgentJobCatalogView {
                agent_id: agent_id.clone(),
                catalog_revision: 0,
                definitions: Vec::new(),
            },
            _ => return rpc_error(id, -32018, "INTERNAL_ERROR", "Job query failed"),
        };
        let catalog_size = catalog
            .definitions
            .iter()
            .try_fold(0usize, |size, definition| {
                size.checked_add(definition.len())
            });
        if catalog.definitions.len() > MAX_WEB_JOB_CATALOG_ENTRIES
            || catalog_size.is_none_or(|size| size > MAX_WEB_JOB_CATALOG_BYTES)
        {
            return rpc_error(id, -32014, "PAYLOAD_TOO_LARGE", "Job catalog is too large");
        }
        let catalog_revision = catalog.catalog_revision.to_string();
        if let Some(job_id) = requested_job_id {
            for definition in catalog.definitions {
                let job = match job_summary(&definition) {
                    Ok(job) => job,
                    Err(()) => {
                        return rpc_error(id, -32018, "INTERNAL_ERROR", "Stored Job is invalid");
                    }
                };
                if job.job_id == job_id {
                    return no_store(json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "result":JobDetail {
                            agent_id,
                            catalog_revision,
                            job,
                        }
                    }));
                }
            }
            return rpc_error(id, -32004, "NOT_FOUND", "Job not found");
        }
        let view = match job_catalog_view(agent_id, catalog.catalog_revision, catalog.definitions) {
            Ok(view) => view,
            Err(()) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Stored Job is invalid"),
        };
        return no_store(json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":view
        }));
    }
    if method == "report.list" {
        let params: ReportListParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(value) => value,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        let limit = params.limit.unwrap_or(50);
        let Some(job_id) = parse_job_id(params.job_id.as_deref()) else {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid Job id");
        };
        if params.agent_id.is_empty()
            || params.agent_id.len() > 64
            || !(1..=100).contains(&limit)
            || !validate_window(params.from_ms, params.to_ms)
            || params.from_ms.is_some_and(|value| value > i64::MAX / 1000)
            || params.to_ms.is_some_and(|value| value > i64::MAX / 1000)
            || params
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() > 512)
        {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid report filters");
        }
        if !matches!(current_session.role, Role::Admin | Role::Operator) {
            return rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed");
        }
        match a.db.find_agent(&params.agent_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return rpc_error(id, -32004, "NOT_FOUND", "Agent not found"),
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Report query failed"),
        }
        let after = match params.cursor.as_deref() {
            Some(cursor) => match parse_list_cursor(cursor) {
                Some((received_at, report_id)) if received_at >= 0 => Some(TaskReportCursor {
                    received_at,
                    report_id: report_id.to_owned(),
                }),
                _ => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid report cursor"),
            },
            None => None,
        };
        let records = match tokio::time::timeout(
            Duration::from_secs(5),
            a.db.query_task_reports(
                Some(&params.agent_id),
                job_id.as_deref(),
                params.from_ms,
                params.to_ms,
                after.as_ref(),
                u64::from(limit) + 1,
            ),
        )
        .await
        {
            Ok(Ok(records)) => records,
            _ => return rpc_error(id, -32018, "INTERNAL_ERROR", "Report query failed"),
        };
        let has_more = records.len() > limit as usize;
        let next_cursor = has_more.then(|| {
            let row = &records[limit as usize - 1];
            cursor_value(row.received_at, &row.report_id)
        });
        let items = records
            .into_iter()
            .take(limit as usize)
            .map(|row| ReportSummary {
                report_id: row.report_id,
                agent_id: row.agent_id,
                job_id: Uuid::from_slice(&row.job_id).ok().map(|id| id.to_string()),
                job_revision: row.job_revision.to_string(),
                run_id: Uuid::from_slice(&row.run_id).ok().map(|id| id.to_string()),
                attempt: u32::try_from(row.attempt).unwrap_or_default(),
                scheduled_at_ms: row.scheduled_at.map(|value| value / 1000),
                started_at_ms: row.started_at.map(|value| value / 1000),
                result_kind: row.result_kind,
                received_at_ms: row.received_at / 1000,
            })
            .collect();
        return no_store(json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":ReportPage { items, next_cursor, has_more }
        }));
    }
    if method == "event.list" {
        let params: EventListParams =
            match serde_json::from_value(obj.get("params").cloned().unwrap_or(Value::Null)) {
                Ok(value) => value,
                Err(_) => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params"),
            };
        let limit = params.limit.unwrap_or(50);
        let Some(job_id) = parse_job_id(params.job_id.as_deref()) else {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid Job id");
        };
        if params.agent_id.is_empty()
            || params.agent_id.len() > 64
            || !(1..=100).contains(&limit)
            || !validate_window(params.from_ms, params.to_ms)
            || params.from_ms.is_some_and(|value| value > i64::MAX / 1000)
            || params.to_ms.is_some_and(|value| value > i64::MAX / 1000)
            || params
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() > 512)
        {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid event filters");
        }
        if !role_allowed("event.list", current_session.role) {
            return rpc_error(id, -32002, "FORBIDDEN", "Access is not allowed");
        }
        match a.db.find_agent(&params.agent_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return rpc_error(id, -32004, "NOT_FOUND", "Agent not found"),
            Err(_) => return rpc_error(id, -32018, "INTERNAL_ERROR", "Event query failed"),
        }
        let after = match params.cursor.as_deref() {
            Some(cursor) => match parse_list_cursor(cursor) {
                Some((emitted_at, event_id)) if emitted_at >= 0 => Some(JobEventCursor {
                    emitted_at,
                    event_id: event_id.to_owned(),
                }),
                _ => return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid event cursor"),
            },
            None => None,
        };
        let records = match tokio::time::timeout(
            Duration::from_secs(5),
            a.db.query_job_events(
                Some(&params.agent_id),
                job_id.as_deref(),
                params.from_ms,
                params.to_ms,
                after.as_ref(),
                u64::from(limit) + 1,
            ),
        )
        .await
        {
            Ok(Ok(records)) => records,
            _ => return rpc_error(id, -32018, "INTERNAL_ERROR", "Event query failed"),
        };
        let has_more = records.len() > limit as usize;
        let next_cursor = has_more.then(|| {
            let row = &records[limit as usize - 1];
            cursor_value(row.emitted_at, &row.event_id)
        });
        let items = records
            .into_iter()
            .take(limit as usize)
            .map(|row| EventSummary {
                event_id: row.event_id,
                agent_id: row.agent_id,
                job_id: Uuid::from_slice(&row.job_id).ok().map(|id| id.to_string()),
                revision: row.revision.to_string(),
                run_id: Uuid::from_slice(&row.run_id).ok().map(|id| id.to_string()),
                attempt: u32::try_from(row.attempt).unwrap_or_default(),
                emitted_at_ms: row.emitted_at / 1000,
                kind: JobEventKind::try_from(row.kind)
                    .map(|kind| kind.as_str_name().to_owned())
                    .unwrap_or_else(|_| "JOB_EVENT_KIND_UNSPECIFIED".to_owned()),
                message: row.message,
                will_retry: row.will_retry,
                gap_detected: row.gap_detected,
            })
            .collect();
        return no_store(json!({
            "jsonrpc":"2.0",
            "id":id,
            "result":EventPage { items, next_cursor, has_more }
        }));
    }
    if method == "session.info" {
        if !obj
            .get("params")
            .and_then(Value::as_object)
            .is_some_and(|p| p.is_empty())
        {
            return rpc_error(id, -32602, "VALIDATION_FAILED", "Invalid params");
        }
        return no_store(json!({"jsonrpc":"2.0","id":id,"result":catalog(current_session.role)}));
    }
    if PLANNED_METHODS.contains(&method) {
        rpc_error(id, -32017, "UNSUPPORTED_FEATURE", "Method unavailable")
    } else {
        rpc_error(id, -32601, "NOT_FOUND", "Method not found")
    }
}
pub(crate) async fn bootstrap(
    db: Arc<ServerDatabase>,
    username: &str,
    password: &str,
) -> anyhow::Result<()> {
    let username = user_name(username).ok_or_else(|| {
        anyhow::anyhow!("Username must contain 3-64 ASCII letters, digits, '.', '_' or '-'.")
    })?;
    anyhow::ensure!(
        valid_password(password),
        "Password must contain 12-128 characters and at most 512 UTF-8 bytes."
    );
    let password = SecretString::from(password.to_owned());
    let phc = tokio::task::spawn_blocking(move || password_hash(password))
        .await?
        .map_err(anyhow::Error::msg)?;
    store::bootstrap(&db,&username,phc).await.map_err(|_|anyhow::anyhow!("Administrator bootstrap failed: already initialized, concurrent initialization or database unavailable"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn username_is_canonical_and_restricted() {
        assert_eq!(user_name("Alice.Admin").as_deref(), Some("alice.admin"));
        assert!(user_name("ab").is_none());
        assert!(user_name("älice").is_none())
    }
    #[test]
    fn password_boundaries_are_enforced() {
        assert!(valid_password("correct-horse-2"));
        assert!(!valid_password("short"));
        assert!(!valid_password(&"é".repeat(129)))
    }
    #[test]
    fn csrf_derivation_is_domain_separated() {
        assert_ne!(csrf(&[1; 32]), hex(&hash(&[1; 32])))
    }
    #[test]
    fn cookie_flags_and_secret_format() {
        let c = cookie(Some(&[1; 32]), true);
        let v = c.to_str().unwrap();
        assert!(v.contains("Secure") && v.contains("HttpOnly") && v.contains("SameSite=Strict"));
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, c);
        assert_eq!(cookie_secret(&h), Some(vec![1; 32]));
        assert!(
            !cookie(Some(&[1; 32]), false)
                .to_str()
                .unwrap()
                .contains("Secure")
        );
    }
    #[test]
    fn job_summary_preserves_installed_plus_task_kind() {
        use smalux_protocol::agent::v1::{
            JobDefinition, PluginTaskConfig, TaskDefinition, task_definition,
        };

        let definition = JobDefinition {
            job_id: Uuid::new_v4().as_bytes().to_vec(),
            revision: 7,
            enabled: true,
            task: Some(TaskDefinition {
                task: Some(task_definition::Task::Plugin(PluginTaskConfig {
                    plugin_id: "vendor.plugin".to_owned(),
                    plugin_version: "1.2.3".to_owned(),
                    task_kind: "vendor.backup.snapshot.v2".to_owned(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        let summary = job_summary(&definition.encode_to_vec()).expect("valid plus job");
        assert_eq!(summary.task_kind, "vendor.backup.snapshot.v2");
        assert_eq!(summary.revision, "7");
    }
}

fn metric_problem(error: crate::web_metrics::MetricsError) -> (i32, &'static str, &'static str) {
    match error {
        crate::web_metrics::MetricsError::InvalidParams => {
            (-32602, "VALIDATION_FAILED", "Invalid metric parameters")
        }
        crate::web_metrics::MetricsError::Forbidden => {
            (-32002, "FORBIDDEN", "Metric scope is not available")
        }
        crate::web_metrics::MetricsError::Internal => {
            (-32018, "INTERNAL_ERROR", "Metric query failed")
        }
    }
}
