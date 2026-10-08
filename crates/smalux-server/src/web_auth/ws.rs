//! Browser WebSocket subscription lifecycle. No ordinary business writes are accepted.
use super::*;
use crate::web_metrics::{self, LatestParams, MetricName, MetricsError, MetricsLatest};
use axum::extract::{
    WebSocketUpgrade,
    ws::{CloseFrame, Message, WebSocket},
};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::atomic::{AtomicU16, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

const CONNECTION_LIMIT: usize = 32;
const SUBSCRIPTION_LIMIT: usize = 16;
const AGENT_LIMIT: usize = 100;
const QUEUE_LIMIT: usize = 32;
const INPUT_LIMIT: usize = 64 * 1024;
const OUTPUT_LIMIT: usize = 1024 * 1024;
const PUSH_INTERVAL: Duration = Duration::from_secs(2);
const AUTH_INTERVAL: Duration = Duration::from_secs(1);
const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(super) struct WsHub {
    slots: Arc<Semaphore>,
    connections: Arc<Mutex<HashMap<uuid::Uuid, (String, CancellationToken)>>>,
}
impl Default for WsHub {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(CONNECTION_LIMIT)),
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
impl WsHub {
    pub(super) fn revoke(&self, secret: &[u8]) {
        let digest = hex(&hash(secret));
        if let Ok(entries) = self.connections.lock() {
            for (key, token) in entries.values() {
                if *key == digest {
                    token.cancel()
                }
            }
        }
    }
    fn acquire(&self, secret: &[u8]) -> Result<Lease, Response> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| rate_limited("1"))?;
        let id = uuid::Uuid::new_v4();
        let cancel = CancellationToken::new();
        self.connections
            .lock()
            .map_err(|_| internal())?
            .insert(id, (hex(&hash(secret)), cancel.clone()));
        Ok(Lease {
            id,
            hub: self.clone(),
            cancel,
            _permit: permit,
        })
    }
}
struct Lease {
    id: uuid::Uuid,
    hub: WsHub,
    cancel: CancellationToken,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Ok(mut entries) = self.hub.connections.lock() {
            entries.remove(&self.id);
        }
    }
}

pub(super) async fn upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let a = &state.web_auth;
    if let Err(r) = read_gate(&headers, &a.config) {
        return r;
    }
    if headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) != Some(a.config.origin.as_str()) {
        return forbidden();
    }
    let Some(secret) = cookie_secret(&headers) else {
        return unauthenticated();
    };
    match tokio::time::timeout(
        IO_TIMEOUT,
        store::session(
            &a.db,
            &secret,
            false,
            a.config.idle_ms,
            a.config.absolute_ms,
        ),
    )
    .await
    {
        Ok(Ok(Some(_))) => {}
        Ok(Ok(None)) => return unauthenticated(),
        _ => return internal(),
    }
    let lease = match a.ws_hub.acquire(&secret) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let mut response = ws
        .max_message_size(INPUT_LIMIT)
        .max_frame_size(INPUT_LIMIT)
        .on_upgrade(move |socket| serve(socket, state, secret, lease))
        .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Subscribe {
    topic: String,
    agent_ids: Vec<String>,
    metrics: Option<Vec<MetricName>>,
    since_cursor: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Unsubscribe {
    subscription_id: String,
    stream_epoch: String,
}
struct Subscription {
    epoch: String,
    sequence: u64,
    params: LatestParams,
    snapshot: Vec<MetricsLatest>,
}
fn fault(id: Value, code: i32, kind: &str, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message,"data":{"kind":kind}}})
}
fn success(id: Value, value: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":value})
}
fn notice(id: &str, s: &Subscription, kind: &str, data: Value) -> Value {
    json!({"jsonrpc":"2.0","method":"stream.notification","params":{"subscriptionId":id,"streamEpoch":s.epoch,"sequence":s.sequence.to_string(),"kind":kind,"data":data}})
}
fn metric_fault(id: Value, e: MetricsError) -> Value {
    let (code, kind, message) = metric_problem(e);
    fault(id, code, kind, message)
}
fn enqueue(
    tx: &mpsc::Sender<Message>,
    value: Value,
    cancel: &CancellationToken,
    code: &AtomicU16,
) -> bool {
    let Ok(text) = serde_json::to_string(&value) else {
        code.store(1011, Ordering::Relaxed);
        cancel.cancel();
        return false;
    };
    if text.len() > OUTPUT_LIMIT || tx.try_send(Message::Text(text.into())).is_err() {
        code.store(1013, Ordering::Relaxed);
        cancel.cancel();
        return false;
    }
    true
}
async fn authorized(state: &AppState, secret: &[u8], ids: &[String]) -> bool {
    let a = &state.web_auth;
    matches!(
        store::session(&a.db, secret, false, a.config.idle_ms, a.config.absolute_ms).await,
        Ok(Some(_))
    ) && (ids.is_empty()
        || web_metrics::authorize_agents(&a.db, &a.metrics, ids)
            .await
            .is_ok())
}
fn subscription_agents(subs: &HashMap<String, Subscription>) -> Vec<String> {
    subs.values()
        .flat_map(|s| s.params.agent_ids.iter().cloned())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

async fn handle(
    text: &str,
    state: &AppState,
    secret: &[u8],
    subs: &mut HashMap<String, Subscription>,
    recent: &mut VecDeque<String>,
    connection_epoch: &str,
) -> Vec<Value> {
    let req: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => {
            return vec![fault(
                Value::Null,
                -32700,
                "VALIDATION_FAILED",
                "Parse error",
            )];
        }
    };
    let Some(obj) = req.as_object() else {
        return vec![fault(
            Value::Null,
            -32600,
            "VALIDATION_FAILED",
            "Invalid request",
        )];
    };
    let id = obj.get("id").cloned().unwrap_or(Value::Null);
    let valid_id = id.as_str().is_some_and(|v| !v.is_empty() && v.len() <= 128)
        || id
            .as_i64()
            .is_some_and(|v| v.unsigned_abs() <= 9_007_199_254_740_991);
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !valid_id
        || obj.get("method").and_then(Value::as_str).is_none()
        || obj
            .keys()
            .any(|k| !["id", "jsonrpc", "method", "params"].contains(&k.as_str()))
    {
        return vec![fault(
            Value::Null,
            -32600,
            "VALIDATION_FAILED",
            "Invalid request",
        )];
    }
    let request_key = id.to_string();
    if recent.contains(&request_key) {
        return vec![fault(
            id,
            -32600,
            "VALIDATION_FAILED",
            "Repeated request id",
        )];
    }
    recent.push_back(request_key);
    if recent.len() > 128 {
        recent.pop_front();
    }
    if !authorized(state, secret, &[]).await {
        return vec![fault(
            id,
            -32001,
            "UNAUTHENTICATED",
            "Authentication required",
        )];
    }
    let method = obj["method"].as_str().unwrap_or_default();
    let params = obj.get("params").cloned().unwrap_or(Value::Null);
    match method {
        "stream.ping" => {
            if serde_json::from_value::<Empty>(params).is_err() {
                return vec![fault(id, -32602, "VALIDATION_FAILED", "Invalid params")];
            }
            vec![success(
                id,
                json!({"serverTimeMs":now_ms(),"streamEpoch":connection_epoch}),
            )]
        }
        "stream.unsubscribe" => {
            let p: Unsubscribe = match serde_json::from_value(params) {
                Ok(v) => v,
                Err(_) => return vec![fault(id, -32602, "VALIDATION_FAILED", "Invalid params")],
            };
            let matches = subs
                .get(&p.subscription_id)
                .is_some_and(|s| s.epoch == p.stream_epoch);
            if matches {
                subs.remove(&p.subscription_id);
            }
            vec![success(
                id,
                json!({"subscriptionId":p.subscription_id,"streamEpoch":p.stream_epoch,"unsubscribed":matches}),
            )]
        }
        "stream.subscribe" => {
            let p: Subscribe = match serde_json::from_value(params) {
                Ok(v) => v,
                Err(_) => return vec![fault(id, -32602, "VALIDATION_FAILED", "Invalid params")],
            };
            if p.topic != "metrics" {
                return vec![fault(
                    id,
                    -32017,
                    "UNSUPPORTED_FEATURE",
                    "Only metrics subscriptions are available",
                )];
            }
            if p.since_cursor
                .as_ref()
                .is_some_and(|c| c.is_empty() || c.len() > 256)
            {
                return vec![fault(id, -32602, "VALIDATION_FAILED", "Invalid cursor")];
            }
            let params = LatestParams {
                agent_ids: p.agent_ids,
                metrics: p
                    .metrics
                    .unwrap_or_else(|| vec![MetricName::Cpu, MetricName::Memory]),
            };
            if let Err(e) = params.validate() {
                return vec![metric_fault(id, e)];
            }
            let mut union: HashSet<_> = subscription_agents(subs).into_iter().collect();
            union.extend(params.agent_ids.iter().cloned());
            if subs.len() >= SUBSCRIPTION_LIMIT || union.len() > AGENT_LIMIT {
                return vec![fault(
                    id,
                    -32013,
                    "RATE_LIMITED",
                    "Subscription limit exceeded",
                )];
            }
            let snapshot = match web_metrics::latest(
                &state.database,
                &state.web_auth.metrics,
                &params,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => return vec![metric_fault(id, e)],
            };
            let key = uuid::Uuid::new_v4().to_string();
            let mut sub = Subscription {
                epoch: uuid::Uuid::new_v4().to_string(),
                sequence: 0,
                params,
                snapshot,
            };
            let mut output = vec![success(
                id,
                json!({"subscriptionId":key,"streamEpoch":sub.epoch,"sequence":"0","snapshot":sub.snapshot}),
            )];
            if p.since_cursor.is_some() {
                sub.sequence = 1;
                output.push(notice(
                    &key,
                    &sub,
                    "resyncRequired",
                    json!({"reason":"epochChanged","snapshotRequired":true,"latestSequence":"1"}),
                ));
            }
            subs.insert(key, sub);
            output
        }
        _ => vec![fault(
            id,
            -32601,
            "NOT_FOUND",
            "Only stream control methods are accepted",
        )],
    }
}
async fn serve(socket: WebSocket, state: AppState, secret: Vec<u8>, lease: Lease) {
    let cancel = lease.cancel.clone();
    let close_code = Arc::new(AtomicU16::new(1008));
    let (mut sink, mut source) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Message>(QUEUE_LIMIT);
    let writer_cancel = cancel.clone();
    let writer_code = close_code.clone();
    let writer = tokio::spawn(async move {
        loop {
            let message = tokio::select! {biased;_=writer_cancel.cancelled()=>break,m=rx.recv()=>match m{Some(v)=>v,None=>break}};
            let sent = tokio::select! {biased;_=writer_cancel.cancelled()=>break,r=tokio::time::timeout(IO_TIMEOUT,sink.send(message))=>r};
            if !matches!(sent, Ok(Ok(()))) {
                writer_code.store(1013, Ordering::Relaxed);
                writer_cancel.cancel();
                break;
            }
        }
        let frame = Message::Close(Some(CloseFrame {
            code: writer_code.load(Ordering::Relaxed),
            reason: "stream closed; restore snapshot on reconnect".into(),
        }));
        let _ = tokio::time::timeout(Duration::from_secs(1), sink.send(frame)).await;
    });
    let (agents_tx, agents_rx) = watch::channel(Vec::<String>::new());
    let guard_state = state.clone();
    let guard_secret = secret.clone();
    let guard_cancel = cancel.clone();
    let guard = tokio::spawn(async move {
        let mut interval = tokio::time::interval(AUTH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {biased;_=guard_cancel.cancelled()=>break,_=guard_state.shutdown.cancelled()=>{guard_cancel.cancel();break},_=interval.tick()=>{
                let ids=agents_rx.borrow().clone();
                let valid=tokio::select!{biased;_=guard_cancel.cancelled()=>break,r=tokio::time::timeout(IO_TIMEOUT,authorized(&guard_state,&guard_secret,&ids))=>r};
                if !matches!(valid,Ok(true)){guard_cancel.cancel();break}
            }}
        }
    });
    let mut subs = HashMap::<String, Subscription>::new();
    let mut recent = VecDeque::new();
    let connection_epoch = uuid::Uuid::new_v4().to_string();
    let mut pushes = tokio::time::interval(PUSH_INTERVAL);
    pushes.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut controls = VecDeque::<Instant>::new();
    'connection: loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            _=state.shutdown.cancelled()=>{close_code.store(1001,Ordering::Relaxed);break},
            msg=source.next()=>{
                let Some(Ok(msg))=msg else{break};
                match msg {
                    Message::Text(text)=>{
                        let now=Instant::now();controls.retain(|t|now.duration_since(*t)<Duration::from_secs(60));
                        if controls.len()>=120{close_code.store(1008,Ordering::Relaxed);break}controls.push_back(now);
                        let result=tokio::select!{biased;_=cancel.cancelled()=>break,r=tokio::time::timeout(IO_TIMEOUT,handle(text.as_str(),&state,&secret,&mut subs,&mut recent,&connection_epoch))=>r};
                        let Ok(replies)=result else{close_code.store(1013,Ordering::Relaxed);break};
                        let _=agents_tx.send(subscription_agents(&subs));
                        for reply in replies{if !enqueue(&tx,reply,&cancel,&close_code){break 'connection}}
                    },
                    Message::Close(_)=>{close_code.store(1000,Ordering::Relaxed);break},
                    Message::Binary(_)=>{close_code.store(1003,Ordering::Relaxed);break},
                    Message::Ping(data)=>{if tx.try_send(Message::Pong(data)).is_err(){close_code.store(1013,Ordering::Relaxed);break}},
                    Message::Pong(_)=>{}
                }
            },
            _=pushes.tick()=>{
                for (key,sub) in &mut subs {
                    let update=async {if !authorized(&state,&secret,&sub.params.agent_ids).await{return Err(MetricsError::Forbidden)}web_metrics::latest(&state.database,&state.web_auth.metrics,&sub.params).await};
                    let result=tokio::select!{biased;_=cancel.cancelled()=>break 'connection,r=tokio::time::timeout(IO_TIMEOUT,update)=>r};
                    let next=match result{Ok(Ok(v))=>v,_=>{close_code.store(1008,Ordering::Relaxed);break 'connection}};
                    if next!=sub.snapshot {
                        let Some(sequence)=sub.sequence.checked_add(1)else{close_code.store(1013,Ordering::Relaxed);break 'connection};sub.sequence=sequence;sub.snapshot=next;
                        if !enqueue(&tx,notice(key,sub,"metrics.update",json!({"items":sub.snapshot})),&cancel,&close_code){break 'connection}
                    }
                }
            }
        }
    }
    cancel.cancel();
    drop(tx);
    guard.abort();
    let _ = guard.await;
    let mut writer = writer;
    if tokio::time::timeout(IO_TIMEOUT, &mut writer).await.is_err() {
        writer.abort();
        let _ = writer.await;
    }
    drop(lease);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queue_overflow_cancels_instead_of_growing() {
        let (tx, _rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let code = AtomicU16::new(1008);
        assert!(enqueue(&tx, json!({"id":1}), &cancel, &code));
        assert!(!enqueue(&tx, json!({"id":2}), &cancel, &code));
        assert!(cancel.is_cancelled());
        assert_eq!(code.load(Ordering::Relaxed), 1013);
    }
    #[test]
    fn hub_limits_and_releases_connections() {
        let hub = WsHub::default();
        let mut leases = Vec::new();
        for _ in 0..CONNECTION_LIMIT {
            leases.push(hub.acquire(&[1; 32]).unwrap());
        }
        assert!(hub.acquire(&[1; 32]).is_err());
        hub.revoke(&[1; 32]);
        assert!(leases.iter().all(|l| l.cancel.is_cancelled()));
        leases.clear();
        assert!(hub.acquire(&[2; 32]).is_ok());
    }
}
