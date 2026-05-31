//! 远程网络探测执行器。
//!
//! 该模块只处理协议无关的探测请求、频率保护和结果投递；Komari 或自有协议的
//! 字段映射放在各自 adapter 中。

use crate::collect::unix_timestamp_secs;
use crate::config::ConfigManager;
use crate::service::message::outbound::{
    OutboundEvent, OutboundSender, OutboundSequence, RemoteProbeResultEnvelope,
};
use serde::Deserialize;
use serde_json::Value;
use smalux_core::utils::validate::ensure_non_empty;
use smalux_protocol::{RemoteProbeResult, RemoteProbeType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// 远程探测目标缓存上限，避免恶意 server 用大量唯一目标撑大内存。
const RATE_STATE_MAX_TARGETS: usize = 1024;

/// server 下发的远程探测请求。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub(crate) struct RemoteProbeRunRequest {
    /// server 侧生成的探测任务 ID；兼容协议可能使用数字或字符串。
    pub(crate) task_id: Value,
    /// 探测类型。
    pub(crate) probe_type: RemoteProbeType,
    /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
    pub(crate) target: String,
}

impl RemoteProbeRunRequest {
    /// 校验探测请求最小字段。
    fn validate(&self) -> anyhow::Result<()> {
        if self.task_id.is_null()
            || self
                .task_id
                .as_str()
                .map(str::trim)
                .is_some_and(str::is_empty)
        {
            anyhow::bail!("remote_probe.task_id cannot be empty");
        }
        ensure_non_empty("remote_probe.target", &self.target)?;
        Ok(())
    }

    /// 用于同目标限频的稳定 key。
    fn target_key(&self) -> String {
        format!(
            "{}:{}",
            self.probe_type.as_str(),
            self.target.trim().to_ascii_lowercase()
        )
    }
}

impl From<smalux_protocol::RemoteProbeRequest> for RemoteProbeRunRequest {
    /// 从自有协议探测请求转换为 service 内部命令。
    fn from(request: smalux_protocol::RemoteProbeRequest) -> Self {
        Self {
            task_id: request.task_id,
            probe_type: request.probe_type,
            target: request.target,
        }
    }
}

/// 远程探测管理器。
#[derive(Debug, Clone)]
pub(crate) struct RemoteProbeManager {
    /// 动态配置管理器。
    config_manager: ConfigManager,
    /// 出站事件发送端。
    outbound_tx: OutboundSender,
    /// 全局出站序号。
    sequence: OutboundSequence,
    /// 本地频率保护状态。
    rate_state: Arc<Mutex<ProbeRateState>>,
    /// HTTP 探测复用连接池。
    http_client: reqwest::Client,
}

impl RemoteProbeManager {
    /// 创建远程探测管理器。
    pub(crate) fn new(
        config_manager: ConfigManager,
        outbound_tx: OutboundSender,
        sequence: OutboundSequence,
    ) -> Self {
        Self {
            config_manager,
            outbound_tx,
            sequence,
            rate_state: Arc::new(Mutex::new(ProbeRateState::default())),
            http_client: reqwest::Client::new(),
        }
    }

    /// 启动一次远程探测；禁用或限频时立即回传 `value=-1`，不排队等待。
    pub(crate) fn start(&self, request: RemoteProbeRunRequest) -> anyhow::Result<()> {
        request.validate()?;
        let config = self.config_manager.current();
        let agent_id = config.agent_id.clone();
        let probe_config = config.remote_probe;

        // remote probe 默认关闭；关闭或限频时仍回传结果，方便 server 看到“请求被拒绝”
        // 而不是一直等待超时。这里不排队，是因为网络探测结果有明显时效性。
        if !probe_config.enabled {
            self.queue_immediate_result(agent_id, request, "remote probe is disabled")?;
            return Ok(());
        }

        if let Some(reason) = self.try_mark_started(&request, &probe_config) {
            self.queue_immediate_result(agent_id, request, &reason)?;
            return Ok(());
        }

        let manager = self.clone();
        let timeout = probe_config.timeout;
        tracing::info!(
            task_id = %display_task_id(&request.task_id),
            probe_type = request.probe_type.as_str(),
            target = %request.target,
            timeout_ms = timeout.as_millis(),
            "remote probe accepted"
        );

        tokio::spawn(async move {
            let task_id = request.task_id.clone();
            let result = execute_remote_probe(request, timeout, manager.http_client.clone()).await;
            let probe_type = result.probe_type.as_str();
            let target = result.target.clone();
            let value = result.value;
            let duration_ms = result.duration_ms;
            let error = result.error.clone();
            manager.send_result(agent_id, result).await;
            tracing::info!(
                task_id = %display_task_id(&task_id),
                probe_type,
                target = %target,
                value,
                duration_ms,
                error = error.as_deref().unwrap_or(""),
                "remote probe finished"
            );
        });

        Ok(())
    }

    /// 检查并记录本次探测启动时间；返回 Some 表示被限频拒绝。
    fn try_mark_started(
        &self,
        request: &RemoteProbeRunRequest,
        config: &crate::config::model::RemoteProbeConfig,
    ) -> Option<String> {
        let now = Instant::now();
        let target_key = request.target_key();
        let mut state = self
            .rate_state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // 先清理过期目标，再判断上限和同目标限频，避免长期运行时 target map 只增不减。
        state.prune(now, config.target_min_interval);

        if state
            .last_global_started_at
            .is_some_and(|started| now.duration_since(started) < config.global_min_interval)
        {
            return Some("remote probe global rate limit reached".to_string());
        }
        if state
            .last_target_started_at
            .get(&target_key)
            .is_some_and(|started| now.duration_since(*started) < config.target_min_interval)
        {
            return Some("remote probe target rate limit reached".to_string());
        }

        state.last_global_started_at = Some(now);
        state.insert_target(target_key, now);
        None
    }

    /// 立即投递失败结果。
    fn queue_immediate_result(
        &self,
        agent_id: String,
        request: RemoteProbeRunRequest,
        reason: &str,
    ) -> anyhow::Result<()> {
        let now = unix_timestamp_secs();
        let task_id_for_log = request.task_id.clone();
        let probe_type_for_log = request.probe_type;
        let target_for_log = request.target.clone();
        let result = RemoteProbeResult {
            task_id: request.task_id,
            probe_type: request.probe_type,
            target: request.target,
            value: -1,
            started_at: now,
            finished_at: now,
            duration_ms: 0,
            error: Some(reason.to_string()),
        };
        let event = self.result_event(agent_id, result);
        self.outbound_tx
            .try_send(event)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("outbound event queue is full")
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("outbound event queue is closed")
                }
            })?;
        tracing::warn!(
            task_id = %display_task_id(&task_id_for_log),
            probe_type = probe_type_for_log.as_str(),
            target = %target_for_log,
            reason,
            "remote probe rejected"
        );
        Ok(())
    }

    /// 异步投递探测结果。
    async fn send_result(&self, agent_id: String, result: RemoteProbeResult) {
        let task_id = result.task_id.clone();
        let event = self.result_event(agent_id, result);
        if self.outbound_tx.send(event).await.is_err() {
            tracing::warn!(
                task_id = %display_task_id(&task_id),
                "outbound event queue closed; remote probe result dropped"
            );
        }
    }

    /// 把探测结果包装成出站事件。
    fn result_event(&self, agent_id: String, result: RemoteProbeResult) -> OutboundEvent {
        let sequence = self.sequence.next();
        OutboundEvent::RemoteProbeResult(RemoteProbeResultEnvelope::new(agent_id, sequence, result))
    }
}

/// 频率保护状态。
#[derive(Debug, Default)]
struct ProbeRateState {
    /// 最近一次探测启动时间。
    last_global_started_at: Option<Instant>,
    /// 每个目标最近一次探测启动时间。
    last_target_started_at: HashMap<String, Instant>,
}

impl ProbeRateState {
    /// 插入目标探测时间，超过上限时移除最旧目标。
    fn insert_target(&mut self, key: String, started_at: Instant) {
        if !self.last_target_started_at.contains_key(&key)
            && self.last_target_started_at.len() >= RATE_STATE_MAX_TARGETS
            && let Some(oldest_key) = self
                .last_target_started_at
                .iter()
                .min_by_key(|(_key, started_at)| *started_at)
                .map(|(key, _started_at)| key.clone())
        {
            self.last_target_started_at.remove(&oldest_key);
        }
        self.last_target_started_at.insert(key, started_at);
    }

    /// 清理已超过目标限频窗口的旧记录。
    fn prune(&mut self, now: Instant, target_min_interval: Duration) {
        self.last_target_started_at
            .retain(|_key, started_at| now.duration_since(*started_at) < target_min_interval);
    }
}

/// 执行远程探测并构建结果。
async fn execute_remote_probe(
    request: RemoteProbeRunRequest,
    timeout: Duration,
    http_client: reqwest::Client,
) -> RemoteProbeResult {
    let started_at = unix_timestamp_secs();
    let started = Instant::now();
    let result = match request.probe_type {
        RemoteProbeType::Tcp => probe_tcp(&request.target, timeout).await,
        RemoteProbeType::Http => probe_http(&http_client, &request.target, timeout).await,
        RemoteProbeType::Icmp => Err(anyhow::anyhow!("icmp probe is not implemented")),
    };
    let duration_ms = started.elapsed().as_millis().min(i64::MAX as u128) as i64;
    let (value, error) = match result {
        Ok(()) => (duration_ms, None),
        Err(error) => (-1, Some(error.to_string())),
    };

    RemoteProbeResult {
        task_id: request.task_id,
        probe_type: request.probe_type,
        target: request.target,
        value,
        started_at,
        finished_at: unix_timestamp_secs(),
        duration_ms: duration_ms.max(0) as u64,
        error,
    }
}

/// 执行 TCP 连接探测。
async fn probe_tcp(target: &str, timeout: Duration) -> anyhow::Result<()> {
    let address = tcp_target_address(target)?;
    tokio::time::timeout(timeout, TcpStream::connect(&address))
        .await
        .map_err(|_| anyhow::anyhow!("tcp probe timed out"))??;
    Ok(())
}

/// 执行 HTTP/HTTPS 探测，按 Komari 约定发送 GET，但不读取响应 body。
async fn probe_http(
    client: &reqwest::Client,
    target: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let url = http_target_url(target)?;
    tokio::time::timeout(timeout, client.get(url).timeout(timeout).send())
        .await
        .map_err(|_| anyhow::anyhow!("http probe timed out"))??;
    Ok(())
}

/// 解析 TCP 目标，兼容 `host:port` 和 `tcp://host:port`。
fn tcp_target_address(target: &str) -> anyhow::Result<String> {
    let target = target.trim();
    if let Ok(url) = reqwest::Url::parse(target) {
        if url.scheme() != "tcp" {
            anyhow::bail!("tcp probe target must use tcp:// or host:port");
        }
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("tcp probe target host is missing"))?;
        let port = url
            .port()
            .ok_or_else(|| anyhow::anyhow!("tcp probe target port is missing"))?;
        return Ok(format!("{host}:{port}"));
    }
    Ok(target.to_string())
}

/// 解析 HTTP 目标，缺少 scheme 时默认按 HTTP 处理。
fn http_target_url(target: &str) -> anyhow::Result<reqwest::Url> {
    let target = target.trim();
    let normalized = if target.contains("://") {
        target.to_string()
    } else {
        format!("http://{target}")
    };
    let url = reqwest::Url::parse(&normalized)?;
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("http probe target must use http or https");
    }
    Ok(url)
}

/// 把兼容协议的 task id 转成日志友好的字符串。
pub(crate) fn display_task_id(task_id: &Value) -> String {
    task_id
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| task_id.to_string())
}

#[cfg(test)]
mod tests {
    //! 远程网络探测测试。

    use super::*;
    use crate::config::{AgentConfig, ConfigManager};
    use crate::service::message::outbound::{OutboundEvent, OutboundSequence, outbound_channel};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 构造测试探测管理器。
    fn probe_manager(
        mut config: AgentConfig,
    ) -> (
        RemoteProbeManager,
        tokio::sync::mpsc::Receiver<OutboundEvent>,
    ) {
        config.agent_id = "agent-probe".to_string();
        let manager = ConfigManager::new(config).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let probe_manager =
            RemoteProbeManager::new(manager, outbound_tx, OutboundSequence::default());

        (probe_manager, outbound_rx)
    }

    /// 构造 TCP 探测请求。
    fn tcp_request(task_id: impl Into<Value>, target: String) -> RemoteProbeRunRequest {
        RemoteProbeRunRequest {
            task_id: task_id.into(),
            probe_type: RemoteProbeType::Tcp,
            target,
        }
    }

    /// 验证默认关闭时不会发起网络探测，而是直接回传 -1。
    #[tokio::test]
    async fn disabled_remote_probe_returns_negative_result() {
        let (manager, mut outbound_rx) = probe_manager(AgentConfig::default());

        manager
            .start(tcp_request("probe-disabled", "127.0.0.1:1".to_string()))
            .unwrap();

        let event = outbound_rx.recv().await.unwrap();
        let OutboundEvent::RemoteProbeResult(result) = event else {
            panic!("expected remote probe result");
        };

        assert_eq!(result.sequence, 1);
        assert_eq!(result.result.value, -1);
        assert!(result.result.error.as_deref().unwrap().contains("disabled"));
    }

    /// 验证全局限频会直接返回 -1，不排队延迟执行。
    #[tokio::test]
    async fn remote_probe_global_rate_limit_returns_negative_result() {
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);
        {
            let mut state = manager.rate_state.lock().unwrap();
            state.last_global_started_at = Some(Instant::now());
        }

        manager
            .start(tcp_request("probe-rate", "127.0.0.1:1".to_string()))
            .unwrap();

        let event = outbound_rx.recv().await.unwrap();
        let OutboundEvent::RemoteProbeResult(result) = event else {
            panic!("expected remote probe result");
        };

        assert_eq!(result.result.value, -1);
        assert!(
            result
                .result
                .error
                .as_deref()
                .unwrap()
                .contains("global rate limit")
        );
    }

    /// 验证 TCP 探测成功时会回传非负耗时。
    #[tokio::test]
    async fn remote_probe_tcp_success_returns_latency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
        });
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);

        manager.start(tcp_request("probe-ok", target)).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let OutboundEvent::RemoteProbeResult(result) = event else {
            panic!("expected remote probe result");
        };
        let _ = server.await;

        assert_eq!(result.result.task_id, Value::String("probe-ok".to_string()));
        assert!(result.result.value >= 0);
        assert_eq!(result.result.error, None);
    }

    /// 验证 HTTP 探测使用 GET 请求并回传非负耗时。
    #[tokio::test]
    async fn remote_probe_http_success_returns_latency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 512];
            let read = stream.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.starts_with("GET "));
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);

        manager
            .start(RemoteProbeRunRequest {
                task_id: Value::from(7),
                probe_type: RemoteProbeType::Http,
                target,
            })
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let OutboundEvent::RemoteProbeResult(result) = event else {
            panic!("expected remote probe result");
        };
        let _ = server.await;

        assert_eq!(result.result.task_id, Value::from(7));
        assert!(result.result.value >= 0);
        assert_eq!(result.result.error, None);
    }
}
