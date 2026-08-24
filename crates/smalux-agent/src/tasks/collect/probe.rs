//! 多节点 ICMP Echo、TCP Connect、UDP Request 与 HTTP 调度任务。

mod http;
mod icmp_echo;
mod model;
mod tcp_connect;
mod udp_request;

use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

use anyhow::anyhow;
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
pub use smalux_protocol::agent::v1::ProbeTaskConfig;
use smalux_protocol::agent::v1::{
    ProbeAttemptSnapshot, ProbeNodeSnapshot, ProbeProtocol, ProbeSnapshot, TaskResult, task_result,
};
use tokio::sync::Mutex;

use crate::scheduler::{ReportingTask, TaskContext, TaskError};

use super::sample::{MetricSample, duration_ms, unix_timestamp_ms};
use http::HttpClients;
use icmp_echo::IcmpClients;

pub use model::ProbeConfigError;
#[cfg(test)]
use model::{HttpStatusRange, ProbeNodeConfig};
use model::{
    ProbeAttemptSnapshot as CollectedAttempt, ProbeNodeSnapshot as CollectedNode,
    ProbeProtocol as CollectedProtocol, ProbeSnapshot as CollectedSnapshot, ProbeTarget,
    ProbeTaskConfig as CompiledProbeTaskConfig, compile_config,
};

/// 一个 Probe Task 跨多轮执行复用的网络资源。
///
/// ICMP socket 在 Task 构造时创建；HTTP Client 延迟到首次 HTTP 探测时创建，并在
/// 后续执行中继续复用连接池、DNS 解析器和 TLS 配置。未配置对应协议时不会分配资源。
struct ProbeRuntime {
    icmp_clients: Option<Arc<IcmpClients>>,
    http_clients: Option<OnceLock<Result<Arc<HttpClients>, String>>>,
}

impl ProbeRuntime {
    fn new(config: &CompiledProbeTaskConfig) -> Self {
        let uses_icmp = config
            .nodes
            .iter()
            .any(|node| matches!(node.target, ProbeTarget::IcmpEcho));
        let uses_http = config
            .nodes
            .iter()
            .any(|node| matches!(node.target, ProbeTarget::Http { .. }));
        Self {
            icmp_clients: uses_icmp.then(|| Arc::new(IcmpClients::new())),
            http_clients: uses_http.then(OnceLock::new),
        }
    }

    fn http_clients(&self) -> Result<Option<Arc<HttpClients>>, String> {
        let Some(clients) = &self.http_clients else {
            return Ok(None);
        };
        clients
            .get_or_init(|| {
                HttpClients::new()
                    .map(Arc::new)
                    .map_err(|error| error.to_string())
            })
            .clone()
            .map(Some)
    }
}

/// 并发执行多个网络节点探测的调度任务。
pub struct ProbeTask {
    config: ProbeTaskConfig,
    compiled: CompiledProbeTaskConfig,
    runtime: ProbeRuntime,
    last_sampled_at: Mutex<Option<Instant>>,
}

impl ProbeTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.probe.network.v1";

    /// 校验配置并创建 Probe Task。
    pub fn try_with_config(config: ProbeTaskConfig) -> Result<Self, ProbeConfigError> {
        let compiled = compile_config(&config)?;
        let runtime = ProbeRuntime::new(&compiled);
        Ok(Self {
            config,
            compiled,
            runtime,
            last_sampled_at: Mutex::new(None),
        })
    }

    /// 返回当前生效的多节点探测配置。
    pub fn config(&self) -> &ProbeTaskConfig {
        &self.config
    }
}

#[async_trait]
impl ReportingTask for ProbeTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        tracing::debug!(
            job_id = %context.job_id,
            run_id = %context.run_id,
            nodes = self.compiled.nodes.len(),
            concurrency = self.compiled.concurrency.get(),
            "network probe task started"
        );
        let cancellation = context.cancellation;
        let sampled_at = Instant::now();
        let sampled_at_ms = unix_timestamp_ms();
        let sample_interval_ms = tokio::select! {
            _ = cancellation.cancelled() => {
                tracing::debug!(
                    job_id = %context.job_id,
                    run_id = %context.run_id,
                    "network probe cancelled before sampling"
                );
                return Err(TaskError::Transient(anyhow!("network probe cancelled before start")));
            }
            mut previous = self.last_sampled_at.lock() => {
                previous.replace(sampled_at)
                    .map(|previous| duration_ms(sampled_at.duration_since(previous)))
            }
        };
        let icmp_clients = self.runtime.icmp_clients.clone();
        let http_clients = self.runtime.http_clients().map_err(|error| {
            tracing::warn!(
                job_id = %context.job_id,
                run_id = %context.run_id,
                error = %error,
                "network probe HTTP client initialization failed"
            );
            TaskError::Permanent(anyhow!("failed to build HTTP probe client: {error}"))
        })?;
        let identifier_seed =
            u16::from_le_bytes([context.run_id.as_bytes()[0], context.run_id.as_bytes()[1]]);
        let work = stream::iter(self.compiled.nodes.iter().cloned().enumerate().map(
            |(index, node)| {
                let cancellation = cancellation.clone();
                let clients = icmp_clients.clone();
                let http_clients = http_clients.clone();
                async move {
                    let result = match node.target {
                        ProbeTarget::IcmpEcho => {
                            icmp_echo::probe(
                                node,
                                identifier_seed.wrapping_add(index as u16),
                                clients.as_deref().expect("ICMP clients are initialized"),
                                cancellation,
                            )
                            .await
                        }
                        ProbeTarget::TcpConnect { .. } => {
                            tcp_connect::probe(node, cancellation).await
                        }
                        ProbeTarget::UdpRequest { .. } => {
                            udp_request::probe(node, cancellation).await
                        }
                        ProbeTarget::Http { .. } => {
                            http::probe(
                                node,
                                http_clients
                                    .as_deref()
                                    .expect("HTTP clients are initialized"),
                                cancellation,
                            )
                            .await
                        }
                    };
                    (index, result)
                }
            },
        ))
        .buffer_unordered(self.compiled.concurrency.get())
        .collect::<Vec<_>>();
        let mut results = tokio::select! {
            _ = cancellation.cancelled() => {
                tracing::debug!(
                    job_id = %context.job_id,
                    run_id = %context.run_id,
                    "network probe cancelled while probing nodes"
                );
                return Err(TaskError::Transient(anyhow!("network probe cancelled")));
            }
            results = work => results,
        };
        if results.iter().any(|(_, result)| result.is_err()) {
            let failed_nodes = results.iter().filter(|(_, result)| result.is_err()).count();
            tracing::warn!(
                job_id = %context.job_id,
                run_id = %context.run_id,
                failed_nodes,
                "network probe node execution was cancelled"
            );
            return Err(TaskError::Transient(anyhow!("network probe cancelled")));
        }
        results.sort_by_key(|(index, _)| *index);
        let nodes = results
            .into_iter()
            .map(|(_, result)| result.expect("cancelled results returned above"))
            .collect::<Vec<_>>();
        let unhealthy_nodes = nodes.iter().filter(|node| node.succeeded == 0).count();
        for node in nodes.iter().filter(|node| node.succeeded < node.attempted) {
            tracing::debug!(
                job_id = %context.job_id,
                run_id = %context.run_id,
                node = %node.name,
                protocol = ?node.protocol,
                attempted = node.attempted,
                succeeded = node.succeeded,
                failure_percent = node.failure_percent,
                "network probe node recorded failed attempts"
            );
        }
        if unhealthy_nodes > 0 {
            tracing::warn!(
                job_id = %context.job_id,
                run_id = %context.run_id,
                unhealthy_nodes,
                total_nodes = nodes.len(),
                "network probe completed with unhealthy nodes"
            );
        }
        let snapshot = CollectedSnapshot {
            total_nodes: nodes.len(),
            healthy_nodes: nodes.iter().filter(|node| node.succeeded > 0).count(),
            nodes,
        };
        tracing::trace!(
            job_id = %context.job_id,
            run_id = %context.run_id,
            total_nodes = snapshot.total_nodes,
            healthy_nodes = snapshot.healthy_nodes,
            "network probe task completed"
        );
        Ok(
            MetricSample::new(sampled_at_ms, sample_interval_ms, snapshot).into_task_result(
                |snapshot| task_result::Result::Probe(into_proto_snapshot(snapshot)),
            ),
        )
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }
}

fn into_proto_snapshot(snapshot: CollectedSnapshot) -> ProbeSnapshot {
    ProbeSnapshot {
        total_nodes: snapshot.total_nodes.try_into().unwrap_or(u32::MAX),
        healthy_nodes: snapshot.healthy_nodes.try_into().unwrap_or(u32::MAX),
        nodes: snapshot.nodes.into_iter().map(into_proto_node).collect(),
    }
}

fn into_proto_node(node: CollectedNode) -> ProbeNodeSnapshot {
    ProbeNodeSnapshot {
        name: node.name,
        host: node.host,
        protocol: match node.protocol {
            CollectedProtocol::IcmpEcho => ProbeProtocol::IcmpEcho as i32,
            CollectedProtocol::TcpConnect => ProbeProtocol::TcpConnect as i32,
            CollectedProtocol::UdpRequest => ProbeProtocol::UdpRequest as i32,
            CollectedProtocol::Http => ProbeProtocol::Http as i32,
        },
        port: node.port.map(u32::from),
        url: node.url,
        resolved_ip: node.resolved_ip,
        attempted: node.attempted.into(),
        succeeded: node.succeeded.into(),
        failure_percent: node.failure_percent,
        min_latency_ms: node.min_latency_ms,
        avg_latency_ms: node.avg_latency_ms,
        max_latency_ms: node.max_latency_ms,
        attempts: node.attempts.into_iter().map(into_proto_attempt).collect(),
    }
}

fn into_proto_attempt(attempt: CollectedAttempt) -> ProbeAttemptSnapshot {
    ProbeAttemptSnapshot {
        sequence: attempt.sequence.into(),
        success: attempt.success,
        latency_ms: attempt.latency_ms,
        status_code: attempt.status_code.map(u32::from),
        error: attempt.error,
    }
}

#[cfg(test)]
impl ProbeTask {
    fn from_compiled_for_test(compiled: CompiledProbeTaskConfig) -> Result<Self, ProbeConfigError> {
        compiled.validate()?;
        let runtime = ProbeRuntime::new(&compiled);
        Ok(Self {
            config: ProbeTaskConfig::default(),
            compiled,
            runtime,
            last_sampled_at: Mutex::new(None),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU16, NonZeroUsize},
        time::Duration,
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, UdpSocket},
        task::JoinHandle,
    };

    use crate::{
        scheduler::{ReportingTask, TaskError},
        tasks::collect::context,
    };

    use super::*;

    fn tcp_node(name: &str, port: u16) -> ProbeNodeConfig {
        ProbeNodeConfig {
            name: name.to_owned(),
            host: "127.0.0.1".to_owned(),
            target: ProbeTarget::TcpConnect {
                port: NonZeroU16::new(port).unwrap(),
            },
            attempts: NonZeroU16::new(1).unwrap(),
            timeout: Duration::from_secs(1),
            interval: Duration::ZERO,
        }
    }

    async fn http_endpoint(status: u16) -> (reqwest::Url, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let response =
                format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        (
            reqwest::Url::parse(&format!("http://{address}/health")).unwrap(),
            server,
        )
    }

    async fn redirecting_http_endpoint() -> (reqwest::Url, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for response in [
                "HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await.unwrap();
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (
            reqwest::Url::parse(&format!("http://{address}/redirect")).unwrap(),
            server,
        )
    }

    fn http_node(name: &str, url: reqwest::Url) -> ProbeNodeConfig {
        ProbeNodeConfig {
            name: name.to_owned(),
            host: url.host_str().unwrap().to_owned(),
            target: ProbeTarget::Http {
                url,
                expected_status: HttpStatusRange { min: 200, max: 299 },
                follow_redirects: false,
            },
            attempts: NonZeroU16::new(1).unwrap(),
            timeout: Duration::from_secs(1),
            interval: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn task_reports_udp_request_response_snapshot() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut request = [0_u8; 64];
            let (received, peer) = socket.recv_from(&mut request).await.unwrap();
            assert_eq!(&request[..received], b"health");
            socket.send_to(b"ok", peer).await.unwrap();
        });
        let task = ProbeTask::from_compiled_for_test(model::ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![ProbeNodeConfig {
                name: "udp".to_owned(),
                host: address.ip().to_string(),
                target: ProbeTarget::UdpRequest {
                    port: NonZeroU16::new(address.port()).unwrap(),
                    request_payload: b"health".to_vec(),
                    expected_response_prefix: Some(b"ok".to_vec()),
                },
                attempts: NonZeroU16::new(1).unwrap(),
                timeout: Duration::from_secs(1),
                interval: Duration::ZERO,
            }],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        let Some(task_result::Result::Probe(snapshot)) = output.result else {
            panic!("probe task must return TaskResult.probe");
        };
        assert_eq!(snapshot.healthy_nodes, 1);
        assert_eq!(snapshot.nodes[0].protocol, ProbeProtocol::UdpRequest as i32);
        assert_eq!(snapshot.nodes[0].port, Some(u32::from(address.port())));
        assert_eq!(snapshot.nodes[0].succeeded, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn task_probes_multiple_tcp_nodes_and_isolates_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open_port = listener.local_addr().unwrap().port();
        let closed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_port = closed_listener.local_addr().unwrap().port();
        drop(closed_listener);
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let task = ProbeTask::from_compiled_for_test(model::ProbeTaskConfig {
            concurrency: NonZeroUsize::new(2).unwrap(),
            nodes: vec![tcp_node("open", open_port), tcp_node("closed", closed_port)],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), ProbeTask::KIND);
        let Some(task_result::Result::Probe(snapshot)) = output.result else {
            panic!("probe task must return TaskResult.probe");
        };
        assert_eq!(snapshot.total_nodes, 2);
        assert_eq!(snapshot.healthy_nodes, 1);
        assert_eq!(snapshot.nodes[0].name, "open");
        assert_eq!(snapshot.nodes[0].succeeded, 1);
        assert_eq!(snapshot.nodes[1].name, "closed");
        assert_eq!(snapshot.nodes[1].succeeded, 0);
        assert_eq!(snapshot.nodes[1].failure_percent, 100.0);
        accept.await.unwrap();
    }

    #[tokio::test]
    async fn task_records_http_status_and_applies_expected_range() {
        let (healthy_url, healthy_server) = http_endpoint(204).await;
        let (failing_url, failing_server) = http_endpoint(503).await;
        let task = ProbeTask::from_compiled_for_test(model::ProbeTaskConfig {
            concurrency: NonZeroUsize::new(2).unwrap(),
            nodes: vec![
                http_node("healthy", healthy_url.clone()),
                http_node("failing", failing_url.clone()),
            ],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        let Some(task_result::Result::Probe(snapshot)) = output.result else {
            panic!("probe task must return TaskResult.probe");
        };
        assert_eq!(snapshot.healthy_nodes, 1);
        assert_eq!(snapshot.nodes[0].protocol, ProbeProtocol::Http as i32);
        assert_eq!(snapshot.nodes[0].url.as_deref(), Some(healthy_url.as_str()));
        assert_eq!(snapshot.nodes[0].attempts[0].status_code, Some(204));
        assert!(snapshot.nodes[0].attempts[0].success);
        assert_eq!(snapshot.nodes[1].url.as_deref(), Some(failing_url.as_str()));
        assert_eq!(snapshot.nodes[1].attempts[0].status_code, Some(503));
        assert!(!snapshot.nodes[1].attempts[0].success);
        assert_eq!(snapshot.nodes[1].min_latency_ms, None);
        assert_eq!(snapshot.nodes[1].avg_latency_ms, None);
        assert_eq!(snapshot.nodes[1].max_latency_ms, None);
        healthy_server.await.unwrap();
        failing_server.await.unwrap();
    }

    #[tokio::test]
    async fn task_follows_http_redirects_when_enabled() {
        let (url, server) = redirecting_http_endpoint().await;
        let mut node = http_node("redirect", url.clone());
        node.target = ProbeTarget::Http {
            url,
            expected_status: HttpStatusRange { min: 200, max: 299 },
            follow_redirects: true,
        };
        let task = ProbeTask::from_compiled_for_test(model::ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![node],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        let Some(task_result::Result::Probe(snapshot)) = output.result else {
            panic!("probe task must return TaskResult.probe");
        };
        assert_eq!(snapshot.healthy_nodes, 1);
        assert_eq!(snapshot.nodes[0].attempts[0].status_code, Some(204));
        assert!(snapshot.nodes[0].attempts[0].success);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn task_honors_pre_cancelled_context() {
        let task = ProbeTask::from_compiled_for_test(model::ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![tcp_node("cancelled", 9)],
        })
        .unwrap();
        let context = context();
        context.cancellation.cancel();

        let error = task.run(context).await.unwrap_err();

        assert!(matches!(error, TaskError::Transient(_)));
    }

    #[test]
    fn task_reuses_initialized_http_clients() {
        let task = ProbeTask::from_compiled_for_test(model::ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![http_node(
                "http",
                reqwest::Url::parse("http://127.0.0.1/health").unwrap(),
            )],
        })
        .unwrap();

        let first = task.runtime.http_clients().unwrap().unwrap();
        let second = task.runtime.http_clients().unwrap().unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert!(task.runtime.icmp_clients.is_none());
    }
}
