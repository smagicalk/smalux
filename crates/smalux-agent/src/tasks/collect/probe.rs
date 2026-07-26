//! 多节点 ICMP Echo、TCP Connect 与 HTTP 调度任务。

mod http;
mod icmp_echo;
mod model;
mod tcp_connect;

use std::{sync::Arc, time::Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tokio::sync::Mutex;

use crate::scheduler::{TaskContext, TaskError, ValueTask};

use super::{
    MetricSample,
    sample::{duration_ms, unix_timestamp_ms},
};
use http::HttpClients;
use icmp_echo::IcmpClients;

pub use model::{
    HttpStatusRange, ProbeAttemptSnapshot, ProbeConfigError, ProbeNodeConfig, ProbeNodeSnapshot,
    ProbeProtocol, ProbeSnapshot, ProbeTarget, ProbeTaskConfig,
};

/// 并发执行多个网络节点探测的调度任务。
pub struct ProbeTask {
    config: ProbeTaskConfig,
    last_sampled_at: Mutex<Option<Instant>>,
}

impl ProbeTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.probe.network.v1";

    /// 校验配置并创建 Probe Task。
    pub fn try_with_config(config: ProbeTaskConfig) -> Result<Self, ProbeConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            last_sampled_at: Mutex::new(None),
        })
    }

    /// 返回当前生效的多节点探测配置。
    pub fn config(&self) -> &ProbeTaskConfig {
        &self.config
    }
}

#[async_trait]
impl ValueTask for ProbeTask {
    type Output = MetricSample<ProbeSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let cancellation = context.cancellation;
        let sampled_at = Instant::now();
        let sampled_at_ms = unix_timestamp_ms();
        let sample_interval_ms = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(TaskError::Transient(anyhow!("network probe cancelled before start")));
            }
            mut previous = self.last_sampled_at.lock() => {
                previous.replace(sampled_at)
                    .map(|previous| duration_ms(sampled_at.duration_since(previous)))
            }
        };
        let icmp_clients = self
            .config
            .nodes
            .iter()
            .any(|node| matches!(node.target, ProbeTarget::IcmpEcho))
            .then(|| Arc::new(IcmpClients::new()));
        let http_clients = self
            .config
            .nodes
            .iter()
            .any(|node| matches!(node.target, ProbeTarget::Http { .. }))
            .then(HttpClients::new)
            .transpose()
            .map_err(|error| {
                TaskError::Permanent(anyhow!("failed to build HTTP probe client: {error}"))
            })?
            .map(Arc::new);
        let identifier_seed =
            u16::from_le_bytes([context.run_id.as_bytes()[0], context.run_id.as_bytes()[1]]);
        let work = stream::iter(self.config.nodes.iter().cloned().enumerate().map(
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
        .buffer_unordered(self.config.concurrency.get())
        .collect::<Vec<_>>();
        let mut results = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(TaskError::Transient(anyhow!("network probe cancelled")));
            }
            results = work => results,
        };
        if results.iter().any(|(_, result)| result.is_err()) {
            return Err(TaskError::Transient(anyhow!("network probe cancelled")));
        }
        results.sort_by_key(|(index, _)| *index);
        let nodes = results
            .into_iter()
            .map(|(_, result)| result.expect("cancelled results returned above"))
            .collect::<Vec<_>>();
        let snapshot = ProbeSnapshot {
            total_nodes: nodes.len(),
            healthy_nodes: nodes.iter().filter(|node| node.succeeded > 0).count(),
            nodes,
        };
        Ok(MetricSample::new(
            sampled_at_ms,
            sample_interval_ms,
            snapshot,
        ))
    }

    fn kind(&self) -> &'static str {
        Self::KIND
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
        net::TcpListener,
        task::JoinHandle,
    };

    use crate::{
        scheduler::{TaskError, ValueTask},
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
    async fn task_probes_multiple_tcp_nodes_and_isolates_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open_port = listener.local_addr().unwrap().port();
        let closed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_port = closed_listener.local_addr().unwrap().port();
        drop(closed_listener);
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let task = ProbeTask::try_with_config(ProbeTaskConfig {
            concurrency: NonZeroUsize::new(2).unwrap(),
            nodes: vec![tcp_node("open", open_port), tcp_node("closed", closed_port)],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), ProbeTask::KIND);
        assert_eq!(output.snapshot.total_nodes, 2);
        assert_eq!(output.snapshot.healthy_nodes, 1);
        assert_eq!(output.snapshot.nodes[0].name, "open");
        assert_eq!(output.snapshot.nodes[0].succeeded, 1);
        assert_eq!(output.snapshot.nodes[1].name, "closed");
        assert_eq!(output.snapshot.nodes[1].succeeded, 0);
        assert_eq!(output.snapshot.nodes[1].failure_percent, 100.0);
        accept.await.unwrap();
    }

    #[tokio::test]
    async fn task_records_http_status_and_applies_expected_range() {
        let (healthy_url, healthy_server) = http_endpoint(204).await;
        let (failing_url, failing_server) = http_endpoint(503).await;
        let task = ProbeTask::try_with_config(ProbeTaskConfig {
            concurrency: NonZeroUsize::new(2).unwrap(),
            nodes: vec![
                http_node("healthy", healthy_url.clone()),
                http_node("failing", failing_url.clone()),
            ],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(output.snapshot.healthy_nodes, 1);
        assert_eq!(output.snapshot.nodes[0].protocol, ProbeProtocol::Http);
        assert_eq!(
            output.snapshot.nodes[0].url.as_deref(),
            Some(healthy_url.as_str())
        );
        assert_eq!(output.snapshot.nodes[0].attempts[0].status_code, Some(204));
        assert!(output.snapshot.nodes[0].attempts[0].success);
        assert_eq!(
            output.snapshot.nodes[1].url.as_deref(),
            Some(failing_url.as_str())
        );
        assert_eq!(output.snapshot.nodes[1].attempts[0].status_code, Some(503));
        assert!(!output.snapshot.nodes[1].attempts[0].success);
        assert_eq!(output.snapshot.nodes[1].min_latency_ms, None);
        assert_eq!(output.snapshot.nodes[1].avg_latency_ms, None);
        assert_eq!(output.snapshot.nodes[1].max_latency_ms, None);
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
        let task = ProbeTask::try_with_config(ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![node],
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(output.snapshot.healthy_nodes, 1);
        assert_eq!(output.snapshot.nodes[0].attempts[0].status_code, Some(204));
        assert!(output.snapshot.nodes[0].attempts[0].success);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn task_honors_pre_cancelled_context() {
        let task = ProbeTask::try_with_config(ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![tcp_node("cancelled", 9)],
        })
        .unwrap();
        let context = context();
        context.cancellation.cancel();

        let error = task.run(context).await.unwrap_err();

        assert!(matches!(error, TaskError::Transient(_)));
    }
}
