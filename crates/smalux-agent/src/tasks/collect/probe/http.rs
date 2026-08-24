//! HTTP GET 单节点执行器。

use std::time::Instant;

use reqwest::{Client, redirect::Policy};
use tokio::time;
use tokio_util::sync::CancellationToken;

use super::model::{
    HttpStatusRange, ProbeAttemptSnapshot, ProbeNodeConfig, ProbeNodeSnapshot, ProbeTarget,
    latency_ms,
};

/// HTTP 探测共享的两种重定向策略客户端。
pub(super) struct HttpClients {
    no_redirect: Client,
    follow_redirects: Client,
}

impl HttpClients {
    /// 构建可跨节点复用连接池、DNS 解析器和 TLS 配置的客户端。
    pub(super) fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            no_redirect: Client::builder().redirect(Policy::none()).build()?,
            follow_redirects: Client::builder().redirect(Policy::limited(10)).build()?,
        })
    }

    fn select(&self, follow_redirects: bool) -> &Client {
        if follow_redirects {
            &self.follow_redirects
        } else {
            &self.no_redirect
        }
    }
}

/// 对一个 HTTP 节点执行全部尝试；取消时返回 `Err(())` 交给上层终止整个 Task。
pub(super) async fn probe(
    node: ProbeNodeConfig,
    clients: &HttpClients,
    cancellation: CancellationToken,
) -> Result<ProbeNodeSnapshot, ()> {
    let (url, expected_status, follow_redirects) = match &node.target {
        ProbeTarget::Http {
            url,
            expected_status,
            follow_redirects,
        } => (url.clone(), *expected_status, *follow_redirects),
        ProbeTarget::IcmpEcho | ProbeTarget::TcpConnect { .. } | ProbeTarget::UdpRequest { .. } => {
            unreachable!("HTTP executor received a non-HTTP node")
        }
    };
    let client = clients.select(follow_redirects);
    let mut attempts = Vec::with_capacity(node.attempts.get() as usize);

    for sequence in 1..=node.attempts.get() {
        let started = Instant::now();
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(()),
            result = time::timeout(node.timeout, client.get(url.clone()).send()) => result,
        };
        attempts.push(attempt_snapshot(
            sequence,
            started,
            response,
            expected_status,
        ));

        if sequence < node.attempts.get() && !node.interval.is_zero() {
            tokio::select! {
                _ = cancellation.cancelled() => return Err(()),
                _ = time::sleep(node.interval) => {}
            }
        }
    }

    Ok(ProbeNodeSnapshot::from_attempts(&node, None, attempts))
}

fn attempt_snapshot(
    sequence: u16,
    started: Instant,
    response: Result<Result<reqwest::Response, reqwest::Error>, time::error::Elapsed>,
    expected_status: HttpStatusRange,
) -> ProbeAttemptSnapshot {
    match response {
        Ok(Ok(response)) => {
            let status = response.status().as_u16();
            let success = expected_status.contains(status);
            ProbeAttemptSnapshot {
                sequence,
                success,
                latency_ms: Some(latency_ms(started.elapsed())),
                status_code: Some(status),
                error: (!success).then(|| {
                    format!(
                        "HTTP status {status} is outside expected range {}..={}",
                        expected_status.min, expected_status.max
                    )
                }),
            }
        }
        Ok(Err(error)) => ProbeAttemptSnapshot {
            sequence,
            success: false,
            latency_ms: None,
            status_code: None,
            error: Some(format!("HTTP request failed: {error}")),
        },
        Err(_) => ProbeAttemptSnapshot {
            sequence,
            success: false,
            latency_ms: None,
            status_code: None,
            error: Some("HTTP request timed out".to_owned()),
        },
    }
}
