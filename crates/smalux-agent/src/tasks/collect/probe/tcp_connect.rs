//! TCP Connect 单节点执行器。

use std::{net::SocketAddr, time::Instant};

use tokio::{
    net::{TcpStream, lookup_host},
    time,
};
use tokio_util::sync::CancellationToken;

use super::model::{
    ProbeAttemptSnapshot, ProbeNodeConfig, ProbeNodeSnapshot, ProbeTarget, latency_ms,
};

pub(super) async fn probe(
    node: ProbeNodeConfig,
    cancellation: CancellationToken,
) -> Result<ProbeNodeSnapshot, ()> {
    let port = match node.target {
        ProbeTarget::TcpConnect { port } => port.get(),
        ProbeTarget::IcmpEcho | ProbeTarget::UdpRequest { .. } | ProbeTarget::Http { .. } => {
            unreachable!("TCP executor received a non-TCP node")
        }
    };
    let addresses = tokio::select! {
        _ = cancellation.cancelled() => return Err(()),
        result = time::timeout(node.timeout, lookup_host((node.host.as_str(), port))) => {
            match result {
                Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
                Ok(Err(error)) => return Ok(ProbeNodeSnapshot::failed(&node, format!("DNS lookup failed: {error}"))),
                Err(_) => return Ok(ProbeNodeSnapshot::failed(&node, "DNS lookup timed out")),
            }
        }
    };
    if addresses.is_empty() {
        return Ok(ProbeNodeSnapshot::failed(
            &node,
            "DNS lookup returned no addresses",
        ));
    }

    let mut attempts = Vec::with_capacity(node.attempts.get() as usize);
    let mut resolved_ip = None;
    for sequence in 1..=node.attempts.get() {
        let started = Instant::now();
        let connected = tokio::select! {
            _ = cancellation.cancelled() => return Err(()),
            result = time::timeout(node.timeout, connect_any(&addresses)) => result,
        };
        match connected {
            Ok(Ok(address)) => {
                resolved_ip = Some(address.ip().to_string());
                attempts.push(ProbeAttemptSnapshot {
                    sequence,
                    success: true,
                    latency_ms: Some(latency_ms(started.elapsed())),
                    status_code: None,
                    error: None,
                });
            }
            Ok(Err(error)) => attempts.push(ProbeAttemptSnapshot {
                sequence,
                success: false,
                latency_ms: None,
                status_code: None,
                error: Some(error),
            }),
            Err(_) => attempts.push(ProbeAttemptSnapshot {
                sequence,
                success: false,
                latency_ms: None,
                status_code: None,
                error: Some("TCP connect timed out".to_owned()),
            }),
        }
        if sequence < node.attempts.get() && !node.interval.is_zero() {
            tokio::select! {
                _ = cancellation.cancelled() => return Err(()),
                _ = time::sleep(node.interval) => {}
            }
        }
    }
    Ok(ProbeNodeSnapshot::from_attempts(
        &node,
        resolved_ip,
        attempts,
    ))
}

async fn connect_any(addresses: &[SocketAddr]) -> Result<SocketAddr, String> {
    let mut errors = Vec::with_capacity(addresses.len());
    for address in addresses {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                drop(stream);
                return Ok(*address);
            }
            Err(error) => errors.push(format!("{address}: {error}")),
        }
    }
    Err(format!("TCP connect failed: {}", errors.join("; ")))
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, time::Duration};

    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn tcp_probe_reports_local_listener_latency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            for _ in 0..2 {
                listener.accept().await.unwrap();
            }
        });
        let node = ProbeNodeConfig {
            name: "local".to_owned(),
            host: address.ip().to_string(),
            target: ProbeTarget::TcpConnect {
                port: NonZeroU16::new(address.port()).unwrap(),
            },
            attempts: NonZeroU16::new(2).unwrap(),
            timeout: Duration::from_secs(1),
            interval: Duration::ZERO,
        };

        let snapshot = probe(node, CancellationToken::new()).await.unwrap();

        assert_eq!(snapshot.attempted, 2);
        assert_eq!(snapshot.succeeded, 2);
        assert_eq!(snapshot.failure_percent, 0.0);
        assert_eq!(snapshot.resolved_ip.as_deref(), Some("127.0.0.1"));
        assert!(snapshot.avg_latency_ms.is_some());
        accept.await.unwrap();
    }
}
