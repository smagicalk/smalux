//! UDP 请求/响应单节点执行器。

use std::{net::SocketAddr, time::Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{net::UdpSocket, time};
use tokio_util::sync::CancellationToken;

use super::model::{
    MAX_UDP_PAYLOAD_BYTES, ProbeAttemptSnapshot, ProbeNodeConfig, ProbeNodeSnapshot, ProbeTarget,
    latency_ms,
};

/// 对一个 UDP 节点执行全部尝试。
///
/// 只有收到目标端返回的数据报，且响应满足可选前缀时才算成功。UDP 的 `send` 成功仅表示
/// 本机接受了数据报，不能证明目标服务可达，因此不会被单独视为成功。
pub(super) async fn probe(
    node: ProbeNodeConfig,
    cancellation: CancellationToken,
) -> Result<ProbeNodeSnapshot, ()> {
    let (port, request_payload, expected_response_prefix) = match &node.target {
        ProbeTarget::UdpRequest {
            port,
            request_payload,
            expected_response_prefix,
        } => (
            port.get(),
            request_payload.as_slice(),
            expected_response_prefix.as_deref(),
        ),
        ProbeTarget::IcmpEcho | ProbeTarget::TcpConnect { .. } | ProbeTarget::Http { .. } => {
            unreachable!("UDP executor received a non-UDP node")
        }
    };

    let addresses = tokio::select! {
        _ = cancellation.cancelled() => return Err(()),
        result = time::timeout(node.timeout, tokio::net::lookup_host((node.host.as_str(), port))) => {
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
        let exchanged = tokio::select! {
            _ = cancellation.cancelled() => return Err(()),
            result = time::timeout(
                node.timeout,
                exchange_any(&addresses, request_payload, expected_response_prefix),
            ) => result,
        };
        match exchanged {
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
                error: Some("UDP response timed out".to_owned()),
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

/// 同时尝试 DNS 返回的 IPv4/IPv6 地址，首个有效响应获胜。
async fn exchange_any(
    addresses: &[SocketAddr],
    request_payload: &[u8],
    expected_response_prefix: Option<&[u8]>,
) -> Result<SocketAddr, String> {
    let mut exchanges = addresses
        .iter()
        .copied()
        .map(|address| exchange(address, request_payload, expected_response_prefix))
        .collect::<FuturesUnordered<_>>();
    let mut errors = Vec::with_capacity(addresses.len());
    while let Some(result) = exchanges.next().await {
        match result {
            Ok(address) => return Ok(address),
            Err(error) => errors.push(error),
        }
    }
    Err(format!("UDP request failed: {}", errors.join("; ")))
}

async fn exchange(
    address: SocketAddr,
    request_payload: &[u8],
    expected_response_prefix: Option<&[u8]>,
) -> Result<SocketAddr, String> {
    let bind_address = if address.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_address)
        .await
        .map_err(|error| format!("{address}: bind failed: {error}"))?;
    socket
        .connect(address)
        .await
        .map_err(|error| format!("{address}: connect failed: {error}"))?;
    socket
        .send(request_payload)
        .await
        .map_err(|error| format!("{address}: send failed: {error}"))?;

    let mut response = [0_u8; MAX_UDP_PAYLOAD_BYTES];
    let received = socket
        .recv(&mut response)
        .await
        .map_err(|error| format!("{address}: receive failed: {error}"))?;
    if let Some(expected) = expected_response_prefix
        && !response[..received].starts_with(expected)
    {
        return Err(format!("{address}: response prefix mismatch"));
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, time::Duration};

    use tokio::net::UdpSocket;

    use super::*;

    async fn echo_server(response: &'static [u8]) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut request = [0_u8; 64];
            let (_, peer) = socket.recv_from(&mut request).await.unwrap();
            socket.send_to(response, peer).await.unwrap();
        });
        (address, server)
    }

    fn udp_node(address: SocketAddr, expected: Option<&[u8]>) -> ProbeNodeConfig {
        ProbeNodeConfig {
            name: "local-udp".to_owned(),
            host: address.ip().to_string(),
            target: ProbeTarget::UdpRequest {
                port: NonZeroU16::new(address.port()).unwrap(),
                request_payload: b"ping".to_vec(),
                expected_response_prefix: expected.map(<[u8]>::to_vec),
            },
            attempts: NonZeroU16::new(1).unwrap(),
            timeout: Duration::from_secs(1),
            interval: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn udp_probe_requires_matching_response() {
        let (address, server) = echo_server(b"pong:ok").await;

        let snapshot = probe(udp_node(address, Some(b"pong")), CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(snapshot.succeeded, 1);
        assert_eq!(snapshot.resolved_ip.as_deref(), Some("127.0.0.1"));
        assert!(snapshot.attempts[0].latency_ms.is_some());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn udp_probe_rejects_unexpected_response() {
        let (address, server) = echo_server(b"wrong").await;

        let snapshot = probe(udp_node(address, Some(b"pong")), CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(snapshot.succeeded, 0);
        let error = snapshot.attempts[0].error.as_deref().unwrap();
        assert!(error.starts_with("UDP request failed: "));
        assert!(error.ends_with("response prefix mismatch"));
        server.await.unwrap();
    }
}
