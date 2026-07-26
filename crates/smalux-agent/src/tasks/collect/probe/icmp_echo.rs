//! ICMP Echo 单节点执行器。

use std::{net::IpAddr, time::Duration};

use surge_ping::{Client, Config, ICMP, PingIdentifier, PingSequence};
use tokio::{net::lookup_host, time};
use tokio_util::sync::CancellationToken;

use super::model::{
    ProbeAttemptSnapshot, ProbeNodeConfig, ProbeNodeSnapshot, ProbeTarget, latency_ms,
};

pub(super) struct IcmpClients {
    v4: Result<Client, String>,
    v6: Result<Client, String>,
}

impl IcmpClients {
    pub(super) fn new() -> Self {
        Self {
            v4: Client::new(&Config::default())
                .map_err(|error| format!("IPv4 ICMP socket unavailable: {error}")),
            v6: Client::new(&Config::builder().kind(ICMP::V6).build())
                .map_err(|error| format!("IPv6 ICMP socket unavailable: {error}")),
        }
    }

    fn client(&self, address: IpAddr) -> Result<Client, String> {
        match address {
            IpAddr::V4(_) => self.v4.clone(),
            IpAddr::V6(_) => self.v6.clone(),
        }
    }
}

pub(super) async fn probe(
    node: ProbeNodeConfig,
    identifier: u16,
    clients: &IcmpClients,
    cancellation: CancellationToken,
) -> Result<ProbeNodeSnapshot, ()> {
    debug_assert!(matches!(node.target, ProbeTarget::IcmpEcho));
    let addresses = tokio::select! {
        _ = cancellation.cancelled() => return Err(()),
        result = time::timeout(node.timeout, lookup_host((node.host.as_str(), 0))) => {
            match result {
                Ok(Ok(addresses)) => addresses.map(|address| address.ip()).collect::<Vec<_>>(),
                Ok(Err(error)) => return Ok(ProbeNodeSnapshot::failed(&node, format!("DNS lookup failed: {error}"))),
                Err(_) => return Ok(ProbeNodeSnapshot::failed(&node, "DNS lookup timed out")),
            }
        }
    };
    let Some((address, client)) = select_client(addresses, clients) else {
        let error = clients
            .v4
            .as_ref()
            .err()
            .or_else(|| clients.v6.as_ref().err())
            .cloned()
            .unwrap_or_else(|| "DNS lookup returned no addresses".to_owned());
        return Ok(ProbeNodeSnapshot::failed(&node, error));
    };

    let mut pinger = client.pinger(address, PingIdentifier(identifier)).await;
    pinger.timeout(node.timeout);
    let payload = [0_u8; 32];
    let mut attempts = Vec::with_capacity(node.attempts.get() as usize);
    for sequence in 1..=node.attempts.get() {
        let result = tokio::select! {
            _ = cancellation.cancelled() => return Err(()),
            result = pinger.ping(PingSequence(sequence - 1), &payload) => result,
        };
        match result {
            Ok((_packet, duration)) => attempts.push(ProbeAttemptSnapshot {
                sequence,
                success: true,
                latency_ms: Some(latency_ms(duration)),
                status_code: None,
                error: None,
            }),
            Err(error) => attempts.push(ProbeAttemptSnapshot {
                sequence,
                success: false,
                latency_ms: None,
                status_code: None,
                error: Some(format!("ICMP ping failed: {error}")),
            }),
        }
        sleep_between_attempts(&node, sequence, &cancellation).await?;
    }
    Ok(ProbeNodeSnapshot::from_attempts(
        &node,
        Some(address.to_string()),
        attempts,
    ))
}

fn select_client(addresses: Vec<IpAddr>, clients: &IcmpClients) -> Option<(IpAddr, Client)> {
    addresses
        .into_iter()
        .find_map(|address| clients.client(address).ok().map(|client| (address, client)))
}

async fn sleep_between_attempts(
    node: &ProbeNodeConfig,
    sequence: u16,
    cancellation: &CancellationToken,
) -> Result<(), ()> {
    if sequence >= node.attempts.get() || node.interval == Duration::ZERO {
        return Ok(());
    }
    tokio::select! {
        _ = cancellation.cancelled() => Err(()),
        _ = time::sleep(node.interval) => Ok(()),
    }
}
