//! IP 快照模型与本地网卡地址采集。

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use sysinfo::Networks;

pub use smalux_protocol::agent::v1::{
    InterfaceAddress, IpScope, IpSnapshot, PublicIpSnapshot, PublicIpState, PublicIpStatus,
};

mod public;

pub(crate) use public::collect_public_families;
pub use public::{collect_public, fetch_public_ips};

pub(crate) fn public_ip_not_requested() -> PublicIpState {
    PublicIpState {
        status: PublicIpStatus::NotRequested as i32,
        address: None,
        message: None,
    }
}

/// 本地接口地址发现状态，与网络流量增量统计相互独立。
pub(crate) struct LocalIpCollector {
    networks: Option<Networks>,
}

impl LocalIpCollector {
    pub(crate) fn new() -> Self {
        Self { networks: None }
    }

    pub(crate) fn collect(&mut self) -> IpSnapshot {
        let networks = self
            .networks
            .get_or_insert_with(Networks::new_with_refreshed_list);
        networks.refresh(true);
        collect(networks)
    }
}

impl Default for LocalIpCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// 从已刷新的网卡对象中提取并去重本地地址。
pub(super) fn collect(networks: &Networks) -> IpSnapshot {
    let mut seen = HashSet::new();
    let mut local = Vec::new();

    for (interface, network) in networks.list() {
        for network_address in network.ip_networks() {
            let key = (
                interface.clone(),
                network_address.addr,
                network_address.prefix,
            );
            if seen.insert(key.clone()) {
                local.push(InterfaceAddress {
                    interface: key.0,
                    address: key.1.to_string(),
                    prefix_length: key.2.into(),
                    scope: scope(key.1) as i32,
                });
            }
        }
    }

    local.sort_by(|left, right| {
        left.interface
            .cmp(&right.interface)
            .then_with(|| left.address.cmp(&right.address))
    });

    IpSnapshot {
        local,
        public_ipv4: Some(public_ip_not_requested()),
        public_ipv6: Some(public_ip_not_requested()),
    }
}

/// 根据地址族分派作用域判断。
fn scope(address: IpAddr) -> IpScope {
    match address {
        IpAddr::V4(address) => scope_v4(address),
        IpAddr::V6(address) => scope_v6(address),
    }
}

/// 按 IPv4 标准范围分类地址作用域。
fn scope_v4(address: Ipv4Addr) -> IpScope {
    if address.is_loopback() {
        IpScope::Loopback
    } else if address.is_private() {
        IpScope::Private
    } else if address.is_link_local() {
        IpScope::LinkLocal
    } else if address.is_unspecified() || address.is_multicast() || address.is_documentation() {
        IpScope::Other
    } else {
        IpScope::Global
    }
}

/// 按 IPv6 标准范围分类地址作用域。
fn scope_v6(address: Ipv6Addr) -> IpScope {
    if address.is_loopback() {
        IpScope::Loopback
    } else if address.is_unique_local() {
        IpScope::Private
    } else if address.is_unicast_link_local() {
        IpScope::LinkLocal
    } else if address.is_unspecified() || address.is_multicast() {
        IpScope::Other
    } else {
        IpScope::Global
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_ip_collector_does_not_request_public_addresses() {
        let mut collector = LocalIpCollector::new();

        let snapshot = collector.collect();

        assert_eq!(
            snapshot.public_ipv4.expect("IPv4 state").status,
            PublicIpStatus::NotRequested as i32
        );
        assert_eq!(
            snapshot.public_ipv6.expect("IPv6 state").status,
            PublicIpStatus::NotRequested as i32
        );
    }

    #[test]
    fn ip_scope_classifies_common_local_addresses() {
        assert_eq!(scope("127.0.0.1".parse().unwrap()), IpScope::Loopback);
        assert_eq!(scope("192.168.1.1".parse().unwrap()), IpScope::Private);
        assert_eq!(scope("169.254.1.1".parse().unwrap()), IpScope::LinkLocal);
        assert_eq!(scope("8.8.8.8".parse().unwrap()), IpScope::Global);
        assert_eq!(scope("::1".parse().unwrap()), IpScope::Loopback);
        assert_eq!(scope("fd00::1".parse().unwrap()), IpScope::Private);
    }
}
