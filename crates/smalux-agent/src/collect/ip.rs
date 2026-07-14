//! IP 快照模型与本地网卡地址采集。

use serde::Serialize;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use sysinfo::Networks;

mod public;

pub use public::{collect_public, fetch_public_ips};

/// IP 地址作用域。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IpScope {
    /// 本机回环地址。
    Loopback,
    /// IPv4 私网或 IPv6 Unique Local 地址。
    Private,
    /// 仅当前链路有效的地址。
    LinkLocal,
    /// 可作为公网或全局单播地址使用。
    Global,
    /// 未指定、组播、文档地址等其他作用域。
    Other,
}

/// 单个网卡地址。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceAddress {
    /// 网卡接口名称。
    pub interface: String,
    /// IPv4 或 IPv6 地址。
    pub address: IpAddr,
    /// CIDR 前缀长度。
    pub prefix_length: u8,
    /// 根据地址规则分类的作用域。
    pub scope: IpScope,
}

/// 公网 IP 查询状态。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PublicIpState {
    /// 本次采集没有请求该地址族。
    NotRequested,
    /// 已取得并校验合法公网地址。
    Ready {
        /// 端点返回的公网 IP 地址。
        address: IpAddr,
    },
    /// 所有内置端点均失败。
    Failed {
        /// 汇总后的失败原因，不包含认证信息。
        message: String,
    },
}

/// 公网 IPv4 与 IPv6 查询结果。
#[derive(Debug, Clone, Serialize)]
pub struct PublicIpSnapshot {
    /// 公网 IPv4 查询状态。
    pub ipv4: PublicIpState,
    /// 公网 IPv6 查询状态。
    pub ipv6: PublicIpState,
}

/// IP 信息快照。
#[derive(Debug, Clone, Serialize)]
pub struct IpSnapshot {
    /// 本机全部去重网卡地址。
    pub local: Vec<InterfaceAddress>,
    /// 公网 IPv4 状态。
    pub public_ipv4: PublicIpState,
    /// 公网 IPv6 状态。
    pub public_ipv6: PublicIpState,
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
                    address: key.1,
                    prefix_length: key.2,
                    scope: scope(key.1),
                });
            }
        }
    }

    local.sort_by(|left, right| {
        left.interface
            .cmp(&right.interface)
            .then_with(|| left.address.to_string().cmp(&right.address.to_string()))
    });

    IpSnapshot {
        local,
        public_ipv4: PublicIpState::NotRequested,
        public_ipv6: PublicIpState::NotRequested,
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
    fn ip_scope_classifies_common_local_addresses() {
        assert_eq!(scope("127.0.0.1".parse().unwrap()), IpScope::Loopback);
        assert_eq!(scope("192.168.1.1".parse().unwrap()), IpScope::Private);
        assert_eq!(scope("169.254.1.1".parse().unwrap()), IpScope::LinkLocal);
        assert_eq!(scope("8.8.8.8".parse().unwrap()), IpScope::Global);
        assert_eq!(scope("::1".parse().unwrap()), IpScope::Loopback);
        assert_eq!(scope("fd00::1".parse().unwrap()), IpScope::Private);
    }
}
