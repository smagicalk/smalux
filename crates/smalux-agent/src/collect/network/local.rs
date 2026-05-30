//! 本地网卡信息映射。

use smalux_core::model::info::{Ip, Network, NetworkInfo};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use sysinfo::Networks;

/// 从已刷新的网络对象构建本地所有网卡的流量、错误计数、MAC 地址和 IP 列表。
pub(crate) fn build_network_info(networks: &Networks) -> NetworkInfo {
    build_network_info_with_elapsed(networks, None)
}

/// 从已刷新的网络对象构建网络信息，并按真实间隔计算速度。
pub(crate) fn build_network_info_with_elapsed(
    networks: &Networks,
    elapsed_secs: Option<f64>,
) -> NetworkInfo {
    build_network_info_with_elapsed_and_filter(networks, elapsed_secs, &[], &[])
}

/// 从已刷新的网络对象构建网络信息，并按 include/exclude 过滤网卡。
pub(crate) fn build_network_info_with_elapsed_and_filter(
    networks: &Networks,
    elapsed_secs: Option<f64>,
    include_interfaces: &[String],
    exclude_interfaces: &[String],
) -> NetworkInfo {
    let filter = InterfaceFilter::new(include_interfaces, exclude_interfaces);
    let mut matched_include_interfaces = HashSet::new();
    let mut res_network = NetworkInfo::default();
    res_network.warmed_up = elapsed_secs.filter(|secs| *secs > 0.0).is_some();

    for (name, network) in networks.list() {
        if !filter.includes(name) {
            continue;
        }
        if filter.uses_include() {
            matched_include_interfaces.insert(name.as_str());
        }

        // 单网卡明细用于展示和诊断，汇总字段同步累加到 NetworkInfo。
        let mut network_info = Network::default();
        network_info.name = name.to_string();
        network_info.mtu = network.mtu();
        network_info.received = network.received();
        network_info.errors_on_received = network.errors_on_received();
        network_info.errors_on_transmitted = network.errors_on_transmitted();
        network_info.mac = network.mac_address().to_string();
        network_info.packets_received = network.packets_received();
        network_info.transmitted = network.transmitted();
        if let Some(elapsed_secs) = elapsed_secs.filter(|secs| *secs > 0.0) {
            network_info.received_bytes_per_sec = network_info.received as f64 / elapsed_secs;
            network_info.transmitted_bytes_per_sec = network_info.transmitted as f64 / elapsed_secs;
            network_info.network_bytes_per_sec =
                network_info.received_bytes_per_sec + network_info.transmitted_bytes_per_sec;
        }

        network_info.total_received = network.total_received();
        network_info.total_errors_on_received = network.total_errors_on_received();
        network_info.total_errors_on_transmitted = network.total_errors_on_transmitted();
        network_info.total_packets_received = network.total_packets_received();
        network_info.total_transmitted = network.total_transmitted();
        network_info.used_traffic_bytes = network_info
            .total_received
            .saturating_add(network_info.total_transmitted);

        // 统计全部网卡在本次刷新周期内的增量值。
        res_network.received += network_info.received;
        res_network.errors_on_received += network_info.errors_on_received;
        res_network.errors_on_transmitted += network_info.errors_on_transmitted;
        res_network.packets_received += network_info.packets_received;
        res_network.transmitted += network_info.transmitted;

        res_network.total_received += network_info.total_received;
        res_network.total_errors_on_received += network_info.total_errors_on_received;
        res_network.total_errors_on_transmitted += network_info.total_errors_on_transmitted;
        res_network.total_packets_received += network_info.total_packets_received;
        res_network.total_transmitted += network_info.total_transmitted;
        res_network.used_traffic_bytes += network_info.used_traffic_bytes;

        for ip in network.ip_networks() {
            // 保留地址和掩码长度，后续可用于展示 CIDR 或筛选内网地址。
            let mut ip_info = Ip::default();
            ip_info.mask_len = ip.prefix;
            ip_info.ip = ip.addr;
            network_info.ip.push(ip_info);
        }
        res_network.networks.push(network_info);
    }

    if let Some(elapsed_secs) = elapsed_secs.filter(|secs| *secs > 0.0) {
        res_network.received_bytes_per_sec = res_network.received as f64 / elapsed_secs;
        res_network.transmitted_bytes_per_sec = res_network.transmitted as f64 / elapsed_secs;
        res_network.network_bytes_per_sec =
            res_network.received_bytes_per_sec + res_network.transmitted_bytes_per_sec;
    }

    log_missing_included_interfaces(include_interfaces, &matched_include_interfaces);

    res_network
}

/// 网卡过滤器；include 非空时优先使用 include，exclude 不参与过滤。
struct InterfaceFilter<'a> {
    include: HashSet<&'a str>,
    exclude: HashSet<&'a str>,
}

impl<'a> InterfaceFilter<'a> {
    /// 构建采样周期内复用的网卡过滤器。
    fn new(include_interfaces: &'a [String], exclude_interfaces: &'a [String]) -> Self {
        Self {
            include: include_interfaces
                .iter()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            exclude: exclude_interfaces
                .iter()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
        }
    }

    /// 判断是否使用 include 白名单模式。
    fn uses_include(&self) -> bool {
        !self.include.is_empty()
    }

    /// 判断当前网卡是否应该进入网络指标汇总。
    fn includes(&self, name: &str) -> bool {
        if self.uses_include() {
            return self.include.contains(name);
        }

        !self.exclude.contains(name)
    }
}

/// 输出 include 中不存在的网卡，方便发现配置拼写或平台名称问题。
fn log_missing_included_interfaces(
    include_interfaces: &[String],
    matched_include_interfaces: &HashSet<&str>,
) {
    if include_interfaces.is_empty() {
        return;
    }

    let missing_interfaces = include_interfaces
        .iter()
        .map(String::as_str)
        .filter(|interface| !matched_include_interfaces.contains(interface))
        .collect::<Vec<_>>();

    if missing_interfaces.is_empty() {
        return;
    }

    if matched_include_interfaces.is_empty() {
        tracing::warn!(
            include_interfaces = ?include_interfaces,
            "Network interface filter matched no interfaces"
        );
    } else {
        tracing::warn!(
            missing_interfaces = ?missing_interfaces,
            "Some configured network interfaces were not found"
        );
    }
}

/// 从网卡信息中提取全部本地 IP。
pub(crate) fn local_ips(network_info: &NetworkInfo) -> Vec<Ip> {
    network_info
        .networks
        .iter()
        .flat_map(|network| network.ip.iter().cloned())
        .filter(|ip| is_reportable_local_ip(&ip.ip))
        .collect()
}

/// 从网卡信息中查找公网候选 IP。
pub(crate) fn interface_public_ip_candidate(network_info: &NetworkInfo) -> Option<IpAddr> {
    network_info
        .networks
        .iter()
        .flat_map(|network| network.ip.iter())
        .map(|ip| ip.ip)
        .find(is_public_ip_candidate)
}

/// 判断是否适合作为本地 IP 上报。
fn is_reportable_local_ip(ip: &IpAddr) -> bool {
    !(ip.is_loopback() || ip.is_unspecified() || ip.is_multicast())
}

/// 判断网卡地址是否可能是真实公网地址。
fn is_public_ip_candidate(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => is_public_ipv4_candidate(ipv4),
        IpAddr::V6(ipv6) => is_public_ipv6_candidate(ipv6),
    }
}

/// 判断 IPv4 是否是公网候选地址。
fn is_public_ipv4_candidate(ip: &Ipv4Addr) -> bool {
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || is_cgnat_ipv4(ip))
}

/// 判断 IPv4 是否属于运营商级 NAT 地址段。
fn is_cgnat_ipv4(ip: &Ipv4Addr) -> bool {
    let [first, second, _, _] = ip.octets();
    first == 100 && (64..=127).contains(&second)
}

/// 判断 IPv6 是否是公网候选地址。
fn is_public_ipv6_candidate(ip: &Ipv6Addr) -> bool {
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_unique_local()
        || ip.is_unicast_link_local())
}

#[cfg(test)]
mod tests {
    //! 本地网卡信息映射测试。

    use super::*;

    /// 验证网卡明细数量和汇总流量一致。
    #[test]
    fn test_build_network_info() {
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh(true);

        let network_info = build_network_info(&networks);
        let received: u64 = network_info
            .networks
            .iter()
            .map(|network| network.received)
            .sum();
        let transmitted: u64 = network_info
            .networks
            .iter()
            .map(|network| network.transmitted)
            .sum();

        assert_eq!(network_info.networks.len(), networks.list().len());
        assert_eq!(network_info.received, received);
        assert_eq!(network_info.transmitted, transmitted);
        assert!(network_info.total_received >= network_info.received);
        assert!(network_info.total_transmitted >= network_info.transmitted);
    }

    /// 验证网络速度按真实间隔计算。
    #[test]
    fn test_build_network_info_with_elapsed_sets_speed() {
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh(true);

        let network_info = build_network_info_with_elapsed(&networks, Some(2.0));

        assert!(network_info.warmed_up);
        assert_eq!(
            network_info.network_bytes_per_sec,
            network_info.received_bytes_per_sec + network_info.transmitted_bytes_per_sec
        );
        assert_eq!(
            network_info.used_traffic_bytes,
            network_info.total_received + network_info.total_transmitted
        );
    }

    /// 验证网卡白名单只保留指定网卡，并且汇总值只统计这些网卡。
    #[test]
    fn test_build_network_info_filters_interfaces() {
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh(true);
        let Some((first_name, _first_network)) = networks.list().iter().next() else {
            return;
        };
        let include_interfaces = vec![first_name.to_string()];

        let network_info = build_network_info_with_elapsed_and_filter(
            &networks,
            Some(2.0),
            &include_interfaces,
            &[],
        );

        assert!(
            network_info
                .networks
                .iter()
                .all(|network| network.name == *first_name)
        );
        assert!(network_info.networks.len() <= 1);
        assert_eq!(
            network_info.received,
            network_info
                .networks
                .iter()
                .map(|network| network.received)
                .sum::<u64>()
        );
        assert_eq!(
            network_info.transmitted,
            network_info
                .networks
                .iter()
                .map(|network| network.transmitted)
                .sum::<u64>()
        );
    }

    /// 验证不存在的网卡名称会得到空汇总，避免误报其他网卡。
    #[test]
    fn test_build_network_info_filters_unknown_interface_to_empty() {
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh(true);
        let include_interfaces = vec!["smalux-non-existent-interface".to_string()];

        let network_info = build_network_info_with_elapsed_and_filter(
            &networks,
            Some(2.0),
            &include_interfaces,
            &[],
        );

        assert!(network_info.networks.is_empty());
        assert_eq!(network_info.received, 0);
        assert_eq!(network_info.transmitted, 0);
        assert_eq!(network_info.total_received, 0);
        assert_eq!(network_info.total_transmitted, 0);
        assert!(network_info.warmed_up);
    }

    /// 验证网卡黑名单会排除指定网卡，并且汇总值只统计剩余网卡。
    #[test]
    fn test_build_network_info_excludes_interfaces() {
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh(true);
        let Some((first_name, _first_network)) = networks.list().iter().next() else {
            return;
        };
        let exclude_interfaces = vec![first_name.to_string()];

        let network_info = build_network_info_with_elapsed_and_filter(
            &networks,
            Some(2.0),
            &[],
            &exclude_interfaces,
        );

        assert!(
            network_info
                .networks
                .iter()
                .all(|network| network.name != *first_name)
        );
        assert_eq!(
            network_info.received,
            network_info
                .networks
                .iter()
                .map(|network| network.received)
                .sum::<u64>()
        );
        assert_eq!(
            network_info.transmitted,
            network_info
                .networks
                .iter()
                .map(|network| network.transmitted)
                .sum::<u64>()
        );
    }

    /// 验证 include 非空时优先使用 include，exclude 不会排除已 include 的网卡。
    #[test]
    fn test_build_network_info_include_takes_precedence_over_exclude() {
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh(true);
        let Some((first_name, _first_network)) = networks.list().iter().next() else {
            return;
        };
        let include_interfaces = vec![first_name.to_string()];
        let exclude_interfaces = vec![first_name.to_string()];

        let network_info = build_network_info_with_elapsed_and_filter(
            &networks,
            Some(2.0),
            &include_interfaces,
            &exclude_interfaces,
        );

        assert_eq!(network_info.networks.len(), 1);
        assert_eq!(network_info.networks[0].name, *first_name);
    }

    /// 验证私网和 CGNAT 不会被当成公网候选。
    #[test]
    fn test_public_ip_candidate_filters_non_public_ranges() {
        assert!(!is_public_ip_candidate(&IpAddr::V4(Ipv4Addr::new(
            192, 168, 1, 1
        ))));
        assert!(!is_public_ip_candidate(&IpAddr::V4(Ipv4Addr::new(
            100, 64, 0, 1
        ))));
        assert!(!is_public_ip_candidate(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_public_ip_candidate(&IpAddr::V4(Ipv4Addr::new(
            8, 8, 8, 8
        ))));
    }
}
