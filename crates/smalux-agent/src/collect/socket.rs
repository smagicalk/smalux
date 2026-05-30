//! Socket 汇总采集实现。
//!
//! 默认 count 级别优先走平台快速计数；light/details 才扫描 socket table。

#[cfg(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
use netstat2::AddressFamilyFlags;
use netstat2::{ProtocolFlags, ProtocolSocketInfo};
use smalux_core::model::info::{
    MetricLevel, SocketAccuracy, SocketDetail, SocketDetailInfo, SocketInfo, SocketLightInfo,
    SocketProtocol, SocketSource, TcpStateCount,
};
use std::collections::BTreeMap;

/// 采样 TCP/UDP socket 信息。
#[cfg(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
/// 支持 socket table 的平台会执行真实采样，失败时沿用上次状态或返回 failed。
pub(crate) fn sample_socket_info(
    previous: Option<&SocketInfo>,
    level: MetricLevel,
    limit: usize,
) -> SocketInfo {
    match sample_socket_info_result(level, limit) {
        Ok(info) => info,
        Err(error) => SocketInfo::stale_or_failed(previous, error.to_string()),
    }
}

/// 采样 TCP/UDP socket 信息。
#[cfg(not(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
)))]
/// 不支持 socket table 的平台返回 unsupported，避免调用方额外分支。
pub(crate) fn sample_socket_info(
    _previous: Option<&SocketInfo>,
    _level: MetricLevel,
    _limit: usize,
) -> SocketInfo {
    SocketInfo::unsupported("socket counting is unsupported on this platform".to_string())
}

/// 根据采集级别选择 socket 采样路径。
#[cfg(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
/// count/light/details 三档在这里统一分流，方便后续替换单档实现。
fn sample_socket_info_result(level: MetricLevel, limit: usize) -> anyhow::Result<SocketInfo> {
    match level {
        MetricLevel::Count => sample_socket_count_info(),
        MetricLevel::Light => sample_socket_light_info(limit),
        MetricLevel::Details => sample_socket_detail_info(limit),
    }
}

/// 采样默认高频总数；支持时优先使用平台快速计数。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn sample_socket_count_info() -> anyhow::Result<SocketInfo> {
    match count_socket_fast() {
        Ok((tcp, udp)) => Ok(SocketInfo::ready(
            tcp,
            udp,
            SocketSource::FastCounter,
            SocketAccuracy::Aggregate,
        )),
        Err(error) => {
            tracing::debug!(error = ?error, "Fast socket counter failed; falling back to socket table");
            let (tcp, udp) = count_socket_table()?;
            Ok(SocketInfo::ready(
                tcp,
                udp,
                SocketSource::SocketTable,
                SocketAccuracy::SocketTable,
            ))
        }
    }
}

/// 采样默认高频总数；当前平台没有稳定快速计数时使用 socket table。
#[cfg(any(target_os = "windows", target_os = "macos", target_os = "ios"))]
fn sample_socket_count_info() -> anyhow::Result<SocketInfo> {
    let (tcp, udp) = count_socket_table()?;
    Ok(SocketInfo::ready(
        tcp,
        udp,
        SocketSource::SocketTable,
        SocketAccuracy::SocketTable,
    ))
}

/// 从平台快速计数读取 TCP/UDP 数量。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn count_socket_fast() -> anyhow::Result<(u64, u64)> {
    let mut tcp = 0u64;
    let mut udp = 0u64;
    let mut found = false;

    for path in ["/proc/net/sockstat", "/proc/net/sockstat6"] {
        match std::fs::read_to_string(path) {
            Ok(content) => {
                let (path_tcp, path_udp, path_found) = parse_sockstat_counts(&content);
                tcp = tcp.saturating_add(path_tcp);
                udp = udp.saturating_add(path_udp);
                found |= path_found;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    if !found {
        anyhow::bail!("no TCP/UDP inuse counters found in procfs sockstat")
    }

    Ok((tcp, udp))
}

/// 解析 Linux `/proc/net/sockstat*` 中的 TCP/UDP inuse 计数。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn parse_sockstat_counts(content: &str) -> (u64, u64, bool) {
    let mut tcp = 0u64;
    let mut udp = 0u64;
    let mut found = false;

    for line in content.lines() {
        let mut fields = line.split_whitespace();
        let Some(kind) = fields.next() else {
            continue;
        };
        if !matches!(kind, "TCP:" | "TCP6:" | "UDP:" | "UDP6:") {
            continue;
        }

        let values = fields.collect::<Vec<_>>();
        let Some(inuse) = read_named_u64(&values, "inuse") else {
            continue;
        };
        found = true;
        if matches!(kind, "TCP:" | "TCP6:") {
            tcp = tcp.saturating_add(inuse);
        } else {
            udp = udp.saturating_add(inuse);
        }
    }

    (tcp, udp, found)
}

/// 从 `key value` 列表中读取指定数字。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_named_u64(fields: &[&str], key: &str) -> Option<u64> {
    fields.windows(2).find_map(|window| {
        (window[0] == key)
            .then(|| window[1].parse::<u64>().ok())
            .flatten()
    })
}

/// 从 socket table 采样轻量聚合。
fn sample_socket_light_info(_limit: usize) -> anyhow::Result<SocketInfo> {
    let sockets = collect_socket_table(false)?;
    let (tcp, udp) = count_socket_entries(&sockets);
    let light = SocketLightInfo {
        tcp_states: tcp_state_counts(&sockets),
    };

    Ok(SocketInfo::ready_with_level(
        tcp,
        udp,
        SocketSource::SocketTable,
        SocketAccuracy::SocketTable,
        MetricLevel::Light,
        Some(light),
        None,
    ))
}

/// 从 socket table 采样完整明细。
fn sample_socket_detail_info(limit: usize) -> anyhow::Result<SocketInfo> {
    let sockets = collect_socket_table(true)?;
    let (tcp, udp) = count_socket_entries(&sockets);
    let mut items = sockets.iter().map(socket_detail).collect::<Vec<_>>();
    let truncated = items.len() > limit;
    items.truncate(limit);
    let details = SocketDetailInfo {
        limit,
        truncated,
        items,
    };

    Ok(SocketInfo::ready_with_level(
        tcp,
        udp,
        SocketSource::SocketTable,
        SocketAccuracy::SocketTable,
        MetricLevel::Details,
        None,
        Some(details),
    ))
}

/// 从 socket table 统计 TCP/UDP 数量。
fn count_socket_table() -> anyhow::Result<(u64, u64)> {
    let sockets = collect_socket_table(false)?;
    Ok(count_socket_entries(&sockets))
}

/// 从 socket table 读取 socket 列表。
#[cfg(target_os = "windows")]
fn collect_socket_table(include_pids: bool) -> anyhow::Result<Vec<netstat2::SocketInfo>> {
    if include_pids {
        collect_socket_iter(netstat2::iterate_sockets_info(
            socket_address_family_flags(),
            socket_protocol_flags(),
        )?)
    } else {
        collect_socket_iter(netstat2::iterate_sockets_info_without_pids(
            socket_protocol_flags(),
        )?)
    }
}

/// 从 socket table 读取 socket 列表。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn collect_socket_table(include_pids: bool) -> anyhow::Result<Vec<netstat2::SocketInfo>> {
    if include_pids {
        collect_socket_iter(netstat2::iterate_sockets_info(
            socket_address_family_flags(),
            socket_protocol_flags(),
        )?)
    } else {
        collect_socket_iter(netstat2::iterate_sockets_info_without_pids(
            socket_address_family_flags(),
            socket_protocol_flags(),
        )?)
    }
}

/// 从 socket table 读取 socket 列表。
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn collect_socket_table(_include_pids: bool) -> anyhow::Result<Vec<netstat2::SocketInfo>> {
    collect_socket_iter(netstat2::iterate_sockets_info(
        socket_address_family_flags(),
        socket_protocol_flags(),
    )?)
}

/// 返回需要统计的协议集合。
fn socket_protocol_flags() -> ProtocolFlags {
    ProtocolFlags::TCP | ProtocolFlags::UDP
}

/// 返回需要统计的地址族集合。
#[cfg(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
/// 同时统计 IPv4 和 IPv6，避免只看单协议族导致连接数偏低。
fn socket_address_family_flags() -> AddressFamilyFlags {
    AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6
}

/// 收集 socket 迭代器。
fn collect_socket_iter(
    sockets: impl Iterator<Item = Result<netstat2::SocketInfo, netstat2::error::Error>>,
) -> anyhow::Result<Vec<netstat2::SocketInfo>> {
    sockets.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// 统计 socket 列表中的 TCP/UDP 数量。
fn count_socket_entries(sockets: &[netstat2::SocketInfo]) -> (u64, u64) {
    let mut tcp = 0u64;
    let mut udp = 0u64;

    for socket in sockets {
        match &socket.protocol_socket_info {
            ProtocolSocketInfo::Tcp(_) => tcp += 1,
            ProtocolSocketInfo::Udp(_) => udp += 1,
        }
    }

    (tcp, udp)
}

/// 统计 TCP 状态分布。
fn tcp_state_counts(sockets: &[netstat2::SocketInfo]) -> Vec<TcpStateCount> {
    let mut counts = BTreeMap::<String, u64>::new();

    for socket in sockets {
        if let ProtocolSocketInfo::Tcp(tcp) = &socket.protocol_socket_info {
            *counts.entry(tcp_state_name(tcp.state)).or_default() += 1;
        }
    }

    counts
        .into_iter()
        .map(|(state, count)| TcpStateCount { state, count })
        .collect()
}

/// 转换为对 JSON 友好的 TCP 状态名。
fn tcp_state_name(state: netstat2::TcpState) -> String {
    state
        .to_string()
        .trim_matches('_')
        .to_ascii_lowercase()
        .replace('-', "_")
}

/// 转换 socket table 明细。
fn socket_detail(socket: &netstat2::SocketInfo) -> SocketDetail {
    match &socket.protocol_socket_info {
        ProtocolSocketInfo::Tcp(tcp) => SocketDetail {
            protocol: SocketProtocol::Tcp,
            local_addr: tcp.local_addr,
            local_port: tcp.local_port,
            remote_addr: Some(tcp.remote_addr),
            remote_port: Some(tcp.remote_port),
            state: Some(tcp_state_name(tcp.state)),
            pids: socket.associated_pids.clone(),
        },
        ProtocolSocketInfo::Udp(udp) => SocketDetail {
            protocol: SocketProtocol::Udp,
            local_addr: udp.local_addr,
            local_port: udp.local_port,
            remote_addr: None,
            remote_port: None,
            state: None,
            pids: socket.associated_pids.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    //! Socket 汇总采集测试。

    use super::*;
    use smalux_core::model::info::{MetricStatus, SocketAccuracy, SocketSource};

    /// 验证失败时没有旧值会返回 failed。
    #[test]
    fn socket_info_stale_or_failed_without_previous_returns_failed() {
        let info = SocketInfo::stale_or_failed(None, "temporary failure".to_string());

        assert_eq!(info.status, MetricStatus::Failed);
        assert_eq!(info.tcp, 0);
        assert_eq!(info.udp, 0);
        assert_eq!(info.error.as_deref(), Some("temporary failure"));
    }

    /// 验证失败时会保留旧 socket 数量。
    #[test]
    fn socket_info_stale_or_failed_keeps_previous_count() {
        let previous = SocketInfo::ready(
            10,
            3,
            SocketSource::SocketTable,
            SocketAccuracy::SocketTable,
        );

        let info = SocketInfo::stale_or_failed(Some(&previous), "temporary failure".to_string());

        assert_eq!(info.status, MetricStatus::Stale);
        assert_eq!(info.tcp, 10);
        assert_eq!(info.udp, 3);
        assert_eq!(info.error.as_deref(), Some("temporary failure"));
    }

    /// 验证 count 级别只返回汇总。
    #[test]
    fn sample_socket_count_returns_summary_only() {
        let info = sample_socket_info(None, MetricLevel::Count, 10);

        assert_eq!(info.level, MetricLevel::Count);
        assert!(info.light.is_none());
        assert!(info.details.is_none());
    }

    /// 验证 light 级别返回 TCP 状态聚合。
    #[test]
    fn sample_socket_light_returns_state_counts() {
        let info = sample_socket_info(None, MetricLevel::Light, 10);

        if info.status == MetricStatus::Ready {
            assert_eq!(info.level, MetricLevel::Light);
            let json = serde_json::to_value(&info).unwrap();
            assert!(json.get("tcp").is_some());
            assert!(json.get("udp").is_some());
            assert!(info.light.is_some());
            assert!(info.details.is_none());
        } else {
            assert!(info.error.is_some());
        }
    }

    /// 验证 details 级别返回受限明细。
    #[test]
    fn sample_socket_details_returns_limited_items() {
        let info = sample_socket_info(None, MetricLevel::Details, 10);

        if info.status == MetricStatus::Ready {
            assert_eq!(info.level, MetricLevel::Details);
            let json = serde_json::to_value(&info).unwrap();
            assert!(json.get("tcp").is_some());
            assert!(json.get("udp").is_some());
            let details = info.details.unwrap();
            assert!(details.items.len() <= 10);
        } else {
            assert!(info.error.is_some());
        }
    }

    /// 验证 TCP 状态名会转换为稳定 snake_case。
    #[test]
    fn tcp_state_name_is_snake_case() {
        assert_eq!(tcp_state_name(netstat2::TcpState::SynReceived), "syn_rcvd");
        assert_eq!(tcp_state_name(netstat2::TcpState::Unknown), "unknown");
    }
}
