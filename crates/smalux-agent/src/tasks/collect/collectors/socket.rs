//! 本机 TCP/UDP socket 汇总与分级明细采集。

use std::{collections::BTreeMap, net::IpAddr, num::NonZeroUsize};

use netstat2::{AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo, SocketInfo, TcpState};
use serde::Serialize;

use crate::tasks::collect::CollectionMode;

/// Basic 模式未配置上限时最多返回的 socket 数量。
pub const DEFAULT_BASIC_SOCKET_ENTRIES: usize = 256;
/// Detailed 模式未配置上限时最多返回的 socket 数量。
pub const DEFAULT_DETAILED_SOCKET_ENTRIES: usize = 128;

/// 本次采集包含的传输层协议。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SocketProtocolSelection {
    /// 同时统计 TCP 和 UDP。
    #[default]
    Both,
    /// 只统计 TCP。
    Tcp,
    /// 只统计 UDP。
    Udp,
}

/// 本次采集包含的 IP 地址族。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SocketAddressFamilySelection {
    /// 同时统计 IPv4 和 IPv6。
    #[default]
    Both,
    /// 只统计 IPv4。
    Ipv4,
    /// 只统计 IPv6。
    Ipv6,
}

/// Snapshot 中稳定的传输层协议值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketProtocol {
    /// TCP socket。
    Tcp,
    /// UDP socket。
    Udp,
}

/// Snapshot 中稳定的 IP 地址族值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketAddressFamily {
    /// IPv4 地址。
    Ipv4,
    /// IPv6 地址。
    Ipv6,
}

/// 跨平台稳定的 TCP 连接状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TcpConnectionState {
    /// 已关闭。
    Closed,
    /// 正在监听。
    Listen,
    /// 已发送 SYN。
    SynSent,
    /// 已收到 SYN。
    SynReceived,
    /// 已建立连接。
    Established,
    /// 第一次 FIN 等待。
    FinWait1,
    /// 第二次 FIN 等待。
    FinWait2,
    /// 等待本地关闭。
    CloseWait,
    /// 双方同时关闭。
    Closing,
    /// 等待最后 ACK。
    LastAck,
    /// 等待旧报文过期。
    TimeWait,
    /// Windows 正在删除 TCB。
    DeleteTcb,
    /// 平台返回了无法识别的状态。
    Unknown,
}

/// 一个 TCP 状态及其 socket 数量。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TcpStateCount {
    /// 稳定状态值。
    pub state: TcpConnectionState,
    /// 该状态下的 TCP socket 数量。
    pub count: usize,
}

/// 一条 socket 明细；Summary 模式不会生成该结构。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SocketEntry {
    /// TCP 或 UDP。
    pub protocol: SocketProtocol,
    /// IPv4 或 IPv6。
    pub address_family: SocketAddressFamily,
    /// 本地绑定地址。
    pub local_address: IpAddr,
    /// 本地端口。
    pub local_port: u16,
    /// TCP 远端地址；UDP 为 `None`。
    pub remote_address: Option<IpAddr>,
    /// TCP 远端端口；UDP 为 `None`。
    pub remote_port: Option<u16>,
    /// TCP 状态；UDP 为 `None`。
    pub tcp_state: Option<TcpConnectionState>,
    /// Detailed 模式为系统返回的关联 PID；Basic 模式为 `None`。
    pub associated_pids: Option<Vec<u32>>,
}

/// Socket 采集可用性；组合采集用它区分零连接和采集失败。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SocketCollectionStatus {
    /// 系统查询成功。
    Available,
    /// 系统查询失败，其他组合指标仍可继续使用。
    Unavailable {
        /// 不包含敏感信息的诊断文本。
        message: String,
    },
}

/// 一次本机 socket 采集结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SocketSnapshot {
    /// 实际使用的采集档位。
    pub mode: CollectionMode,
    /// 本次系统查询是否成功。
    pub status: SocketCollectionStatus,
    /// 选中范围内的 TCP socket 总数，包含监听 socket。
    pub tcp_total: usize,
    /// 选中范围内的 UDP socket 总数。
    pub udp_total: usize,
    /// 按状态拆分的 TCP socket 数量，只保留非零状态。
    pub tcp_states: Vec<TcpStateCount>,
    /// Basic 或 Detailed 模式返回的有限明细。
    pub entries: Vec<SocketEntry>,
    /// 完整匹配数量是否超过返回列表上限。
    pub truncated: bool,
}

impl SocketSnapshot {
    /// 构造组合采集使用的不可用状态，不把失败伪装成成功零值。
    pub fn unavailable(mode: CollectionMode, message: impl Into<String>) -> Self {
        Self {
            mode,
            status: SocketCollectionStatus::Unavailable {
                message: message.into(),
            },
            tcp_total: 0,
            udp_total: 0,
            tcp_states: Vec::new(),
            entries: Vec::new(),
            truncated: false,
        }
    }
}

/// netstat2 查询失败。
#[derive(Debug, thiserror::Error)]
#[error("socket collection failed: {0}")]
pub struct SocketCollectionError(#[from] netstat2::error::Error);

/// 无持久状态的本机 socket 采集器。
pub struct SocketCollector;

impl SocketCollector {
    /// 按档位、协议和地址族采集本机 socket。
    pub fn collect(
        mode: CollectionMode,
        protocols: SocketProtocolSelection,
        families: SocketAddressFamilySelection,
        max_entries: Option<NonZeroUsize>,
    ) -> Result<SocketSnapshot, SocketCollectionError> {
        let address_flags = address_flags(families);
        let protocol_flags = protocol_flags(protocols);
        let limit = resolved_limit(mode, max_entries);

        if mode == CollectionMode::Detailed {
            return collect_iterator(
                netstat2::iterate_sockets_info(address_flags, protocol_flags)?,
                mode,
                limit,
            );
        }

        collect_without_pids(address_flags, protocol_flags, families, mode, limit)
    }
}

/// 在当前平台尽量使用不关联 PID 的低成本查询。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn collect_without_pids(
    address_flags: AddressFamilyFlags,
    protocol_flags: ProtocolFlags,
    _families: SocketAddressFamilySelection,
    mode: CollectionMode,
    limit: usize,
) -> Result<SocketSnapshot, SocketCollectionError> {
    collect_iterator(
        netstat2::iterate_sockets_info_without_pids(address_flags, protocol_flags)?,
        mode,
        limit,
    )
}

/// Windows 的无 PID API 只覆盖 IPv4；请求 IPv6 时使用完整 API保证统计不缺失。
#[cfg(target_os = "windows")]
fn collect_without_pids(
    address_flags: AddressFamilyFlags,
    protocol_flags: ProtocolFlags,
    families: SocketAddressFamilySelection,
    mode: CollectionMode,
    limit: usize,
) -> Result<SocketSnapshot, SocketCollectionError> {
    if families == SocketAddressFamilySelection::Ipv4 {
        collect_iterator(
            netstat2::iterate_sockets_info_without_pids(protocol_flags)?,
            mode,
            limit,
        )
    } else {
        collect_iterator(
            netstat2::iterate_sockets_info(address_flags, protocol_flags)?,
            mode,
            limit,
        )
    }
}

/// 其他平台没有独立无 PID API，使用统一查询并在输出层省略 PID。
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
fn collect_without_pids(
    address_flags: AddressFamilyFlags,
    protocol_flags: ProtocolFlags,
    _families: SocketAddressFamilySelection,
    mode: CollectionMode,
    limit: usize,
) -> Result<SocketSnapshot, SocketCollectionError> {
    collect_iterator(
        netstat2::iterate_sockets_info(address_flags, protocol_flags)?,
        mode,
        limit,
    )
}

fn collect_iterator<I>(
    sockets: I,
    mode: CollectionMode,
    limit: usize,
) -> Result<SocketSnapshot, SocketCollectionError>
where
    I: Iterator<Item = Result<SocketInfo, netstat2::error::Error>>,
{
    let mut tcp_total = 0;
    let mut udp_total = 0;
    let mut tcp_states = BTreeMap::<TcpConnectionState, usize>::new();
    let mut entries = Vec::with_capacity(limit);
    let keep_entries = mode != CollectionMode::Summary;
    let include_pids = mode == CollectionMode::Detailed;

    for socket in sockets {
        let socket = socket?;
        let entry = match socket.protocol_socket_info {
            ProtocolSocketInfo::Tcp(tcp) => {
                tcp_total += 1;
                let state = tcp_state(tcp.state);
                *tcp_states.entry(state).or_default() += 1;
                SocketEntry {
                    protocol: SocketProtocol::Tcp,
                    address_family: address_family(tcp.local_addr),
                    local_address: tcp.local_addr,
                    local_port: tcp.local_port,
                    remote_address: Some(tcp.remote_addr),
                    remote_port: Some(tcp.remote_port),
                    tcp_state: Some(state),
                    associated_pids: include_pids.then_some(socket.associated_pids),
                }
            }
            ProtocolSocketInfo::Udp(udp) => {
                udp_total += 1;
                SocketEntry {
                    protocol: SocketProtocol::Udp,
                    address_family: address_family(udp.local_addr),
                    local_address: udp.local_addr,
                    local_port: udp.local_port,
                    remote_address: None,
                    remote_port: None,
                    tcp_state: None,
                    associated_pids: include_pids.then_some(socket.associated_pids),
                }
            }
        };
        if keep_entries && entries.len() < limit {
            entries.push(entry);
        }
    }

    let matched = tcp_total + udp_total;
    Ok(SocketSnapshot {
        mode,
        status: SocketCollectionStatus::Available,
        tcp_total,
        udp_total,
        tcp_states: tcp_states
            .into_iter()
            .map(|(state, count)| TcpStateCount { state, count })
            .collect(),
        truncated: keep_entries && matched > entries.len(),
        entries,
    })
}

fn resolved_limit(mode: CollectionMode, configured: Option<NonZeroUsize>) -> usize {
    if mode == CollectionMode::Summary {
        return 0;
    }
    configured.map(NonZeroUsize::get).unwrap_or(match mode {
        CollectionMode::Summary => 0,
        CollectionMode::Basic => DEFAULT_BASIC_SOCKET_ENTRIES,
        CollectionMode::Detailed => DEFAULT_DETAILED_SOCKET_ENTRIES,
    })
}

fn address_flags(selection: SocketAddressFamilySelection) -> AddressFamilyFlags {
    match selection {
        SocketAddressFamilySelection::Both => AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6,
        SocketAddressFamilySelection::Ipv4 => AddressFamilyFlags::IPV4,
        SocketAddressFamilySelection::Ipv6 => AddressFamilyFlags::IPV6,
    }
}

fn protocol_flags(selection: SocketProtocolSelection) -> ProtocolFlags {
    match selection {
        SocketProtocolSelection::Both => ProtocolFlags::TCP | ProtocolFlags::UDP,
        SocketProtocolSelection::Tcp => ProtocolFlags::TCP,
        SocketProtocolSelection::Udp => ProtocolFlags::UDP,
    }
}

fn address_family(address: IpAddr) -> SocketAddressFamily {
    match address {
        IpAddr::V4(_) => SocketAddressFamily::Ipv4,
        IpAddr::V6(_) => SocketAddressFamily::Ipv6,
    }
}

fn tcp_state(state: TcpState) -> TcpConnectionState {
    match state {
        TcpState::Closed => TcpConnectionState::Closed,
        TcpState::Listen => TcpConnectionState::Listen,
        TcpState::SynSent => TcpConnectionState::SynSent,
        TcpState::SynReceived => TcpConnectionState::SynReceived,
        TcpState::Established => TcpConnectionState::Established,
        TcpState::FinWait1 => TcpConnectionState::FinWait1,
        TcpState::FinWait2 => TcpConnectionState::FinWait2,
        TcpState::CloseWait => TcpConnectionState::CloseWait,
        TcpState::Closing => TcpConnectionState::Closing,
        TcpState::LastAck => TcpConnectionState::LastAck,
        TcpState::TimeWait => TcpConnectionState::TimeWait,
        TcpState::DeleteTcb => TcpConnectionState::DeleteTcb,
        TcpState::Unknown => TcpConnectionState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, num::NonZeroUsize};

    use super::*;

    #[test]
    fn summary_counts_a_listening_tcp_socket_and_returns_no_entries() {
        let _listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let snapshot = SocketCollector::collect(
            CollectionMode::Summary,
            SocketProtocolSelection::Tcp,
            SocketAddressFamilySelection::Ipv4,
            None,
        )
        .unwrap();

        assert!(snapshot.tcp_total >= 1);
        assert_eq!(snapshot.udp_total, 0);
        assert!(
            snapshot
                .tcp_states
                .iter()
                .any(|item| item.state == TcpConnectionState::Listen && item.count >= 1)
        );
        assert!(snapshot.entries.is_empty());
        assert!(!snapshot.truncated);
    }

    #[test]
    fn basic_caps_entries_without_losing_complete_counts() {
        let _first = TcpListener::bind("127.0.0.1:0").unwrap();
        let _second = TcpListener::bind("127.0.0.1:0").unwrap();

        let snapshot = SocketCollector::collect(
            CollectionMode::Basic,
            SocketProtocolSelection::Tcp,
            SocketAddressFamilySelection::Ipv4,
            NonZeroUsize::new(1),
        )
        .unwrap();

        assert!(snapshot.tcp_total >= 2);
        assert_eq!(snapshot.entries.len(), 1);
        assert!(snapshot.entries[0].associated_pids.is_none());
        assert!(snapshot.truncated);
    }

    #[test]
    fn detailed_can_associate_a_listening_socket_with_the_current_process() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let snapshot = SocketCollector::collect(
            CollectionMode::Detailed,
            SocketProtocolSelection::Tcp,
            SocketAddressFamilySelection::Ipv4,
            NonZeroUsize::new(16_384),
        )
        .unwrap();

        let entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.local_port == port)
            .expect("the listening socket should be present");
        assert!(
            entry
                .associated_pids
                .as_ref()
                .is_some_and(|pids| pids.contains(&std::process::id()))
        );
    }

    #[test]
    fn unavailable_snapshot_distinguishes_query_failure_from_zero_sockets() {
        let snapshot = SocketSnapshot::unavailable(
            CollectionMode::Summary,
            "operating system socket query failed",
        );

        assert!(matches!(
            snapshot.status,
            SocketCollectionStatus::Unavailable { ref message }
                if message == "operating system socket query failed"
        ));
        assert_eq!(snapshot.tcp_total, 0);
        assert_eq!(snapshot.udp_total, 0);
        assert!(snapshot.entries.is_empty());
    }
}
