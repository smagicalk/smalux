//! 采集条目的精确选择与汇总重算。

use super::collectors::io::{DiskIoSnapshot, NetworkIoSnapshot};
use super::collectors::ip::IpSnapshot;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// 公网地址采集请求的 IP 协议族。
pub enum IpFamilySelection {
    /// 同时请求 IPv4 与 IPv6。
    #[default]
    Both,
    /// 只请求 IPv4。
    V4,
    /// 只请求 IPv6。
    V6,
}

impl IpFamilySelection {
    pub(crate) const fn requested(self) -> (bool, bool) {
        match self {
            Self::Both => (true, true),
            Self::V4 => (true, false),
            Self::V6 => (false, true),
        }
    }
}

/// 按设备名称或挂载点选择磁盘；任一 include 列表非空时忽略全部 exclude。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiskSelection {
    /// 允许的设备名称；任一 include 非空时启用白名单模式。
    pub include_names: Vec<String>,
    /// 允许的挂载点；与 `include_names` 使用 OR 关系。
    pub include_mount_points: Vec<String>,
    /// 白名单模式未启用时排除的设备名称。
    pub exclude_names: Vec<String>,
    /// 白名单模式未启用时排除的挂载点。
    pub exclude_mount_points: Vec<String>,
}

impl DiskSelection {
    /// 判断给定设备名称和挂载点是否满足当前筛选规则。
    pub fn matches(&self, name: &str, mount_point: &str) -> bool {
        let has_include = !self.include_names.is_empty() || !self.include_mount_points.is_empty();
        if has_include {
            return self.include_names.iter().any(|value| value == name)
                || self
                    .include_mount_points
                    .iter()
                    .any(|value| value == mount_point);
        }
        !self.exclude_names.iter().any(|value| value == name)
            && !self
                .exclude_mount_points
                .iter()
                .any(|value| value == mount_point)
    }
}

/// 按接口完整名称选择或排除网卡；非空 include 优先于 exclude。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InterfaceSelection {
    /// 非空时只允许完全匹配的接口名称，并忽略 `exclude`。
    pub include: Vec<String>,
    /// `include` 为空时排除完全匹配的接口名称。
    pub exclude: Vec<String>,
}

impl InterfaceSelection {
    /// 判断完整接口名称是否满足当前筛选规则。
    pub fn matches(&self, interface: &str) -> bool {
        if !self.include.is_empty() {
            return self.include.iter().any(|name| name == interface);
        }
        !self.exclude.iter().any(|name| name == interface)
    }
}

pub(super) fn filter_network(snapshot: &mut NetworkIoSnapshot, selection: &InterfaceSelection) {
    snapshot
        .interfaces
        .retain(|network| selection.matches(&network.interface));
    snapshot.received_bytes = snapshot
        .interfaces
        .iter()
        .map(|network| network.received_bytes)
        .sum();
    snapshot.transmitted_bytes = snapshot
        .interfaces
        .iter()
        .map(|network| network.transmitted_bytes)
        .sum();
    snapshot.received_bytes_per_second = snapshot.warmed_up.then(|| {
        snapshot
            .interfaces
            .iter()
            .filter_map(|network| network.received_bytes_per_second)
            .sum()
    });
    snapshot.transmitted_bytes_per_second = snapshot.warmed_up.then(|| {
        snapshot
            .interfaces
            .iter()
            .filter_map(|network| network.transmitted_bytes_per_second)
            .sum()
    });
}

pub(super) fn filter_disk(snapshot: &mut DiskIoSnapshot, selection: &DiskSelection) {
    snapshot
        .devices
        .retain(|disk| selection.matches(&disk.name, &disk.mount_point));
    snapshot.read_bytes = snapshot.devices.iter().map(|disk| disk.read_bytes).sum();
    snapshot.written_bytes = snapshot.devices.iter().map(|disk| disk.written_bytes).sum();
    snapshot.read_bytes_per_second = snapshot.warmed_up.then(|| {
        snapshot
            .devices
            .iter()
            .filter_map(|disk| disk.read_bytes_per_second)
            .sum()
    });
    snapshot.written_bytes_per_second = snapshot.warmed_up.then(|| {
        snapshot
            .devices
            .iter()
            .filter_map(|disk| disk.written_bytes_per_second)
            .sum()
    });
}

pub(super) fn filter_local_ip(snapshot: &mut IpSnapshot, selection: &InterfaceSelection) {
    snapshot
        .local
        .retain(|address| selection.matches(&address.interface));
}

#[cfg(test)]
mod tests {
    use crate::tasks::collect::collectors::io::{
        DiskDeviceSnapshot, DiskIoSnapshot, NetworkInterfaceSnapshot, NetworkIoSnapshot,
    };
    use crate::tasks::collect::collectors::ip::{
        InterfaceAddress, IpScope, IpSnapshot, PublicIpState,
    };

    use super::{
        DiskSelection, InterfaceSelection, IpFamilySelection, filter_disk, filter_local_ip,
        filter_network,
    };

    fn disk(name: &str, mount: &str, read: u64, written: u64) -> DiskDeviceSnapshot {
        DiskDeviceSnapshot {
            name: name.to_owned(),
            mount_point: mount.to_owned(),
            file_system: String::new(),
            kind: String::new(),
            total_space_bytes: 0,
            available_space_bytes: 0,
            read_bytes: read,
            written_bytes: written,
            total_read_bytes: read,
            total_written_bytes: written,
            read_bytes_per_second: Some(read as f64),
            written_bytes_per_second: Some(written as f64),
        }
    }

    fn network(interface: &str, received: u64, transmitted: u64) -> NetworkInterfaceSnapshot {
        NetworkInterfaceSnapshot {
            interface: interface.to_owned(),
            mac_address: String::new(),
            mtu: 1_500,
            received_bytes: received,
            transmitted_bytes: transmitted,
            total_received_bytes: received,
            total_transmitted_bytes: transmitted,
            received_bytes_per_second: Some(received as f64),
            transmitted_bytes_per_second: Some(transmitted as f64),
            received_packets: 0,
            transmitted_packets: 0,
            receive_errors: 0,
            transmit_errors: 0,
        }
    }

    #[test]
    fn non_empty_include_takes_priority_over_exclude() {
        let selection = InterfaceSelection {
            include: vec!["eth0".to_owned()],
            exclude: vec!["eth0".to_owned(), "eth1".to_owned()],
        };

        assert!(selection.matches("eth0"));
        assert!(!selection.matches("eth1"));
    }

    #[test]
    fn network_filter_recomputes_aggregates_from_selected_interfaces() {
        let mut snapshot = NetworkIoSnapshot {
            warmed_up: true,
            interfaces: vec![network("eth0", 10, 20), network("eth1", 30, 40)],
            received_bytes: 40,
            transmitted_bytes: 60,
            received_bytes_per_second: Some(40.0),
            transmitted_bytes_per_second: Some(60.0),
        };

        filter_network(
            &mut snapshot,
            &InterfaceSelection {
                include: vec!["eth1".to_owned()],
                exclude: vec!["eth1".to_owned()],
            },
        );

        assert_eq!(snapshot.interfaces.len(), 1);
        assert_eq!(snapshot.interfaces[0].interface, "eth1");
        assert_eq!(snapshot.received_bytes, 30);
        assert_eq!(snapshot.transmitted_bytes, 40);
        assert_eq!(snapshot.received_bytes_per_second, Some(30.0));
        assert_eq!(snapshot.transmitted_bytes_per_second, Some(40.0));
    }

    #[test]
    fn disk_filter_matches_name_or_mount_and_recomputes_aggregates() {
        let mut snapshot = DiskIoSnapshot {
            warmed_up: true,
            devices: vec![disk("sda", "/", 10, 20), disk("sdb", "/data", 30, 40)],
            read_bytes: 40,
            written_bytes: 60,
            read_bytes_per_second: Some(40.0),
            written_bytes_per_second: Some(60.0),
        };

        filter_disk(
            &mut snapshot,
            &DiskSelection {
                include_mount_points: vec!["/data".to_owned()],
                exclude_names: vec!["sdb".to_owned()],
                ..DiskSelection::default()
            },
        );

        assert_eq!(snapshot.devices.len(), 1);
        assert_eq!(snapshot.devices[0].name, "sdb");
        assert_eq!(snapshot.read_bytes, 30);
        assert_eq!(snapshot.written_bytes, 40);
        assert_eq!(snapshot.read_bytes_per_second, Some(30.0));
        assert_eq!(snapshot.written_bytes_per_second, Some(40.0));
    }

    #[test]
    fn local_ip_filter_uses_interface_selection() {
        let mut snapshot = IpSnapshot {
            local: vec![
                InterfaceAddress {
                    interface: "eth0".to_owned(),
                    address: "192.0.2.1".parse().unwrap(),
                    prefix_length: 24,
                    scope: IpScope::Global,
                },
                InterfaceAddress {
                    interface: "eth1".to_owned(),
                    address: "198.51.100.1".parse().unwrap(),
                    prefix_length: 24,
                    scope: IpScope::Global,
                },
            ],
            public_ipv4: PublicIpState::NotRequested,
            public_ipv6: PublicIpState::NotRequested,
        };

        filter_local_ip(
            &mut snapshot,
            &InterfaceSelection {
                include: Vec::new(),
                exclude: vec!["eth0".to_owned()],
            },
        );

        assert_eq!(snapshot.local.len(), 1);
        assert_eq!(snapshot.local[0].interface, "eth1");
    }

    #[test]
    fn public_ip_family_selection_exposes_requested_families() {
        assert_eq!(IpFamilySelection::default(), IpFamilySelection::Both);
        assert_eq!(IpFamilySelection::Both.requested(), (true, true));
        assert_eq!(IpFamilySelection::V4.requested(), (true, false));
        assert_eq!(IpFamilySelection::V6.requested(), (false, true));
    }
}
