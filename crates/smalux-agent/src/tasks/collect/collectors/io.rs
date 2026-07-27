//! 磁盘与网络 IO 指标采集。
//!
//! `read_bytes`、`written_bytes`、`received_bytes` 和 `transmitted_bytes`
//! 表示当前刷新周期增量；`total_*` 表示系统累计值。

pub use smalux_protocol::agent::v1::{
    DiskDeviceSnapshot, DiskIoSnapshot, NetworkInterfaceSnapshot, NetworkIoSnapshot,
};
use std::time::{Duration, Instant};
use sysinfo::{Disks, Networks};

use super::{elapsed_since, ip};

/// 磁盘列表及其增量速率采样基线。
pub(crate) struct DiskIoCollector {
    disks: Disks,
    last_sampled_at: Option<Instant>,
}

impl DiskIoCollector {
    pub(crate) fn new() -> Self {
        Self {
            disks: Disks::new_with_refreshed_list(),
            last_sampled_at: None,
        }
    }

    pub(crate) fn collect(&mut self) -> DiskIoSnapshot {
        self.collect_at(Instant::now())
    }

    pub(crate) fn collect_at(&mut self, now: Instant) -> DiskIoSnapshot {
        self.disks.refresh(true);
        let elapsed = elapsed_since(&mut self.last_sampled_at, now);
        collect_disks(&self.disks, elapsed)
    }
}

impl Default for DiskIoCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// 网络接口流量状态及其增量速率采样基线。
pub(crate) struct NetworkIoCollector {
    networks: Networks,
    last_sampled_at: Option<Instant>,
}

impl NetworkIoCollector {
    pub(crate) fn new() -> Self {
        Self {
            networks: Networks::new_with_refreshed_list(),
            last_sampled_at: None,
        }
    }

    pub(crate) fn collect(&mut self) -> NetworkIoSnapshot {
        self.collect_at(Instant::now())
    }

    pub(crate) fn collect_at(&mut self, now: Instant) -> NetworkIoSnapshot {
        self.networks.refresh(true);
        let elapsed = elapsed_since(&mut self.last_sampled_at, now);
        collect_networks(&self.networks, elapsed)
    }

    pub(crate) fn collect_with_local_ip_at(
        &mut self,
        now: Instant,
    ) -> (NetworkIoSnapshot, ip::IpSnapshot) {
        self.networks.refresh(true);
        let elapsed = elapsed_since(&mut self.last_sampled_at, now);
        (
            collect_networks(&self.networks, elapsed),
            ip::collect(&self.networks),
        )
    }
}

impl Default for NetworkIoCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// 从已刷新的磁盘对象中提取 IO 指标。
pub(super) fn collect_disks(disks: &Disks, elapsed: Option<Duration>) -> DiskIoSnapshot {
    let valid_elapsed = elapsed.filter(|duration| !duration.is_zero());
    let devices = disks
        .list()
        .iter()
        .map(|disk| {
            let usage = disk.usage();
            DiskDeviceSnapshot {
                name: disk.name().to_string_lossy().into_owned(),
                mount_point: disk.mount_point().to_string_lossy().into_owned(),
                file_system: disk.file_system().to_string_lossy().into_owned(),
                kind: disk.kind().to_string(),
                total_space_bytes: disk.total_space(),
                available_space_bytes: disk.available_space(),
                read_bytes: usage.read_bytes,
                written_bytes: usage.written_bytes,
                total_read_bytes: usage.total_read_bytes,
                total_written_bytes: usage.total_written_bytes,
                read_bytes_per_second: rate(usage.read_bytes, valid_elapsed),
                written_bytes_per_second: rate(usage.written_bytes, valid_elapsed),
            }
        })
        .collect::<Vec<_>>();

    let read_bytes = devices.iter().map(|disk| disk.read_bytes).sum();
    let written_bytes = devices.iter().map(|disk| disk.written_bytes).sum();

    DiskIoSnapshot {
        warmed_up: valid_elapsed.is_some(),
        devices,
        read_bytes,
        written_bytes,
        read_bytes_per_second: rate(read_bytes, valid_elapsed),
        written_bytes_per_second: rate(written_bytes, valid_elapsed),
    }
}

/// 从已刷新的网卡对象中提取 IO 指标。
pub(super) fn collect_networks(
    networks: &Networks,
    elapsed: Option<Duration>,
) -> NetworkIoSnapshot {
    let valid_elapsed = elapsed.filter(|duration| !duration.is_zero());
    let interfaces = networks
        .list()
        .iter()
        .map(|(interface, network)| NetworkInterfaceSnapshot {
            interface: interface.clone(),
            mac_address: network.mac_address().to_string(),
            mtu: network.mtu(),
            received_bytes: network.received(),
            transmitted_bytes: network.transmitted(),
            total_received_bytes: network.total_received(),
            total_transmitted_bytes: network.total_transmitted(),
            received_bytes_per_second: rate(network.received(), valid_elapsed),
            transmitted_bytes_per_second: rate(network.transmitted(), valid_elapsed),
            received_packets: network.packets_received(),
            transmitted_packets: network.packets_transmitted(),
            receive_errors: network.errors_on_received(),
            transmit_errors: network.errors_on_transmitted(),
        })
        .collect::<Vec<_>>();

    let received_bytes = interfaces
        .iter()
        .map(|network| network.received_bytes)
        .sum();
    let transmitted_bytes = interfaces
        .iter()
        .map(|network| network.transmitted_bytes)
        .sum();

    NetworkIoSnapshot {
        warmed_up: valid_elapsed.is_some(),
        interfaces,
        received_bytes,
        transmitted_bytes,
        received_bytes_per_second: rate(received_bytes, valid_elapsed),
        transmitted_bytes_per_second: rate(transmitted_bytes, valid_elapsed),
    }
}

/// 使用实际采样间隔计算每秒字节数；缺少基线或间隔为零时返回 None。
fn rate(bytes: u64, elapsed: Option<Duration>) -> Option<f64> {
    let seconds = elapsed?.as_secs_f64();
    (seconds > 0.0).then_some(bytes as f64 / seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_io_collector_owns_its_sampling_baseline() {
        let mut collector = DiskIoCollector::new();

        assert!(!collector.collect().warmed_up);
        assert!(collector.collect().warmed_up);
    }

    #[test]
    fn network_io_collector_owns_its_sampling_baseline() {
        let mut collector = NetworkIoCollector::new();

        assert!(!collector.collect().warmed_up);
        assert!(collector.collect().warmed_up);
    }

    #[test]
    fn rate_uses_real_elapsed_time() {
        assert_eq!(rate(1_024, Some(Duration::from_secs(2))), Some(512.0));
        assert_eq!(rate(1_024, None), None);
        assert_eq!(rate(1_024, Some(Duration::ZERO)), None);
    }

    #[test]
    fn io_snapshots_aggregate_device_deltas() {
        let mut disks = Disks::new_with_refreshed_list();
        let mut networks = Networks::new_with_refreshed_list();
        disks.refresh(true);
        networks.refresh(true);

        let disk_snapshot = collect_disks(&disks, Some(Duration::from_secs(1)));
        let network_snapshot = collect_networks(&networks, Some(Duration::from_secs(1)));
        let disk_read_sum: u64 = disk_snapshot
            .devices
            .iter()
            .map(|disk| disk.read_bytes)
            .sum();
        let network_receive_sum: u64 = network_snapshot
            .interfaces
            .iter()
            .map(|network| network.received_bytes)
            .sum();

        assert_eq!(disk_snapshot.read_bytes, disk_read_sum);
        assert_eq!(network_snapshot.received_bytes, network_receive_sum);
        assert!(disk_snapshot.warmed_up);
        assert!(network_snapshot.warmed_up);
    }
}
