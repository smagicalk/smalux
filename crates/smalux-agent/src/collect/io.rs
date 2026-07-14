//! 磁盘与网络 IO 指标采集。
//!
//! `read_bytes`、`written_bytes`、`received_bytes` 和 `transmitted_bytes`
//! 表示当前刷新周期增量；`total_*` 表示系统累计值。

use serde::Serialize;
use std::time::Duration;
use sysinfo::{Disks, Networks};

/// 单块磁盘的容量与 IO 指标。
#[derive(Debug, Clone, Serialize)]
pub struct DiskDeviceSnapshot {
    /// 系统报告的设备名称。
    pub name: String,
    /// 文件系统挂载点。
    pub mount_point: String,
    /// 文件系统类型，例如 NTFS、ext4。
    pub file_system: String,
    /// 磁盘介质类型文本。
    pub kind: String,
    /// 文件系统总容量。
    pub total_space_bytes: u64,
    /// 当前可用容量。
    pub available_space_bytes: u64,
    /// 本次刷新周期新增读取字节数。
    pub read_bytes: u64,
    /// 本次刷新周期新增写入字节数。
    pub written_bytes: u64,
    /// 系统启动或计数器建立以来累计读取字节数。
    pub total_read_bytes: u64,
    /// 系统启动或计数器建立以来累计写入字节数。
    pub total_written_bytes: u64,
    /// 按实际采样间隔计算的读取速度；首次采样为 None。
    pub read_bytes_per_second: Option<f64>,
    /// 按实际采样间隔计算的写入速度；首次采样为 None。
    pub written_bytes_per_second: Option<f64>,
}

/// 磁盘 IO 汇总快照。
#[derive(Debug, Clone, Serialize)]
pub struct DiskIoSnapshot {
    /// 是否具有可用于计算速度的前一次采样。
    pub warmed_up: bool,
    /// 每块磁盘的容量和 IO 指标。
    pub devices: Vec<DiskDeviceSnapshot>,
    /// 所有磁盘本次刷新周期读取增量之和。
    pub read_bytes: u64,
    /// 所有磁盘本次刷新周期写入增量之和。
    pub written_bytes: u64,
    /// 汇总读取速度；首次采样为 None。
    pub read_bytes_per_second: Option<f64>,
    /// 汇总写入速度；首次采样为 None。
    pub written_bytes_per_second: Option<f64>,
}

/// 单个网卡的 IO 指标。
#[derive(Debug, Clone, Serialize)]
pub struct NetworkInterfaceSnapshot {
    /// 网卡接口名称。
    pub interface: String,
    /// 网卡 MAC 地址文本。
    pub mac_address: String,
    /// 最大传输单元，单位字节。
    pub mtu: u64,
    /// 本次刷新周期接收字节增量。
    pub received_bytes: u64,
    /// 本次刷新周期发送字节增量。
    pub transmitted_bytes: u64,
    /// 系统累计接收字节数。
    pub total_received_bytes: u64,
    /// 系统累计发送字节数。
    pub total_transmitted_bytes: u64,
    /// 按实际采样间隔计算的接收速度；首次采样为 None。
    pub received_bytes_per_second: Option<f64>,
    /// 按实际采样间隔计算的发送速度；首次采样为 None。
    pub transmitted_bytes_per_second: Option<f64>,
    /// 本次刷新周期接收数据包数量。
    pub received_packets: u64,
    /// 本次刷新周期发送数据包数量。
    pub transmitted_packets: u64,
    /// 本次刷新周期接收错误数量。
    pub receive_errors: u64,
    /// 本次刷新周期发送错误数量。
    pub transmit_errors: u64,
}

/// 网络 IO 汇总快照。
#[derive(Debug, Clone, Serialize)]
pub struct NetworkIoSnapshot {
    /// 是否具有可用于计算速度的前一次采样。
    pub warmed_up: bool,
    /// 每个网卡接口的 IO 指标。
    pub interfaces: Vec<NetworkInterfaceSnapshot>,
    /// 所有网卡本次刷新周期接收字节增量之和。
    pub received_bytes: u64,
    /// 所有网卡本次刷新周期发送字节增量之和。
    pub transmitted_bytes: u64,
    /// 汇总接收速度；首次采样为 None。
    pub received_bytes_per_second: Option<f64>,
    /// 汇总发送速度；首次采样为 None。
    pub transmitted_bytes_per_second: Option<f64>,
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
