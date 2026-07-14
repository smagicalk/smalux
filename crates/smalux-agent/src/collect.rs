//! Agent 本机指标采集入口。
//!
//! `LocalCollector` 长期持有 `sysinfo` 的刷新状态。CPU 和 IO 都依赖前后两次
//! 采样，因此不要为每次采样重新创建采集器。

use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{Disks, Networks, System};

pub mod cpu;
pub mod host;
pub mod io;
pub mod ip;
pub mod load;
pub mod memory;

/// 一次完整的本机指标快照。
#[derive(Debug, Clone, Serialize)]
pub struct SystemSnapshot {
    /// Unix 时间戳，单位毫秒。
    pub sampled_at_ms: u64,
    /// 与上次采样的间隔；首次采样为 `None`。
    pub sample_interval_ms: Option<u64>,
    /// 主机和操作系统基础信息。
    pub host: host::HostSnapshot,
    /// CPU 汇总与各逻辑核心明细。
    pub cpu: cpu::CpuSnapshot,
    /// 物理内存与交换空间使用情况。
    pub memory: memory::MemorySnapshot,
    /// 1、5、15 分钟系统负载。
    pub load: load::LoadSnapshot,
    /// 磁盘容量与读写 IO。
    pub disk_io: io::DiskIoSnapshot,
    /// 网卡流量与网络 IO。
    pub network_io: io::NetworkIoSnapshot,
    /// 本地网卡地址与可选公网地址状态。
    pub ip: ip::IpSnapshot,
}

/// 本机指标采集器。
pub struct LocalCollector {
    /// CPU 和内存刷新状态；CPU 使用率依赖前后两次刷新。
    system: System,
    /// 磁盘列表和 IO 增量基线。
    disks: Disks,
    /// 网络流量采样状态；不能被本地地址刷新干扰。
    networks: Networks,
    /// 本地地址发现状态，与网络流量采样状态相互独立。
    identity_networks: Option<Networks>,
    /// 完整快照上一次采样的单调时间。
    last_full_sampled_at: Option<Instant>,
    /// CPU 上一次独立或完整采样的单调时间。
    last_cpu_sampled_at: Option<Instant>,
    /// 磁盘 IO 上一次独立或完整采样的单调时间。
    last_disk_sampled_at: Option<Instant>,
    /// 网络 IO 上一次独立或完整采样的单调时间。
    last_network_sampled_at: Option<Instant>,
}

impl LocalCollector {
    /// 创建采集器并初始化系统、磁盘和网卡列表。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// let mut collector = LocalCollector::new();
    /// let first = collector.collect();
    /// tokio::time::sleep(Duration::from_secs(1)).await;
    /// let second = collector.collect();
    /// assert!(second.sample_interval_ms.is_some());
    /// ```
    pub fn new() -> Self {
        Self {
            system: System::new(),
            disks: Disks::new_with_refreshed_list(),
            networks: Networks::new_with_refreshed_list(),
            identity_networks: None,
            last_full_sampled_at: None,
            last_cpu_sampled_at: None,
            last_disk_sampled_at: None,
            last_network_sampled_at: None,
        }
    }

    /// 刷新并返回一次完整快照，同时更新各独立采集器共用的采样基线。
    pub fn collect(&mut self) -> SystemSnapshot {
        let now = Instant::now();
        let elapsed = elapsed_since(&mut self.last_full_sampled_at, now);

        self.system.refresh_cpu_all();
        self.system.refresh_memory();
        self.disks.refresh(true);
        self.networks.refresh(true);

        let cpu_elapsed = elapsed_since(&mut self.last_cpu_sampled_at, now);
        let disk_elapsed = elapsed_since(&mut self.last_disk_sampled_at, now);
        let network_elapsed = elapsed_since(&mut self.last_network_sampled_at, now);
        let cpu_warmed_up = cpu_elapsed
            .map(|duration| duration >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL)
            .unwrap_or(false);

        SystemSnapshot {
            sampled_at_ms: unix_timestamp_ms(),
            sample_interval_ms: elapsed.map(duration_ms),
            host: host::collect(),
            cpu: cpu::collect(&self.system, cpu_warmed_up),
            memory: memory::collect(&self.system),
            load: load::collect(),
            disk_io: io::collect_disks(&self.disks, disk_elapsed),
            network_io: io::collect_networks(&self.networks, network_elapsed),
            ip: ip::collect(&self.networks),
        }
    }

    /// 仅刷新并采集 CPU 指标；首次或间隔过短时 warmed_up 为 false。
    pub fn collect_cpu(&mut self) -> cpu::CpuSnapshot {
        let now = Instant::now();
        self.system.refresh_cpu_all();
        let elapsed = elapsed_since(&mut self.last_cpu_sampled_at, now);
        let warmed_up = elapsed
            .map(|duration| duration >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL)
            .unwrap_or(false);
        cpu::collect(&self.system, warmed_up)
    }

    /// 仅刷新并采集内存与交换空间指标，不依赖前一次采样。
    pub fn collect_memory(&mut self) -> memory::MemorySnapshot {
        self.system.refresh_memory();
        memory::collect(&self.system)
    }

    /// 采集系统平均负载；该指标不需要维护刷新状态，Windows 会标记 unsupported。
    pub fn collect_load(&self) -> load::LoadSnapshot {
        load::collect()
    }

    /// 采集变化频率较低的主机与操作系统信息，缺失字符串使用 unknown。
    pub fn collect_host(&self) -> host::HostSnapshot {
        host::collect()
    }

    /// 仅刷新并采集磁盘容量与 IO 指标，并按真实间隔计算速度。
    pub fn collect_disk_io(&mut self) -> io::DiskIoSnapshot {
        let now = Instant::now();
        self.disks.refresh(true);
        let elapsed = elapsed_since(&mut self.last_disk_sampled_at, now);
        io::collect_disks(&self.disks, elapsed)
    }

    /// 仅刷新并采集网卡流量与 IO 指标，并按真实间隔计算速度。
    pub fn collect_network_io(&mut self) -> io::NetworkIoSnapshot {
        let now = Instant::now();
        self.networks.refresh(true);
        let elapsed = elapsed_since(&mut self.last_network_sampled_at, now);
        io::collect_networks(&self.networks, elapsed)
    }

    /// 仅刷新并采集本地网卡地址，不影响网络流量的增量统计基线。
    pub fn collect_local_ip(&mut self) -> ip::IpSnapshot {
        let networks = self
            .identity_networks
            .get_or_insert_with(Networks::new_with_refreshed_list);
        networks.refresh(true);
        ip::collect(networks)
    }

    /// 使用内置端点并发采集公网 IPv4 和 IPv6，失败通过状态返回。
    pub async fn collect_public_ip(timeout: Duration) -> ip::PublicIpSnapshot {
        ip::collect_public(timeout).await
    }

    /// 采集本机指标，并通过内置端点低频查询公网 IPv4 和 IPv6。
    ///
    /// 公网 IP 查询失败只记录在快照状态中，不会丢弃其他本机指标。
    pub async fn collect_with_public_ip(&mut self, timeout: Duration) -> SystemSnapshot {
        let mut snapshot = self.collect();
        let public_ip = Self::collect_public_ip(timeout).await;
        snapshot.ip.public_ipv4 = public_ip.ipv4;
        snapshot.ip.public_ipv6 = public_ip.ipv6;
        snapshot
    }
}

impl Default for LocalCollector {
    /// 等价于 LocalCollector::new。
    fn default() -> Self {
        Self::new()
    }
}

/// 获取当前 Unix 毫秒时间戳；系统时间早于纪元时返回零。
fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// 把 Duration 转为 u64 毫秒，溢出时饱和。
fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

/// 更新采样基线并返回与前一次单调时间的间隔。
fn elapsed_since(last_sampled_at: &mut Option<Instant>, now: Instant) -> Option<Duration> {
    last_sampled_at
        .replace(now)
        .map(|previous| now.duration_since(previous))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_snapshot_contains_core_metrics() {
        let mut collector = LocalCollector::new();
        let snapshot = collector.collect();

        assert!(snapshot.sampled_at_ms > 0);
        assert_eq!(snapshot.cpu.logical_cpu_count, snapshot.cpu.cpus.len());
        assert!(snapshot.memory.total_bytes >= snapshot.memory.used_bytes);
        assert!(snapshot.load.one.is_finite());
    }

    #[test]
    fn second_snapshot_contains_sample_interval() {
        let mut collector = LocalCollector::new();
        let first = collector.collect();
        let second = collector.collect();

        assert!(first.sample_interval_ms.is_none());
        assert!(second.sample_interval_ms.is_some());
        assert!(second.disk_io.warmed_up);
        assert!(second.network_io.warmed_up);
    }

    #[test]
    fn individual_collectors_own_their_refresh_and_sampling_state() {
        let mut collector = LocalCollector::new();

        let first_disk = collector.collect_disk_io();
        let first_network = collector.collect_network_io();
        let local_ip = collector.collect_local_ip();
        let cpu = collector.collect_cpu();
        let memory = collector.collect_memory();
        let load = collector.collect_load();
        let host = collector.collect_host();

        assert!(!first_disk.warmed_up);
        assert!(!first_network.warmed_up);
        assert!(matches!(
            local_ip.public_ipv4,
            ip::PublicIpState::NotRequested
        ));
        assert!(matches!(
            local_ip.public_ipv6,
            ip::PublicIpState::NotRequested
        ));
        assert_eq!(cpu.logical_cpu_count, cpu.cpus.len());
        assert!(memory.total_bytes >= memory.used_bytes);
        assert!(load.one.is_finite());
        assert!(!host.hostname.is_empty());

        assert!(collector.collect_disk_io().warmed_up);
        assert!(collector.collect_network_io().warmed_up);
    }

    #[test]
    fn full_collection_updates_each_individual_sampling_baseline() {
        let mut collector = LocalCollector::new();

        let snapshot = collector.collect();
        let disk = collector.collect_disk_io();
        let network = collector.collect_network_io();

        assert!(!snapshot.disk_io.warmed_up);
        assert!(!snapshot.network_io.warmed_up);
        assert!(disk.warmed_up);
        assert!(network.warmed_up);
    }
}
