//! Agent 本机信息采集入口。
//!
//! 这里统一管理 `sysinfo` 的刷新对象，子模块只负责把原始数据映射成领域模型。

use crate::config::PublicIpConfig;
use smalux_core::model::info::{
    CoreInfo, CpuInfo, DiskInfo, IdentityInfo, LoadAverageInfo, MemoryInfo, MetricLevel,
    NetworkInfo, ProcessInfo, PublicIpInfo, PublicIpSource, SocketInfo, SystemInfo,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{Disks, Networks, System};

/// CPU 信息采集。
pub(crate) mod cpu;
/// 磁盘信息采集。
pub(crate) mod disk;
/// 内存信息采集。
pub(crate) mod memory;
/// 网络信息采集。
pub(crate) mod network;
/// 进程汇总采集。
pub(crate) mod process;
/// Socket 汇总采集。
pub(crate) mod socket;

/// 带采样时间的核心指标。
#[derive(Debug, Default, Clone)]
pub(crate) struct CoreSample {
    /// 采样时间，Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// 核心指标。
    pub value: CoreInfo,
}

/// 带采样时间的磁盘指标。
#[derive(Debug, Default, Clone)]
pub(crate) struct DiskSample {
    /// 采样时间，Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// 磁盘指标。
    pub value: DiskInfo,
}

/// 带采样时间的网络指标。
#[derive(Debug, Default, Clone)]
pub(crate) struct NetworkSample {
    /// 采样时间，Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// 网络指标。
    pub value: NetworkInfo,
}

/// 带采样时间的进程汇总指标。
#[derive(Debug, Default, Clone)]
pub(crate) struct ProcessSample {
    /// 采样时间，Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// 进程汇总指标。
    pub value: ProcessInfo,
}

/// 带采样时间的 Socket 汇总指标。
#[derive(Debug, Default, Clone)]
pub(crate) struct SocketSample {
    /// 采样时间，Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// Socket 汇总指标。
    pub value: SocketInfo,
}

/// 本机采集器。
///
/// 这个对象持有 `sysinfo` 的长期状态，避免每次采样都重新构建对象。
pub(crate) struct LocalCollector {
    system: System,
    disks: Disks,
    networks: Networks,
    identity_networks: Networks,
    last_disk_sampled_at: Option<Instant>,
    last_network_sampled_at: Option<Instant>,
    last_socket_info: Option<SocketInfo>,
}

impl LocalCollector {
    /// 创建一个新的本机采集器。
    pub(crate) fn new() -> Self {
        Self {
            system: System::new_all(),
            disks: Disks::new_with_refreshed_list(),
            networks: Networks::new_with_refreshed_list(),
            identity_networks: Networks::new_with_refreshed_list(),
            last_disk_sampled_at: None,
            last_network_sampled_at: None,
            last_socket_info: None,
        }
    }

    /// 采样 CPU 信息。
    pub(crate) fn sample_cpu(&mut self) -> CpuInfo {
        // CPU 使用率需要保持固定刷新间隔，调用方需要自己控制采样节奏。
        self.system.refresh_cpu_all();
        cpu::build_cpu_info(&self.system)
    }

    /// 采样内存信息。
    pub(crate) fn sample_memory(&mut self) -> MemoryInfo {
        self.system.refresh_memory();
        memory::build_memory_info(&self.system)
    }

    /// 采样磁盘信息。
    pub(crate) fn sample_disk(&mut self, include_per_device: bool) -> DiskSample {
        let elapsed_secs = elapsed_since(&mut self.last_disk_sampled_at);
        self.disks.refresh(true);

        let mut value = disk::build_disk_info_with_elapsed(&self.disks, elapsed_secs);
        if !include_per_device {
            value.disks.clear();
        }

        DiskSample {
            sampled_at: unix_timestamp_secs(),
            value,
        }
    }

    /// 采样网络信息。
    pub(crate) fn sample_network(
        &mut self,
        include_per_interface: bool,
        include_interfaces: &[String],
        exclude_interfaces: &[String],
    ) -> NetworkSample {
        let elapsed_secs = elapsed_since(&mut self.last_network_sampled_at);
        self.networks.refresh(true);

        let mut value = network::build_network_info_with_elapsed_and_filter(
            &self.networks,
            elapsed_secs,
            include_interfaces,
            exclude_interfaces,
        );
        if !include_per_interface {
            value.networks.clear();
        }

        NetworkSample {
            sampled_at: unix_timestamp_secs(),
            value,
        }
    }

    /// 采样核心指标。
    pub(crate) fn sample_core(&mut self) -> CoreSample {
        CoreSample {
            sampled_at: unix_timestamp_secs(),
            value: CoreInfo {
                cpu: self.sample_cpu(),
                memory: self.sample_memory(),
                load_avg: get_load_avg(),
            },
        }
    }

    /// 采样进程指标。
    pub(crate) fn sample_processes(&mut self, level: MetricLevel, limit: usize) -> ProcessSample {
        ProcessSample {
            sampled_at: unix_timestamp_secs(),
            value: process::sample_processes(&mut self.system, level, limit),
        }
    }

    /// 采样 Socket 指标。
    pub(crate) fn sample_sockets(&mut self, level: MetricLevel, limit: usize) -> SocketSample {
        let value = socket::sample_socket_info(self.last_socket_info.as_ref(), level, limit);
        self.last_socket_info = Some(value.clone());

        SocketSample {
            sampled_at: unix_timestamp_secs(),
            value,
        }
    }

    /// 采样身份信息；公网 IP 失败时写入失败状态而不是中断身份上报。
    pub(crate) async fn sample_identity(
        &mut self,
        agent_id: String,
        public_ip_config: &PublicIpConfig,
    ) -> IdentityInfo {
        self.identity_networks.refresh(true);
        let network_info = network::build_network_info(&self.identity_networks);
        let local_ips = network::local_ips(&network_info);
        let public_ip = resolve_public_ip(&network_info, public_ip_config).await;

        IdentityInfo {
            agent_id,
            hostname: System::host_name().unwrap_or_else(|| "unknown".to_string()),
            public_ip,
            local_ips,
        }
    }
}

impl Default for LocalCollector {
    /// 默认创建一个新的本机采集器。
    fn default() -> Self {
        Self::new()
    }
}

/// 采集当前操作系统和主机的基础信息。
///
/// 这些字段大多来自 `sysinfo::System` 的静态方法，不需要持有可刷新的 `System` 实例。
pub(crate) fn get_info() -> SystemInfo {
    SystemInfo {
        name: System::name().unwrap_or("unknown".to_string()),
        kernel_version: System::kernel_version().unwrap_or("unknown".to_string()),
        kernel_long_version: System::kernel_long_version(),
        os_version: System::os_version().unwrap_or("unknown".to_string()),
        long_os_version: System::long_os_version().unwrap_or("unknown".to_string()),
        hostname: System::host_name().unwrap_or("unknown".to_string()),
        distribution_id: System::distribution_id(),
        uptime: System::uptime(),
        boot_time: System::boot_time(),
        supported: sysinfo::IS_SUPPORTED_SYSTEM,
        core_num: sysinfo::System::physical_core_count().unwrap_or(0),
        cpu_arch: System::cpu_arch(),
    }
}

/// 采集系统 1/5/15 分钟平均负载。
///
/// Windows 等平台可能由 `sysinfo` 做兼容返回，调用方需要按平台能力解释结果。
pub(crate) fn get_load_avg() -> LoadAverageInfo {
    let load_avg = sysinfo::System::load_average();
    LoadAverageInfo {
        one: load_avg.one,
        five: load_avg.five,
        fifteen: load_avg.fifteen,
        supported: !cfg!(target_os = "windows"),
    }
}

/// 获取当前 Unix 时间戳，单位秒。
pub(crate) fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 记录本次采样时间，并返回距离上次采样的真实秒数。
fn elapsed_since(last_sampled_at: &mut Option<Instant>) -> Option<f64> {
    let now = Instant::now();
    last_sampled_at
        .replace(now)
        .map(|last| now.duration_since(last).as_secs_f64())
}

/// 获取公网 IP，优先使用网卡公网候选地址。
async fn resolve_public_ip(network_info: &NetworkInfo, config: &PublicIpConfig) -> PublicIpInfo {
    if !config.enabled {
        return PublicIpInfo::disabled();
    }

    let attempted_at = unix_timestamp_secs();

    if config.prefer_interface_candidate
        && let Some(ip) = network::interface_public_ip_candidate(network_info)
    {
        tracing::info!(ip = %ip, "Public IP resolved from interface candidate");
        let sampled_at = unix_timestamp_secs();
        let mut public_ip =
            PublicIpInfo::ready(ip, PublicIpSource::InterfaceCandidate, sampled_at, None);

        if config.verify_interface_candidate
            && let Ok(verified_ip) = lookup_external_public_ip(config).await
        {
            if Some(verified_ip) != public_ip.ip {
                tracing::info!(
                    interface_ip = %ip,
                    verified_ip = %verified_ip,
                    "Interface public IP candidate replaced by external verification"
                );
                public_ip = PublicIpInfo::ready(
                    verified_ip,
                    PublicIpSource::ExternalHttp,
                    sampled_at,
                    Some(unix_timestamp_secs()),
                );
            } else {
                public_ip.verified_at = Some(unix_timestamp_secs());
            }
        }

        return public_ip;
    }

    match lookup_external_public_ip(config).await {
        Ok(ip) => {
            let sampled_at = unix_timestamp_secs();
            tracing::info!(ip = %ip, "Public IP resolved from external service");
            PublicIpInfo::ready(
                ip,
                PublicIpSource::ExternalHttp,
                sampled_at,
                Some(sampled_at),
            )
        }
        Err(err) => {
            tracing::warn!(error = ?err, "Public IP lookup failed");
            PublicIpInfo::failed(err.to_string(), attempted_at)
        }
    }
}

/// 通过外部服务获取公网 IP。
async fn lookup_external_public_ip(config: &PublicIpConfig) -> anyhow::Result<std::net::IpAddr> {
    let (v4, v6) = tokio::time::timeout(config.lookup_timeout, async {
        futures_util::future::join(
            network::get_public_network_v4_with_concurrency(config.max_concurrency),
            network::get_public_network_v6_with_concurrency(config.max_concurrency),
        )
        .await
    })
    .await
    .map_err(|_| anyhow::anyhow!("Public IP lookup timed out"))?;

    v4.or(v6)
        .map_err(|err| anyhow::anyhow!("Public IP lookup failed: {err}"))
}

#[cfg(test)]
mod tests {
    //! 采集模块的基础行为测试。

    use super::*;
    use std::thread;

    /// 验证基础系统信息字段能正常采集。
    #[test]
    fn test_get_info() {
        let info = get_info();
        assert_eq!(info.supported, sysinfo::IS_SUPPORTED_SYSTEM);
        assert_eq!(info.core_num, System::physical_core_count().unwrap_or(0));
        assert!(!info.name.is_empty());
    }

    /// 验证平均负载能正常读取。
    #[test]
    fn test_get_load_avg() {
        let load_avg = get_load_avg();
        assert!(load_avg.one.is_finite());
        assert!(load_avg.five.is_finite());
        assert!(load_avg.fifteen.is_finite());
    }

    /// 验证采集器可以单独采样 CPU。
    #[test]
    fn test_local_collector_sample_cpu() {
        let mut collector = LocalCollector::new();
        let _ = collector.sample_cpu();
        thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);

        let cpu = collector.sample_cpu();
        assert_eq!(cpu.cpu_num, cpu.cpus.len());
        assert!(cpu.cpu_usage.is_finite());
    }

    /// 验证采集器可以单独采样内存。
    #[test]
    fn test_local_collector_sample_memory() {
        let mut collector = LocalCollector::new();
        let memory = collector.sample_memory();

        assert!(memory.memory_total >= memory.memory_usage);
        assert!(memory.swap_total >= memory.swap_usage);
    }

    /// 验证采集器可以单独采样磁盘。
    #[test]
    fn test_local_collector_sample_disk() {
        let mut collector = LocalCollector::new();
        let disk = collector.sample_disk(true).value;
        let total_space: u64 = disk.disks.iter().map(|disk| disk.total_space).sum();
        let available_space: u64 = disk.disks.iter().map(|disk| disk.available_space).sum();

        assert_eq!(disk.total_space, total_space);
        assert_eq!(disk.available_space, available_space);
        assert!(disk.total_space >= disk.available_space);
    }

    /// 验证采集器可以单独采样网络。
    #[test]
    fn test_local_collector_sample_network() {
        let mut collector = LocalCollector::new();
        let network = collector.sample_network(true, &[], &[]).value;

        assert!(network.total_received >= network.received);
        assert!(network.total_transmitted >= network.transmitted);
    }

    /// 验证采集器可以单独采样进程汇总。
    #[test]
    fn test_local_collector_sample_processes() {
        let mut collector = LocalCollector::new();
        let processes = collector.sample_processes(MetricLevel::Count, 10);

        assert!(processes.sampled_at > 0);
        if sysinfo::IS_SUPPORTED_SYSTEM {
            assert!(processes.value.count > 0);
        }
    }

    /// 验证采集器可以单独采样 Socket 汇总。
    #[test]
    fn test_local_collector_sample_sockets() {
        let mut collector = LocalCollector::new();
        let sockets = collector.sample_sockets(MetricLevel::Count, 10);

        assert!(sockets.sampled_at > 0);
    }

    /// 验证关闭单磁盘明细时仍保留汇总值。
    #[test]
    fn test_local_collector_sample_disk_can_hide_devices() {
        let mut collector = LocalCollector::new();
        let disk = collector.sample_disk(false).value;

        assert!(disk.disks.is_empty());
        assert!(disk.total_space >= disk.available_space);
    }

    /// 验证关闭单网卡明细时仍保留汇总值。
    #[test]
    fn test_local_collector_sample_network_can_hide_interfaces() {
        let mut collector = LocalCollector::new();
        let network = collector.sample_network(false, &[], &[]).value;

        assert!(network.networks.is_empty());
        assert!(network.total_received >= network.received);
    }

    /// 验证核心分组会带采样时间和 CPU/内存数据。
    #[test]
    fn test_local_collector_sample_core() {
        let mut collector = LocalCollector::new();
        let _ = collector.sample_cpu();
        thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);

        let core = collector.sample_core();
        assert!(core.sampled_at > 0);
        assert_eq!(core.value.cpu.cpu_num, core.value.cpu.cpus.len());
        assert!(core.value.memory.memory_total >= core.value.memory.memory_usage);
    }

    /// 验证磁盘分组第一次采样未预热，第二次采样已预热。
    #[test]
    fn test_local_collector_sample_disk_warmup() {
        let mut collector = LocalCollector::new();
        let first = collector.sample_disk(true);
        thread::sleep(std::time::Duration::from_millis(10));
        let second = collector.sample_disk(true);

        assert!(!first.value.warmed_up);
        assert!(second.value.warmed_up);
        assert!(second.sampled_at >= first.sampled_at);
    }

    /// 验证网络分组第一次采样未预热，第二次采样已预热。
    #[test]
    fn test_local_collector_sample_network_warmup() {
        let mut collector = LocalCollector::new();
        let first = collector.sample_network(true, &[], &[]);
        thread::sleep(std::time::Duration::from_millis(10));
        let second = collector.sample_network(true, &[], &[]);

        assert!(!first.value.warmed_up);
        assert!(second.value.warmed_up);
        assert!(second.sampled_at >= first.sampled_at);
    }

    /// 通过带 JSONPath 的公网服务获取 IP。
    ///
    /// 该测试依赖公网服务，默认跳过；需要手工联调时使用 `cargo test test_fetch_public_network -- --ignored`。
    #[ignore = "requires external network access"]
    #[tokio::test]
    async fn test_fetch_public_network() {
        let url = "https://api.iplocation.net/?cmd=get-ip";
        let json = "$.ip";
        let ip = network::fetch_public_network(
            reqwest::Client::new(),
            url,
            Some(serde_json_path::JsonPath::parse(json).unwrap()),
        )
        .await
        .unwrap();
        println!("{}", serde_json::to_string_pretty(&ip).unwrap());
    }

    /// 同时尝试 IPv4 和 IPv6 公网地址获取。
    ///
    /// 该测试依赖公网服务，默认跳过；需要手工联调时使用 `cargo test test_get_public_network -- --ignored`。
    #[ignore = "requires external network access"]
    #[tokio::test]
    async fn test_get_public_network() {
        println!("{:?}", network::get_public_network().await)
    }

    /// 获取 IPv4 公网地址。
    ///
    /// 该测试依赖公网服务，默认跳过；需要手工联调时使用 `cargo test test_get_public_network_v4 -- --ignored`。
    #[ignore = "requires external network access"]
    #[tokio::test]
    async fn test_get_public_network_v4() {
        let ip_v4 = network::get_public_network_v4().await.unwrap();
        println!("{:?}", ip_v4);
    }

    /// 获取 IPv6 公网地址。
    ///
    /// 该测试依赖公网服务，默认跳过；需要手工联调时使用 `cargo test test_get_public_network_v6 -- --ignored`。
    #[ignore = "requires external network access"]
    #[tokio::test]
    async fn test_get_public_network_v6() {
        let ip_v6 = network::get_public_network_v6().await.unwrap();
        println!("{:?}", ip_v6);
    }
}
