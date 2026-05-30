//! 网络信息模型。

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr};

/// 网卡上的一个 IP 地址。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ip {
    /// IP 地址。
    pub ip: IpAddr,
    /// 网络前缀长度。
    pub mask_len: u8,
}

impl Default for Ip {
    /// 默认使用未指定 IPv4 地址。
    fn default() -> Self {
        Self {
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            mask_len: 0,
        }
    }
}

/// 单个网卡的采集信息。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Network {
    /// 网卡名称。
    pub name: String,
    /// 最大传输单元。
    pub mtu: u64,
    /// 本次刷新周期内接收字节数。
    pub received: u64,
    /// 本次刷新周期内接收错误数。
    pub errors_on_received: u64,
    /// 本次刷新周期内发送错误数。
    pub errors_on_transmitted: u64,
    /// 本次刷新周期内接收包数量。
    pub packets_received: u64,
    /// 本次刷新周期内发送字节数。
    pub transmitted: u64,
    /// 当前接收速度，单位字节/秒。
    pub received_bytes_per_sec: f64,
    /// 当前发送速度，单位字节/秒。
    pub transmitted_bytes_per_sec: f64,
    /// 当前总网络速度，单位字节/秒。
    pub network_bytes_per_sec: f64,
    /// 系统启动后累计接收字节数。
    pub total_received: u64,
    /// 系统启动后累计接收错误数。
    pub total_errors_on_received: u64,
    /// 系统启动后累计发送错误数。
    pub total_errors_on_transmitted: u64,
    /// 系统启动后累计接收包数量。
    pub total_packets_received: u64,
    /// 系统启动后累计发送字节数。
    pub total_transmitted: u64,
    /// 系统启动后累计使用流量，单位字节。
    pub used_traffic_bytes: u64,
    /// MAC 地址。
    pub mac: String,
    /// 网卡 IP 地址列表。
    pub ip: Vec<Ip>,
}

/// 网络汇总信息。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetworkInfo {
    /// 当前网络速度是否已经完成预热。
    pub warmed_up: bool,
    /// 网卡明细列表。
    pub networks: Vec<Network>,
    /// 所有网卡本次刷新周期内接收字节数。
    pub received: u64,
    /// 所有网卡本次刷新周期内接收错误数。
    pub errors_on_received: u64,
    /// 所有网卡本次刷新周期内发送错误数。
    pub errors_on_transmitted: u64,
    /// 所有网卡本次刷新周期内接收包数量。
    pub packets_received: u64,
    /// 所有网卡本次刷新周期内发送字节数。
    pub transmitted: u64,
    /// 所有网卡当前接收速度，单位字节/秒。
    pub received_bytes_per_sec: f64,
    /// 所有网卡当前发送速度，单位字节/秒。
    pub transmitted_bytes_per_sec: f64,
    /// 所有网卡当前总网络速度，单位字节/秒。
    pub network_bytes_per_sec: f64,
    /// 所有网卡系统启动后累计接收字节数。
    pub total_received: u64,
    /// 所有网卡系统启动后累计接收错误数。
    pub total_errors_on_received: u64,
    /// 所有网卡系统启动后累计发送错误数。
    pub total_errors_on_transmitted: u64,
    /// 所有网卡系统启动后累计接收包数量。
    pub total_packets_received: u64,
    /// 所有网卡系统启动后累计发送字节数。
    pub total_transmitted: u64,
    /// 所有网卡系统启动后累计使用流量，单位字节。
    pub used_traffic_bytes: u64,
}

impl Network {
    /// 返回该网卡上可对外展示的 IP，过滤 loopback 和 unspecified 地址。
    pub fn get_ips(&self) -> Vec<IpAddr> {
        let mut ips: Vec<IpAddr> = Vec::new();
        for ip in self.ip.iter() {
            if ip.ip.is_loopback() || ip.ip.is_unspecified() {
                continue;
            }
            ips.push(ip.ip.clone());
        }
        ips
    }
}

impl NetworkInfo {
    /// 返回所有网卡上可对外展示的 IP。
    pub fn get_ips(&self) -> Vec<IpAddr> {
        let mut ips: Vec<IpAddr> = Vec::new();
        for network in self.networks.iter() {
            ips.append(&mut network.get_ips());
        }
        ips
    }
}
