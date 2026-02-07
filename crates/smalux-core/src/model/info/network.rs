
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr};



#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Ip{
    pub ip:IpAddr,
    pub mask_len:u8
}

impl Default for Ip{
    fn default() -> Self {
        Self{
            ip:IpAddr::V4(Ipv4Addr::new(0,0,0,0)),
            mask_len:0
        }
    }
}

#[derive(Debug,Default,Clone,Serialize,Deserialize)]
pub struct Network{
    pub name: String,
    pub mtu:u64,
    pub received:u64,
    pub errors_on_received:u64,
    pub errors_on_transmitted:u64,
    pub packets_received:u64,
    pub transmitted:u64,
    pub total_received:u64,
    pub total_errors_on_received:u64,
    pub total_errors_on_transmitted:u64,
    pub total_packets_received:u64,
    pub total_transmitted:u64,
    pub mac:String,
    pub ip:Vec<Ip>

}

#[derive(Debug,Default,Clone,Serialize,Deserialize)]
pub struct NetworkInfo{
    pub networks: Vec<Network>,
    pub received:u64,
    pub errors_on_received:u64,
    pub errors_on_transmitted:u64,
    pub packets_received:u64,
    pub transmitted:u64,
    pub total_received:u64,
    pub total_errors_on_received:u64,
    pub total_errors_on_transmitted:u64,
    pub total_packets_received:u64,
    pub total_transmitted:u64,
}

impl Network{
    pub fn get_ips(&self)->Vec<IpAddr>{
        let mut ips:Vec<IpAddr> = Vec::new();
        for ip in self.ip.iter() {
            if ip.ip.is_loopback() || ip.ip.is_unspecified() {
                continue;
            }
            ips.push(ip.ip.clone());
        }
        ips
    }
}

impl NetworkInfo{
    pub fn get_ips(&self)->Vec<IpAddr>{
        let mut ips:Vec<IpAddr> = Vec::new();
        for network in self.networks.iter() {
            ips.append(&mut network.get_ips());
        }
        ips
    }
}