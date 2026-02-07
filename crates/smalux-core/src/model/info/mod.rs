mod network;

use serde::{Deserialize, Serialize};
pub use network::Network;
pub use network::NetworkInfo;
pub use network::Ip;

pub mod disk;
pub use disk::Disk;
pub use disk::DiskInfo;

pub mod memory;
pub use memory::MemoryInfo;


pub mod cpu;
pub use cpu::CpuInfo;
pub use cpu::Cpu;


#[derive(Debug,Clone,Serialize,Deserialize,Hash,Eq,PartialEq,Default)]
pub struct SystemInfo{
    pub name: String,
    pub kernel_version: String,
    pub kernel_long_version: String,
    pub os_version: String,
    pub long_os_version: String,
    pub hostname: String,
    pub distribution_id: String,
    pub uptime: u64,
    pub boot_time: u64,
    pub supported: bool,
    pub core_num: usize,
    pub cpu_arch: String,
}

