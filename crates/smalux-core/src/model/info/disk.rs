//! 磁盘信息模型。

use serde::{Deserialize, Serialize};

/// 单块磁盘的采集信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Disk {
    /// 磁盘名称。
    pub name: String,
    /// 总容量，单位字节。
    pub total_space: u64,
    /// 可用容量，单位字节。
    pub available_space: u64,
    /// 磁盘类型。
    pub kind: String,
    /// 文件系统名称。
    pub file_system: String,
    /// 是否只读。
    pub is_read_only: bool,
    /// 是否可移动设备。
    pub is_removable: bool,
    /// 挂载点路径。
    pub mount_point: String,
    /// 本次刷新周期内读取字节数。
    pub read_bytes: u64,
    /// 本次刷新周期内写入字节数。
    pub write_bytes: u64,
    /// 当前读取速度，单位字节/秒。
    pub read_bytes_per_sec: f64,
    /// 当前写入速度，单位字节/秒。
    pub write_bytes_per_sec: f64,
    /// 当前总 IO 速度，单位字节/秒。
    pub io_bytes_per_sec: f64,
    /// 系统启动后累计读取字节数。
    pub total_read_bytes: u64,
    /// 系统启动后累计写入字节数。
    pub total_written_bytes: u64,
    /// 系统启动后累计 IO 字节数。
    pub total_io_bytes: u64,
}

/// 磁盘汇总信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskInfo {
    /// 当前磁盘速度是否已经完成预热。
    pub warmed_up: bool,
    /// 磁盘明细列表。
    pub disks: Vec<Disk>,
    /// 所有磁盘总容量，单位字节。
    pub total_space: u64,
    /// 所有磁盘可用容量，单位字节。
    pub available_space: u64,
    /// 所有磁盘本次刷新周期内读取字节数。
    pub read_bytes: u64,
    /// 所有磁盘本次刷新周期内写入字节数。
    pub write_bytes: u64,
    /// 所有磁盘当前读取速度，单位字节/秒。
    pub read_bytes_per_sec: f64,
    /// 所有磁盘当前写入速度，单位字节/秒。
    pub write_bytes_per_sec: f64,
    /// 所有磁盘当前总 IO 速度，单位字节/秒。
    pub io_bytes_per_sec: f64,
    /// 所有磁盘系统启动后累计读取字节数。
    pub total_read_bytes: u64,
    /// 所有磁盘系统启动后累计写入字节数。
    pub total_written_bytes: u64,
    /// 所有磁盘系统启动后累计 IO 字节数。
    pub total_io_bytes: u64,
}

impl Default for DiskInfo {
    /// 创建空的磁盘汇总信息。
    fn default() -> Self {
        Self {
            warmed_up: false,
            disks: vec![],
            total_space: 0,
            available_space: 0,
            read_bytes: 0,
            write_bytes: 0,
            read_bytes_per_sec: 0.0,
            write_bytes_per_sec: 0.0,
            io_bytes_per_sec: 0.0,
            total_written_bytes: 0,
            total_read_bytes: 0,
            total_io_bytes: 0,
        }
    }
}
