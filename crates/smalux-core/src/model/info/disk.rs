use serde::{Deserialize, Serialize};

#[derive(Debug,Clone,Serialize,Deserialize,Default)]
pub struct Disk {
    pub name: String,
    pub total_space: u64,
    pub available_space:u64,
    pub kind:String,
    pub file_system: String,
    pub is_read_only: bool,
    pub is_removable: bool,
    pub mount_point: String,
    pub read_bytes:u64,
    pub write_bytes:u64,
    pub total_read_bytes:u64,
    pub total_written_bytes:u64,
}




#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct DiskInfo{
    pub disks: Vec<Disk>,
    pub total_space: u64,
    pub available_space:u64,
    pub read_bytes:u64,
    pub write_bytes:u64,
    pub total_read_bytes:u64,
    pub total_written_bytes:u64,
}

impl Default for DiskInfo {
    fn default() -> Self {
        Self{
            disks:vec![],
            total_space:0,
            available_space:0,
            read_bytes:0,
            write_bytes:0,
            total_written_bytes:0,
            total_read_bytes:0
        }
    }
}
