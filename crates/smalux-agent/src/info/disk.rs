use smalux_core::model::info::{Disk, DiskInfo};



pub(crate) async fn get_disk_info(disks: &mut sysinfo::Disks) -> anyhow::Result<DiskInfo>{
    disks.refresh(true);

    let mut res_disk = DiskInfo::default();
    for disk in disks.list() {
        let mut disk_info = Disk::default();
        disk_info.name = disk.name().to_string_lossy().into_owned();
        
        disk_info.total_space = disk.total_space();
        res_disk.total_space +=  disk_info.total_space;
        
        disk_info.available_space = disk.available_space();
        res_disk.available_space += disk_info.available_space;

        disk_info.kind = disk.kind().to_string();
        disk_info.file_system = disk.file_system().to_string_lossy().into_owned();

        disk_info.is_read_only = disk.is_read_only();
        disk_info.is_removable=disk.is_removable();
        disk_info.mount_point=disk.mount_point().to_string_lossy().into_owned();

        let speed = disk.usage();
        disk_info.read_bytes = speed.read_bytes;
        disk_info.write_bytes = speed.written_bytes;
        disk_info.total_read_bytes = speed.total_read_bytes;
        disk_info.total_written_bytes = speed.total_written_bytes;

        res_disk.read_bytes += speed.read_bytes;
        res_disk.write_bytes += speed.written_bytes;
        res_disk.total_read_bytes += speed.total_read_bytes;
        res_disk.total_written_bytes += speed.total_written_bytes;
        
        res_disk.disks.push(disk_info);
    }
    
    Ok(res_disk)
}