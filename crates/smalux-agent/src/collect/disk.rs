//! 磁盘采集实现。
//!
//! 这里同时采集每块磁盘的静态信息和两次刷新之间的读写增量。

use smalux_core::model::info::{Disk, DiskInfo};
use sysinfo::Disks;

/// 从已刷新的磁盘对象构建所有磁盘的容量、挂载点、文件系统和读写统计。
#[cfg(test)]
fn build_disk_info(disks: &Disks) -> DiskInfo {
    build_disk_info_with_elapsed(disks, None)
}

/// 从已刷新的磁盘对象构建磁盘信息，并按真实间隔计算速度。
pub(crate) fn build_disk_info_with_elapsed(disks: &Disks, elapsed_secs: Option<f64>) -> DiskInfo {
    let warmed_up = elapsed_secs.filter(|secs| *secs > 0.0).is_some();
    let mut res_disk = DiskInfo {
        warmed_up,
        ..DiskInfo::default()
    };
    for disk in disks.list() {
        // 单盘字段用于明细展示，汇总字段同步累加到 DiskInfo。
        let mut disk_info = Disk {
            name: disk.name().to_string_lossy().into_owned(),
            total_space: disk.total_space(),
            available_space: disk.available_space(),
            kind: disk.kind().to_string(),
            file_system: disk.file_system().to_string_lossy().into_owned(),
            is_read_only: disk.is_read_only(),
            is_removable: disk.is_removable(),
            mount_point: disk.mount_point().to_string_lossy().into_owned(),
            ..Disk::default()
        };
        res_disk.total_space += disk_info.total_space;
        res_disk.available_space += disk_info.available_space;

        let speed = disk.usage();
        // `usage` 表示本次刷新周期内的增量，`total_*` 表示系统启动后的累计值。
        disk_info.read_bytes = speed.read_bytes;
        disk_info.write_bytes = speed.written_bytes;
        disk_info.total_read_bytes = speed.total_read_bytes;
        disk_info.total_written_bytes = speed.total_written_bytes;
        disk_info.total_io_bytes = speed
            .total_read_bytes
            .saturating_add(speed.total_written_bytes);

        if let Some(elapsed_secs) = elapsed_secs.filter(|secs| *secs > 0.0) {
            disk_info.read_bytes_per_sec = disk_info.read_bytes as f64 / elapsed_secs;
            disk_info.write_bytes_per_sec = disk_info.write_bytes as f64 / elapsed_secs;
            disk_info.io_bytes_per_sec =
                disk_info.read_bytes_per_sec + disk_info.write_bytes_per_sec;
        }

        res_disk.read_bytes += speed.read_bytes;
        res_disk.write_bytes += speed.written_bytes;
        res_disk.total_read_bytes += speed.total_read_bytes;
        res_disk.total_written_bytes += speed.total_written_bytes;
        res_disk.total_io_bytes += disk_info.total_io_bytes;

        res_disk.disks.push(disk_info);
    }

    if let Some(elapsed_secs) = elapsed_secs.filter(|secs| *secs > 0.0) {
        res_disk.read_bytes_per_sec = res_disk.read_bytes as f64 / elapsed_secs;
        res_disk.write_bytes_per_sec = res_disk.write_bytes as f64 / elapsed_secs;
        res_disk.io_bytes_per_sec = res_disk.read_bytes_per_sec + res_disk.write_bytes_per_sec;
    }

    res_disk
}

#[cfg(test)]
mod tests {
    //! 磁盘信息映射测试。

    use super::*;

    /// 验证磁盘明细数量和汇总容量一致。
    #[test]
    fn test_build_disk_info() {
        let mut disks = Disks::new_with_refreshed_list();
        disks.refresh(true);

        let disk_info = build_disk_info(&disks);
        let total_space: u64 = disk_info.disks.iter().map(|disk| disk.total_space).sum();
        let available_space: u64 = disk_info
            .disks
            .iter()
            .map(|disk| disk.available_space)
            .sum();

        assert_eq!(disk_info.disks.len(), disks.list().len());
        assert_eq!(disk_info.total_space, total_space);
        assert_eq!(disk_info.available_space, available_space);
        assert!(disk_info.total_space >= disk_info.available_space);
    }

    /// 验证磁盘速度按真实间隔计算。
    #[test]
    fn test_build_disk_info_with_elapsed_sets_speed() {
        let mut disks = Disks::new_with_refreshed_list();
        disks.refresh(true);

        let disk_info = build_disk_info_with_elapsed(&disks, Some(2.0));

        assert!(disk_info.warmed_up);
        assert_eq!(
            disk_info.io_bytes_per_sec,
            disk_info.read_bytes_per_sec + disk_info.write_bytes_per_sec
        );
        assert_eq!(
            disk_info.total_io_bytes,
            disk_info.total_read_bytes + disk_info.total_written_bytes
        );
    }
}
