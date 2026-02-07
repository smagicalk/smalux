use smalux_core::model::info::{Cpu, CpuInfo};


pub (crate) async fn  get_cpu_info(system :&mut sysinfo::System)-> anyhow::Result<CpuInfo> {
    system.refresh_cpu_all();

    let mut  res_cpu = CpuInfo::default();

    let cpus = system.cpus();

    res_cpu.cpu_num = cpus.len();

    for cpu in cpus {
        let mut cpu_info = Cpu::default();
        cpu_info.name = cpu.name().to_string();
        cpu_info.usage = cpu.cpu_usage();
        cpu_info.frequency = cpu.frequency();
        cpu_info.brand = cpu.brand().to_string();
        cpu_info.vendor_id = cpu.vendor_id().to_string();
        res_cpu.cpus.push(cpu_info);
    }

    res_cpu.cpu_usage = system.global_cpu_usage();
    Ok(res_cpu)
}




#[cfg(test)]
mod tests{

    use std::{cmp::Ordering, thread};
    use sysinfo::{
        Components, Disks, Networks, System, MINIMUM_CPU_UPDATE_INTERVAL,
    };

    fn bytes_to_gib(bytes: u64) -> f64 {
        bytes as f64 / 1024.0 / 1024.0 / 1024.0
    }

    fn bytes_to_mib(bytes: u64) -> f64 {
        bytes as f64 / 1024.0 / 1024.0
    }

    #[tokio::test]
    async fn test() {
        // 初始化系统信息（一次创建，多次 refresh）
        let mut sys = System::new_all();

        // 刷新一次基础信息
        sys.refresh_all();

        // CPU 使用率：必须至少隔 MINIMUM_CPU_UPDATE_INTERVAL 再 refresh 一次才准确
        sys.refresh_cpu_usage();
        thread::sleep(MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_cpu_usage();

        println!("========== 系统信息 ==========");
        println!("System name:     {:?}", System::name());
        println!("Kernel version:  {:?}", System::kernel_version());
        println!("OS version:      {:?}", System::os_version());
        println!("Host name:       {:?}", System::host_name());
        println!("Uptime (s):      {}", sysinfo::System::uptime());
        println!("Boot time (ts):  {}", sysinfo::System::boot_time());
        println!("Supported?:      {}", sysinfo::IS_SUPPORTED_SYSTEM);

        println!("\n========== 内存 ==========");
        println!(
            "Memory total: {:.2} GiB ({} bytes)",
            bytes_to_gib(sys.total_memory()),
            sys.total_memory()
        );
        println!(
            "Memory used : {:.2} GiB ({} bytes)",
            bytes_to_gib(sys.used_memory()),
            sys.used_memory()
        );
        println!(
            "Swap total  : {:.2} GiB ({} bytes)",
            bytes_to_gib(sys.total_swap()),
            sys.total_swap()
        );
        println!(
            "Swap used   : {:.2} GiB ({} bytes)",
            bytes_to_gib(sys.used_swap()),
            sys.used_swap()
        );

        println!("\n========== CPU ==========");
        println!("CPU count: {}", sys.cpus().len());
        println!(
            "Global CPU usage: {:.2}%",
            sys.global_cpu_usage()
        );
        for (i, cpu) in sys.cpus().iter().enumerate() {
            println!(
                "CPU #{:<2} {:<28} usage={:>6.2}% freq={}MHz",
                i,
                cpu.name(),
                cpu.cpu_usage(),
                cpu.frequency()
            );
        }

        println!("\n========== 磁盘 ==========");
        let disks = Disks::new_with_refreshed_list();
        for d in &disks {
            let total = d.total_space();
            let avail = d.available_space();
            let used = total.saturating_sub(avail);

            println!(
                "disk={:<12} mount={:<20} fs={:<8} total={:>6.2}GiB used={:>6.2}GiB avail={:>6.2}GiB removable={}",
                d.name().to_string_lossy(),
                d.mount_point().to_string_lossy(),
                d.file_system().to_string_lossy(),
                bytes_to_gib(total),
                bytes_to_gib(used),
                bytes_to_gib(avail),
                d.is_removable(),
            );
        }

        println!("\n========== 网络 ==========");
        let networks = Networks::new_with_refreshed_list();
        for (name, data) in &networks {
            println!(
                "{:<12}: total_down={}B total_up={}B | down={}B up={}B (since refresh)",
                name,
                data.total_received(),
                data.total_transmitted(),
                data.received(),
                data.transmitted(),
            );
        }

        println!("\n========== 传感器 / 温度（可用则有） ==========");
        let components = Components::new_with_refreshed_list();
        if components.is_empty() {
            println!("(no components available on this platform)");
        } else {
            for c in &components {
                // Debug 输出包含 label/温度等信息
                println!("{c:?}");
            }
        }

        println!("\n========== 进程 Top10（按 CPU） ==========");
        let mut procs: Vec<_> = sys.processes().values().collect();
        procs.sort_by(|a, b| {
            b.cpu_usage()
                .partial_cmp(&a.cpu_usage())
                .unwrap_or(Ordering::Equal)
        });

        for p in procs.iter().take(10) {
            println!(
                "pid={:<7} cpu={:>6.2}% mem={:>7.2}MiB name={}",
                p.pid().as_u32(),
                p.cpu_usage(),
                bytes_to_mib(p.memory()),
                p.name().to_string_lossy(),
            );
        }

        println!("\n========== 进程 Top10（按内存） ==========");
        procs.sort_by_key(|p| std::cmp::Reverse(p.memory()));
        for p in procs.iter().take(10) {
            println!(
                "pid={:<7} mem={:>7.2}MiB cpu={:>6.2}% name={}",
                p.pid().as_u32(),
                bytes_to_mib(p.memory()),
                p.cpu_usage(),
                p.name().to_string_lossy(),
            );
        }

        // 简单断言：确保能拿到 CPU
        assert!(!sys.cpus().is_empty());
    }

}