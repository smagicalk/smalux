use serde::{Deserialize, Serialize};
use sysinfo::System;
use smalux_core::model::info::SystemInfo;
pub(crate) mod cpu;
pub(crate) mod memory;
pub(crate) mod disk;
pub(crate) mod network;


pub(crate)  fn get_info()->anyhow::Result<SystemInfo>{

    let mut res_system_info = SystemInfo::default();
    res_system_info.name = System::name().unwrap_or("unknown".to_string());
    res_system_info.kernel_version = System::kernel_version().unwrap_or("unknown".to_string());
    res_system_info.kernel_long_version = System::kernel_long_version();
    res_system_info.os_version = System::os_version().unwrap_or("unknown".to_string());
    res_system_info.long_os_version = System::long_os_version().unwrap_or("unknown".to_string());
    res_system_info.hostname = System::host_name().unwrap_or("unknown".to_string());
    res_system_info.distribution_id = System::distribution_id();
    res_system_info.uptime = System::uptime();
    res_system_info.boot_time = System::boot_time();
    res_system_info.supported = sysinfo::IS_SUPPORTED_SYSTEM;
    res_system_info.core_num =  sysinfo::System::physical_core_count().unwrap_or(0);
    res_system_info.cpu_arch = System::cpu_arch();

    Ok(res_system_info)

}

pub(crate) fn get_load_avg()->anyhow::Result<(f64,f64,f64)>{
    let load_avg = sysinfo::System::load_average();
    Ok((load_avg.one, load_avg.five, load_avg.fifteen))
}


#[cfg(test)]
mod tests{
    use crate::info::{cpu, disk, get_info, memory, network};
    use crate::info::network::fetch_public_network;

    #[tokio::test]
    async fn test(){
        let load_avg = sysinfo::System::load_average();
        println!(
            "one minute: {}%, five minutes: {}%, fifteen minutes: {}%",
            load_avg.one,
            load_avg.five,
            load_avg.fifteen,
        );
    }


    #[tokio::test]
    async fn test_get_info(){
        let info = get_info();
        println!("{}", serde_json::to_string_pretty(&info.unwrap()).unwrap());
    }

    #[tokio::test]
    async fn test_cpu(){
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();
         tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
         println!("{}",serde_json::to_string_pretty(&cpu::get_cpu_info(&mut sys).await.unwrap()).unwrap())
    }

    #[tokio::test]
    async fn test_memory(){
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();
        tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
        println!("{}",serde_json::to_string_pretty(&memory::get_memory_info(&mut sys).await.unwrap()).unwrap())
    }

    #[tokio::test]
    async fn test_disk(){
        let mut disks = sysinfo::Disks::new_with_refreshed_list();
        tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;

        println!("{}",serde_json::to_string_pretty(&disk::get_disk_info(&mut disks).await.unwrap()).unwrap())
    }

    #[tokio::test]
    async fn test_network_info(){
        let mut networks = sysinfo::Networks::new_with_refreshed_list();
        tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
        let network_info = network::get_network_info(&mut networks).await.unwrap();
        println!("{}",serde_json::to_string_pretty(&network_info).unwrap());
        println!("{:?}",network_info.get_ips())
    }

    #[tokio::test]
    async fn test_fetch_public_network(){
        let url = "https://api.iplocation.net/?cmd=get-ip";
        let json = "$.ip";
        let ip = fetch_public_network(reqwest::Client::new(),url,Some(serde_json_path::JsonPath::parse(json).unwrap())).await.unwrap();
        println!("{}",serde_json::to_string_pretty(&ip).unwrap());
    }

    #[tokio::test]
    async fn test_get_public_network(){
        println!("{:?}",network::get_public_network().await)
    }

    #[tokio::test]
    async fn test_get_public_network_v4(){
        let ip_v4 = network::get_public_network_v4().await.unwrap();
        println!("{:?}",ip_v4);
    }

    #[tokio::test]
    async fn test_get_public_network_v6(){
        let ip_v6 = network::get_public_network_v6().await.unwrap();
        println!("{:?}",ip_v6);
    }
}