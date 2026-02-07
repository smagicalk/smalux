use smalux_core::model::info::MemoryInfo;

pub(crate) async fn get_memory_info(system:&mut sysinfo::System) -> anyhow::Result<MemoryInfo>{

    system.refresh_memory();
    let mut res_memory = MemoryInfo::default();
    res_memory.memory_usage = system.used_memory();
    res_memory.memory_total = system.total_memory();
    res_memory.memory_available = system.available_memory();
    res_memory.memory_free = system.free_memory();
    res_memory.swap_total = system.total_swap();
    res_memory.swap_usage = system.used_swap();
    res_memory.swap_free = system.free_swap();

    Ok(res_memory)

}