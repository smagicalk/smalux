//! CPU 与内存共享的最小系统采集状态。

use std::time::Instant;

use sysinfo::System;

use super::{cpu, elapsed_since, memory};

/// CPU 与内存采集所需的最小可变状态。
pub(crate) struct SystemCollector {
    system: System,
    last_cpu_sampled_at: Option<Instant>,
}

impl SystemCollector {
    /// 创建尚未建立 CPU 采样基线的系统采集器。
    pub(crate) fn new() -> Self {
        Self {
            system: System::new(),
            last_cpu_sampled_at: None,
        }
    }

    /// 独立刷新并采集 CPU。
    pub(crate) fn collect_cpu(&mut self) -> cpu::CpuSnapshot {
        self.collect_cpu_at(Instant::now())
    }

    /// 使用调用方提供的统一采样时刻刷新并采集 CPU。
    pub(crate) fn collect_cpu_at(&mut self, now: Instant) -> cpu::CpuSnapshot {
        self.system.refresh_cpu_all();
        let elapsed = elapsed_since(&mut self.last_cpu_sampled_at, now);
        let warmed_up = elapsed
            .map(|duration| duration >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL)
            .unwrap_or(false);
        cpu::collect(&self.system, warmed_up)
    }

    /// 刷新并采集内存与交换空间。
    pub(crate) fn collect_memory(&mut self) -> memory::MemorySnapshot {
        self.system.refresh_memory();
        memory::collect(&self.system)
    }

    /// 使用统一采样时刻在同一轮刷新中采集 CPU 与内存。
    pub(crate) fn collect_cpu_and_memory_at(
        &mut self,
        now: Instant,
    ) -> (cpu::CpuSnapshot, memory::MemorySnapshot) {
        self.system.refresh_cpu_all();
        self.system.refresh_memory();
        let elapsed = elapsed_since(&mut self.last_cpu_sampled_at, now);
        let warmed_up = elapsed
            .map(|duration| duration >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL)
            .unwrap_or(false);
        (
            cpu::collect(&self.system, warmed_up),
            memory::collect(&self.system),
        )
    }
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::SystemCollector;

    #[test]
    fn system_collector_collects_cpu_and_memory() {
        let mut collector = SystemCollector::new();

        let (cpu, memory) = collector.collect_cpu_and_memory_at(Instant::now());

        assert!(!cpu.warmed_up);
        assert_eq!(cpu.logical_cpu_count, cpu.cpus.len());
        assert!(memory.total_bytes >= memory.used_bytes);
    }
}
