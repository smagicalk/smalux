//! 进程采集实现。

use smalux_core::model::info::{
    MetricLevel, ProcessDetail, ProcessDetailInfo, ProcessInfo, ProcessLight, ProcessLightInfo,
};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

/// 刷新进程列表并返回指定级别的进程信息。
pub(crate) fn sample_processes(
    system: &mut System,
    level: MetricLevel,
    limit: usize,
) -> ProcessInfo {
    if !sysinfo::IS_SUPPORTED_SYSTEM {
        return ProcessInfo::unsupported(
            level,
            "sysinfo does not support this platform".to_string(),
        );
    }

    refresh_processes_for_level(system, level);
    let count = system.processes().len() as u64;

    match level {
        MetricLevel::Count => ProcessInfo::ready_with_level(count, level, None, None),
        MetricLevel::Light => {
            let light = build_process_light_info(system, limit);
            ProcessInfo::ready_with_level(count, level, Some(light), None)
        }
        MetricLevel::Details => {
            let details = build_process_detail_info(system, limit);
            ProcessInfo::ready_with_level(count, level, None, Some(details))
        }
    }
}

/// 根据采集级别选择进程刷新成本。
fn refresh_processes_for_level(system: &mut System, level: MetricLevel) {
    let refresh_kind = match level {
        // 只需要总数时不刷新 CPU、内存、磁盘等进程明细。
        MetricLevel::Count => ProcessRefreshKind::nothing().without_tasks(),
        // light 只需要排序和展示基础资源占用。
        MetricLevel::Light => ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory()
            .without_tasks(),
        // details 是按需诊断路径，可以接受更高成本。
        MetricLevel::Details => ProcessRefreshKind::everything().without_tasks(),
    };

    system.refresh_processes_specifics(ProcessesToUpdate::All, true, refresh_kind);
}

/// 构造进程轻量信息。
fn build_process_light_info(system: &System, limit: usize) -> ProcessLightInfo {
    let mut items = system
        .processes()
        .iter()
        .map(|(pid, process)| ProcessLight {
            pid: pid.as_u32(),
            name: process.name().to_string_lossy().to_string(),
            status: process.status().to_string(),
            cpu_usage: process.cpu_usage(),
            memory_bytes: process.memory(),
        })
        .collect::<Vec<_>>();

    items.sort_by(|left, right| {
        right
            .memory_bytes
            .cmp(&left.memory_bytes)
            .then_with(|| right.cpu_usage.total_cmp(&left.cpu_usage))
            .then_with(|| left.pid.cmp(&right.pid))
    });

    let truncated = items.len() > limit;
    items.truncate(limit);

    ProcessLightInfo {
        limit,
        truncated,
        items,
    }
}

/// 构造进程完整明细。
fn build_process_detail_info(system: &System, limit: usize) -> ProcessDetailInfo {
    let mut items = system
        .processes()
        .iter()
        .map(|(pid, process)| ProcessDetail {
            pid: pid.as_u32(),
            parent_pid: process.parent().map(|pid| pid.as_u32()),
            name: process.name().to_string_lossy().to_string(),
            status: process.status().to_string(),
            cpu_usage: process.cpu_usage(),
            memory_bytes: process.memory(),
            virtual_memory_bytes: process.virtual_memory(),
            start_time: process.start_time(),
            run_time: process.run_time(),
            exe: process.exe().map(|path| path.to_string_lossy().to_string()),
            cmd: process
                .cmd()
                .iter()
                .map(|value| value.to_string_lossy().to_string())
                .collect(),
        })
        .collect::<Vec<_>>();

    items.sort_by(|left, right| {
        right
            .memory_bytes
            .cmp(&left.memory_bytes)
            .then_with(|| right.cpu_usage.total_cmp(&left.cpu_usage))
            .then_with(|| left.pid.cmp(&right.pid))
    });

    let truncated = items.len() > limit;
    items.truncate(limit);

    ProcessDetailInfo {
        limit,
        truncated,
        items,
    }
}

#[cfg(test)]
mod tests {
    //! 进程采集测试。

    use super::*;

    /// 验证进程数量可以被采集。
    #[test]
    fn sample_process_count_returns_count() {
        let mut system = System::new();

        let info = sample_processes(&mut system, MetricLevel::Count, 10);

        if sysinfo::IS_SUPPORTED_SYSTEM {
            assert!(info.count > 0);
        }
        assert_eq!(info.level, MetricLevel::Count);
        assert!(info.light.is_none());
        assert!(info.details.is_none());
        assert!(info.error.is_none() || !info.error.as_ref().unwrap().is_empty());
    }

    /// 验证 light 级别会返回有限进程列表。
    #[test]
    fn sample_process_light_returns_limited_items() {
        let mut system = System::new();

        let info = sample_processes(&mut system, MetricLevel::Light, 5);

        if sysinfo::IS_SUPPORTED_SYSTEM {
            assert!(info.count > 0);
            let light = info.light.unwrap();
            assert!(light.items.len() <= 5);
        } else {
            assert_eq!(
                info.status,
                smalux_core::model::info::MetricStatus::Unsupported
            );
        }
        assert_eq!(info.level, MetricLevel::Light);
    }

    /// 验证 details 级别会返回有限进程明细。
    #[test]
    fn sample_process_details_returns_limited_items() {
        let mut system = System::new();

        let info = sample_processes(&mut system, MetricLevel::Details, 5);

        if sysinfo::IS_SUPPORTED_SYSTEM {
            assert!(info.count > 0);
            let details = info.details.unwrap();
            assert!(details.items.len() <= 5);
        } else {
            assert_eq!(
                info.status,
                smalux_core::model::info::MetricStatus::Unsupported
            );
        }
        assert_eq!(info.level, MetricLevel::Details);
    }
}
