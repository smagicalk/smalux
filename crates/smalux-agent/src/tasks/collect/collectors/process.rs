//! 本机进程数量、轻量列表和资源明细采集。

use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    time::Instant,
};

use serde::Serialize;
use sysinfo::{ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System, UpdateKind};

use crate::tasks::collect::CollectionMode;
pub use smalux_protocol::agent::v1::{ProcessRanking, ProcessSelection};

/// Basic 模式未配置上限时最多返回的进程数量。
pub const DEFAULT_BASIC_PROCESS_ENTRIES: usize = 256;
/// Detailed 模式未配置上限时最多返回的进程数量。
pub const DEFAULT_DETAILED_PROCESS_ENTRIES: usize = 128;

fn process_matches(selection: &ProcessSelection, pid: u32, name: &str) -> bool {
    if !selection.include_pids.is_empty() || !selection.include_names.is_empty() {
        return selection.include_pids.contains(&pid)
            || selection.include_names.iter().any(|value| value == name);
    }
    !selection.exclude_names.iter().any(|value| value == name)
}

/// 跨平台稳定的进程状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    /// 空闲内核线程。
    Idle,
    /// 正在运行。
    Running,
    /// 可中断睡眠。
    Sleeping,
    /// 已停止。
    Stopped,
    /// 僵尸进程。
    Zombie,
    /// 正被跟踪或调试。
    Tracing,
    /// 已结束。
    Dead,
    /// 等待内核强制唤醒。
    Wakekill,
    /// 正在唤醒。
    Waking,
    /// 已驻留等待。
    Parked,
    /// 被锁阻塞。
    LockBlocked,
    /// 不可中断磁盘睡眠。
    UninterruptibleDiskSleep,
    /// 被系统挂起。
    Suspended,
    /// 平台返回无法识别的状态。
    Unknown,
}

/// 一个进程状态及其匹配进程数量。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessStateCount {
    /// 稳定状态值。
    pub state: ProcessState,
    /// 处于该状态的匹配进程数量。
    pub count: usize,
}

/// Detailed 模式才返回的资源和命令字段。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProcessDetails {
    /// 进程总 CPU 使用率百分比，多核进程可能大于 100。
    pub cpu_usage_percent: f32,
    /// 当前映射到物理内存的字节数。
    pub memory_bytes: u64,
    /// 当前虚拟内存字节数。
    pub virtual_memory_bytes: u64,
    /// 自上一次刷新后的读取字节数；Windows 表示全部 IO。
    pub read_bytes: u64,
    /// 自上一次刷新后的写入字节数；Windows 表示全部 IO。
    pub written_bytes: u64,
    /// 进程累计读取字节数。
    pub total_read_bytes: u64,
    /// 进程累计写入字节数。
    pub total_written_bytes: u64,
    /// 可读取时返回可执行文件完整路径。
    pub executable: Option<String>,
    /// 命令及参数；权限不足时可能为空。
    pub command: Vec<String>,
}

/// Basic 或 Detailed 模式中的一条进程记录。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProcessEntry {
    /// 系统进程 ID。
    pub pid: u32,
    /// 父进程 ID；系统无法提供时为 `None`。
    pub parent_pid: Option<u32>,
    /// 平台返回的进程名称。
    pub name: String,
    /// 当前进程状态。
    pub state: ProcessState,
    /// Unix 纪元起的进程启动秒数。
    pub started_at_seconds: u64,
    /// Detailed 模式的资源和命令字段；Basic 模式为 `None`。
    pub details: Option<ProcessDetails>,
}

/// 一次本机进程采集结果。
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessSnapshot {
    /// 实际使用的采集档位。
    pub mode: CollectionMode,
    /// 系统枚举到的真实进程总数，不包含 Linux threads/tasks。
    pub total_processes: usize,
    /// 应用 PID/名称选择后的进程数量。
    pub matched_processes: usize,
    /// 对匹配进程按状态聚合的计数。
    pub states: Vec<ProcessStateCount>,
    /// Basic 或 Detailed 模式返回的有限列表。
    pub entries: Vec<ProcessEntry>,
    /// 匹配数量是否超过实际返回的列表数量。
    pub truncated: bool,
    /// Detailed CPU 数据是否已经跨过 sysinfo 要求的最小采样间隔。
    pub cpu_warmed_up: bool,
}

/// 进程 Task 配置不满足模式约束时返回的错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProcessConfigError {
    /// protobuf 模式使用了零值或未知值。
    #[error("process collection mode is unspecified or unknown")]
    InvalidMode,
    /// protobuf 排名使用了零值或未知值。
    #[error("process ranking is unspecified or unknown")]
    InvalidRanking,
    /// 显式列表上限必须大于零。
    #[error("process max_entries must be greater than zero")]
    InvalidMaxEntries,
    /// Summary/Basic 不允许高成本 TopN 排名。
    #[error("CPU or memory ranking requires detailed process collection")]
    RankingRequiresDetailed,
    /// 进程名称筛选不能包含空字符串。
    #[error("process name selection cannot contain blank values")]
    BlankProcessName,
}

/// 持有 sysinfo 进程刷新状态和 CPU 采样基线的采集器。
pub struct ProcessCollector {
    system: System,
    last_cpu_sampled_at: Option<Instant>,
    cpu_baseline_pids: HashSet<u32>,
}

impl ProcessCollector {
    /// 创建尚未建立进程 CPU 基线的采集器。
    pub fn new() -> Self {
        Self {
            system: System::new(),
            last_cpu_sampled_at: None,
            cpu_baseline_pids: HashSet::new(),
        }
    }

    /// 校验模式、选择和排名组合。
    pub fn validate(
        mode: CollectionMode,
        selection: &ProcessSelection,
        ranking: ProcessRanking,
    ) -> Result<(), ProcessConfigError> {
        if mode == CollectionMode::Unspecified {
            return Err(ProcessConfigError::InvalidMode);
        }
        if ranking == ProcessRanking::Unspecified {
            return Err(ProcessConfigError::InvalidRanking);
        }
        if mode != CollectionMode::Detailed && ranking != ProcessRanking::Pid {
            return Err(ProcessConfigError::RankingRequiresDetailed);
        }
        if selection
            .include_names
            .iter()
            .chain(&selection.exclude_names)
            .any(|name| name.trim().is_empty())
        {
            return Err(ProcessConfigError::BlankProcessName);
        }
        Ok(())
    }

    /// 按档位、筛选、排名和列表上限采集进程。
    pub fn collect(
        &mut self,
        mode: CollectionMode,
        selection: &ProcessSelection,
        ranking: ProcessRanking,
        max_entries: Option<NonZeroUsize>,
    ) -> Result<ProcessSnapshot, ProcessConfigError> {
        Self::validate(mode, selection, ranking)?;
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().without_tasks(),
        );

        let total_processes = self.system.processes().len();
        let mut states = BTreeMap::<ProcessState, usize>::new();
        let mut matched = self
            .system
            .processes()
            .iter()
            .filter_map(|(pid, process)| {
                let pid = pid.as_u32();
                let name = process.name().to_string_lossy();
                process_matches(selection, pid, &name).then_some(pid)
            })
            .collect::<Vec<_>>();
        matched.sort_unstable();

        for pid in &matched {
            if let Some(process) = self.system.process(sysinfo::Pid::from_u32(*pid)) {
                *states.entry(process_state(process.status())).or_default() += 1;
            }
        }

        let matched_processes = matched.len();
        let limit = resolved_limit(mode, max_entries);
        let (entries, cpu_warmed_up) = match mode {
            CollectionMode::Unspecified => (Vec::new(), false),
            CollectionMode::Summary => (Vec::new(), false),
            CollectionMode::Basic => (
                matched
                    .into_iter()
                    .take(limit)
                    .filter_map(|pid| self.process_entry(pid, false))
                    .collect(),
                false,
            ),
            CollectionMode::Detailed => self.collect_detailed_entries(matched, ranking, limit),
        };
        Ok(ProcessSnapshot {
            mode,
            total_processes,
            matched_processes,
            states: states
                .into_iter()
                .map(|(state, count)| ProcessStateCount { state, count })
                .collect(),
            truncated: mode != CollectionMode::Summary && matched_processes > entries.len(),
            entries,
            cpu_warmed_up,
        })
    }

    fn collect_detailed_entries(
        &mut self,
        mut matched: Vec<u32>,
        ranking: ProcessRanking,
        limit: usize,
    ) -> (Vec<ProcessEntry>, bool) {
        let mut cpu_refreshed_pids = None;
        if ranking == ProcessRanking::CpuUsage {
            self.refresh_pids(&matched, ProcessRefreshKind::nothing().with_cpu());
            cpu_refreshed_pids = Some(matched.iter().copied().collect::<HashSet<_>>());
            matched.sort_by(|left, right| {
                let left_usage = self.process_cpu(*left);
                let right_usage = self.process_cpu(*right);
                right_usage.total_cmp(&left_usage).then(left.cmp(right))
            });
        } else if ranking == ProcessRanking::Memory {
            self.refresh_pids(&matched, ProcessRefreshKind::nothing().with_memory());
            matched.sort_by(|left, right| {
                self.process_memory(*right)
                    .cmp(&self.process_memory(*left))
                    .then(left.cmp(right))
            });
        }

        let selected = matched.into_iter().take(limit).collect::<Vec<_>>();
        let mut details = ProcessRefreshKind::nothing()
            .with_memory()
            .with_disk_usage()
            .with_exe(UpdateKind::OnlyIfNotSet)
            .with_cmd(UpdateKind::OnlyIfNotSet)
            .without_tasks();
        if ranking != ProcessRanking::CpuUsage {
            details = details.with_cpu();
        }
        self.refresh_pids(&selected, details);

        let now = Instant::now();
        let cpu_warmed_up = self
            .last_cpu_sampled_at
            .replace(now)
            .is_some_and(|previous| {
                now.duration_since(previous) >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL
                    && selected
                        .iter()
                        .all(|pid| self.cpu_baseline_pids.contains(pid))
            });
        self.cpu_baseline_pids =
            cpu_refreshed_pids.unwrap_or_else(|| selected.iter().copied().collect());

        (
            selected
                .into_iter()
                .filter_map(|pid| self.process_entry(pid, true))
                .collect(),
            cpu_warmed_up,
        )
    }

    fn refresh_pids(&mut self, pids: &[u32], refresh: ProcessRefreshKind) {
        let pids = pids
            .iter()
            .copied()
            .map(sysinfo::Pid::from_u32)
            .collect::<Vec<_>>();
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&pids),
            true,
            refresh.without_tasks(),
        );
    }

    fn process_cpu(&self, pid: u32) -> f32 {
        self.system
            .process(sysinfo::Pid::from_u32(pid))
            .map(sysinfo::Process::cpu_usage)
            .unwrap_or_default()
    }

    fn process_memory(&self, pid: u32) -> u64 {
        self.system
            .process(sysinfo::Pid::from_u32(pid))
            .map(sysinfo::Process::memory)
            .unwrap_or_default()
    }

    fn process_entry(&self, pid: u32, detailed: bool) -> Option<ProcessEntry> {
        let process = self.system.process(sysinfo::Pid::from_u32(pid))?;
        Some(ProcessEntry {
            pid,
            parent_pid: process.parent().map(sysinfo::Pid::as_u32),
            name: process.name().to_string_lossy().into_owned(),
            state: process_state(process.status()),
            started_at_seconds: process.start_time(),
            details: detailed.then(|| {
                let disk = process.disk_usage();
                ProcessDetails {
                    cpu_usage_percent: process.cpu_usage(),
                    memory_bytes: process.memory(),
                    virtual_memory_bytes: process.virtual_memory(),
                    read_bytes: disk.read_bytes,
                    written_bytes: disk.written_bytes,
                    total_read_bytes: disk.total_read_bytes,
                    total_written_bytes: disk.total_written_bytes,
                    executable: process
                        .exe()
                        .map(|path| path.to_string_lossy().into_owned()),
                    command: process
                        .cmd()
                        .iter()
                        .map(|value| value.to_string_lossy().into_owned())
                        .collect(),
                }
            }),
        })
    }
}

impl Default for ProcessCollector {
    fn default() -> Self {
        Self::new()
    }
}

fn resolved_limit(mode: CollectionMode, configured: Option<NonZeroUsize>) -> usize {
    if mode == CollectionMode::Summary {
        return 0;
    }
    configured.map(NonZeroUsize::get).unwrap_or(match mode {
        CollectionMode::Unspecified => 0,
        CollectionMode::Summary => 0,
        CollectionMode::Basic => DEFAULT_BASIC_PROCESS_ENTRIES,
        CollectionMode::Detailed => DEFAULT_DETAILED_PROCESS_ENTRIES,
    })
}

fn process_state(status: ProcessStatus) -> ProcessState {
    match status {
        ProcessStatus::Idle => ProcessState::Idle,
        ProcessStatus::Run => ProcessState::Running,
        ProcessStatus::Sleep => ProcessState::Sleeping,
        ProcessStatus::Stop => ProcessState::Stopped,
        ProcessStatus::Zombie => ProcessState::Zombie,
        ProcessStatus::Tracing => ProcessState::Tracing,
        ProcessStatus::Dead => ProcessState::Dead,
        ProcessStatus::Wakekill => ProcessState::Wakekill,
        ProcessStatus::Waking => ProcessState::Waking,
        ProcessStatus::Parked => ProcessState::Parked,
        ProcessStatus::LockBlocked => ProcessState::LockBlocked,
        ProcessStatus::UninterruptibleDiskSleep => ProcessState::UninterruptibleDiskSleep,
        ProcessStatus::Suspended => ProcessState::Suspended,
        ProcessStatus::Unknown(_) => ProcessState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_counts_processes_without_returning_a_list() {
        let snapshot = ProcessCollector::new()
            .collect(
                CollectionMode::Summary,
                &ProcessSelection::default(),
                ProcessRanking::Pid,
                None,
            )
            .unwrap();

        assert!(snapshot.total_processes >= 1);
        assert_eq!(snapshot.total_processes, snapshot.matched_processes);
        assert!(snapshot.entries.is_empty());
        assert!(!snapshot.truncated);
    }

    #[test]
    fn basic_returns_the_current_process_without_detailed_resources() {
        let current_pid = std::process::id();
        let snapshot = ProcessCollector::new()
            .collect(
                CollectionMode::Basic,
                &ProcessSelection {
                    include_pids: vec![current_pid],
                    ..ProcessSelection::default()
                },
                ProcessRanking::Pid,
                None,
            )
            .unwrap();

        assert_eq!(snapshot.matched_processes, 1);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].pid, current_pid);
        assert!(snapshot.entries[0].details.is_none());
    }

    #[test]
    fn detailed_returns_resources_and_command_for_the_selected_process() {
        let current_pid = std::process::id();
        let snapshot = ProcessCollector::new()
            .collect(
                CollectionMode::Detailed,
                &ProcessSelection {
                    include_pids: vec![current_pid],
                    ..ProcessSelection::default()
                },
                ProcessRanking::Pid,
                None,
            )
            .unwrap();

        assert_eq!(snapshot.entries.len(), 1);
        let details = snapshot.entries[0]
            .details
            .as_ref()
            .expect("detailed mode should return resources");
        assert!(details.memory_bytes > 0);
        assert!(!details.command.is_empty());
    }

    #[test]
    fn include_selection_has_priority_over_excludes() {
        let selection = ProcessSelection {
            include_pids: vec![7],
            include_names: vec!["allowed".to_owned()],
            exclude_names: vec!["allowed".to_owned(), "blocked".to_owned()],
        };

        assert!(process_matches(&selection, 7, "blocked"));
        assert!(process_matches(&selection, 8, "allowed"));
        assert!(!process_matches(&selection, 8, "blocked"));
    }

    #[test]
    fn expensive_ranking_is_rejected_outside_detailed_mode() {
        let error = ProcessCollector::validate(
            CollectionMode::Basic,
            &ProcessSelection::default(),
            ProcessRanking::CpuUsage,
        )
        .unwrap_err();

        assert_eq!(error, ProcessConfigError::RankingRequiresDetailed);
    }

    #[test]
    fn detailed_memory_ranking_is_descending_and_respects_the_limit() {
        let snapshot = ProcessCollector::new()
            .collect(
                CollectionMode::Detailed,
                &ProcessSelection::default(),
                ProcessRanking::Memory,
                NonZeroUsize::new(4),
            )
            .unwrap();

        assert!(snapshot.entries.len() <= 4);
        assert_eq!(
            snapshot.truncated,
            snapshot.matched_processes > snapshot.entries.len()
        );
        assert!(snapshot.entries.windows(2).all(|pair| {
            pair[0].details.as_ref().unwrap().memory_bytes
                >= pair[1].details.as_ref().unwrap().memory_bytes
        }));
    }

    #[test]
    fn detailed_cpu_ranking_is_descending_and_respects_the_limit() {
        let snapshot = ProcessCollector::new()
            .collect(
                CollectionMode::Detailed,
                &ProcessSelection::default(),
                ProcessRanking::CpuUsage,
                NonZeroUsize::new(4),
            )
            .unwrap();

        assert!(snapshot.entries.len() <= 4);
        assert_eq!(
            snapshot.truncated,
            snapshot.matched_processes > snapshot.entries.len()
        );
        assert!(snapshot.entries.windows(2).all(|pair| {
            pair[0].details.as_ref().unwrap().cpu_usage_percent
                >= pair[1].details.as_ref().unwrap().cpu_usage_percent
        }));
    }

    #[test]
    fn repeated_detailed_collection_warms_up_process_cpu_values() {
        let current_pid = std::process::id();
        let selection = ProcessSelection {
            include_pids: vec![current_pid],
            ..ProcessSelection::default()
        };
        let mut collector = ProcessCollector::new();

        let first = collector
            .collect(
                CollectionMode::Detailed,
                &selection,
                ProcessRanking::Pid,
                None,
            )
            .unwrap();
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let second = collector
            .collect(
                CollectionMode::Detailed,
                &selection,
                ProcessRanking::Pid,
                None,
            )
            .unwrap();

        assert!(!first.cpu_warmed_up);
        assert!(second.cpu_warmed_up);
    }
}
