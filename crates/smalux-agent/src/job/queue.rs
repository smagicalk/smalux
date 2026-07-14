//! Job Scheduler 时间项与到期执行优先队列。

use crate::job::{JobId, JobPriority, RunId};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

/// DelayQueue 中的计时项。
#[derive(Debug, Clone)]
pub(crate) struct TimerEntry {
    /// 计时项所属 Job。
    pub job_id: JobId,
    /// 创建计时项时的 Job 版本；版本不一致时直接丢弃。
    pub version: u64,
    /// 用于事件、快照和重新锚定的 UTC 墙上时间。
    pub run_at: DateTime<Utc>,
    /// 正常触发、立即运行或 Retry 的语义信息。
    pub kind: TimerKind,
}

/// 正常 Trigger 和 Retry 使用同一时间队列，但保持独立语义。
#[derive(Debug, Clone)]
pub(crate) enum TimerKind {
    /// Trigger 产生的正常周期计时项。
    Normal,
    /// JobPatch 的 RunNow 产生的一次立即计时项。
    RunNow,
    /// 某次逻辑执行失败后产生的 Retry 计时项。
    Retry {
        /// Retry 前后保持不变的逻辑执行标识。
        run_id: RunId,
        /// 原始 Trigger 计划时间。
        scheduled_at: DateTime<Utc>,
        /// Retry 到期后要执行的尝试次数。
        attempt: u32,
    },
}

/// 已到期、尚未执行的实例类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingKind {
    /// 可被 KeepLatest 或 ReplaceOldestTrigger 替换的正常周期实例。
    Normal,
    /// 一次性 Job 实例，必须使用 Backpressure 保留。
    Once,
    /// 用户明确请求立即执行的实例，不能被普通容量策略丢弃。
    RunNow,
    /// Retry 实例，必须保持原 RunId 且不能被正常 Trigger 替换。
    Retry,
}

impl PendingKind {
    /// 返回该实例是否允许被“保留最新”类策略替换。
    pub fn is_replaceable_trigger(self) -> bool {
        matches!(self, Self::Normal)
    }
}

/// 已到期但尚未开始运行的权威记录。
#[derive(Debug, Clone)]
pub(crate) struct PendingExecution {
    /// 执行实例所属 Job。
    pub job_id: JobId,
    /// 入队时 Job 版本；调度前再次校验。
    pub version: u64,
    /// 单次逻辑执行标识；Retry 保持不变。
    pub run_id: RunId,
    /// 当前尝试次数，从 1 开始。
    pub attempt: u32,
    /// 原始 Trigger 计划时间，用于稳定排序和观测延迟。
    pub scheduled_at: DateTime<Utc>,
    /// Pending 的不可丢失/可替换语义。
    pub kind: PendingKind,
    /// Job 优先级快照；资源 Patch 时会迁移到新值。
    pub priority: JobPriority,
    /// 同一次 DelayQueue 唤醒分配的批次号，旧批次优先于新批次。
    pub batch_id: u64,
}

/// BinaryHeap 使用的轻量排序键；权威 Pending 数据仍保存在 ReadyQueue.pending。
#[derive(Debug, Clone, Eq, PartialEq)]
struct ReadyKey {
    /// 到期批次，数值越小表示越早到期。
    batch_id: u64,
    /// 批次内优先级，数值越大越先执行。
    priority: JobPriority,
    /// 同批次同优先级时，计划时间更早者优先。
    scheduled_at: DateTime<Utc>,
    /// 保证相同时间和优先级下排序稳定的本地序号。
    sequence: u64,
    /// 回查权威 Pending Map 的键。
    run_id: RunId,
}

impl Ord for ReadyKey {
    /// 把业务排序规则转换为 BinaryHeap 的“最大元素优先”顺序。
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap 先弹出“最大”元素，因此较旧批次和较早时间需要反向比较。
        other
            .batch_id
            .cmp(&self.batch_id)
            .then_with(|| self.priority.cmp(&other.priority))
            .then_with(|| other.scheduled_at.cmp(&self.scheduled_at))
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| self.run_id.cmp(&other.run_id))
    }
}

impl PartialOrd for ReadyKey {
    /// ReadyKey 是全序，直接复用 Ord 实现。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 已到期执行总队列。
pub(crate) struct ReadyQueue {
    /// 允许保留失效键的排序堆；达到阈值后统一压缩。
    heap: BinaryHeap<ReadyKey>,
    /// 尚未开始执行的权威记录，删除即表示 Pending 失效。
    pending: HashMap<RunId, PendingExecution>,
    /// 生成排序键时使用的本地递增序号。
    sequence: u64,
}

impl ReadyQueue {
    /// 创建空 ReadyQueue。
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            pending: HashMap::new(),
            sequence: 0,
        }
    }

    /// 返回权威 Pending 数量，不包含 Heap 中的失效键。
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// 判断是否没有任何权威 Pending。
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// 统计指定 Job 的权威 Pending 数量。
    pub fn count_job(&self, job_id: JobId) -> usize {
        self.pending
            .values()
            .filter(|pending| pending.job_id == job_id)
            .count()
    }

    /// 同时写入权威 Map 和排序 Heap，并按需压缩失效键。
    pub fn insert(&mut self, pending: PendingExecution) {
        self.sequence = self.sequence.wrapping_add(1);
        self.heap.push(ReadyKey {
            batch_id: pending.batch_id,
            priority: pending.priority,
            scheduled_at: pending.scheduled_at,
            sequence: self.sequence,
            run_id: pending.run_id,
        });
        self.pending.insert(pending.run_id, pending);
        self.compact_if_needed();
    }

    /// 取出当前排序中第一个满足并发限制的实例。
    ///
    /// 暂时不能运行的键会放回 Heap，因此一个达到单 Job 并发上限的任务不会阻塞其他 Job。
    pub fn pop_dispatchable(
        &mut self,
        mut can_run: impl FnMut(&PendingExecution) -> bool,
    ) -> Option<PendingExecution> {
        let mut blocked = Vec::new();
        let selected = loop {
            let key = self.heap.pop()?;
            let Some(pending) = self.pending.get(&key.run_id) else {
                continue;
            };
            if can_run(pending) {
                break self.pending.remove(&key.run_id);
            }
            blocked.push(key);
        };
        self.heap.extend(blocked);
        selected
    }

    /// 删除同 Job 最旧的可替换正常 Trigger。
    pub fn remove_oldest_trigger(&mut self, job_id: JobId) -> Option<PendingExecution> {
        let run_id = self
            .pending
            .values()
            .filter(|pending| pending.job_id == job_id && pending.kind.is_replaceable_trigger())
            .min_by_key(|pending| pending.scheduled_at)
            .map(|pending| pending.run_id)?;
        self.pending.remove(&run_id)
    }

    /// KeepLatest 使用：删除该 Job 所有尚未执行的正常 Trigger。
    pub fn remove_normal_triggers(&mut self, job_id: JobId) -> Vec<PendingExecution> {
        let run_ids = self
            .pending
            .values()
            .filter(|pending| pending.job_id == job_id && pending.kind.is_replaceable_trigger())
            .map(|pending| pending.run_id)
            .collect::<Vec<_>>();
        run_ids
            .into_iter()
            .filter_map(|run_id| self.pending.remove(&run_id))
            .collect()
    }

    /// 删除指定 Job 的全部 Pending，并返回被移除记录用于事件或统计。
    pub fn remove_job(&mut self, job_id: JobId) -> Vec<PendingExecution> {
        let run_ids = self
            .pending
            .values()
            .filter(|pending| pending.job_id == job_id)
            .map(|pending| pending.run_id)
            .collect::<Vec<_>>();
        run_ids
            .into_iter()
            .filter_map(|run_id| self.pending.remove(&run_id))
            .collect()
    }

    /// 仅修改资源策略时保留 Pending，并写入新版本排序键。
    pub fn migrate_job(&mut self, job_id: JobId, version: u64, priority: JobPriority) {
        let run_ids = self
            .pending
            .values_mut()
            .filter(|pending| pending.job_id == job_id)
            .map(|pending| {
                pending.version = version;
                pending.priority = priority;
                pending.run_id
            })
            .collect::<Vec<_>>();
        for run_id in run_ids {
            if let Some(pending) = self.pending.get(&run_id) {
                self.sequence = self.sequence.wrapping_add(1);
                self.heap.push(ReadyKey {
                    batch_id: pending.batch_id,
                    priority: pending.priority,
                    scheduled_at: pending.scheduled_at,
                    sequence: self.sequence,
                    run_id,
                });
            }
        }
        self.compact_if_needed();
    }

    /// 清空权威 Map 和排序 Heap。
    pub fn clear(&mut self) {
        self.heap.clear();
        self.pending.clear();
    }

    /// 失效 Heap 键明显多于权威数据时重建 Heap，限制惰性删除的内存占用。
    fn compact_if_needed(&mut self) {
        if self.heap.len() <= self.pending.len().saturating_mul(2).saturating_add(64) {
            return;
        }
        self.heap = self
            .pending
            .values()
            .map(|pending| ReadyKey {
                batch_id: pending.batch_id,
                priority: pending.priority,
                scheduled_at: pending.scheduled_at,
                sequence: {
                    self.sequence = self.sequence.wrapping_add(1);
                    self.sequence
                },
                run_id: pending.run_id,
            })
            .collect();
    }
}

impl Default for ReadyQueue {
    /// 默认创建空 ReadyQueue。
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(batch_id: u64, priority: JobPriority) -> PendingExecution {
        PendingExecution {
            job_id: JobId::new_v4(),
            version: 0,
            run_id: RunId::new_v4(),
            attempt: 1,
            scheduled_at: Utc::now(),
            kind: PendingKind::Normal,
            priority,
            batch_id,
        }
    }

    #[test]
    fn older_batch_wins_before_priority() {
        let mut queue = ReadyQueue::new();
        let old = pending(1, JobPriority::LOW);
        let new = pending(2, JobPriority::CRITICAL);
        let old_id = old.run_id;
        queue.insert(new);
        queue.insert(old);

        assert_eq!(queue.pop_dispatchable(|_| true).unwrap().run_id, old_id);
    }

    #[test]
    fn blocked_job_does_not_block_another_job() {
        let mut queue = ReadyQueue::new();
        let blocked = pending(1, JobPriority::HIGH);
        let allowed = pending(2, JobPriority::LOW);
        let blocked_job = blocked.job_id;
        let allowed_id = allowed.run_id;
        queue.insert(blocked);
        queue.insert(allowed);

        assert_eq!(
            queue
                .pop_dispatchable(|pending| pending.job_id != blocked_job)
                .unwrap()
                .run_id,
            allowed_id
        );
    }
}
