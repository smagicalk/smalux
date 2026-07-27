//! Scheduler Actor 运行循环、执行队列和生命周期管理。

mod execution;
mod management;
mod scheduling;
mod timers;

use super::timing::{effective_queue_policies, first_run_at, validate_config, validate_trigger};
use super::{Command, TIMER_REANCHOR_INTERVAL};
use crate::scheduler::event::{SchedulerEvent, SchedulerEventKind};
use crate::scheduler::queue::{PendingExecution, PendingKind, ReadyQueue, TimerEntry, TimerKind};
use crate::scheduler::task::{ScheduledTask, TaskCancellationMode, TaskRunResult};
use crate::scheduler::*;
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::time::{DelayQueue, delay_queue};

/// Actor 内保存的 Job 权威状态；只能由 Actor 线程修改。
struct JobEntry {
    /// Job 唯一标识。
    id: JobId,
    /// 当前乐观锁版本，任何语义变更都会递增。
    version: u64,
    /// 类型擦除后的 Task 和输出适配器。
    task: Arc<dyn ScheduledTask>,
    /// Task 在取消、超时和关闭时必须遵循的完成语义。
    cancellation_mode: TaskCancellationMode,
    /// 当前触发规则和单次超时。
    trigger: Trigger,
    /// Job 专属并发限制；None 表示继承配置。
    concurrency: Option<NonZeroUsize>,
    /// Job 专属 Pending 限制；None 表示继承配置。
    max_pending: Option<usize>,
    /// 到期批次内使用的优先级。
    priority: JobPriority,
    /// 正常 Trigger 的合并策略。
    coalescing: TriggerCoalescing,
    /// Pending 达到容量时的处理策略。
    capacity: CapacityPolicy,
    /// Task 失败后的执行重试策略。
    retry: ExecutionRetryPolicy,
    /// 最终失败累计和自动停用策略。
    failure: FailurePolicy,
    /// 当前生命周期状态。
    state: JobState,
    /// DelayQueue 中唯一正常计时项的 Key。
    normal_timer_key: Option<delay_queue::Key>,
    /// 与 normal_timer_key 对应的 UTC 时间。
    next_run_at: Option<DateTime<Utc>>,
    /// Backpressure 暂存且尚未进入 ReadyQueue 的实例。
    blocked: VecDeque<PendingExecution>,
    /// 当前仍占用该 Job 并发槽位的实例数。
    running_count: usize,
    /// 连续最终失败次数，成功后归零。
    consecutive_failures: u32,
    /// 最近一次实际开始时间。
    last_started_at: Option<DateTime<Utc>>,
    /// 最近一次完成时间。
    last_finished_at: Option<DateTime<Utc>>,
    /// 最近一次最终结果摘要。
    last_outcome: Option<String>,
    /// 最近一次开始执行实例的原计划时间。
    last_fired_at: Option<DateTime<Utc>>,
    /// 创建时间。
    created_at: DateTime<Utc>,
    /// 配置或状态更新时间。
    updated_at: DateTime<Utc>,
}

/// 正在执行实例的取消索引。
struct RunningExecution {
    /// 实例所属 Job。
    job_id: JobId,
    /// 实例启动时的 Job 版本。
    version: u64,
    /// 删除、停用、超时或关闭时触发的取消令牌。
    cancellation: CancellationToken,
}

/// Retry 定时项索引，用于版本失效时立即清理并修正墙上时间偏移。
struct RetryTimer {
    /// Retry 所属 Job，用于按 Job 批量清理。
    job_id: JobId,
    /// DelayQueue 中计时项的 Key。
    key: delay_queue::Key,
    /// Retry UTC 墙上时间，用于时钟重新锚定。
    run_at: DateTime<Utc>,
}

/// JoinSet 返回给 Actor 的完整执行结果。
struct TaskCompletion {
    /// 执行所属 Job。
    job_id: JobId,
    /// 执行启动时的 Job 版本。
    version: u64,
    /// 单次逻辑执行标识。
    run_id: RunId,
    /// 本次尝试次数。
    attempt: u32,
    /// Trigger 原计划时间。
    scheduled_at: DateTime<Utc>,
    /// 本次尝试的单调时钟耗时。
    elapsed: Duration,
    /// Task、Callback、超时、panic 或取消结果。
    outcome: CompletionOutcome,
}

/// 失败处理需要的轻量执行元数据，避免传递并移动完整 TaskCompletion。
#[derive(Clone, Copy)]
struct ExecutionMetadata {
    /// 执行所属 Job。
    job_id: JobId,
    /// 执行启动时的 Job 版本。
    version: u64,
    /// 单次逻辑执行标识。
    run_id: RunId,
    /// 本次尝试次数。
    attempt: u32,
    /// Trigger 原计划时间。
    scheduled_at: DateTime<Utc>,
}

impl TaskCompletion {
    /// 提取失败处理需要且可 Copy 的执行元数据。
    fn metadata(&self) -> ExecutionMetadata {
        ExecutionMetadata {
            job_id: self.job_id,
            version: self.version,
            run_id: self.run_id,
            attempt: self.attempt,
            scheduled_at: self.scheduled_at,
        }
    }
}

/// Scheduler 包装层观察到的单次尝试结果。
enum CompletionOutcome {
    /// Task 和输出适配器正常返回的统一结果。
    Finished(TaskRunResult),
    /// 超过 Trigger timeout。
    TimedOut,
    /// Task Future panic，携带已格式化 payload。
    Panicked(String),
    /// CancellationToken 先于执行 Future 完成。
    Cancelled,
}

/// 独占所有 Job 和队列状态的单线程异步 Actor。
pub(super) struct SchedulerActor {
    /// 当前动态配置。
    config: SchedulerConfig,
    /// 配置乐观锁 revision。
    config_revision: u64,
    /// JobId 到权威 Job 状态的映射。
    jobs: HashMap<JobId, JobEntry>,
    /// 正常 Trigger 与 Retry 共用的单调时间队列。
    timers: DelayQueue<TimerEntry>,
    /// RunId 到 Retry Timer 的反向索引。
    retry_timers: HashMap<RunId, RetryTimer>,
    /// 已到期、等待并发槽位的优先队列。
    ready: ReadyQueue,
    /// 所有正在执行的 Task 包装 Future。
    tasks: JoinSet<TaskCompletion>,
    /// RunId 到取消信息的索引。
    running: HashMap<RunId, RunningExecution>,
    /// 当前全局占用的并发槽位数。
    total_running: usize,
    /// Scheduler 句柄提交命令的接收端。
    command_rx: mpsc::Receiver<Command>,
    /// 生命周期事件广播发送端。
    event_tx: broadcast::Sender<Arc<SchedulerEvent>>,
    /// 最新 SchedulerStatus 发送端。
    status_tx: watch::Sender<SchedulerStatus>,
    /// Runtime Drop 时使用的兜底关闭令牌。
    shutdown: CancellationToken,
    /// 事件本地递增序号。
    event_sequence: u64,
    /// DelayQueue 每次到期批处理的递增批次号。
    batch_sequence: u64,
    /// Shutdown 命令的响应端，优雅关闭完成后回复。
    shutdown_response: Option<oneshot::Sender<Result<(), SchedulerError>>>,
}

impl SchedulerActor {
    /// 使用已创建的通道构造空 Actor；Job、Timer 和运行实例初始均为空。
    pub(super) fn new(
        config: SchedulerConfig,
        command_rx: mpsc::Receiver<Command>,
        event_tx: broadcast::Sender<Arc<SchedulerEvent>>,
        status_tx: watch::Sender<SchedulerStatus>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            config_revision: 0,
            jobs: HashMap::new(),
            timers: DelayQueue::new(),
            retry_timers: HashMap::new(),
            ready: ReadyQueue::new(),
            tasks: JoinSet::new(),
            running: HashMap::new(),
            total_running: 0,
            command_rx,
            event_tx,
            status_tx,
            shutdown,
            event_sequence: 0,
            batch_sequence: 0,
            shutdown_response: None,
        }
    }

    /// 运行 Actor 主循环，串行处理命令、完成事件、到期 Timer 和时钟维护。
    ///
    /// 每轮 select 前先恢复 Backpressure 并派发 ReadyQueue，确保释放槽位后尽快推进任务。
    pub(super) async fn run(mut self) -> Result<(), SchedulerError> {
        self.emit(SchedulerEventKind::SchedulerStarted);
        let mut maintenance = tokio::time::interval(TIMER_REANCHOR_INTERVAL);
        maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            self.restore_blocked();
            self.dispatch_ready();

            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                command = self.command_rx.recv() => {
                    let Some(command) = command else { break };
                    if self.handle_command(command)? { break; }
                }
                completion = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(result) = completion {
                        match result {
                            Ok(completion) => self.handle_completion(completion),
                            Err(error) => {
                                tracing::error!(error = %error, "scheduler task wrapper failed");
                            }
                        }
                    }
                }
                expired = self.timers.next(), if !self.timers.is_empty() => {
                    if let Some(expired) = expired {
                        self.handle_expired_batch(expired.into_inner());
                    }
                }
                _ = maintenance.tick() => {
                    self.reanchor_timers();
                }
            }
        }

        self.graceful_shutdown().await;
        Ok(())
    }

    /// 串行执行一条命令并通过 Oneshot 返回结果。
    ///
    /// 返回 true 表示收到 Shutdown，主循环应进入优雅关闭。
    fn handle_command(&mut self, command: Command) -> Result<bool, SchedulerError> {
        match command {
            Command::Add {
                job_id,
                generation,
                enabled,
                trigger,
                task,
                options,
                response,
            } => {
                let _ = response
                    .send(self.add_job(job_id, generation, enabled, trigger, task, options));
            }
            Command::Update {
                job_id,
                expected_version,
                patch,
                response,
            } => {
                let _ = response.send(self.update_job(job_id, expected_version, patch));
            }
            Command::Get { job_id, response } => {
                let result = Ok(self.jobs.get(&job_id).map(|job| self.snapshot(job)));
                let _ = response.send(result);
            }
            Command::List { response } => {
                let result = Ok(self.jobs.values().map(|job| self.snapshot(job)).collect());
                let _ = response.send(result);
            }
            Command::Enable {
                job_id,
                expected_version,
                response,
            } => {
                let _ = response.send(self.enable_job(job_id, expected_version));
            }
            Command::Disable {
                job_id,
                expected_version,
                reason,
                response,
            } => {
                let _ = response.send(self.disable_job(job_id, expected_version, reason));
            }
            Command::Delete {
                job_id,
                expected_version,
                response,
            } => {
                let _ = response.send(self.delete_job(job_id, expected_version, true));
            }
            Command::GetConfig { response } => {
                let _ = response.send(Ok(SchedulerConfigSnapshot {
                    revision: self.config_revision,
                    config: self.config.clone(),
                }));
            }
            Command::UpdateConfig {
                expected_revision,
                patch,
                response,
            } => {
                let _ = response.send(self.update_config(expected_revision, patch));
            }
            Command::Shutdown { response } => {
                self.shutdown_response = Some(response);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// 因永久错误或失败阈值自动停用 Job。
    ///
    /// 该操作递增版本、清理全部 Timer/Pending，并取消旧版本运行实例。
    fn force_disable(&mut self, job_id: JobId, reason: String) -> Result<(), SchedulerError> {
        let (old_version, version) = {
            let job = self
                .jobs
                .get_mut(&job_id)
                .ok_or(SchedulerError::JobNotFound(job_id))?;
            let old_version = job.version;
            job.version = job
                .version
                .checked_add(1)
                .ok_or(SchedulerError::VersionOverflow(job_id))?;
            if let Some(key) = job.normal_timer_key.take() {
                let _ = self.timers.try_remove(&key);
            }
            job.next_run_at = None;
            job.blocked.clear();
            job.state = JobState::Disabled {
                reason: reason.clone(),
            };
            job.updated_at = Utc::now();
            (old_version, job.version)
        };
        self.clear_retry_timers(job_id);
        self.ready.remove_job(job_id);
        self.cancel_job_version(job_id, old_version);
        self.emit(SchedulerEventKind::JobDisabled {
            job_id,
            version,
            reason,
        });
        Ok(())
    }

    /// 触发指定 Job 版本所有运行实例的协作取消令牌。
    fn cancel_job_version(&self, job_id: JobId, version: u64) {
        for running in self.running.values() {
            if running.job_id == job_id && running.version == version {
                running.cancellation.cancel();
            }
        }
    }

    /// 分配事件序号并广播调度事件；没有订阅者时允许静默丢弃。
    fn emit(&mut self, kind: SchedulerEventKind) {
        self.event_sequence = self.event_sequence.wrapping_add(1);
        let event = Arc::new(SchedulerEvent {
            sequence: self.event_sequence,
            emitted_at: Utc::now(),
            kind,
        });
        let _ = self.event_tx.send(event);
    }

    /// 清空未执行工作、取消运行实例，并在超时后强制 Abort 剩余包装任务。
    async fn graceful_shutdown(&mut self) {
        let _ = self.status_tx.send(SchedulerStatus::Stopping);
        self.emit(SchedulerEventKind::SchedulerStopping);
        self.ready.clear();
        self.timers.clear();
        self.retry_timers.clear();
        for running in self.running.values() {
            running.cancellation.cancel();
        }

        let deadline = tokio::time::sleep(self.config.shutdown_timeout);
        tokio::pin!(deadline);
        loop {
            if self.tasks.is_empty() {
                break;
            }
            tokio::select! {
                _ = &mut deadline => {
                    self.tasks.abort_all();
                    while self.tasks.join_next().await.is_some() {}
                    break;
                }
                _ = self.tasks.join_next() => {}
            }
        }
        self.running.clear();
        self.total_running = 0;
        let _ = self.status_tx.send(SchedulerStatus::Stopped);
        self.emit(SchedulerEventKind::SchedulerStopped);
        if let Some(response) = self.shutdown_response.take() {
            let _ = response.send(Ok(()));
        }
    }
}

/// 用于选择 Retry 条件和最终失败计数规则的内部失败分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureClass {
    /// Task 返回临时错误。
    Transient,
    /// Task 返回永久错误。
    Permanent,
    /// Scheduler timeout 到期。
    Timeout,
    /// Task Future panic。
    Panic,
}
