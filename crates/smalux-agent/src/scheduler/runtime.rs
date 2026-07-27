//! Scheduler 并发句柄、唯一生命周期 Runtime 和公共命令接口。

mod engine;
mod timing;

use super::event::SchedulerEvent;
use super::task::{ActionTask, AsyncCallback, ReportingTask, TaskBinding, ValueTask};
use super::*;
use engine::SchedulerActor;
use smalux_protocol::agent::v1::TaskResult;
use std::sync::Arc;
use std::time::Duration;
use timing::validate_config;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const TIMER_REANCHOR_INTERVAL: Duration = Duration::from_secs(30);

/// 可跨线程 Clone 的异步调度句柄。
///
/// 所有方法只向单一 Actor 提交命令，因此多个线程共享 Scheduler 不会直接并发修改内部 Map 或队列。
#[derive(Clone)]
pub struct Scheduler {
    /// 命令、事件和状态通道的共享发送端。
    inner: Arc<SchedulerInner>,
}

/// Scheduler 各类通道的共享所有者。
struct SchedulerInner {
    /// 向 Actor 提交 CRUD 和关闭命令的有界通道。
    command_tx: mpsc::Sender<Command>,
    /// 向多个观察者广播调度生命周期事件。
    event_tx: broadcast::Sender<Arc<SchedulerEvent>>,
    /// 保存最新 SchedulerStatus 的 Watch 通道。
    status_tx: watch::Sender<SchedulerStatus>,
}

/// Scheduler Actor 的唯一生命周期所有者。
///
/// Drop 会触发取消，但生产代码应优先调用 [`SchedulerRuntime::shutdown`]，等待运行实例退出并获得错误结果。
pub struct SchedulerRuntime {
    /// 提供给业务线程 Clone 使用的命令句柄。
    scheduler: Scheduler,
    /// Actor Tokio 任务；Option 用于确保只等待一次。
    actor: Option<JoinHandle<Result<(), SchedulerError>>>,
    /// Runtime 被直接 Drop 或命令通道关闭时使用的兜底取消令牌。
    shutdown: CancellationToken,
}

impl SchedulerRuntime {
    /// 校验配置并在当前 Tokio Runtime 上启动 Scheduler Actor。
    ///
    /// # 错误
    ///
    /// 配置包含零容量/非法限制，或当前线程不在 Tokio Runtime 中时返回错误。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// let runtime = SchedulerRuntime::start(SchedulerConfig::default())?;
    /// let scheduler = runtime.scheduler();
    /// // 使用 scheduler 注册 Job，程序退出前调用 runtime.shutdown().await。
    /// # Ok::<(), SchedulerError>(())
    /// ```
    pub fn start(config: SchedulerConfig) -> Result<Self, SchedulerError> {
        validate_config(&config)?;
        tokio::runtime::Handle::try_current().map_err(|error| {
            SchedulerError::Actor(format!("Tokio runtime is required: {error}"))
        })?;

        let (command_tx, command_rx) = mpsc::channel(config.command_channel_capacity);
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity);
        let (status_tx, _) = watch::channel(SchedulerStatus::Running);
        let shutdown = CancellationToken::new();
        let actor = SchedulerActor::new(
            config,
            command_rx,
            event_tx.clone(),
            status_tx.clone(),
            shutdown.clone(),
        );
        let actor = tokio::spawn(actor.run());

        Ok(Self {
            scheduler: Scheduler {
                inner: Arc::new(SchedulerInner {
                    command_tx,
                    event_tx,
                    status_tx,
                }),
            },
            actor: Some(actor),
            shutdown,
        })
    }

    /// 获取一个可跨线程 Clone 的 Scheduler 命令句柄。
    pub fn scheduler(&self) -> Scheduler {
        self.scheduler.clone()
    }

    /// 请求优雅关闭并等待 Actor 完全退出。
    ///
    /// 关闭期间会清空 Pending、取消运行实例，并最多等待 `shutdown_timeout`。
    pub async fn shutdown(mut self) -> Result<(), SchedulerError> {
        let (response_tx, response_rx) = oneshot::channel();
        if self
            .scheduler
            .inner
            .command_tx
            .send(Command::Shutdown {
                response: response_tx,
            })
            .await
            .is_ok()
        {
            response_rx.await.map_err(|_| SchedulerError::Closed)??;
        } else {
            self.shutdown.cancel();
        }
        self.await_actor().await
    }

    /// 仅等待 Actor 自行退出，不主动发送关闭命令。
    ///
    /// 通常由另一个持有 Scheduler 的任务发出关闭命令时使用；否则可能一直等待。
    pub async fn wait(mut self) -> Result<(), SchedulerError> {
        self.await_actor().await
    }

    /// 取出并等待唯一 Actor JoinHandle，把 JoinError 转换为 SchedulerError。
    async fn await_actor(&mut self) -> Result<(), SchedulerError> {
        let actor = self.actor.take().ok_or(SchedulerError::Closed)?;
        actor
            .await
            .map_err(|error| SchedulerError::Join(error.to_string()))?
    }
}

impl Drop for SchedulerRuntime {
    /// Runtime 未显式关闭时触发兜底取消，防止 Actor 永久后台运行。
    fn drop(&mut self) {
        if self.actor.is_some() {
            self.shutdown.cancel();
        }
    }
}

impl Scheduler {
    /// 订阅 Scheduler 生命周期事件。
    ///
    /// Receiver 落后超过广播容量时会返回 `RecvError::Lagged`，调用方应按需重新读取快照。
    pub fn subscribe_events(&self) -> broadcast::Receiver<Arc<SchedulerEvent>> {
        self.inner.event_tx.subscribe()
    }

    /// 订阅 Scheduler 最新运行状态；新订阅者会立即看到当前值。
    pub fn subscribe_status(&self) -> watch::Receiver<SchedulerStatus> {
        self.inner.status_tx.subscribe()
    }

    /// 注册不产生业务输出的 ActionTask，返回随机 JobId。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// let id = scheduler
    ///     .add_action(
    ///         Trigger::interval(Duration::from_secs(5)),
    ///         Arc::new(CleanupTask),
    ///         JobOptions::default(),
    ///     )
    ///     .await?;
    /// ```
    #[allow(dead_code)]
    pub(crate) async fn add_action<T>(
        &self,
        trigger: Trigger,
        task: Arc<T>,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError>
    where
        T: ActionTask,
    {
        self.add(trigger, TaskBinding::action(task), options).await
    }

    /// 注册 ValueTask，并把每次成功输出发送到有界 Channel。
    ///
    /// 接收端关闭会触发 `OutputChannelClosed` 并自动删除 Job。
    #[allow(dead_code)]
    pub(crate) async fn add_value_channel<T>(
        &self,
        trigger: Trigger,
        task: Arc<T>,
        sender: mpsc::Sender<T::Output>,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError>
    where
        T: ValueTask,
    {
        self.add(trigger, TaskBinding::channel(task, sender), options)
            .await
    }

    /// 注册 ValueTask，并使用异步 Callback 消费每次成功输出。
    ///
    /// Callback 临时失败只记录交付失败，不会重新运行已经成功的 Task。
    #[allow(dead_code)]
    pub(crate) async fn add_value_callback<T, C>(
        &self,
        trigger: Trigger,
        task: Arc<T>,
        callback: C,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError>
    where
        T: ValueTask,
        C: AsyncCallback<T::Output>,
    {
        self.add(trigger, TaskBinding::callback(task, callback), options)
            .await
    }

    /// 注册标准上报 Task，并把每次 Proto 结果发送到有界 Channel。
    #[allow(dead_code)]
    pub(crate) async fn add_reporting_channel<T>(
        &self,
        trigger: Trigger,
        task: Arc<T>,
        sender: mpsc::Sender<TaskResult>,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError>
    where
        T: ReportingTask,
    {
        self.add(
            trigger,
            TaskBinding::reporting_channel(task, sender),
            options,
        )
        .await
    }

    /// 将三种公开注册方式统一为类型擦除后的 Add 命令。
    #[allow(dead_code)]
    async fn add(
        &self,
        trigger: Trigger,
        task: TaskBinding,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError> {
        self.request(|response| Command::Add {
            job_id: None,
            generation: 0,
            enabled: true,
            trigger,
            task,
            options,
            response,
        })
        .await
    }

    /// 使用调用方 UUID 和私有 generation 安装已校验 Job。
    pub(crate) async fn install(
        &self,
        job_id: JobId,
        generation: u64,
        enabled: bool,
        trigger: Trigger,
        task: TaskBinding,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError> {
        self.request(|response| Command::Add {
            job_id: Some(job_id),
            generation,
            enabled,
            trigger,
            task,
            options,
            response,
        })
        .await
    }

    /// 使用 expected_version 原子更新 Job，并返回更新后快照。
    ///
    /// Patch 校验失败不会修改原 Job；版本冲突时调用方应重新 get 后决定是否重试。
    pub(crate) async fn update(
        &self,
        job_id: JobId,
        expected_version: u64,
        patch: JobPatch,
    ) -> Result<JobSnapshot, SchedulerError> {
        self.request(|response| Command::Update {
            job_id,
            expected_version,
            patch,
            response,
        })
        .await
    }

    /// 查询单个 Job 的一致性快照；不存在时返回 `Ok(None)`。
    pub(crate) async fn get(&self, job_id: JobId) -> Result<Option<JobSnapshot>, SchedulerError> {
        self.request(|response| Command::Get { job_id, response })
            .await
    }

    /// 返回 Actor 当前保存的全部 Job 快照，顺序不保证稳定。
    #[allow(dead_code)]
    pub(crate) async fn list(&self) -> Result<Vec<JobSnapshot>, SchedulerError> {
        self.request(|response| Command::List { response }).await
    }

    /// 启用 Disabled 或 Completed Job，并重新安排首次 Trigger。
    ///
    /// 对已经 Enabled 的同版本 Job 为幂等操作，不增加版本。
    pub(crate) async fn enable(
        &self,
        job_id: JobId,
        expected_version: u64,
    ) -> Result<JobSnapshot, SchedulerError> {
        self.request(|response| Command::Enable {
            job_id,
            expected_version,
            response,
        })
        .await
    }

    /// 停用 Job、清理 Pending/Retry，并取消该版本正在执行的实例。
    pub(crate) async fn disable(
        &self,
        job_id: JobId,
        expected_version: u64,
        reason: impl Into<String>,
    ) -> Result<JobSnapshot, SchedulerError> {
        self.request(|response| Command::Disable {
            job_id,
            expected_version,
            reason: reason.into(),
            response,
        })
        .await
    }

    /// 删除 Job 并取消该版本正在执行的实例。
    pub(crate) async fn delete(
        &self,
        job_id: JobId,
        expected_version: u64,
    ) -> Result<(), SchedulerError> {
        self.request(|response| Command::Delete {
            job_id,
            expected_version,
            response,
        })
        .await
    }

    /// 读取 Scheduler 动态配置及当前 revision。
    pub async fn get_config(&self) -> Result<SchedulerConfigSnapshot, SchedulerError> {
        self.request(|response| Command::GetConfig { response })
            .await
    }

    /// 使用 expected_revision 原子更新 Scheduler 配置。
    ///
    /// 新安全限制会立即检查已有 Job，违反限制的 Job 会自动停用。
    pub async fn update_config(
        &self,
        expected_revision: u64,
        patch: SchedulerConfigPatch,
    ) -> Result<SchedulerConfigSnapshot, SchedulerError> {
        self.request(|response| Command::UpdateConfig {
            expected_revision,
            patch,
            response,
        })
        .await
    }

    /// 发送一条带 Oneshot 响应的 Actor 命令，并统一处理通道关闭。
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, SchedulerError>>) -> Command,
    ) -> Result<T, SchedulerError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .command_tx
            .send(command(response_tx))
            .await
            .map_err(|_| SchedulerError::Closed)?;
        response_rx.await.map_err(|_| SchedulerError::Closed)?
    }
}

/// Scheduler 句柄向 Actor 提交的内部命令。
enum Command {
    /// 注册新 Job。
    Add {
        job_id: Option<JobId>,
        generation: u64,
        enabled: bool,
        trigger: Trigger,
        task: TaskBinding,
        options: JobOptions,
        response: oneshot::Sender<Result<JobId, SchedulerError>>,
    },
    /// 部分更新已有 Job。
    Update {
        job_id: JobId,
        expected_version: u64,
        patch: JobPatch,
        response: oneshot::Sender<Result<JobSnapshot, SchedulerError>>,
    },
    /// 查询单个 Job。
    Get {
        job_id: JobId,
        response: oneshot::Sender<Result<Option<JobSnapshot>, SchedulerError>>,
    },
    /// 查询全部 Job。
    #[allow(dead_code)]
    List {
        response: oneshot::Sender<Result<Vec<JobSnapshot>, SchedulerError>>,
    },
    /// 启用 Job。
    Enable {
        job_id: JobId,
        expected_version: u64,
        response: oneshot::Sender<Result<JobSnapshot, SchedulerError>>,
    },
    /// 停用 Job。
    Disable {
        job_id: JobId,
        expected_version: u64,
        reason: String,
        response: oneshot::Sender<Result<JobSnapshot, SchedulerError>>,
    },
    /// 删除 Job。
    Delete {
        job_id: JobId,
        expected_version: u64,
        response: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// 查询动态配置。
    GetConfig {
        response: oneshot::Sender<Result<SchedulerConfigSnapshot, SchedulerError>>,
    },
    /// 部分更新动态配置。
    UpdateConfig {
        expected_revision: u64,
        patch: SchedulerConfigPatch,
        response: oneshot::Sender<Result<SchedulerConfigSnapshot, SchedulerError>>,
    },
    /// 请求 Actor 优雅关闭。
    Shutdown {
        response: oneshot::Sender<Result<(), SchedulerError>>,
    },
}

#[cfg(test)]
mod tests {
    use super::timing::{datetime_nanos, due_occurrences, effective_queue_policies};
    use super::*;
    use anyhow::anyhow;
    use chrono::{TimeDelta, Utc};
    use std::future::pending;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;

    struct CountingTask(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ActionTask for CountingTask {
        async fn run(&self, _context: TaskContext) -> Result<(), super::super::task::TaskError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct CountingValueTask(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ValueTask for CountingValueTask {
        type Output = usize;

        async fn run(
            &self,
            _context: TaskContext,
        ) -> Result<Self::Output, super::super::task::TaskError> {
            Ok(self.0.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    struct RetryOnceTask(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ActionTask for RetryOnceTask {
        async fn run(&self, _context: TaskContext) -> Result<(), super::super::task::TaskError> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(super::super::task::TaskError::Transient(anyhow!(
                    "temporary failure"
                )))
            } else {
                Ok(())
            }
        }
    }

    struct HangingTask(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ActionTask for HangingTask {
        async fn run(&self, _context: TaskContext) -> Result<(), super::super::task::TaskError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            pending::<()>().await;
            Ok(())
        }
    }

    struct CancellationAwareTask {
        started: Arc<AtomicUsize>,
        cancellation_observed: Arc<AtomicUsize>,
    }

    struct PanicTask(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ActionTask for PanicTask {
        async fn run(&self, _context: TaskContext) -> Result<(), super::super::task::TaskError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("intentional scheduler test panic");
        }
    }

    struct PermanentFailureCancelsPeerTask {
        started: Arc<AtomicUsize>,
        cancellation_observed: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ActionTask for PermanentFailureCancelsPeerTask {
        async fn run(&self, context: TaskContext) -> Result<(), super::super::task::TaskError> {
            let execution = self.started.fetch_add(1, Ordering::SeqCst);
            if execution == 0 {
                while self.started.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
                Err(super::super::task::TaskError::Permanent(anyhow!(
                    "invalid task configuration"
                )))
            } else {
                let observed = self.cancellation_observed.clone();
                tokio::spawn(async move {
                    context.cancellation.cancelled().await;
                    observed.fetch_add(1, Ordering::SeqCst);
                });
                pending::<()>().await;
                Ok(())
            }
        }
    }

    struct ConcurrencyTask {
        running: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl ActionTask for ConcurrencyTask {
        async fn run(&self, _context: TaskContext) -> Result<(), super::super::task::TaskError> {
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(running, Ordering::SeqCst);
            self.started.fetch_add(1, Ordering::SeqCst);
            self.release.notified().await;
            self.running.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ActionTask for CancellationAwareTask {
        async fn run(&self, context: TaskContext) -> Result<(), super::super::task::TaskError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            let observed = self.cancellation_observed.clone();
            tokio::spawn(async move {
                context.cancellation.cancelled().await;
                observed.fetch_add(1, Ordering::SeqCst);
            });
            pending::<()>().await;
            Ok(())
        }
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition should become true before timeout");
    }

    fn fast_config() -> SchedulerConfig {
        SchedulerConfig {
            minimum_interval: Duration::from_millis(5),
            minimum_retry_delay: Duration::from_millis(5),
            ..SchedulerConfig::default()
        }
    }

    #[tokio::test]
    async fn add_run_query_and_delete_job() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let count = Arc::new(AtomicUsize::new(0));
        let id = scheduler
            .add_action(
                Trigger::once(Utc::now()),
                Arc::new(CountingTask(count.clone())),
                JobOptions::default(),
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let snapshot = scheduler.get(id).await.unwrap().unwrap();
        scheduler.delete(id, snapshot.version).await.unwrap();
        assert!(scheduler.get(id).await.unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_update_returns_version_conflict() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let id = scheduler
            .add_action(
                Trigger::once(Utc::now() + TimeDelta::seconds(60)),
                Arc::new(CountingTask(Arc::new(AtomicUsize::new(0)))),
                JobOptions::default(),
            )
            .await
            .unwrap();
        scheduler.update(id, 0, JobPatch::new()).await.unwrap();
        assert!(matches!(
            scheduler.update(id, 0, JobPatch::new()).await,
            Err(SchedulerError::VersionConflict { .. })
        ));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_update_keeps_original_job_unchanged() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let original_trigger = Trigger::interval(Duration::from_secs(10));
        let id = scheduler
            .add_action(
                original_trigger.clone(),
                Arc::new(CountingTask(Arc::new(AtomicUsize::new(0)))),
                JobOptions::default(),
            )
            .await
            .unwrap();
        let mut patch = JobPatch::new();
        patch.trigger = Some(Trigger::interval(Duration::from_millis(1)));

        assert!(matches!(
            scheduler.update(id, 0, patch).await,
            Err(SchedulerError::InvalidTrigger(_))
        ));
        let snapshot = scheduler.get(id).await.unwrap().unwrap();
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.trigger, original_trigger);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn enabling_enabled_job_is_idempotent() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let id = scheduler
            .add_action(
                Trigger::once(Utc::now() + TimeDelta::seconds(60)),
                Arc::new(CountingTask(Arc::new(AtomicUsize::new(0)))),
                JobOptions::default(),
            )
            .await
            .unwrap();

        let snapshot = scheduler.enable(id, 0).await.unwrap();
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.state, JobState::Enabled);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn closed_output_channel_deletes_job() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let count = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let id = scheduler
            .add_value_channel(
                Trigger::once(Utc::now()),
                Arc::new(CountingValueTask(count.clone())),
                sender,
                JobOptions::default(),
            )
            .await
            .unwrap();

        wait_until(|| count.load(Ordering::SeqCst) == 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while scheduler.get(id).await.unwrap().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn once_job_without_retry_reaches_terminal_state_after_failure() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let count = Arc::new(AtomicUsize::new(0));
        let id = scheduler
            .add_action(
                Trigger::once(Utc::now()),
                Arc::new(RetryOnceTask(count.clone())),
                JobOptions::default(),
            )
            .await
            .unwrap();

        wait_until(|| count.load(Ordering::SeqCst) == 1).await;
        let snapshot = scheduler.get(id).await.unwrap().unwrap();
        assert_eq!(snapshot.state, JobState::Completed);
        assert_eq!(snapshot.consecutive_failures, 1);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn transient_failure_retries_and_then_completes() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let count = Arc::new(AtomicUsize::new(0));
        let options = JobOptions {
            retry: ExecutionRetryPolicy::Exponential {
                max_attempts: NonZeroU32::new(2).unwrap(),
                initial_delay: Duration::from_millis(5),
                max_delay: Duration::from_millis(5),
                retry_on: RetryCondition::default(),
            },
            ..JobOptions::default()
        };
        let id = scheduler
            .add_action(
                Trigger::once(Utc::now()),
                Arc::new(RetryOnceTask(count.clone())),
                options,
            )
            .await
            .unwrap();

        wait_until(|| count.load(Ordering::SeqCst) == 2).await;
        let snapshot = scheduler.get(id).await.unwrap().unwrap();
        assert_eq!(snapshot.state, JobState::Completed);
        assert_eq!(snapshot.consecutive_failures, 0);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn timeout_releases_global_concurrency_slot() {
        let mut config = fast_config();
        config.global_concurrency = NonZeroUsize::MIN;
        let runtime = SchedulerRuntime::start(config).unwrap();
        let scheduler = runtime.scheduler();
        let hanging_count = Arc::new(AtomicUsize::new(0));
        let completed_count = Arc::new(AtomicUsize::new(0));
        scheduler
            .add_action(
                Trigger::once(Utc::now()).with_timeout(Some(Duration::from_millis(20))),
                Arc::new(HangingTask(hanging_count.clone())),
                JobOptions {
                    priority: JobPriority::CRITICAL,
                    ..JobOptions::default()
                },
            )
            .await
            .unwrap();
        wait_until(|| hanging_count.load(Ordering::SeqCst) == 1).await;
        scheduler
            .add_action(
                Trigger::once(Utc::now()),
                Arc::new(CountingTask(completed_count.clone())),
                JobOptions::default(),
            )
            .await
            .unwrap();

        wait_until(|| completed_count.load(Ordering::SeqCst) == 1).await;
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn updating_disabled_job_does_not_enable_or_schedule_it() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let id = scheduler
            .add_action(
                Trigger::interval(Duration::from_secs(10)),
                Arc::new(CountingTask(Arc::new(AtomicUsize::new(0)))),
                JobOptions::default(),
            )
            .await
            .unwrap();
        let disabled = scheduler.disable(id, 0, "maintenance").await.unwrap();
        let mut patch = JobPatch::new();
        patch.trigger = Some(Trigger::interval(Duration::from_secs(20)));

        let updated = scheduler.update(id, disabled.version, patch).await.unwrap();
        assert_eq!(
            updated.state,
            JobState::Disabled {
                reason: "maintenance".to_owned()
            }
        );
        assert_eq!(updated.next_run_at, None);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn deleting_job_cancels_running_execution() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let started = Arc::new(AtomicUsize::new(0));
        let cancellation_observed = Arc::new(AtomicUsize::new(0));
        let id = scheduler
            .add_action(
                Trigger::once(Utc::now()).with_timeout(None),
                Arc::new(CancellationAwareTask {
                    started: started.clone(),
                    cancellation_observed: cancellation_observed.clone(),
                }),
                JobOptions::default(),
            )
            .await
            .unwrap();
        wait_until(|| started.load(Ordering::SeqCst) == 1).await;

        scheduler.delete(id, 0).await.unwrap();
        wait_until(|| cancellation_observed.load(Ordering::SeqCst) == 1).await;
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn callback_failure_does_not_rerun_value_task() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let task_count = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_counter = callback_count.clone();
        let id = scheduler
            .add_value_callback(
                Trigger::once(Utc::now()),
                Arc::new(CountingValueTask(task_count.clone())),
                super::super::task::async_callback(move |_context, _value| {
                    let callback_counter = callback_counter.clone();
                    async move {
                        callback_counter.fetch_add(1, Ordering::SeqCst);
                        Err(super::super::task::CallbackError::Transient(anyhow!(
                            "delivery unavailable"
                        )))
                    }
                }),
                JobOptions::default(),
            )
            .await
            .unwrap();

        wait_until(|| callback_count.load(Ordering::SeqCst) == 1).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(task_count.load(Ordering::SeqCst), 1);
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler.get(id).await.unwrap().unwrap().state,
            JobState::Completed
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scheduler_config_update_uses_revision_lock() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let updated = scheduler
            .update_config(
                0,
                SchedulerConfigPatch {
                    global_max_pending: Some(16),
                    ..SchedulerConfigPatch::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.revision, 1);
        assert_eq!(updated.config.global_max_pending, 16);
        assert!(matches!(
            scheduler
                .update_config(0, SchedulerConfigPatch::default())
                .await,
            Err(SchedulerError::ConfigRevisionConflict {
                expected: 0,
                actual: 1
            })
        ));
        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn catch_up_defaults_to_lossless_queue_policies() {
        let trigger = Trigger {
            schedule: Schedule::Interval {
                every: Duration::from_secs(1),
                start_at: None,
            },
            misfire: MisfirePolicy::CatchUp {
                max_runs: NonZeroU32::new(3).unwrap(),
            },
            timeout: None,
        };

        assert_eq!(
            effective_queue_policies(&trigger, &JobOptions::default()),
            (TriggerCoalescing::KeepAll, CapacityPolicy::Backpressure)
        );
    }

    #[test]
    fn long_interval_backlog_is_bounded_and_preserves_phase() {
        let now = Utc::now();
        let scheduled_at = now - TimeDelta::days(365);
        let trigger = Trigger {
            schedule: Schedule::Interval {
                every: Duration::from_millis(100),
                start_at: Some(scheduled_at),
            },
            misfire: MisfirePolicy::CatchUp {
                max_runs: NonZeroU32::new(3).unwrap(),
            },
            timeout: None,
        };

        let due = due_occurrences(&trigger, scheduled_at, now, 3).unwrap();
        assert_eq!(due.occurrences.len(), 3);
        let next = due.next.unwrap();
        assert!(next > now);
        let phase = (datetime_nanos(next) - datetime_nanos(scheduled_at))
            % i128::try_from(Duration::from_millis(100).as_nanos()).unwrap();
        assert_eq!(phase, 0);
    }

    #[tokio::test]
    async fn panic_releases_global_concurrency_slot() {
        let mut config = fast_config();
        config.global_concurrency = NonZeroUsize::MIN;
        let runtime = SchedulerRuntime::start(config).unwrap();
        let scheduler = runtime.scheduler();
        let panic_count = Arc::new(AtomicUsize::new(0));
        let completed_count = Arc::new(AtomicUsize::new(0));
        scheduler
            .add_action(
                Trigger::once(Utc::now()),
                Arc::new(PanicTask(panic_count.clone())),
                JobOptions::default(),
            )
            .await
            .unwrap();
        wait_until(|| panic_count.load(Ordering::SeqCst) == 1).await;
        scheduler
            .add_action(
                Trigger::once(Utc::now()),
                Arc::new(CountingTask(completed_count.clone())),
                JobOptions::default(),
            )
            .await
            .unwrap();

        wait_until(|| completed_count.load(Ordering::SeqCst) == 1).await;
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn permanent_failure_cancels_same_version_peer() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let started = Arc::new(AtomicUsize::new(0));
        let cancellation_observed = Arc::new(AtomicUsize::new(0));
        let trigger = Trigger {
            schedule: Schedule::Interval {
                every: Duration::from_millis(5),
                start_at: Some(Utc::now() - TimeDelta::milliseconds(20)),
            },
            misfire: MisfirePolicy::CatchUp {
                max_runs: NonZeroU32::new(2).unwrap(),
            },
            timeout: None,
        };
        let id = scheduler
            .add_action(
                trigger,
                Arc::new(PermanentFailureCancelsPeerTask {
                    started: started.clone(),
                    cancellation_observed: cancellation_observed.clone(),
                }),
                JobOptions {
                    concurrency: NonZeroUsize::new(2),
                    ..JobOptions::default()
                },
            )
            .await
            .unwrap();

        wait_until(|| cancellation_observed.load(Ordering::SeqCst) == 1).await;
        assert!(matches!(
            scheduler.get(id).await.unwrap().unwrap().state,
            JobState::Disabled { .. }
        ));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scheduler_respects_job_and_global_concurrency_limits() {
        let mut config = fast_config();
        config.global_concurrency = NonZeroUsize::new(2).unwrap();
        let runtime = SchedulerRuntime::start(config).unwrap();
        let scheduler = runtime.scheduler();
        let running = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let id = scheduler
            .add_action(
                Trigger {
                    schedule: Schedule::Interval {
                        every: Duration::from_millis(5),
                        start_at: None,
                    },
                    misfire: MisfirePolicy::FireOnce,
                    timeout: Some(Duration::from_secs(30)),
                },
                Arc::new(ConcurrencyTask {
                    running: running.clone(),
                    maximum: maximum.clone(),
                    started: started.clone(),
                    release: release.clone(),
                }),
                JobOptions {
                    concurrency: NonZeroUsize::new(2),
                    coalescing: Some(TriggerCoalescing::KeepAll),
                    capacity: Some(CapacityPolicy::Backpressure),
                    ..JobOptions::default()
                },
            )
            .await
            .unwrap();

        wait_until(|| started.load(Ordering::SeqCst) >= 2).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
        let snapshot = scheduler.get(id).await.unwrap().unwrap();
        scheduler
            .disable(id, snapshot.version, "test complete")
            .await
            .unwrap();
        release.notify_waiters();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn successful_value_delivery_emits_output_event() {
        let runtime = SchedulerRuntime::start(fast_config()).unwrap();
        let scheduler = runtime.scheduler();
        let mut events = scheduler.subscribe_events();
        let (sender, mut receiver) = mpsc::channel(1);
        let id = scheduler
            .add_value_channel(
                Trigger::once(Utc::now()),
                Arc::new(CountingValueTask(Arc::new(AtomicUsize::new(0)))),
                sender,
                JobOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(receiver.recv().await, Some(1));

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let event = events.recv().await.unwrap();
                if matches!(
                    event.kind,
                    SchedulerEventKind::OutputDelivered { job_id, .. } if job_id == id
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        runtime.shutdown().await.unwrap();
    }
}
