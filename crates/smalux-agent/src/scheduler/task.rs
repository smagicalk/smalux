//! 可调度 Task、强类型输出和类型擦除适配器。
//!
//! Scheduler 只负责“何时执行”和“如何处理执行状态”，不理解 CPU、网络等业务结果。
//! 具体 Task 直接返回值，再由 Adapter 选择 Channel、Callback 或标准 Proto 上报出口：
//!
//! ```text
//! Scheduler -> ScheduledTask::execute -> ReportingTask::run
//!           -> TaskResult -> ReportingAdapter -> TaskReportSink
//! ```
//!
//! `ScheduledTask` 和 `TaskRunResult` 是内部类型擦除边界。它们让 Scheduler 可以把不同
//! 输出类型的 Task 存在同一 Job Map 中，同时保留“Task 失败”和“结果交付失败”的区别。

use super::{JobId, RunId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use smalux_protocol::agent::v1::{TaskReport, TaskResult};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// 单次执行上下文。
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// 当前 Job 标识。
    pub job_id: JobId,
    /// 执行开始时 Job 的版本；更新后的旧版本结果会被 Scheduler 忽略。
    pub version: u64,
    /// 单次逻辑执行标识；同一逻辑执行的 Retry 保持相同 RunId。
    pub run_id: RunId,
    /// 当前尝试次数，从 1 开始。
    pub attempt: u32,
    /// Trigger 原计划时间，而不是实际启动时间。
    pub scheduled_at: DateTime<Utc>,
    /// Scheduler 实际启动本次 Task 的时间。
    pub started_at: DateTime<Utc>,
    /// 删除、停用、超时或关闭时触发的协作取消令牌。
    ///
    /// Task 派生后台工作时应把该令牌传递下去，避免主 Future 被丢弃后留下子任务。
    pub cancellation: CancellationToken,
}

/// Task 主体错误。
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// 临时错误，可按 [`ExecutionRetryPolicy`](super::ExecutionRetryPolicy) 重试。
    #[error("transient task error: {0:#}")]
    Transient(#[source] anyhow::Error),
    /// 永久错误，立即停用 Job，不执行重试。
    #[error("permanent task error: {0:#}")]
    Permanent(#[source] anyhow::Error),
}

/// Scheduler 对 Task 取消和超时的执行语义。
///
/// `NonCancellable` 适用于已经进入 `spawn_blocking` 的同步工作。此类工作无法被
/// Tokio 中止；Scheduler 必须保留执行槽位直到它实际返回，并拒绝为它配置执行超时。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCancellationMode {
    /// Task 通过 [`TaskContext::cancellation`] 协作取消，允许 Scheduler 设置超时。
    Cooperative,
    /// Task 一旦开始就必须等待实际完成，不能配置 Scheduler 超时。
    NonCancellable,
}

/// 异步 Callback 错误。
#[derive(Debug, thiserror::Error)]
pub enum CallbackError {
    /// 本次结果交付失败；不会重新运行 Task，下一 Trigger 仍可继续。
    #[error("transient callback error: {0:#}")]
    Transient(#[source] anyhow::Error),
    /// 结果无法继续交付，Scheduler 会停用 Job。
    #[error("permanent callback error: {0:#}")]
    Permanent(#[source] anyhow::Error),
}

/// 不产生业务值的异步 Task。
///
/// # 示例
///
/// ```ignore
/// struct CleanupTask;
///
/// #[async_trait::async_trait]
/// impl ActionTask for CleanupTask {
///     async fn run(&self, context: TaskContext) -> Result<(), TaskError> {
///         if context.cancellation.is_cancelled() {
///             return Ok(());
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
#[allow(dead_code)]
pub trait ActionTask: Send + Sync + 'static {
    /// 执行一次任务；返回值决定成功、重试或永久停用。
    async fn run(&self, context: TaskContext) -> Result<(), TaskError>;

    /// 返回用于快照和诊断的稳定 Task 类型名称。
    ///
    /// 默认使用完整 Rust 类型名；需要跨版本稳定名称时可自行覆盖。
    fn kind(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 返回 Task 的取消语义。
    fn cancellation_mode(&self) -> TaskCancellationMode {
        TaskCancellationMode::Cooperative
    }
}

/// 每次运行产生一个强类型值的异步 Task。
///
/// 值必须通过 Channel 或 Callback 适配器消费，Scheduler 事件本身不携带业务值。
#[async_trait]
#[allow(dead_code)]
pub(crate) trait ValueTask: Send + Sync + 'static {
    /// 单次执行产生的业务值类型。
    type Output: Send + Sync + 'static;

    /// 收集或计算一次业务值。
    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError>;

    /// 返回用于快照和诊断的稳定 Task 类型名称。
    fn kind(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 返回 Task 的取消语义。
    fn cancellation_mode(&self) -> TaskCancellationMode {
        TaskCancellationMode::Cooperative
    }
}

/// 每次执行返回标准 Proto [`TaskResult`] 的采集或探测任务。
#[async_trait]
pub trait ReportingTask: Send + Sync + 'static {
    /// 执行一次任务并返回可直接上报的协议结果。
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError>;

    /// 返回用于诊断和 TaskFactory 映射的稳定类型名称。
    fn kind(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 返回 Task 的协作取消或不可取消语义。
    fn cancellation_mode(&self) -> TaskCancellationMode {
        TaskCancellationMode::Cooperative
    }
}

/// 把标准 Task 结果交给持久化、Channel 或连接层的异步出口。
///
/// Sink 只负责交付已经完成的结果，不应重新执行原 Task。临时交付错误只影响本次报告，
/// 永久交付错误则会由 Scheduler 按 [`TaskRunResult::CallbackPermanent`] 停用 Job。
pub trait TaskReportSink: Send + Sync + 'static {
    /// 消费一次带 Job revision 和执行身份的完整报告。
    fn report(&self, report: TaskReport) -> CallbackFuture;
}

impl<F, Fut> TaskReportSink for F
where
    F: Fn(TaskReport) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CallbackError>> + Send + 'static,
{
    fn report(&self, report: TaskReport) -> CallbackFuture {
        Box::pin((self)(report))
    }
}

/// 为不同闭包返回类型提供统一 ABI 的异步交付 Future。
type CallbackFuture = Pin<Box<dyn Future<Output = Result<(), CallbackError>> + Send>>;

/// 类型擦除后的异步 Callback。
#[allow(dead_code)]
pub(crate) trait AsyncCallback<T>: Send + Sync + 'static {
    /// 异步消费一次 Task 输出，不应在失败时自行重新运行原 Task。
    fn call(&self, context: TaskContext, value: T) -> CallbackFuture;
}

impl<T, F, Fut> AsyncCallback<T> for F
where
    T: Send + 'static,
    F: Fn(TaskContext, T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CallbackError>> + Send + 'static,
{
    /// 把异步闭包返回的 Future 装箱为类型擦除 CallbackFuture。
    fn call(&self, context: TaskContext, value: T) -> CallbackFuture {
        Box::pin((self)(context, value))
    }
}

/// 保留异步闭包 Callback 的类型推导。
///
/// # 示例
///
/// ```ignore
/// let callback = async_callback(|context: TaskContext, value: Metric| async move {
///     upload(context.run_id, value).await
///         .map_err(CallbackError::Transient)
/// });
/// ```
#[allow(dead_code)]
pub(crate) fn async_callback<T, F, Fut>(callback: F) -> F
where
    T: Send + 'static,
    F: Fn(TaskContext, T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CallbackError>> + Send + 'static,
{
    callback
}

/// 调度器内部统一执行结果。
pub(crate) enum TaskRunResult {
    /// ActionTask 成功完成。
    #[allow(dead_code)]
    Completed,
    /// ValueTask 的结果已成功送入 Channel 或 Callback。
    OutputDelivered,
    /// Task 返回临时错误。
    TaskTransient(String),
    /// Task 返回永久错误。
    TaskPermanent(String),
    /// Callback 返回临时错误。
    CallbackTransient(String),
    /// Callback 返回永久错误。
    CallbackPermanent(String),
    /// Value 输出 Channel 的接收端已经关闭。
    #[allow(dead_code)]
    ChannelClosed,
}

#[async_trait]
pub(crate) trait ScheduledTask: Send + Sync + 'static {
    /// 返回类型擦除前的 Task 名称。
    fn kind(&self) -> &'static str;
    /// 执行 Task 及其输出适配器，并返回统一内部结果。
    async fn execute(&self, context: TaskContext) -> TaskRunResult;
}

/// 已完成类型擦除、可存入 Job Map 的 Task。
#[derive(Clone)]
pub(crate) struct TaskBinding {
    /// 类型擦除后的实际 Task 适配器，仅供 Scheduler 内部执行。
    pub(crate) inner: Arc<dyn ScheduledTask>,
    /// Scheduler 在超时、删除和关闭时应遵循的执行语义。
    pub(crate) cancellation_mode: TaskCancellationMode,
}

impl TaskBinding {
    /// 把不产生业务值的 [`ActionTask`] 注册为统一 Task。
    #[allow(dead_code)]
    pub fn action<T>(task: Arc<T>) -> Self
    where
        T: ActionTask,
    {
        let cancellation_mode = task.cancellation_mode();
        Self {
            inner: Arc::new(ActionAdapter { task }),
            cancellation_mode,
        }
    }

    /// 把 [`ValueTask`] 输出发送到有界 Tokio Channel。
    ///
    /// Channel 满时发送会异步等待，并受 Trigger timeout 和取消控制；接收端关闭会自动删除 Job。
    #[allow(dead_code)]
    pub fn channel<T>(task: Arc<T>, sender: mpsc::Sender<T::Output>) -> Self
    where
        T: ValueTask,
    {
        let cancellation_mode = task.cancellation_mode();
        Self {
            inner: Arc::new(ChannelAdapter { task, sender }),
            cancellation_mode,
        }
    }

    /// 把 [`ValueTask`] 输出交给异步 Callback。
    ///
    /// Callback 临时失败不会重新运行 Task，避免重复采集或重复副作用。
    #[allow(dead_code)]
    pub fn callback<T, C>(task: Arc<T>, callback: C) -> Self
    where
        T: ValueTask,
        C: AsyncCallback<T::Output>,
    {
        let cancellation_mode = task.cancellation_mode();
        Self {
            inner: Arc::new(CallbackAdapter {
                task,
                callback: Arc::new(callback),
            }),
            cancellation_mode,
        }
    }

    /// 绑定标准上报 Task 与结果出口。
    pub fn reporting<T, S>(task: Arc<T>, job_revision: u64, sink: Arc<S>) -> Self
    where
        T: ReportingTask,
        S: TaskReportSink + ?Sized,
    {
        let cancellation_mode = task.cancellation_mode();
        Self {
            inner: Arc::new(ReportingAdapter {
                task,
                job_revision,
                sink,
            }),
            cancellation_mode,
        }
    }

    /// 把标准 Proto 结果发送到有界 Tokio Channel。
    #[allow(dead_code)]
    pub fn reporting_channel<T>(task: Arc<T>, sender: mpsc::Sender<TaskResult>) -> Self
    where
        T: ReportingTask,
    {
        Self::reporting(task, 0, Arc::new(ReportChannelSink { sender }))
    }

    /// 返回注册 Task 的取消和超时语义。
    pub fn cancellation_mode(&self) -> TaskCancellationMode {
        self.cancellation_mode
    }
}

#[allow(dead_code)]
struct ReportChannelSink {
    /// 只转发报告中的业务结果；容量和背压由调用方创建 Channel 时决定。
    sender: mpsc::Sender<TaskResult>,
}

impl TaskReportSink for ReportChannelSink {
    fn report(&self, report: TaskReport) -> CallbackFuture {
        // clone 仅复制 Sender 句柄，使返回 Future 不借用 self。
        let sender = self.sender.clone();
        Box::pin(async move {
            // reporting_channel 的公开语义是传 TaskResult，缺失结果属于不可恢复的协议错误。
            let result = report.result.ok_or_else(|| {
                CallbackError::Permanent(anyhow::anyhow!("task report result is missing"))
            })?;
            sender.send(result).await.map_err(|_| {
                CallbackError::Permanent(anyhow::anyhow!("task report channel is closed"))
            })
        })
    }
}

struct ReportingAdapter<T, S>
where
    T: ReportingTask,
    S: TaskReportSink + ?Sized,
{
    /// 返回标准 Proto 结果的具体采集 Task。
    task: Arc<T>,
    /// Server 的业务配置版本；不能使用 TaskContext 中的 Scheduler generation 替代。
    job_revision: u64,
    /// 接收完整 TaskReport 的共享异步出口。
    sink: Arc<S>,
}

#[async_trait]
impl<T, S> ScheduledTask for ReportingAdapter<T, S>
where
    T: ReportingTask,
    S: TaskReportSink + ?Sized,
{
    fn kind(&self) -> &'static str {
        self.task.kind()
    }

    async fn execute(&self, context: TaskContext) -> TaskRunResult {
        let kind = self.kind();
        log_task_started(kind, &context);
        // 先运行 Task；Task 失败时没有业务结果，因此不会调用 Sink。
        let result = match self.task.run(context.clone()).await {
            Ok(result) => result,
            Err(error) => {
                let outcome = map_task_error(error);
                log_task_finished(kind, &context, &outcome);
                return outcome;
            }
        };
        // Adapter 在统一位置补充 Job、Run、尝试次数和时间信息，采集器无需重复组装。
        let report = TaskReport {
            job_id: context.job_id.as_bytes().to_vec(),
            job_revision: self.job_revision,
            run_id: context.run_id.as_bytes().to_vec(),
            attempt: context.attempt,
            scheduled_at: Some(timestamp(context.scheduled_at)),
            started_at: Some(timestamp(context.started_at)),
            result: Some(result),
        };
        // 交付结果与 Task 执行结果分开映射，防止交付失败触发昂贵采集的重复执行。
        let outcome = match self.sink.report(report).await {
            Ok(()) => TaskRunResult::OutputDelivered,
            Err(CallbackError::Transient(error)) => {
                TaskRunResult::CallbackTransient(format!("{error:#}"))
            }
            Err(CallbackError::Permanent(error)) => {
                TaskRunResult::CallbackPermanent(format!("{error:#}"))
            }
        };
        log_task_finished(kind, &context, &outcome);
        outcome
    }
}

fn timestamp(value: DateTime<Utc>) -> prost_types::Timestamp {
    // chrono 纳秒部分保证落在 Prost Timestamp 要求的 0..1_000_000_000 范围内。
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

#[allow(dead_code)]
struct ActionAdapter<T> {
    /// 被类型擦除的 ActionTask。
    task: Arc<T>,
}

#[async_trait]
impl<T> ScheduledTask for ActionAdapter<T>
where
    T: ActionTask,
{
    /// 转发 ActionTask 的诊断名称。
    fn kind(&self) -> &'static str {
        self.task.kind()
    }

    /// 执行 ActionTask 并映射公共错误。
    async fn execute(&self, context: TaskContext) -> TaskRunResult {
        let kind = self.kind();
        log_task_started(kind, &context);
        let outcome = match self.task.run(context.clone()).await {
            Ok(()) => TaskRunResult::Completed,
            Err(error) => map_task_error(error),
        };
        log_task_finished(kind, &context, &outcome);
        outcome
    }
}

#[allow(dead_code)]
struct ChannelAdapter<T>
where
    T: ValueTask,
{
    /// 产生业务值的 Task。
    task: Arc<T>,
    /// 接收业务值的有界 Channel 发送端。
    sender: mpsc::Sender<T::Output>,
}

#[async_trait]
impl<T> ScheduledTask for ChannelAdapter<T>
where
    T: ValueTask,
{
    /// 转发 ValueTask 的诊断名称。
    fn kind(&self) -> &'static str {
        self.task.kind()
    }

    /// 先执行 ValueTask，再把成功值发送到有界 Channel。
    async fn execute(&self, context: TaskContext) -> TaskRunResult {
        let kind = self.kind();
        log_task_started(kind, &context);
        let value = match self.task.run(context.clone()).await {
            Ok(value) => value,
            Err(error) => {
                let outcome = map_task_error(error);
                log_task_finished(kind, &context, &outcome);
                return outcome;
            }
        };
        let outcome = match self.sender.send(value).await {
            Ok(()) => TaskRunResult::OutputDelivered,
            Err(_) => TaskRunResult::ChannelClosed,
        };
        log_task_finished(kind, &context, &outcome);
        outcome
    }
}

#[allow(dead_code)]
struct CallbackAdapter<T, C>
where
    T: ValueTask,
{
    /// 产生业务值的 Task。
    task: Arc<T>,
    /// 消费业务值的共享异步 Callback。
    callback: Arc<C>,
}

#[async_trait]
impl<T, C> ScheduledTask for CallbackAdapter<T, C>
where
    T: ValueTask,
    C: AsyncCallback<T::Output>,
{
    /// 转发 ValueTask 的诊断名称。
    fn kind(&self) -> &'static str {
        self.task.kind()
    }

    /// 先执行 ValueTask，再异步调用 Callback 消费成功值。
    async fn execute(&self, context: TaskContext) -> TaskRunResult {
        let kind = self.kind();
        log_task_started(kind, &context);
        let value = match self.task.run(context.clone()).await {
            Ok(value) => value,
            Err(error) => {
                let outcome = map_task_error(error);
                log_task_finished(kind, &context, &outcome);
                return outcome;
            }
        };
        let outcome = match self.callback.call(context.clone(), value).await {
            Ok(()) => TaskRunResult::OutputDelivered,
            Err(CallbackError::Transient(error)) => {
                TaskRunResult::CallbackTransient(format!("{error:#}"))
            }
            Err(CallbackError::Permanent(error)) => {
                TaskRunResult::CallbackPermanent(format!("{error:#}"))
            }
        };
        log_task_finished(kind, &context, &outcome);
        outcome
    }
}

/// 记录统一 Task 入口，避免每个采集器重复实现相同的生命周期日志。
fn log_task_started(kind: &'static str, context: &TaskContext) {
    tracing::trace!(
        task_kind = kind,
        job_id = %context.job_id,
        version = context.version,
        run_id = %context.run_id,
        attempt = context.attempt,
        "agent task started"
    );
}

/// 记录统一 Task 结果；业务输出本身不写入日志，只记录交付状态。
fn log_task_finished(kind: &'static str, context: &TaskContext, outcome: &TaskRunResult) {
    match outcome {
        TaskRunResult::Completed | TaskRunResult::OutputDelivered => {
            tracing::trace!(
                task_kind = kind,
                job_id = %context.job_id,
                version = context.version,
                run_id = %context.run_id,
                attempt = context.attempt,
                outcome = task_outcome_name(outcome),
                "agent task finished"
            );
        }
        TaskRunResult::TaskTransient(error)
        | TaskRunResult::TaskPermanent(error)
        | TaskRunResult::CallbackTransient(error)
        | TaskRunResult::CallbackPermanent(error) => {
            tracing::warn!(
                task_kind = kind,
                job_id = %context.job_id,
                version = context.version,
                run_id = %context.run_id,
                attempt = context.attempt,
                outcome = task_outcome_name(outcome),
                error = %error,
                "agent task failed"
            );
        }
        TaskRunResult::ChannelClosed => {
            tracing::warn!(
                task_kind = kind,
                job_id = %context.job_id,
                version = context.version,
                run_id = %context.run_id,
                attempt = context.attempt,
                outcome = "channel_closed",
                "agent task output channel closed"
            );
        }
    }
}

/// 返回不包含业务错误正文的稳定结果标签，供结构化日志聚合使用。
fn task_outcome_name(outcome: &TaskRunResult) -> &'static str {
    match outcome {
        TaskRunResult::Completed => "completed",
        TaskRunResult::OutputDelivered => "output_delivered",
        TaskRunResult::TaskTransient(_) => "task_transient",
        TaskRunResult::TaskPermanent(_) => "task_permanent",
        TaskRunResult::CallbackTransient(_) => "callback_transient",
        TaskRunResult::CallbackPermanent(_) => "callback_permanent",
        TaskRunResult::ChannelClosed => "channel_closed",
    }
}

/// 把公共 TaskError 映射为 Scheduler 内部统一结果。
fn map_task_error(error: TaskError) -> TaskRunResult {
    match error {
        TaskError::Transient(error) => TaskRunResult::TaskTransient(format!("{error:#}")),
        TaskError::Permanent(error) => TaskRunResult::TaskPermanent(format!("{error:#}")),
    }
}
