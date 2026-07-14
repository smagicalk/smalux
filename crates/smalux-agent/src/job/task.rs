//! 可调度 Task、强类型输出和类型擦除适配器。

use super::{JobId, RunId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
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
pub trait ActionTask: Send + Sync + 'static {
    /// 执行一次任务；返回值决定成功、重试或永久停用。
    async fn run(&self, context: TaskContext) -> Result<(), TaskError>;

    /// 返回用于快照和诊断的稳定 Task 类型名称。
    ///
    /// 默认使用完整 Rust 类型名；需要跨版本稳定名称时可自行覆盖。
    fn kind(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// 每次运行产生一个强类型值的异步 Task。
///
/// 值必须通过 Channel 或 Callback 适配器消费，Scheduler 事件本身不携带业务值。
#[async_trait]
pub trait ValueTask: Send + Sync + 'static {
    /// 单次执行产生的业务值类型。
    type Output: Send + Sync + 'static;

    /// 收集或计算一次业务值。
    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError>;

    /// 返回用于快照和诊断的稳定 Task 类型名称。
    fn kind(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

type CallbackFuture = Pin<Box<dyn Future<Output = Result<(), CallbackError>> + Send>>;

/// 类型擦除后的异步 Callback。
pub trait AsyncCallback<T>: Send + Sync + 'static {
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
pub fn async_callback<T, F, Fut>(callback: F) -> F
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
pub struct TaskRegistration {
    /// 类型擦除后的实际 Task 适配器，仅供 Scheduler 内部执行。
    pub(crate) inner: Arc<dyn ScheduledTask>,
}

impl TaskRegistration {
    /// 把不产生业务值的 [`ActionTask`] 注册为统一 Task。
    pub fn action<T>(task: Arc<T>) -> Self
    where
        T: ActionTask,
    {
        Self {
            inner: Arc::new(ActionAdapter { task }),
        }
    }

    /// 把 [`ValueTask`] 输出发送到有界 Tokio Channel。
    ///
    /// Channel 满时发送会异步等待，并受 Trigger timeout 和取消控制；接收端关闭会自动删除 Job。
    pub fn channel<T>(task: Arc<T>, sender: mpsc::Sender<T::Output>) -> Self
    where
        T: ValueTask,
    {
        Self {
            inner: Arc::new(ChannelAdapter { task, sender }),
        }
    }

    /// 把 [`ValueTask`] 输出交给异步 Callback。
    ///
    /// Callback 临时失败不会重新运行 Task，避免重复采集或重复副作用。
    pub fn callback<T, C>(task: Arc<T>, callback: C) -> Self
    where
        T: ValueTask,
        C: AsyncCallback<T::Output>,
    {
        Self {
            inner: Arc::new(CallbackAdapter {
                task,
                callback: Arc::new(callback),
            }),
        }
    }

    /// 返回注册中实际 Task 的诊断名称。
    pub fn kind(&self) -> &'static str {
        self.inner.kind()
    }
}

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
        match self.task.run(context).await {
            Ok(()) => TaskRunResult::Completed,
            Err(error) => map_task_error(error),
        }
    }
}

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
        let value = match self.task.run(context).await {
            Ok(value) => value,
            Err(error) => return map_task_error(error),
        };
        match self.sender.send(value).await {
            Ok(()) => TaskRunResult::OutputDelivered,
            Err(_) => TaskRunResult::ChannelClosed,
        }
    }
}

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
        let value = match self.task.run(context.clone()).await {
            Ok(value) => value,
            Err(error) => return map_task_error(error),
        };
        match self.callback.call(context, value).await {
            Ok(()) => TaskRunResult::OutputDelivered,
            Err(CallbackError::Transient(error)) => {
                TaskRunResult::CallbackTransient(format!("{error:#}"))
            }
            Err(CallbackError::Permanent(error)) => {
                TaskRunResult::CallbackPermanent(format!("{error:#}"))
            }
        }
    }
}

/// 把公共 TaskError 映射为 Scheduler 内部统一结果。
fn map_task_error(error: TaskError) -> TaskRunResult {
    match error {
        TaskError::Transient(error) => TaskRunResult::TaskTransient(format!("{error:#}")),
        TaskError::Permanent(error) => TaskRunResult::TaskPermanent(format!("{error:#}")),
    }
}
