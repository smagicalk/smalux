//! 异步 Job 调度公共入口。
//!
//! 调用方通常只需要从本模块导入 Trigger、Task、Scheduler 和快照模型；内部队列与
//! Actor 实现保持私有，避免业务代码依赖调度细节。

mod config;
pub mod event;
mod model;
mod queue;
mod runtime;
pub mod task;

pub use config::{
    SchedulerConfig, SchedulerConfigPatch, SchedulerConfigSnapshot, SchedulerError, SchedulerStatus,
};
pub use event::{SchedulerEvent, SchedulerEventKind};
pub use model::{
    CapacityPolicy, ExecutionRetryPolicy, FailurePolicy, JobId, JobOptions, JobPatch, JobPriority,
    JobSnapshot, JobState, MisfirePolicy, PatchValue, RescheduleMode, RetryCondition, RunId,
    Schedule, Trigger, TriggerCoalescing,
};
pub use runtime::{Scheduler, SchedulerRuntime};
pub use task::{
    ActionTask, CallbackError, TaskBinding, TaskCancellationMode, TaskContext, TaskError,
    ValueTask, async_callback,
};
