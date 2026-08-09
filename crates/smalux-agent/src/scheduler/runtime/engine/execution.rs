//! Task 启动、完成处理、失败重试和并发槽位管理。

use super::*;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use std::time::Instant as StdInstant;

impl SchedulerActor {
    /// 在全局并发限制内，从 ReadyQueue 选择满足单 Job 并发限制的实例。
    ///
    /// ReadyQueue 会跳过暂时受限的 Job，因此不会产生队头阻塞。
    pub(super) fn dispatch_ready(&mut self) {
        while self.total_running < self.config.global_concurrency.get() && !self.ready.is_empty() {
            let jobs = &self.jobs;
            let default_concurrency = self.config.default_job_concurrency.get();
            let pending = self.ready.pop_dispatchable(|pending| {
                jobs.get(&pending.job_id).is_some_and(|job| {
                    job.version == pending.version
                        && matches!(job.state, JobState::Enabled)
                        && job.running_count
                            < job
                                .concurrency
                                .map(NonZeroUsize::get)
                                .unwrap_or(default_concurrency)
                })
            });
            let Some(pending) = pending else { break };
            self.spawn_execution(pending);
        }
    }

    /// 占用全局和单 Job 槽位，构造 TaskContext，并把包装 Future 加入 JoinSet。
    ///
    /// 包装层统一处理 timeout、panic 和 cancellation，保证每种退出路径都返回 TaskCompletion。
    fn spawn_execution(&mut self, pending: PendingExecution) {
        let Some(job) = self.jobs.get_mut(&pending.job_id) else {
            return;
        };
        if job.version != pending.version {
            return;
        }
        let task = job.task.clone();
        let task_kind = task.kind();
        let cancellation_mode = job.cancellation_mode;
        let timeout = job.trigger.timeout;
        let started_at = Utc::now();
        job.running_count += 1;
        job.last_started_at = Some(started_at);
        job.last_fired_at = Some(pending.scheduled_at);
        self.total_running += 1;
        let cancellation = CancellationToken::new();
        self.running.insert(
            pending.run_id,
            RunningExecution {
                job_id: pending.job_id,
                version: pending.version,
                cancellation: cancellation.clone(),
            },
        );
        self.emit(SchedulerEventKind::ExecutionStarted {
            job_id: pending.job_id,
            version: pending.version,
            run_id: pending.run_id,
            attempt: pending.attempt,
            scheduled_at: pending.scheduled_at,
        });
        tracing::debug!(
            task_kind,
            job_id = %pending.job_id,
            version = pending.version,
            run_id = %pending.run_id,
            attempt = pending.attempt,
            timeout = ?timeout,
            cancellation_mode = ?cancellation_mode,
            "agent scheduler execution started"
        );

        self.tasks.spawn(async move {
            let context = TaskContext {
                job_id: pending.job_id,
                version: pending.version,
                run_id: pending.run_id,
                attempt: pending.attempt,
                scheduled_at: pending.scheduled_at,
                started_at,
                cancellation: cancellation.clone(),
            };
            let started = StdInstant::now();
            let execution = AssertUnwindSafe(task.execute(context)).catch_unwind();
            let completion = async {
                match timeout {
                    Some(timeout) => match tokio::time::timeout(timeout, execution).await {
                        Ok(Ok(result)) => CompletionOutcome::Finished(result),
                        Ok(Err(payload)) => CompletionOutcome::Panicked(panic_message(payload)),
                        Err(_) => CompletionOutcome::TimedOut,
                    },
                    None => match execution.await {
                        Ok(result) => CompletionOutcome::Finished(result),
                        Err(payload) => CompletionOutcome::Panicked(panic_message(payload)),
                    },
                }
            };
            let outcome = match cancellation_mode {
                TaskCancellationMode::Cooperative => tokio::select! {
                    _ = cancellation.cancelled() => CompletionOutcome::Cancelled,
                    result = completion => result,
                },
                // 已开始的 spawn_blocking 工作无法被 Tokio 取消，必须等待实际结束。
                TaskCancellationMode::NonCancellable => completion.await,
            };
            if matches!(outcome, CompletionOutcome::TimedOut) {
                cancellation.cancel();
            }
            TaskCompletion {
                job_id: pending.job_id,
                version: pending.version,
                run_id: pending.run_id,
                attempt: pending.attempt,
                scheduled_at: pending.scheduled_at,
                elapsed: started.elapsed(),
                outcome,
            }
        });
    }

    /// 释放执行槽位并根据统一结果更新 Job、发布事件或安排 Retry。
    ///
    /// 旧版本完成结果只释放资源，不再修改新版本 Job 状态。
    pub(super) fn handle_completion(&mut self, completion: TaskCompletion) {
        self.running.remove(&completion.run_id);
        self.total_running = self.total_running.saturating_sub(1);
        if let Some(job) = self.jobs.get_mut(&completion.job_id) {
            job.running_count = job.running_count.saturating_sub(1);
            job.last_finished_at = Some(Utc::now());
        }
        let current = self
            .jobs
            .get(&completion.job_id)
            .is_some_and(|job| job.version == completion.version);
        if !current {
            tracing::debug!(
                job_id = %completion.job_id,
                version = completion.version,
                run_id = %completion.run_id,
                attempt = completion.attempt,
                "agent scheduler ignored stale execution completion"
            );
            return;
        }
        let task_kind = self
            .jobs
            .get(&completion.job_id)
            .map(|job| job.task.kind())
            .unwrap_or("unknown");
        let metadata = completion.metadata();

        match completion.outcome {
            CompletionOutcome::Finished(TaskRunResult::Completed)
            | CompletionOutcome::Finished(TaskRunResult::OutputDelivered) => {
                let output_delivered = matches!(
                    completion.outcome,
                    CompletionOutcome::Finished(TaskRunResult::OutputDelivered)
                );
                if let Some(job) = self.jobs.get_mut(&completion.job_id) {
                    job.consecutive_failures = 0;
                    job.last_outcome = Some("success".to_owned());
                    if matches!(job.trigger.schedule, Schedule::Once { .. }) {
                        job.state = JobState::Completed;
                    }
                }
                self.emit(SchedulerEventKind::ExecutionSucceeded {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                    attempt: completion.attempt,
                    duration_ms: duration_ms(completion.elapsed),
                });
                tracing::debug!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    attempt = completion.attempt,
                    duration_ms = duration_ms(completion.elapsed),
                    output_delivered,
                    "agent scheduler execution succeeded"
                );
                if output_delivered {
                    self.emit(SchedulerEventKind::OutputDelivered {
                        job_id: completion.job_id,
                        version: completion.version,
                        run_id: completion.run_id,
                    });
                }
            }
            CompletionOutcome::Finished(TaskRunResult::TaskTransient(error)) => {
                self.handle_execution_failure(metadata, error, FailureClass::Transient);
            }
            CompletionOutcome::Finished(TaskRunResult::TaskPermanent(error)) => {
                self.handle_execution_failure(metadata, error, FailureClass::Permanent);
            }
            CompletionOutcome::Finished(TaskRunResult::CallbackTransient(error)) => {
                tracing::warn!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    error = %error,
                    "agent scheduler callback delivery failed"
                );
                self.emit(SchedulerEventKind::CallbackFailed {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                    error: error.clone(),
                });
                self.record_final_failure(completion.job_id, error, false);
            }
            CompletionOutcome::Finished(TaskRunResult::CallbackPermanent(error)) => {
                tracing::error!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    error = %error,
                    "agent scheduler callback permanently failed"
                );
                self.emit(SchedulerEventKind::CallbackFailed {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                    error: error.clone(),
                });
                let _ = self.force_disable(completion.job_id, error);
            }
            CompletionOutcome::Finished(TaskRunResult::ChannelClosed) => {
                tracing::warn!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    "agent scheduler output channel closed; deleting job"
                );
                self.emit(SchedulerEventKind::OutputChannelClosed {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                });
                self.cancel_job_version(completion.job_id, completion.version);
                let _ = self.delete_job(completion.job_id, completion.version, true);
            }
            CompletionOutcome::TimedOut => {
                tracing::warn!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    attempt = completion.attempt,
                    elapsed_ms = duration_ms(completion.elapsed),
                    "agent scheduler execution timed out"
                );
                self.emit(SchedulerEventKind::ExecutionTimedOut {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                    attempt: completion.attempt,
                });
                self.handle_execution_failure(
                    metadata,
                    "task execution timed out".to_owned(),
                    FailureClass::Timeout,
                );
            }
            CompletionOutcome::Panicked(message) => {
                tracing::error!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    attempt = completion.attempt,
                    panic = %message,
                    "agent scheduler execution panicked"
                );
                self.emit(SchedulerEventKind::ExecutionPanicked {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                    attempt: completion.attempt,
                    message: message.clone(),
                });
                self.handle_execution_failure(metadata, message, FailureClass::Panic);
            }
            CompletionOutcome::Cancelled => {
                tracing::debug!(
                    task_kind,
                    job_id = %completion.job_id,
                    version = completion.version,
                    run_id = %completion.run_id,
                    "agent scheduler execution cancelled"
                );
                self.emit(SchedulerEventKind::ExecutionCancelled {
                    job_id: completion.job_id,
                    version: completion.version,
                    run_id: completion.run_id,
                });
            }
        }
    }

    /// 处理 Task 错误、超时和 panic，决定立即停用、安排 Retry 或记录最终失败。
    fn handle_execution_failure(
        &mut self,
        execution: ExecutionMetadata,
        error: String,
        class: FailureClass,
    ) {
        let ExecutionMetadata {
            job_id,
            version,
            run_id,
            attempt,
            scheduled_at,
        } = execution;
        if class == FailureClass::Permanent {
            tracing::error!(
                job_id = %job_id,
                version,
                run_id = %run_id,
                attempt,
                failure_class = ?class,
                error = %error,
                "agent scheduler permanent task failure"
            );
            self.emit(SchedulerEventKind::ExecutionFailed {
                job_id,
                version,
                run_id,
                attempt,
                error: error.clone(),
                will_retry: false,
            });
            let _ = self.force_disable(job_id, error);
            return;
        }
        let retry = self.retry_delay(job_id, attempt, class);
        tracing::warn!(
            job_id = %job_id,
            version,
            run_id = %run_id,
            attempt,
            failure_class = ?class,
            error = %error,
            will_retry = retry.is_some(),
            "agent scheduler task execution failed"
        );
        self.emit(SchedulerEventKind::ExecutionFailed {
            job_id,
            version,
            run_id,
            attempt,
            error: error.clone(),
            will_retry: retry.is_some(),
        });
        if let Some(delay) = retry {
            let run_at = Utc::now() + TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX);
            self.schedule_retry(job_id, version, run_id, scheduled_at, attempt + 1, run_at);
        } else {
            let count = match class {
                FailureClass::Timeout => self.jobs[&job_id].failure.count_timeout,
                FailureClass::Panic => self.jobs[&job_id].failure.count_panic,
                _ => true,
            };
            self.record_final_failure(job_id, error, count);
        }
    }

    /// 根据 Job 重试策略和当前失败类型计算下一次指数退避时长。
    ///
    /// `attempt` 已包含当前尝试；达到 max_attempts 或类型未启用时返回 None。
    fn retry_delay(&self, job_id: JobId, attempt: u32, class: FailureClass) -> Option<Duration> {
        let job = self.jobs.get(&job_id)?;
        let ExecutionRetryPolicy::Exponential {
            max_attempts,
            initial_delay,
            max_delay,
            retry_on,
        } = &job.retry
        else {
            return None;
        };
        if attempt >= max_attempts.get() {
            return None;
        }
        let enabled = match class {
            FailureClass::Transient => retry_on.transient_error,
            FailureClass::Timeout => retry_on.timeout,
            FailureClass::Panic => retry_on.panic,
            FailureClass::Permanent => false,
        };
        if !enabled {
            return None;
        }
        let exponent = attempt.saturating_sub(1).min(31);
        Some(
            initial_delay
                .saturating_mul(1_u32 << exponent)
                .min(*max_delay),
        )
    }

    /// 记录一次耗尽 Retry 的最终失败，并按阈值停用或结束一次性 Job。
    fn record_final_failure(&mut self, job_id: JobId, error: String, count: bool) {
        let (disable, complete) = if let Some(job) = self.jobs.get_mut(&job_id) {
            job.last_outcome = Some(error.clone());
            if count {
                job.consecutive_failures = job.consecutive_failures.saturating_add(1);
            }
            (
                job.failure
                    .disable_after_consecutive_failures
                    .is_some_and(|limit| job.consecutive_failures >= limit.get()),
                matches!(job.trigger.schedule, Schedule::Once { .. }),
            )
        } else {
            (false, false)
        };
        if disable {
            let _ = self.force_disable(job_id, error);
        } else if complete && let Some(job) = self.jobs.get_mut(&job_id) {
            job.state = JobState::Completed;
            job.updated_at = Utc::now();
        }
    }
}

/// 把常见 panic payload 转换成可写入事件的文本。
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "task panicked with a non-string payload".to_owned()
    }
}

/// 把 Duration 安全转换为事件使用的毫秒数，溢出时饱和到 u64::MAX。
fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
