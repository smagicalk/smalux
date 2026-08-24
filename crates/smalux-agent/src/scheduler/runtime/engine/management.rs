//! Job CRUD、配置更新和一致性快照。

use super::*;

/// 已完成全部校验、可以原子应用到 Job 的更新计划。
struct JobUpdatePlan {
    next_version: u64,
    next_run_at: Option<DateTime<Utc>>,
    reschedule: RescheduleMode,
    trigger_changed: bool,
    task_changed: bool,
}

impl SchedulerActor {
    /// 校验 Job 数量、Trigger 和策略，生成唯一 JobId 并安排首次运行。
    pub(super) fn add_job(
        &mut self,
        requested_id: Option<JobId>,
        generation: u64,
        enabled: bool,
        trigger: Trigger,
        task: TaskBinding,
        options: JobOptions,
    ) -> Result<JobId, SchedulerError> {
        if self.jobs.len() >= self.config.max_jobs {
            tracing::warn!(
                max_jobs = self.config.max_jobs,
                "agent scheduler rejected job because maximum job count was reached"
            );
            return Err(SchedulerError::MaximumJobsReached(self.config.max_jobs));
        }
        validate_trigger(&trigger, &options, &self.config)?;
        validate_task_cancellation_mode(&trigger, task.cancellation_mode())?;
        let now = Utc::now();
        let id = if let Some(id) = requested_id {
            if self.jobs.contains_key(&id) {
                return Err(SchedulerError::JobAlreadyExists(id));
            }
            id
        } else {
            loop {
                let id = JobId::new_v4();
                if !self.jobs.contains_key(&id) {
                    break id;
                }
            }
        };
        let (coalescing, capacity) = effective_queue_policies(&trigger, &options);
        let next_run = first_run_at(&trigger, now)?;
        let task_kind = task.inner.kind().to_owned();
        self.jobs.insert(
            id,
            JobEntry {
                id,
                version: generation,
                task: task.inner,
                cancellation_mode: task.cancellation_mode,
                trigger,
                concurrency: options.concurrency,
                max_pending: options.max_pending,
                priority: options.priority,
                coalescing,
                capacity,
                retry: options.retry,
                failure: options.failure,
                state: if enabled {
                    JobState::Enabled
                } else {
                    JobState::Disabled {
                        reason: "installed disabled".to_owned(),
                    }
                },
                normal_timer_key: None,
                next_run_at: None,
                blocked: VecDeque::new(),
                running_count: 0,
                consecutive_failures: 0,
                last_started_at: None,
                last_finished_at: None,
                last_outcome: None,
                last_fired_at: None,
                created_at: now,
                updated_at: now,
            },
        );
        if enabled {
            self.schedule_normal(id, next_run, TimerKind::Normal);
        }
        self.emit(SchedulerEventKind::JobAdded {
            job_id: id,
            version: generation,
        });
        tracing::info!(
            job_id = %id,
            version = generation,
            task_kind,
            enabled,
            "agent scheduler job added"
        );
        Ok(id)
    }

    /// 使用 expected_version 原子应用 JobPatch。
    ///
    /// 方法先构造并校验候选配置，所有可能失败的计算完成后才提交，保证错误不会污染原 Job。
    pub(super) fn update_job(
        &mut self,
        job_id: JobId,
        expected_version: u64,
        mut patch: JobPatch,
    ) -> Result<JobSnapshot, SchedulerError> {
        // 在移出权威 Job 之前完成所有可能失败的计算；失败时无需回滚或重新插入。
        let plan = self.plan_job_update(job_id, expected_version, &patch)?;
        let mut job = self.jobs.remove(&job_id).expect("validated Job must exist");
        let JobUpdatePlan {
            next_version,
            next_run_at,
            reschedule,
            trigger_changed,
            task_changed,
        } = plan;
        if let Some(trigger) = patch.trigger.take() {
            job.trigger = trigger;
        }
        if let Some(task) = patch.task.take() {
            job.task = task.inner;
            job.cancellation_mode = task.cancellation_mode;
        }
        apply_patch_value(&mut job.concurrency, patch.concurrency);
        apply_patch_value(&mut job.max_pending, patch.max_pending);
        if let Some(priority) = patch.priority {
            job.priority = priority;
        }
        if let Some(coalescing) = patch.coalescing {
            job.coalescing = coalescing;
        }
        if let Some(capacity) = patch.capacity {
            job.capacity = capacity;
        }
        if let Some(retry) = patch.retry {
            job.retry = retry;
        }
        if let Some(failure) = patch.failure {
            job.failure = failure;
        }
        job.version = next_version;
        job.updated_at = Utc::now();
        if let Some(key) = job.normal_timer_key.take() {
            let _ = self.timers.try_remove(&key);
        }
        self.clear_retry_timers(job_id);
        job.next_run_at = None;

        if trigger_changed || task_changed {
            self.ready.remove_job(job_id);
            job.blocked.clear();
            job.consecutive_failures = 0;
        } else {
            self.ready.migrate_job(job_id, job.version, job.priority);
            for blocked in &mut job.blocked {
                blocked.version = job.version;
                blocked.priority = job.priority;
            }
            if job.coalescing == TriggerCoalescing::KeepLatest {
                self.ready.remove_normal_triggers(job_id);
            }
        }

        let timer_kind = if reschedule == RescheduleMode::RunNow {
            TimerKind::RunNow
        } else {
            TimerKind::Normal
        };
        let version = job.version;
        let should_schedule = matches!(job.state, JobState::Enabled);
        self.jobs.insert(job_id, job);
        if should_schedule && let Some(next) = next_run_at {
            self.schedule_normal(job_id, next, timer_kind);
        }
        if trigger_changed || task_changed {
            self.cancel_job_version(job_id, expected_version);
        }
        self.emit(SchedulerEventKind::JobUpdated { job_id, version });
        tracing::info!(
            job_id = %job_id,
            version,
            task_kind = self.jobs[&job_id].task.kind(),
            "agent scheduler job updated"
        );
        Ok(self.snapshot(self.jobs.get(&job_id).unwrap()))
    }

    /// 从当前 Job 和 Patch 生成不可失败的提交计划，不修改任何 Actor 状态。
    fn plan_job_update(
        &self,
        job_id: JobId,
        expected_version: u64,
        patch: &JobPatch,
    ) -> Result<JobUpdatePlan, SchedulerError> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(SchedulerError::JobNotFound(job_id))?;
        if job.version != expected_version {
            return Err(SchedulerError::VersionConflict {
                job_id,
                expected: expected_version,
                actual: job.version,
            });
        }

        let candidate_trigger = patch.trigger.as_ref().unwrap_or(&job.trigger);
        let candidate_cancellation_mode = patch
            .task
            .as_ref()
            .map(TaskBinding::cancellation_mode)
            .unwrap_or(job.cancellation_mode);
        let candidate_options = patched_job_options(job, patch);
        validate_trigger(candidate_trigger, &candidate_options, &self.config)?;
        validate_task_cancellation_mode(candidate_trigger, candidate_cancellation_mode)?;

        let next_version = job
            .version
            .checked_add(1)
            .ok_or(SchedulerError::VersionOverflow(job_id))?;
        let trigger_changed = patch.trigger.is_some();
        let task_changed = patch.task.is_some();
        let reschedule = if trigger_changed && patch.reschedule == RescheduleMode::Preserve {
            RescheduleMode::Recalculate
        } else {
            patch.reschedule
        };
        let now = Utc::now();
        let next_run_at = match reschedule {
            RescheduleMode::Preserve => match job.next_run_at {
                Some(next) => Some(next),
                None if matches!(job.state, JobState::Enabled) => {
                    Some(first_run_at(candidate_trigger, now)?)
                }
                None => None,
            },
            RescheduleMode::Recalculate => Some(first_run_at(candidate_trigger, now)?),
            RescheduleMode::RunNow => Some(now),
        };

        Ok(JobUpdatePlan {
            next_version,
            next_run_at,
            reschedule,
            trigger_changed,
            task_changed,
        })
    }

    /// 启用 Disabled/Completed Job；对 Enabled Job 保持幂等。
    pub(super) fn enable_job(
        &mut self,
        job_id: JobId,
        expected_version: u64,
    ) -> Result<JobSnapshot, SchedulerError> {
        self.check_version(job_id, expected_version)?;
        if matches!(self.jobs[&job_id].state, JobState::Enabled) {
            return Ok(self.snapshot(&self.jobs[&job_id]));
        }
        self.ready.remove_job(job_id);
        self.clear_retry_timers(job_id);
        let now = Utc::now();
        let next = first_run_at(&self.jobs[&job_id].trigger, now)?;
        let next_version = self.jobs[&job_id]
            .version
            .checked_add(1)
            .ok_or(SchedulerError::VersionOverflow(job_id))?;
        let next = {
            let job = self.jobs.get_mut(&job_id).unwrap();
            job.version = next_version;
            job.state = JobState::Enabled;
            job.consecutive_failures = 0;
            job.updated_at = now;
            next
        };
        self.schedule_normal(job_id, next, TimerKind::Normal);
        let version = self.jobs[&job_id].version;
        self.emit(SchedulerEventKind::JobEnabled { job_id, version });
        tracing::info!(job_id = %job_id, version, "agent scheduler job enabled");
        Ok(self.snapshot(&self.jobs[&job_id]))
    }

    /// 校验版本后停用 Job，并返回停用后的完整快照。
    pub(super) fn disable_job(
        &mut self,
        job_id: JobId,
        expected_version: u64,
        reason: String,
    ) -> Result<JobSnapshot, SchedulerError> {
        self.check_version(job_id, expected_version)?;
        self.force_disable(job_id, reason)?;
        tracing::info!(
            job_id = %job_id,
            expected_version,
            "agent scheduler job disabled"
        );
        Ok(self.snapshot(&self.jobs[&job_id]))
    }

    /// 删除 Job 及全部 Timer/Pending，可选择是否取消正在运行的同版本实例。
    pub(super) fn delete_job(
        &mut self,
        job_id: JobId,
        expected_version: u64,
        cancel_running: bool,
    ) -> Result<(), SchedulerError> {
        self.check_version(job_id, expected_version)?;
        let mut job = self.jobs.remove(&job_id).unwrap();
        if let Some(key) = job.normal_timer_key.take() {
            let _ = self.timers.try_remove(&key);
        }
        self.clear_retry_timers(job_id);
        self.ready.remove_job(job_id);
        if cancel_running {
            self.cancel_job_version(job_id, expected_version);
        }
        self.emit(SchedulerEventKind::JobDeleted {
            job_id,
            version: expected_version,
        });
        tracing::info!(
            job_id = %job_id,
            version = expected_version,
            "agent scheduler job deleted"
        );
        Ok(())
    }

    /// 使用 revision 乐观锁应用动态配置 Patch，并重新校验已有 Job。
    pub(super) fn update_config(
        &mut self,
        expected_revision: u64,
        patch: SchedulerConfigPatch,
    ) -> Result<SchedulerConfigSnapshot, SchedulerError> {
        if self.config_revision != expected_revision {
            return Err(SchedulerError::ConfigRevisionConflict {
                expected: expected_revision,
                actual: self.config_revision,
            });
        }
        let mut config = self.config.clone();
        apply_config_patch(&mut config, patch);
        validate_config(&config)?;
        self.config_revision = self
            .config_revision
            .checked_add(1)
            .ok_or(SchedulerError::ConfigRevisionOverflow)?;
        self.config = config;
        self.disable_jobs_violating_limits();
        self.emit(SchedulerEventKind::SchedulerConfigUpdated {
            revision: self.config_revision,
        });
        tracing::info!(
            revision = self.config_revision,
            "agent scheduler configuration updated"
        );
        Ok(SchedulerConfigSnapshot {
            revision: self.config_revision,
            config: self.config.clone(),
        })
    }

    /// 校验 Job 是否存在且版本等于调用方期望值。
    fn check_version(&self, job_id: JobId, expected: u64) -> Result<(), SchedulerError> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(SchedulerError::JobNotFound(job_id))?;
        if job.version == expected {
            Ok(())
        } else {
            Err(SchedulerError::VersionConflict {
                job_id,
                expected,
                actual: job.version,
            })
        }
    }

    /// 把 Actor 权威状态转换为对外只读快照，并展开继承的默认限制。
    pub(super) fn snapshot(&self, job: &JobEntry) -> JobSnapshot {
        JobSnapshot {
            id: job.id,
            version: job.version,
            task_kind: job.task.kind().to_owned(),
            state: job.state.clone(),
            trigger: job.trigger.clone(),
            priority: job.priority,
            concurrency: job
                .concurrency
                .map(NonZeroUsize::get)
                .unwrap_or(self.config.default_job_concurrency.get()),
            max_pending: job
                .max_pending
                .unwrap_or(self.config.default_job_max_pending),
            running_count: job.running_count,
            pending_count: self.ready.count_job(job.id) + job.blocked.len(),
            consecutive_failures: job.consecutive_failures,
            next_run_at: job.next_run_at,
            last_started_at: job.last_started_at,
            last_finished_at: job.last_finished_at,
            last_outcome: job.last_outcome.clone(),
            created_at: job.created_at,
            updated_at: job.updated_at,
        }
    }

    /// 动态收紧安全限制后，停用不再满足约束的已有 Job。
    fn disable_jobs_violating_limits(&mut self) {
        let invalid = self
            .jobs
            .iter()
            .filter_map(|(id, job)| {
                (validate_trigger(&job.trigger, &job_options_from_entry(job), &self.config)
                    .is_err()
                    || validate_task_cancellation_mode(&job.trigger, job.cancellation_mode)
                        .is_err())
                .then_some(*id)
            })
            .collect::<Vec<_>>();
        for job_id in invalid {
            let _ = self.force_disable(job_id, "scheduler safety limits changed".to_owned());
        }
    }
}

/// 对可继承字段应用 Keep/Set/Inherit 三态 Patch。
fn apply_patch_value<T>(target: &mut Option<T>, patch: PatchValue<T>) {
    match patch {
        PatchValue::Keep => {}
        PatchValue::Set(value) => *target = Some(value),
        PatchValue::Inherit => *target = None,
    }
}

/// 把非 None 配置字段覆盖到候选 SchedulerConfig。
fn apply_config_patch(config: &mut SchedulerConfig, patch: SchedulerConfigPatch) {
    if let Some(value) = patch.global_concurrency {
        config.global_concurrency = value;
    }
    if let Some(value) = patch.default_job_concurrency {
        config.default_job_concurrency = value;
    }
    if let Some(value) = patch.global_max_pending {
        config.global_max_pending = value;
    }
    if let Some(value) = patch.default_job_max_pending {
        config.default_job_max_pending = value;
    }
    if let Some(value) = patch.due_batch_size {
        config.due_batch_size = value;
    }
    if let Some(value) = patch.max_jobs {
        config.max_jobs = value;
    }
    if let Some(value) = patch.shutdown_timeout {
        config.shutdown_timeout = value;
    }
    if let Some(value) = patch.minimum_interval {
        config.minimum_interval = value;
    }
    if let Some(value) = patch.minimum_retry_delay {
        config.minimum_retry_delay = value;
    }
    if let Some(value) = patch.maximum_catch_up {
        config.maximum_catch_up = value;
    }
    if let Some(value) = patch.maximum_retry_attempts {
        config.maximum_retry_attempts = value;
    }
}

/// 从 JobEntry 重建校验所需的 JobOptions，不包含运行时状态。
fn job_options_from_entry(job: &JobEntry) -> JobOptions {
    JobOptions {
        concurrency: job.concurrency,
        max_pending: job.max_pending,
        priority: job.priority,
        coalescing: Some(job.coalescing),
        capacity: Some(job.capacity),
        retry: job.retry.clone(),
        failure: job.failure.clone(),
    }
}

/// 将 Patch 覆盖到当前选项副本，仅供更新计划校验使用。
fn patched_job_options(job: &JobEntry, patch: &JobPatch) -> JobOptions {
    let mut options = job_options_from_entry(job);
    options.concurrency = match &patch.concurrency {
        PatchValue::Keep => options.concurrency,
        PatchValue::Set(value) => Some(*value),
        PatchValue::Inherit => None,
    };
    options.max_pending = match &patch.max_pending {
        PatchValue::Keep => options.max_pending,
        PatchValue::Set(value) => Some(*value),
        PatchValue::Inherit => None,
    };
    if let Some(value) = patch.priority {
        options.priority = value;
    }
    if let Some(value) = patch.coalescing {
        options.coalescing = Some(value);
    }
    if let Some(value) = patch.capacity {
        options.capacity = Some(value);
    }
    if let Some(value) = &patch.retry {
        options.retry = value.clone();
    }
    if let Some(value) = &patch.failure {
        options.failure = value.clone();
    }
    options
}

/// 阻塞工作一旦开始就不能由 Tokio 中止，禁止把“超时已结束”的错误状态暴露给 Scheduler。
fn validate_task_cancellation_mode(
    trigger: &Trigger,
    cancellation_mode: TaskCancellationMode,
) -> Result<(), SchedulerError> {
    if cancellation_mode == TaskCancellationMode::NonCancellable && trigger.timeout.is_some() {
        return Err(SchedulerError::InvalidTrigger(
            "a wait-for-completion task cannot use scheduler timeout".to_owned(),
        ));
    }
    Ok(())
}
