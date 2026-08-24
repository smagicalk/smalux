//! 同步采集状态的串行阻塞执行模块。

use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::Arc,
    time::Instant,
};

use anyhow::anyhow;
use tokio::sync::Mutex;

use crate::scheduler::{TaskContext, TaskError};

use super::sample::{MetricSample, duration_ms, unix_timestamp_ms};

struct CollectInner<C> {
    collector: C,
    last_sampled_at: Option<Instant>,
    failed: bool,
}

/// 串行维护同步 collector，并把阻塞刷新移出 Tokio 工作线程。
pub(crate) struct BlockingCollectorState<C> {
    inner: Arc<Mutex<CollectInner<C>>>,
}

impl<C> BlockingCollectorState<C>
where
    C: Send + 'static,
{
    pub(crate) fn new(collector: C) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CollectInner {
                collector,
                last_sampled_at: None,
                failed: false,
            })),
        }
    }

    pub(crate) async fn collect<T, F>(
        &self,
        context: TaskContext,
        collect: F,
    ) -> Result<MetricSample<T>, TaskError>
    where
        T: Send + 'static,
        F: FnOnce(&mut C) -> T + Send + 'static,
    {
        self.try_collect(context, move |collector| Ok(collect(collector)))
            .await
    }

    /// 执行可能返回系统查询错误的同步 collector，并把错误映射为可重试 Task 失败。
    pub(crate) async fn try_collect<T, F>(
        &self,
        context: TaskContext,
        collect: F,
    ) -> Result<MetricSample<T>, TaskError>
    where
        T: Send + 'static,
        F: FnOnce(&mut C) -> anyhow::Result<T> + Send + 'static,
    {
        let job_id = context.job_id;
        let run_id = context.run_id;
        let version = context.version;
        let attempt = context.attempt;
        tracing::debug!(
            job_id = %job_id,
            run_id = %run_id,
            version,
            attempt,
            "blocking collector waiting for state"
        );
        let cancellation = context.cancellation;
        let inner = self.inner.clone();
        let guard = tokio::select! {
            _ = cancellation.cancelled() => {
                tracing::debug!(
                    job_id = %job_id,
                    run_id = %run_id,
                    version,
                    attempt,
                    "blocking collector cancelled before state lock"
                );
                return Err(TaskError::Transient(anyhow!("collection cancelled before start")));
            }
            guard = inner.lock_owned() => guard,
        };

        if guard.failed {
            tracing::error!(
                job_id = %job_id,
                run_id = %run_id,
                version,
                attempt,
                "blocking collector state is unavailable after a panic"
            );
            return Err(TaskError::Permanent(anyhow!(
                "collector state is unavailable after a previous panic"
            )));
        }

        let execution = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            let sampled_at = Instant::now();
            let sampled_at_ms = unix_timestamp_ms();
            let sample_interval_ms = guard
                .last_sampled_at
                .replace(sampled_at)
                .map(|previous| duration_ms(sampled_at.duration_since(previous)));

            match catch_unwind(AssertUnwindSafe(|| collect(&mut guard.collector))) {
                Ok(Ok(snapshot)) => Ok(MetricSample::new(
                    sampled_at_ms,
                    sample_interval_ms,
                    snapshot,
                )),
                Ok(Err(error)) => Err(error),
                Err(payload) => {
                    guard.failed = true;
                    resume_unwind(payload)
                }
            }
        })
        .await;

        match execution {
            Ok(Ok(collected)) => {
                tracing::trace!(
                    job_id = %job_id,
                    run_id = %run_id,
                    version,
                    attempt,
                    sample_interval_ms = ?collected.sample_interval_ms,
                    "blocking collector completed"
                );
                Ok(collected)
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    job_id = %job_id,
                    run_id = %run_id,
                    version,
                    attempt,
                    error = %error,
                    "blocking collector query failed"
                );
                Err(TaskError::Transient(error))
            }
            Err(error) if error.is_panic() => {
                tracing::error!(
                    job_id = %job_id,
                    run_id = %run_id,
                    version,
                    attempt,
                    "blocking collector panicked"
                );
                resume_unwind(error.into_panic())
            }
            Err(error) => {
                tracing::error!(
                    job_id = %job_id,
                    run_id = %run_id,
                    version,
                    attempt,
                    error = %error,
                    "blocking collector task did not complete"
                );
                Err(TaskError::Transient(anyhow!(
                    "blocking collection did not complete: {error}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use crate::scheduler::TaskContext;

    use super::BlockingCollectorState;

    fn context() -> TaskContext {
        TaskContext {
            job_id: Uuid::new_v4(),
            version: 0,
            run_id: Uuid::new_v4(),
            attempt: 1,
            scheduled_at: Utc::now(),
            started_at: Utc::now(),
            cancellation: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn collect_state_preserves_state_and_sampling_interval() {
        let state = BlockingCollectorState::new(0_u64);

        let first = state
            .collect(context(), |value| {
                *value += 1;
                *value
            })
            .await
            .unwrap();
        let second = state
            .collect(context(), |value| {
                *value += 1;
                *value
            })
            .await
            .unwrap();

        assert_eq!(first.snapshot, 1);
        assert_eq!(first.sample_interval_ms, None);
        assert_eq!(second.snapshot, 2);
        assert!(second.sample_interval_ms.is_some());
    }

    #[tokio::test]
    async fn try_collect_maps_query_errors_to_transient_and_allows_reuse() {
        let state = BlockingCollectorState::new(0_u64);

        let error = state
            .try_collect(context(), |_| -> anyhow::Result<()> {
                anyhow::bail!("query unavailable")
            })
            .await
            .unwrap_err();
        assert!(matches!(error, crate::scheduler::TaskError::Transient(_)));

        let recovered = state.collect(context(), |_| 7).await.unwrap();
        assert_eq!(recovered.snapshot, 7);
    }

    #[tokio::test]
    async fn collect_state_propagates_panic_and_rejects_reuse() {
        let state = BlockingCollectorState::new(());

        let panic = AssertUnwindSafe(state.collect(context(), |_| -> () {
            panic!("collector failed");
        }))
        .catch_unwind()
        .await;
        assert!(panic.is_err());

        let error = state.collect(context(), |_| ()).await.unwrap_err();
        assert!(matches!(error, crate::scheduler::TaskError::Permanent(_)));
    }
}
