//! Agent 进程内的 Job 结果出口。
//!
//! 这个 module 目前只负责内存队列：断线时保留结果，重新连接后按原顺序发送。
//! 持久化队列属于后续功能，届时可以在这个 seam 后面增加具体 Adapter，不改变主事件循环。

use std::{collections::VecDeque, future::Future};

use smalux_protocol::agent::v1::{JobCommandResult, TaskReport};

use smalux_agent::client::{SmaluxClientError, SmaluxClientHandle};
use smalux_agent::management::JobResultBufferStats;

/// 短时断线期间保存采集结果的有界内存队列。
///
/// 报告进入队列即视为被 Agent 本地接受，不会让 Scheduler 重新执行已经完成的 Task。
/// 队列不跨进程恢复，也不等待 Server 业务 ACK；成功写入当前加密会话后即可出队。
pub(super) struct TaskReportOutbox {
    pending: VecDeque<TaskReport>,
    capacity: usize,
    dropped: u64,
}

impl TaskReportOutbox {
    /// 创建固定容量队列；零容量会使所有成功结果静默丢失，因此直接拒绝。
    pub(super) fn new(capacity: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            capacity > 0,
            "Task report outbox capacity must be greater than zero"
        );
        Ok(Self {
            pending: VecDeque::with_capacity(capacity),
            capacity,
            dropped: 0,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn dropped_count(&self) -> u64 {
        self.dropped
    }

    /// 在线且没有积压时直接发送；临时断线或已有积压时按 FIFO 入队。
    pub(super) async fn submit(
        &mut self,
        client: &SmaluxClientHandle,
        report: TaskReport,
    ) -> anyhow::Result<()> {
        self.submit_with(report, |report| client.send_task_report(report))
            .await
    }

    async fn submit_with<F, Fut>(&mut self, report: TaskReport, send: F) -> anyhow::Result<()>
    where
        F: FnOnce(TaskReport) -> Fut,
        Fut: Future<Output = Result<(), SmaluxClientError>>,
    {
        if !self.pending.is_empty() {
            self.push(report);
            return Ok(());
        }
        match send(report.clone()).await {
            Ok(()) => Ok(()),
            Err(SmaluxClientError::TemporarilyUnavailable) => {
                self.push(report);
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// 按采集顺序补发；再次断线时保留当前项及其后的全部报告。
    pub(super) async fn flush(&mut self, client: &SmaluxClientHandle) -> anyhow::Result<()> {
        self.flush_with(|report| client.send_task_report(report))
            .await
    }

    async fn flush_with<F, Fut>(&mut self, mut send: F) -> anyhow::Result<()>
    where
        F: FnMut(TaskReport) -> Fut,
        Fut: Future<Output = Result<(), SmaluxClientError>>,
    {
        while let Some(report) = self.pending.front().cloned() {
            match send(report).await {
                Ok(()) => {
                    self.pending.pop_front();
                }
                Err(SmaluxClientError::TemporarilyUnavailable) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn push(&mut self, report: TaskReport) {
        if self.pending.len() == self.capacity {
            self.pending.pop_front();
            self.dropped = self.dropped.saturating_add(1);
            tracing::warn!(
                capacity = self.capacity,
                dropped_reports = self.dropped,
                "Agent Task report outbox dropped its oldest report"
            );
        }
        self.pending.push_back(report);
    }
}

/// 维护尚未成功发送到 Server 的 Job 结果。
pub(super) struct JobResultOutbox {
    pending: VecDeque<JobCommandResult>,
    capacity: usize,
    dropped: u64,
    stats: std::sync::Arc<JobResultBufferStats>,
}

impl JobResultOutbox {
    pub(super) fn new(
        capacity: usize,
        stats: std::sync::Arc<JobResultBufferStats>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            capacity > 0,
            "Job result outbox capacity must be greater than zero"
        );
        Ok(Self {
            pending: VecDeque::with_capacity(capacity),
            capacity,
            dropped: 0,
            stats,
        })
    }

    /// 当前是否没有待发送结果。
    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn dropped_count(&self) -> u64 {
        self.dropped
    }

    /// 立即发送结果；只有临时断线才放入队列，其他错误继续终止事件循环。
    pub(super) async fn submit(
        &mut self,
        client: &SmaluxClientHandle,
        result: JobCommandResult,
    ) -> anyhow::Result<()> {
        self.submit_with(result, |result| client.send_job_command_result(result))
            .await
    }

    /// `submit` 的内部可测试实现；发送函数仍返回正式 Client 错误分类。
    async fn submit_with<F, Fut>(&mut self, result: JobCommandResult, send: F) -> anyhow::Result<()>
    where
        F: FnOnce(JobCommandResult) -> Fut,
        Fut: Future<Output = Result<(), SmaluxClientError>>,
    {
        if !self.pending.is_empty() {
            self.push(result);
            return Ok(());
        }
        match send(result.clone()).await {
            Ok(()) => Ok(()),
            Err(SmaluxClientError::TemporarilyUnavailable) => {
                tracing::warn!(
                    pending_results = self.pending.len() + 1,
                    "Agent queued a Job command result until the session reconnects"
                );
                self.push(result);
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// 按原顺序重发断线期间积累的结果；再次断线时保留尚未发送的尾部。
    pub(super) async fn flush(&mut self, client: &SmaluxClientHandle) -> anyhow::Result<()> {
        self.flush_with(|result| client.send_job_command_result(result))
            .await
    }

    /// `flush` 的内部可测试实现，保证测试与生产路径共用同一队列算法。
    async fn flush_with<F, Fut>(&mut self, mut send: F) -> anyhow::Result<()>
    where
        F: FnMut(JobCommandResult) -> Fut,
        Fut: Future<Output = Result<(), SmaluxClientError>>,
    {
        while let Some(result) = self.pending.front().cloned() {
            match send(result).await {
                Ok(()) => {
                    self.pending.pop_front();
                    self.stats.update(self.pending.len(), self.dropped);
                }
                Err(SmaluxClientError::TemporarilyUnavailable) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn push(&mut self, result: JobCommandResult) {
        if self.pending.len() == self.capacity {
            self.pending.pop_front();
            self.dropped = self.dropped.saturating_add(1);
            tracing::warn!(
                capacity = self.capacity,
                dropped_results = self.dropped,
                "Agent Job result outbox dropped its oldest result"
            );
        }
        self.pending.push_back(result);
        self.stats.update(self.pending.len(), self.dropped);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use smalux_protocol::agent::v1::JobCommandResult;

    use super::{JobResultOutbox, SmaluxClientError, TaskReportOutbox};
    use smalux_agent::management::JobResultBufferStats;

    fn result(command_id: &str) -> JobCommandResult {
        JobCommandResult {
            command_id: command_id.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn task_report_is_buffered_when_the_session_is_temporarily_unavailable() {
        let mut outbox = TaskReportOutbox::new(4).unwrap();
        let report = smalux_protocol::agent::v1::TaskReport {
            run_id: b"run-1".to_vec(),
            ..Default::default()
        };

        outbox
            .submit_with(report, |_| async {
                Err(SmaluxClientError::TemporarilyUnavailable)
            })
            .await
            .unwrap();

        assert_eq!(outbox.pending_len(), 1);
        assert_eq!(outbox.dropped_count(), 0);
    }

    #[tokio::test]
    async fn task_report_outbox_drops_oldest_and_flushes_retained_reports_in_order() {
        let mut outbox = TaskReportOutbox::new(2).unwrap();
        for run_id in ["first", "second", "third"] {
            let report = smalux_protocol::agent::v1::TaskReport {
                run_id: run_id.as_bytes().to_vec(),
                ..Default::default()
            };
            outbox.push(report);
        }
        let sent = Arc::new(Mutex::new(Vec::new()));

        outbox
            .flush_with({
                let sent = Arc::clone(&sent);
                move |report| {
                    let sent = Arc::clone(&sent);
                    async move {
                        sent.lock().unwrap().push(report.run_id);
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(outbox.dropped_count(), 1);
        assert_eq!(
            &*sent.lock().unwrap(),
            &[b"second".to_vec(), b"third".to_vec()]
        );
        assert!(outbox.is_empty());
    }

    #[tokio::test]
    async fn pending_results_are_not_bypassed_by_a_new_send_attempt() {
        let stats = std::sync::Arc::new(JobResultBufferStats::default());
        let mut outbox = JobResultOutbox::new(2, stats).unwrap();
        outbox
            .submit_with(result("queued"), |_| async {
                Err(SmaluxClientError::TemporarilyUnavailable)
            })
            .await
            .unwrap();
        assert_eq!(outbox.pending.len(), 1);

        outbox
            .submit_with(result("rejected"), |_| async {
                Err(SmaluxClientError::NotConnected)
            })
            .await
            .expect("new results must be queued behind the existing FIFO tail");
        assert_eq!(outbox.pending.len(), 2);
    }

    #[tokio::test]
    async fn flush_preserves_order_and_keeps_unsent_tail_after_disconnect() {
        let stats = std::sync::Arc::new(JobResultBufferStats::default());
        let mut outbox = JobResultOutbox::new(4, stats).unwrap();
        outbox
            .pending
            .extend([result("first"), result("second"), result("third")]);
        let attempts = Arc::new(AtomicUsize::new(0));
        let sent = Arc::new(Mutex::new(Vec::new()));

        outbox
            .flush_with({
                let attempts = Arc::clone(&attempts);
                let sent = Arc::clone(&sent);
                move |result| {
                    let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                    let sent = Arc::clone(&sent);
                    async move {
                        if attempt == 1 {
                            return Err(SmaluxClientError::TemporarilyUnavailable);
                        }
                        sent.lock().unwrap().push(result.command_id);
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(*sent.lock().unwrap(), [b"first".to_vec()]);
        assert_eq!(
            outbox
                .pending
                .iter()
                .map(|result| result.command_id.as_slice())
                .collect::<Vec<_>>(),
            [b"second".as_slice(), b"third".as_slice()]
        );

        outbox
            .flush_with({
                let sent = Arc::clone(&sent);
                move |result| {
                    let sent = Arc::clone(&sent);
                    async move {
                        sent.lock().unwrap().push(result.command_id);
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert!(outbox.is_empty());
        assert_eq!(
            *sent.lock().unwrap(),
            [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
    }

    #[test]
    fn job_result_outbox_drops_oldest_when_full() {
        let stats = std::sync::Arc::new(JobResultBufferStats::default());
        let mut outbox = JobResultOutbox::new(2, stats).unwrap();
        outbox.push(result("first"));
        outbox.push(result("second"));
        outbox.push(result("third"));
        assert_eq!(outbox.dropped_count(), 1);
        assert_eq!(
            outbox
                .pending
                .iter()
                .map(|value| value.command_id.as_slice())
                .collect::<Vec<_>>(),
            [b"second".as_slice(), b"third".as_slice()]
        );
    }
}
