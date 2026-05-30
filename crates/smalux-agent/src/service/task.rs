//! 远程非交互任务静态能力开关。
//!
//! 远程任务用于后续备份、更新、一次性动作等非交互能力。它和交互式 shell
//! 分开建模，默认关闭，且只能由 CLI 启动参数开启。任务并发数放在动态
//! `AgentConfig.remote_task` 中，server patch 可以在能力已开启后调整。

use super::outbound::{OutboundEvent, OutboundSender, OutboundSequence, RemoteTaskResultEnvelope};
use crate::collect::unix_timestamp_secs;
use crate::config::ConfigManager;
use serde::Deserialize;
use smalux_core::utils::validate::ensure_non_empty;
use smalux_protocol::{RemoteTaskResult, RemoteTaskStatus};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

/// 远程非交互任务运行选项。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RemoteTaskOptions {
    /// 是否启用远程任务；只能由 CLI 启动参数开启。
    pub enabled: bool,
}

impl Default for RemoteTaskOptions {
    /// 默认关闭远程任务。
    fn default() -> Self {
        Self { enabled: false }
    }
}

impl RemoteTaskOptions {
    /// 校验远程任务静态选项。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// server 下发的远程非交互任务请求。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub(crate) struct RemoteTaskRunRequest {
    /// server 侧生成的任务 ID。
    pub(crate) task_id: String,
    /// 要执行的程序路径或程序名。
    pub(crate) program: String,
    /// 直接传给程序的参数；agent 不做 shell 拼接。
    #[serde(default)]
    pub(crate) args: Vec<String>,
    /// 本次任务期望超时；不能超过 agent 当前配置的 `remote_task.timeout`。
    #[serde(default, with = "humantime_serde")]
    pub(crate) timeout: Option<Duration>,
}

impl RemoteTaskRunRequest {
    /// 校验任务请求最小字段。
    fn validate(&self) -> anyhow::Result<()> {
        ensure_non_empty("remote_task.task_id", &self.task_id)?;
        ensure_non_empty("remote_task.program", &self.program)?;
        Ok(())
    }
}

/// 远程任务执行管理器。
#[derive(Debug, Clone)]
pub(crate) struct RemoteTaskManager {
    /// CLI-only 静态开关。
    options: RemoteTaskOptions,
    /// 动态配置管理器。
    config_manager: ConfigManager,
    /// 出站事件发送端。
    outbound_tx: OutboundSender,
    /// 全局出站序号。
    sequence: OutboundSequence,
    /// 当前运行中的任务数。
    running: Arc<AtomicUsize>,
}

impl RemoteTaskManager {
    /// 创建远程任务管理器。
    pub(crate) fn new(
        options: RemoteTaskOptions,
        config_manager: ConfigManager,
        outbound_tx: OutboundSender,
        sequence: OutboundSequence,
    ) -> Self {
        Self {
            options,
            config_manager,
            outbound_tx,
            sequence,
            running: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 启动一个远程非交互任务。
    pub(crate) fn start(&self, request: RemoteTaskRunRequest) -> anyhow::Result<()> {
        request.validate()?;
        let config = self.config_manager.current();
        let agent_id = config.agent_id.clone();

        if !self.options.enabled {
            self.queue_rejected_result(agent_id, request, "remote task is disabled")?;
            return Ok(());
        }

        let remote_task = config.remote_task;
        if !self.try_acquire_slot(remote_task.max_concurrent) {
            self.queue_rejected_result(agent_id, request, "remote task concurrency limit reached")?;
            return Ok(());
        }

        let timeout = request
            .timeout
            .map(|requested| requested.min(remote_task.timeout))
            .unwrap_or(remote_task.timeout);
        let manager = self.clone();
        tracing::info!(
            task_id = %request.task_id,
            program = %request.program,
            timeout_ms = timeout.as_millis(),
            max_stdout_bytes = remote_task.max_stdout_bytes,
            max_stderr_bytes = remote_task.max_stderr_bytes,
            "remote task accepted"
        );

        tokio::spawn(async move {
            let task_id = request.task_id.clone();
            let result = execute_remote_task(
                request,
                timeout,
                remote_task.max_stdout_bytes,
                remote_task.max_stderr_bytes,
            )
            .await;
            manager.running.fetch_sub(1, Ordering::AcqRel);
            manager.send_result(agent_id, result).await;
            tracing::info!(task_id = %task_id, "remote task finished");
        });

        Ok(())
    }

    /// 尝试占用一个并发槽位。
    fn try_acquire_slot(&self, max_concurrent: usize) -> bool {
        loop {
            let current = self.running.load(Ordering::Acquire);
            if current >= max_concurrent {
                return false;
            }
            if self
                .running
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// 生成拒绝结果并投递到出站队列。
    fn queue_rejected_result(
        &self,
        agent_id: String,
        request: RemoteTaskRunRequest,
        reason: &str,
    ) -> anyhow::Result<()> {
        let now = unix_timestamp_secs();
        let result = RemoteTaskResult {
            task_id: request.task_id,
            status: RemoteTaskStatus::Rejected,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at: now,
            finished_at: now,
            duration_ms: 0,
            timed_out: false,
            stdout_truncated: false,
            stderr_truncated: false,
            error: Some(reason.to_string()),
        };
        let event = self.result_event(agent_id, result);
        self.outbound_tx
            .try_send(event)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("outbound event queue is full")
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("outbound event queue is closed")
                }
            })?;
        tracing::warn!(reason, "remote task rejected");
        Ok(())
    }

    /// 异步投递执行结果。
    async fn send_result(&self, agent_id: String, result: RemoteTaskResult) {
        let task_id = result.task_id.clone();
        let event = self.result_event(agent_id, result);
        if self.outbound_tx.send(event).await.is_err() {
            tracing::warn!(task_id = %task_id, "outbound event queue closed; remote task result dropped");
        }
    }

    /// 把协议结果包装成出站事件。
    fn result_event(&self, agent_id: String, result: RemoteTaskResult) -> OutboundEvent {
        let sequence = self.sequence.next();
        OutboundEvent::RemoteTaskResult(RemoteTaskResultEnvelope::new(agent_id, sequence, result))
    }
}

/// 执行远程任务并构建协议结果。
async fn execute_remote_task(
    request: RemoteTaskRunRequest,
    timeout: Duration,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
) -> RemoteTaskResult {
    let started_at = unix_timestamp_secs();
    let started = Instant::now();
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // 远程任务是一次性执行，不允许子进程在 agent 放弃任务后继续留在后台。
        .kill_on_drop(true);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return task_result(
                request.task_id,
                RemoteTaskStatus::Failed,
                None,
                OutputText::empty(),
                OutputText::empty(),
                started_at,
                started,
                false,
                Some(error.to_string()),
            );
        }
    };

    // stdout/stderr 必须并行 drain；只等待 child.wait() 可能让高输出命令卡在 pipe 缓冲区。
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = tokio::spawn(read_limited_output(stdout, max_stdout_bytes));
    let stderr_task = tokio::spawn(read_limited_output(stderr, max_stderr_bytes));

    let wait_result = tokio::time::timeout(timeout, child.wait()).await;
    let (status, exit_code, timed_out, error) = match wait_result {
        Ok(Ok(status)) if status.success() => {
            (RemoteTaskStatus::Success, status.code(), false, None)
        }
        Ok(Ok(status)) => (RemoteTaskStatus::Failed, status.code(), false, None),
        Ok(Err(error)) => (
            RemoteTaskStatus::Failed,
            None,
            false,
            Some(error.to_string()),
        ),
        Err(_elapsed) => {
            // 超时后主动 kill，再 wait 一次回收进程句柄，避免 zombie/后台残留。
            let kill_error = child.kill().await.err().map(|error| error.to_string());
            let _ = child.wait().await;
            (RemoteTaskStatus::TimedOut, None, true, kill_error)
        }
    };

    let stdout = join_output_task(stdout_task).await;
    let stderr = join_output_task(stderr_task).await;
    task_result(
        request.task_id,
        status,
        exit_code,
        stdout,
        stderr,
        started_at,
        started,
        timed_out,
        error,
    )
}

/// 远程任务输出文本和截断状态。
#[derive(Debug, Clone, Eq, PartialEq)]
struct OutputText {
    /// 已保留的输出文本。
    text: String,
    /// 原始输出是否超过上限。
    truncated: bool,
}

impl OutputText {
    /// 空输出。
    fn empty() -> Self {
        Self {
            text: String::new(),
            truncated: false,
        }
    }
}

/// 读取 pipe 并持续 drain，超过上限的部分只丢弃不存储，避免子进程阻塞。
async fn read_limited_output<R>(reader: Option<R>, limit: usize) -> OutputText
where
    R: AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return OutputText::empty();
    };
    let mut stored = Vec::with_capacity(limit.min(8 * 1024));
    let mut truncated = false;
    let mut buffer = [0_u8; 8 * 1024];

    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => {
                truncated = true;
                tracing::warn!(error = ?error, "remote task output read failed");
                break;
            }
        };

        if stored.len() < limit {
            let remaining = limit - stored.len();
            let keep = read.min(remaining);
            stored.extend_from_slice(&buffer[..keep]);
            if keep < read {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }

    OutputText {
        text: String::from_utf8_lossy(&stored).into_owned(),
        truncated,
    }
}

/// 等待读取任务结束。
async fn join_output_task(task: tokio::task::JoinHandle<OutputText>) -> OutputText {
    match task.await {
        Ok(output) => output,
        Err(error) => OutputText {
            text: String::new(),
            truncated: true,
        }
        .with_logged_join_error(error),
    }
}

impl OutputText {
    /// 记录 join 错误并返回当前输出状态。
    fn with_logged_join_error(self, error: tokio::task::JoinError) -> Self {
        tracing::warn!(error = ?error, "remote task output reader failed");
        self
    }
}

/// 组装远程任务结果。
fn task_result(
    task_id: String,
    status: RemoteTaskStatus,
    exit_code: Option<i32>,
    stdout: OutputText,
    stderr: OutputText,
    started_at: u64,
    started: Instant,
    timed_out: bool,
    error: Option<String>,
) -> RemoteTaskResult {
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    RemoteTaskResult {
        task_id,
        status,
        exit_code,
        stdout: stdout.text,
        stderr: stderr.text,
        started_at,
        finished_at: unix_timestamp_secs(),
        duration_ms,
        timed_out,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        error,
    }
}

#[cfg(test)]
mod tests {
    //! 远程任务选项测试。

    use super::*;
    use crate::config::{AgentConfig, ConfigManager};
    use crate::service::outbound::{OutboundEvent, OutboundSequence, outbound_channel};

    /// 验证默认远程任务关闭。
    #[test]
    fn remote_task_is_disabled_by_default() {
        let options = RemoteTaskOptions::default();

        assert!(!options.enabled);
        options.validate().unwrap();
    }

    /// 构造测试任务管理器。
    fn task_manager(
        enabled: bool,
    ) -> (
        RemoteTaskManager,
        tokio::sync::mpsc::Receiver<OutboundEvent>,
    ) {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let task_manager = RemoteTaskManager::new(
            RemoteTaskOptions { enabled },
            manager,
            outbound_tx,
            OutboundSequence::default(),
        );

        (task_manager, outbound_rx)
    }

    /// 验证远程任务关闭时会回传 rejected 结果。
    #[tokio::test]
    async fn disabled_remote_task_returns_rejected_result() {
        let (manager, mut outbound_rx) = task_manager(false);

        manager
            .start(RemoteTaskRunRequest {
                task_id: "task-disabled".to_string(),
                program: test_program(),
                args: test_args("ok"),
                timeout: None,
            })
            .unwrap();

        let event = outbound_rx.recv().await.unwrap();
        let OutboundEvent::RemoteTaskResult(result) = event else {
            panic!("expected remote task result");
        };

        assert_eq!(result.sequence, 1);
        assert_eq!(result.result.status, RemoteTaskStatus::Rejected);
    }

    /// 验证启用后可以执行一个非交互命令并回传 stdout。
    #[tokio::test]
    async fn enabled_remote_task_executes_command() {
        let (manager, mut outbound_rx) = task_manager(true);

        manager
            .start(RemoteTaskRunRequest {
                task_id: "task-ok".to_string(),
                program: test_program(),
                args: test_args("smalux-task-ok"),
                timeout: Some(Duration::from_secs(5)),
            })
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let OutboundEvent::RemoteTaskResult(result) = event else {
            panic!("expected remote task result");
        };

        assert_eq!(result.result.task_id, "task-ok");
        assert_eq!(result.result.status, RemoteTaskStatus::Success);
        assert!(result.result.stdout.contains("smalux-task-ok"));
    }

    /// 返回当前平台可用的测试程序。
    fn test_program() -> String {
        if cfg!(windows) {
            "cmd.exe".to_string()
        } else {
            "/bin/sh".to_string()
        }
    }

    /// 返回当前平台 echo 参数。
    fn test_args(text: &str) -> Vec<String> {
        if cfg!(windows) {
            vec!["/C".to_string(), format!("echo {text}")]
        } else {
            vec!["-c".to_string(), format!("printf {text}")]
        }
    }
}
