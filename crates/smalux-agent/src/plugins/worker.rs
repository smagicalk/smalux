//! Agent 与单个 Plus Worker 进程的生命周期和请求关联。

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use smalux_plus_core::{
    PluginError,
    framing::{read_frame, write_frame},
    protocol::{
        self, CancelTask, ExecuteTask, Hello, InitializeWorker, Shutdown, WorkerFrame,
        WorkerRequest, WorkerResponse, worker_frame, worker_request, worker_response,
    },
};
use tokio::{
    io::BufReader,
    process::{Child, ChildStdin, Command},
    sync::{Mutex, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::InstalledPlugin;

/// Worker 任务完成时由 IPC 转换的结果。
#[derive(Clone, Debug)]
pub struct WorkerTaskOutput {
    pub summary: String,
    pub metrics: Vec<(String, f64)>,
    pub payload: Vec<u8>,
}

/// Worker 启动、协议或任务失败。
#[derive(Debug, thiserror::Error)]
pub enum PluginWorkerError {
    #[error("failed to start Plus Worker: {0}")]
    Start(#[source] std::io::Error),
    #[error("Plus Worker protocol failure: {0}")]
    Protocol(#[from] PluginError),
    #[error("Plus Worker {plugin_id} returned an invalid handshake response")]
    InvalidHandshake { plugin_id: String },
    #[error("Plus Worker request was cancelled")]
    Cancelled,
    #[error("Plus Worker request channel closed")]
    Closed,
    #[error("Plus Worker task failed: {0}")]
    TaskFailed(String),
    #[error("Plus Worker rejected runtime initialization: {0}")]
    InitializationFailed(String),
    #[error("Plus Worker timed out during {phase}")]
    Timeout { phase: &'static str },
}

pub(super) struct WorkerStartOptions {
    pub config_revision: u64,
    pub runtime_config: Vec<u8>,
    pub max_concurrency: u32,
    pub agent_context: smalux_plus_core::protocol::AgentContextMessage,
    pub task_timeout: std::time::Duration,
    pub startup_timeout: std::time::Duration,
    pub shutdown_timeout: std::time::Duration,
}

type Pending = oneshot::Sender<Result<WorkerTaskOutput, PluginWorkerError>>;

/// 一个可复用的插件子进程。所有 stdout 帧由单个 reader 任务解码并按 request_id 分发。
pub struct PluginWorkerClient {
    writer: Arc<Mutex<ChildStdin>>,
    child: Mutex<Child>,
    task_timeout: std::time::Duration,
    shutdown_timeout: std::time::Duration,
    effective_concurrency: u32,
    pending: Arc<Mutex<HashMap<String, Pending>>>,
    task_kinds: Vec<String>,
    reader_closed: Arc<AtomicBool>,
    _reader: tokio::task::JoinHandle<()>,
}

impl PluginWorkerClient {
    pub async fn pid(&self) -> Option<u32> {
        self.child.lock().await.id()
    }

    /// 非阻塞检查 Worker 是否已经退出；主动 shutdown 也会返回退出状态。
    pub async fn try_wait(&self) -> Result<Option<std::process::ExitStatus>, std::io::Error> {
        self.child.lock().await.try_wait()
    }

    pub fn reader_closed(&self) -> bool {
        self.reader_closed.load(Ordering::Acquire)
    }

    pub fn effective_concurrency(&self) -> u32 {
        self.effective_concurrency
    }

    async fn write_request(
        &self,
        body: worker_request::Body,
        limit: std::time::Duration,
    ) -> Result<(), PluginWorkerError> {
        timeout(limit, async {
            let mut writer = self.writer.lock().await;
            send_request(&mut writer, body).await
        })
        .await
        .map_err(|_| PluginWorkerError::Timeout { phase: "IPC write" })?
        .map_err(Into::into)
    }
    /// 启动 Worker，并在公开为可执行前完成 Hello 和 Initialize 两次确认。
    pub(super) async fn start(
        plugin: &InstalledPlugin,
        options: WorkerStartOptions,
    ) -> Result<Self, PluginWorkerError> {
        let WorkerStartOptions {
            config_revision,
            runtime_config,
            max_concurrency,
            agent_context,
            task_timeout,
            startup_timeout,
            shutdown_timeout,
        } = options;
        let mut child = Command::new(plugin.entrypoint())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            // Worker 的业务日志只允许写 stderr，继承后可被 Agent 日志采集器处理。
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(PluginWorkerError::Start)?;
        let mut writer = match child.stdin.take() {
            Some(writer) => writer,
            None => {
                terminate_child(&mut child).await;
                return Err(PluginWorkerError::Closed);
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                terminate_child(&mut child).await;
                return Err(PluginWorkerError::Closed);
            }
        };
        let mut reader = BufReader::new(stdout);
        let expected_task_kinds = plugin.manifest.task_types.clone();

        let handshake = async {
            timeout(
                startup_timeout,
                send_request(
                    &mut writer,
                    worker_request::Body::Hello(Hello {
                        protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                        expected_plugin_id: plugin.manifest.plugin_id.clone(),
                    }),
                ),
            )
            .await
            .map_err(|_| PluginWorkerError::Timeout {
                phase: "Hello write",
            })??;
            let first_ready = timeout(
                startup_timeout,
                read_ready(
                    &mut reader,
                    &plugin.manifest.plugin_id,
                    &expected_task_kinds,
                ),
            )
            .await
            .map_err(|_| PluginWorkerError::Timeout { phase: "Hello" })??;

            timeout(
                startup_timeout,
                send_request(
                    &mut writer,
                    worker_request::Body::Initialize(InitializeWorker {
                        protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                        plugin_id: plugin.manifest.plugin_id.clone(),
                        config_revision,
                        runtime_config,
                        max_concurrency,
                        agent_context: Some(agent_context),
                    }),
                ),
            )
            .await
            .map_err(|_| PluginWorkerError::Timeout {
                phase: "Initialize write",
            })??;
            let ready = timeout(
                startup_timeout,
                read_ready(
                    &mut reader,
                    &plugin.manifest.plugin_id,
                    &expected_task_kinds,
                ),
            )
            .await
            .map_err(|_| PluginWorkerError::Timeout {
                phase: "Initialize",
            })??;
            if ready.config_revision != config_revision
                || ready.task_kinds != first_ready.task_kinds
            {
                return Err(PluginWorkerError::InvalidHandshake {
                    plugin_id: plugin.manifest.plugin_id.clone(),
                });
            }
            if ready.effective_max_concurrency == 0
                || ready.effective_max_concurrency > max_concurrency
            {
                return Err(PluginWorkerError::InvalidHandshake {
                    plugin_id: plugin.manifest.plugin_id.clone(),
                });
            }
            Ok::<(Vec<String>, u32), PluginWorkerError>((
                ready.task_kinds,
                ready.effective_max_concurrency,
            ))
        }
        .await;
        let (task_kinds, effective_concurrency) = match handshake {
            Ok(value) => value,
            Err(error) => {
                terminate_child(&mut child).await;
                return Err(error);
            }
        };

        let pending: Arc<Mutex<HashMap<String, Pending>>> = Arc::new(Mutex::new(HashMap::new()));
        let reader_pending = Arc::clone(&pending);
        let reader_closed = Arc::new(AtomicBool::new(false));
        let reader_closed_flag = Arc::clone(&reader_closed);
        let reader_task = tokio::spawn(async move {
            while let Ok(Some(frame)) = read_frame(&mut reader).await {
                let Some(worker_frame::Body::Response(WorkerResponse { body })) = frame.body else {
                    continue;
                };
                match body {
                    Some(worker_response::Body::Result(result)) => {
                        let response = match protocol::TaskStatus::try_from(result.status) {
                            Ok(protocol::TaskStatus::Succeeded) => Ok(WorkerTaskOutput {
                                summary: result.summary,
                                metrics: result
                                    .metrics
                                    .into_iter()
                                    .map(|metric| (metric.name, metric.value))
                                    .collect(),
                                payload: result.payload,
                            }),
                            _ => {
                                Err(PluginWorkerError::TaskFailed(result.error.unwrap_or_else(
                                    || "Worker task did not succeed".to_owned(),
                                )))
                            }
                        };
                        if let Some(sender) = reader_pending.lock().await.remove(&result.request_id)
                        {
                            let _ = sender.send(response);
                        }
                    }
                    Some(worker_response::Body::Error(error)) => {
                        if let Some(sender) = reader_pending.lock().await.remove(&error.request_id)
                        {
                            let _ = sender.send(Err(PluginWorkerError::TaskFailed(error.message)));
                        }
                    }
                    _ => {}
                }
            }
            let mut pending = reader_pending.lock().await;
            for (_, sender) in pending.drain() {
                let _ = sender.send(Err(PluginWorkerError::Closed));
            }
            reader_closed_flag.store(true, Ordering::Release);
        });

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            child: Mutex::new(child),
            task_timeout,
            effective_concurrency,
            pending,
            task_kinds,
            shutdown_timeout,
            reader_closed,
            _reader: reader_task,
        })
    }

    /// 请求 Worker 优雅停止，并在异常时强制回收子进程。
    pub async fn shutdown(&self) {
        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        let _ = self
            .write_request(
                worker_request::Body::Shutdown(Shutdown {
                    reason: "Agent replaced the Plus runtime configuration".to_owned(),
                }),
                self.shutdown_timeout,
            )
            .await;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let mut child = self.child.lock().await;
        if timeout(remaining, child.wait()).await.is_err() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }

    /// 强制终止失控 Worker，并等待操作系统回收子进程。
    pub async fn terminate(&self) {
        let mut child = self.child.lock().await;
        terminate_child(&mut child).await;
    }

    /// 执行一个已由 Manifest 声明的 Task，并把 Agent 的协作取消传递给 Worker。
    pub async fn execute(
        &self,
        run_id: Uuid,
        task_kind: &str,
        schema_version: u32,
        config: Vec<u8>,
        cancellation: CancellationToken,
    ) -> Result<WorkerTaskOutput, PluginWorkerError> {
        if !self.task_kinds.iter().any(|kind| kind == task_kind) {
            return Err(PluginWorkerError::TaskFailed(
                "Worker does not declare requested Task kind".to_owned(),
            ));
        }
        let request_id = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(request_id.clone(), sender);
        let write_result = match timeout(self.task_timeout, async {
            let mut writer = self.writer.lock().await;
            send_request(
                &mut writer,
                worker_request::Body::Execute(ExecuteTask {
                    request_id: request_id.clone(),
                    run_id: run_id.as_bytes().to_vec(),
                    task_kind: task_kind.to_owned(),
                    schema_version,
                    config,
                    deadline_unix_millis: (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .saturating_add(self.task_timeout)
                        .as_millis()) as u64,
                }),
            )
            .await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                self.terminate().await;
                return Err(PluginWorkerError::Timeout {
                    phase: "Execute write",
                });
            }
        };
        if let Err(error) = write_result {
            self.pending.lock().await.remove(&request_id);
            return Err(error.into());
        }
        tokio::select! {
            result = receiver => result.map_err(|_| PluginWorkerError::Closed)?,
            () = cancellation.cancelled() => {
                self.pending.lock().await.remove(&request_id);
                let _ = self.write_request(
                    worker_request::Body::Cancel(CancelTask {
                        request_id,
                        reason: "Agent Scheduler cancelled the Task".to_owned(),
                    }),
                    self.task_timeout,
                )
                .await;
                Err(PluginWorkerError::Cancelled)
            }
            _ = tokio::time::sleep(self.task_timeout) => {
                self.pending.lock().await.remove(&request_id);
                let _ = self.write_request(
                    worker_request::Body::Cancel(CancelTask {
                        request_id,
                        reason: "Agent Worker task deadline exceeded".to_owned(),
                    }),
                    self.task_timeout,
                )
                .await;
                self.terminate().await;
                Err(PluginWorkerError::Timeout { phase: "Execute" })
            }
        }
    }
}

async fn terminate_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill().await;
    }
    let _ = child.wait().await;
}

async fn send_request(
    writer: &mut ChildStdin,
    body: worker_request::Body,
) -> Result<(), PluginError> {
    write_frame(
        writer,
        &WorkerFrame {
            body: Some(worker_frame::Body::Request(WorkerRequest {
                body: Some(body),
            })),
        },
    )
    .await
}

async fn read_ready(
    reader: &mut BufReader<tokio::process::ChildStdout>,
    expected_plugin_id: &str,
    expected_task_kinds: &[String],
) -> Result<protocol::WorkerReady, PluginWorkerError> {
    let frame = read_frame(reader).await?.ok_or(PluginWorkerError::Closed)?;
    let Some(worker_frame::Body::Response(WorkerResponse { body })) = frame.body else {
        return Err(PluginWorkerError::InvalidHandshake {
            plugin_id: expected_plugin_id.to_owned(),
        });
    };
    let ready = match body {
        Some(worker_response::Body::Ready(ready)) => ready,
        Some(worker_response::Body::Error(error)) if error.code == "initialization_failed" => {
            return Err(PluginWorkerError::InitializationFailed(error.message));
        }
        Some(worker_response::Body::Error(error)) => {
            return Err(PluginWorkerError::TaskFailed(error.message));
        }
        _ => {
            return Err(PluginWorkerError::InvalidHandshake {
                plugin_id: expected_plugin_id.to_owned(),
            });
        }
    };
    if ready.protocol_version != protocol::WORKER_PROTOCOL_VERSION
        || ready.plugin_id != expected_plugin_id
    {
        return Err(PluginWorkerError::InvalidHandshake {
            plugin_id: expected_plugin_id.to_owned(),
        });
    }
    if ready.task_kinds != expected_task_kinds {
        return Err(PluginWorkerError::InvalidHandshake {
            plugin_id: expected_plugin_id.to_owned(),
        });
    }
    Ok(ready)
}
