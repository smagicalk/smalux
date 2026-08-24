//! Plus Worker 的 stdin/stdout 运行时。

use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader, stdin, stdout},
    sync::Mutex,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    PluginError, PlusTask, PlusTaskContext, PlusTaskError, PlusTaskOutput,
    framing::{read_frame, write_frame},
    protocol::{
        self, WorkerFrame, WorkerRequest, WorkerResponse, worker_frame, worker_request,
        worker_response,
    },
};

/// Worker SDK 的构造器；插件只需注册任务后调用 [`PlusWorker::run_stdio`]。
pub struct PlusWorker {
    plugin_id: String,
    tasks: HashMap<String, Arc<dyn PlusTask>>,
    max_concurrency: usize,
}

impl PlusWorker {
    pub fn builder(plugin_id: impl Into<String>) -> PlusWorkerBuilder {
        PlusWorkerBuilder {
            plugin_id: plugin_id.into(),
            tasks: HashMap::new(),
            max_concurrency: 1,
        }
    }

    /// 在标准输入输出上运行 Worker；日志必须写 stderr。
    pub async fn run_stdio(self) -> Result<(), PluginError> {
        self.run(BufReader::new(stdin()), stdout()).await
    }

    /// 在任意异步字节流上运行 Worker 核心协议；供 stdio Adapter 与内存流测试共用。
    pub async fn run<R, W>(self, mut reader: R, writer: W) -> Result<(), PluginError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        // 多个任务可以同时完成；用一个异步互斥锁保证长度前缀和 protobuf 内容不会交叉。
        let writer = Arc::new(Mutex::new(writer));
        let first = read_frame(&mut reader).await?.ok_or_else(|| {
            PluginError::ExecutionFailed("Worker input closed before Hello".to_owned())
        })?;
        let Some(worker_frame::Body::Request(WorkerRequest {
            body: Some(worker_request::Body::Hello(hello)),
        })) = first.body
        else {
            return Err(PluginError::ExecutionFailed(
                "Worker must receive Hello as its first message".to_owned(),
            ));
        };
        if hello.protocol_version != protocol::WORKER_PROTOCOL_VERSION
            || hello.expected_plugin_id != self.plugin_id
        {
            return Err(PluginError::IdentityMismatch {
                expected: self.plugin_id.clone(),
                actual: hello.expected_plugin_id,
            });
        }
        let plugin_id = self.plugin_id;
        let worker_max_concurrency = self.max_concurrency;
        let tasks = Arc::new(self.tasks);
        let task_kinds = sorted_task_kinds(&tasks);
        send_response(
            &writer,
            ready_frame(&plugin_id, &task_kinds, 0, worker_max_concurrency),
        )
        .await?;

        let initialize = read_frame(&mut reader).await?.ok_or_else(|| {
            PluginError::ExecutionFailed("Worker input closed before Initialize".to_owned())
        })?;
        let Some(worker_frame::Body::Request(WorkerRequest {
            body: Some(worker_request::Body::Initialize(initialize)),
        })) = initialize.body
        else {
            return Err(PluginError::ExecutionFailed(
                "Worker must receive Initialize after Hello".to_owned(),
            ));
        };
        if initialize.plugin_id != plugin_id {
            return Err(PluginError::IdentityMismatch {
                expected: plugin_id,
                actual: initialize.plugin_id,
            });
        }
        if initialize.protocol_version != protocol::WORKER_PROTOCOL_VERSION {
            return Err(PluginError::ExecutionFailed(format!(
                "unsupported Worker protocol version {}",
                initialize.protocol_version
            )));
        }
        let agent_context = initialize.agent_context.map(Into::into).unwrap_or_default();
        let requested_concurrency =
            usize::try_from(initialize.max_concurrency).unwrap_or(usize::MAX);
        if requested_concurrency == 0 {
            return Err(PluginError::ExecutionFailed(
                "Agent must initialize Worker with non-zero max_concurrency".to_owned(),
            ));
        }
        let effective_concurrency = worker_max_concurrency.min(requested_concurrency);
        for kind in &task_kinds {
            if let Err(error) = tasks[kind]
                .initialize(&agent_context, &initialize.runtime_config)
                .await
            {
                let message = format!("task {kind} rejected Worker runtime configuration: {error}");
                send_response(&writer, error_frame("", "initialization_failed", &message)).await?;
                return Err(PluginError::ExecutionFailed(message));
            }
        }
        send_response(
            &writer,
            ready_frame(
                &plugin_id,
                &task_kinds,
                initialize.config_revision,
                effective_concurrency,
            ),
        )
        .await?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(effective_concurrency));
        let cancellations: Arc<Mutex<HashMap<String, CancellationToken>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut executions = JoinSet::new();
        while let Some(frame) = read_frame(&mut reader).await? {
            let Some(worker_frame::Body::Request(request)) = frame.body else {
                continue;
            };
            match request.body {
                Some(worker_request::Body::Initialize(_)) => {
                    return Err(PluginError::ExecutionFailed(
                        "Worker may only be initialized once".to_owned(),
                    ));
                }
                Some(worker_request::Body::Execute(execute)) => {
                    let Some(task) = tasks.get(&execute.task_kind).cloned() else {
                        send_response(
                            &writer,
                            error_frame(
                                &execute.request_id,
                                "unknown_task",
                                "task kind is not registered",
                            ),
                        )
                        .await?;
                        continue;
                    };
                    let request_id = execute.request_id.clone();
                    let run_id = execute.run_id.clone();
                    let cancellation = CancellationToken::new();
                    cancellations
                        .lock()
                        .await
                        .insert(request_id.clone(), cancellation.clone());
                    // 先确认任务已经接收，再异步执行，令读取循环继续处理 Ping/Cancel/Shutdown。
                    send_response(
                        &writer,
                        WorkerFrame {
                            body: Some(worker_frame::Body::Response(WorkerResponse {
                                body: Some(worker_response::Body::Started(protocol::TaskStarted {
                                    request_id: request_id.clone(),
                                })),
                            })),
                        },
                    )
                    .await?;

                    let task_config = execute.config;
                    let agent_context = agent_context.clone();
                    let deadline = deadline_duration(execute.deadline_unix_millis);
                    let semaphore = semaphore.clone();
                    let cancellations = cancellations.clone();
                    let writer = writer.clone();
                    executions.spawn(async move {
                        let result = async {
                            let permit = semaphore.acquire_owned().await.map_err(|_| {
                                PlusTaskError::Failed("Worker semaphore closed".to_owned())
                            })?;
                            let context = PlusTaskContext {
                                request_id: request_id.clone(),
                                run_id: run_id.clone(),
                                deadline,
                                cancellation,
                                agent: agent_context,
                            };
                            let result = task.execute(context, &task_config).await;
                            drop(permit);
                            result
                        };
                        let result = match deadline {
                            Some(duration) => tokio::time::timeout(duration, result)
                                .await
                                .unwrap_or(Err(PlusTaskError::Timeout)),
                            None => result.await,
                        };
                        cancellations.lock().await.remove(&request_id);
                        let response = task_result(request_id, run_id, result);
                        if let Err(error) = send_response(
                            &writer,
                            WorkerFrame {
                                body: Some(worker_frame::Body::Response(WorkerResponse {
                                    body: Some(worker_response::Body::Result(response)),
                                })),
                            },
                        )
                        .await
                        {
                            eprintln!("plus worker failed to write task result: {error}");
                        }
                    });
                }
                Some(worker_request::Body::Cancel(cancel)) => {
                    let token = cancellations.lock().await.get(&cancel.request_id).cloned();
                    if let Some(token) = token {
                        token.cancel();
                        send_response(
                            &writer,
                            WorkerFrame {
                                body: Some(worker_frame::Body::Response(WorkerResponse {
                                    body: Some(worker_response::Body::Cancelled(
                                        protocol::TaskCancelled {
                                            request_id: cancel.request_id,
                                        },
                                    )),
                                })),
                            },
                        )
                        .await?;
                    }
                }
                Some(worker_request::Body::Ping(ping)) => {
                    send_response(
                        &writer,
                        WorkerFrame {
                            body: Some(worker_frame::Body::Response(WorkerResponse {
                                body: Some(worker_response::Body::Pong(protocol::Pong {
                                    nonce: ping.nonce,
                                })),
                            })),
                        },
                    )
                    .await?;
                }
                Some(worker_request::Body::Shutdown(shutdown)) => {
                    let active_cancellations = cancellations
                        .lock()
                        .await
                        .values()
                        .cloned()
                        .collect::<Vec<_>>();
                    for token in active_cancellations {
                        token.cancel();
                    }
                    while let Some(result) = executions.join_next().await {
                        if let Err(error) = result {
                            eprintln!("plus worker task join failed during shutdown: {error}");
                        }
                    }
                    for kind in &task_kinds {
                        if let Err(error) = tasks[kind].shutdown(&shutdown.reason).await {
                            eprintln!("plus task {kind} failed during Worker shutdown: {error}");
                        }
                    }
                    send_response(
                        &writer,
                        WorkerFrame {
                            body: Some(worker_frame::Body::Response(WorkerResponse {
                                body: Some(worker_response::Body::Stopped(protocol::Stopped {
                                    reason: shutdown.reason,
                                })),
                            })),
                        },
                    )
                    .await?;
                    return Ok(());
                }
                Some(worker_request::Body::Hello(_)) | None => {
                    return Err(PluginError::ExecutionFailed(
                        "Worker received an unexpected protocol frame".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn ready_frame(
    plugin_id: &str,
    task_kinds: &[String],
    config_revision: u64,
    effective_max_concurrency: usize,
) -> WorkerFrame {
    WorkerFrame {
        body: Some(worker_frame::Body::Response(WorkerResponse {
            body: Some(worker_response::Body::Ready(protocol::WorkerReady {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                plugin_id: plugin_id.to_owned(),
                config_revision,
                task_kinds: task_kinds.to_vec(),
                effective_max_concurrency: effective_max_concurrency.try_into().unwrap_or(u32::MAX),
            })),
        })),
    }
}

fn sorted_task_kinds(tasks: &HashMap<String, Arc<dyn PlusTask>>) -> Vec<String> {
    let mut task_kinds = tasks.keys().cloned().collect::<Vec<_>>();
    task_kinds.sort_unstable();
    task_kinds
}

fn error_frame(request_id: &str, code: &str, message: &str) -> WorkerFrame {
    WorkerFrame {
        body: Some(worker_frame::Body::Response(WorkerResponse {
            body: Some(worker_response::Body::Error(protocol::ErrorResponse {
                request_id: request_id.to_owned(),
                code: code.to_owned(),
                message: message.to_owned(),
            })),
        })),
    }
}

async fn send_response<W>(
    writer: &Arc<Mutex<W>>,
    frame: WorkerFrame,
) -> Result<(), PluginError>
where
    W: AsyncWrite + Unpin,
{
    let mut writer = writer.lock().await;
    write_frame(&mut *writer, &frame).await
}

fn deadline_duration(deadline_unix_millis: u64) -> Option<std::time::Duration> {
    if deadline_unix_millis == 0 {
        return None;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    Some(std::time::Duration::from_millis(
        deadline_unix_millis.saturating_sub(now),
    ))
}

pub struct PlusWorkerBuilder {
    plugin_id: String,
    tasks: HashMap<String, Arc<dyn PlusTask>>,
    max_concurrency: usize,
}

impl PlusWorkerBuilder {
    pub fn max_concurrency(mut self, value: usize) -> Self {
        self.max_concurrency = value.max(1);
        self
    }

    pub fn register<T>(mut self, task: T) -> Self
    where
        T: PlusTask,
    {
        self.tasks.insert(task.kind().to_owned(), Arc::new(task));
        self
    }

    pub fn build(self) -> PlusWorker {
        PlusWorker {
            plugin_id: self.plugin_id,
            tasks: self.tasks,
            max_concurrency: self.max_concurrency,
        }
    }

    pub async fn run_stdio(self) -> Result<(), PluginError> {
        self.build().run_stdio().await
    }
}

fn task_result(
    request_id: String,
    run_id: Vec<u8>,
    result: Result<PlusTaskOutput, PlusTaskError>,
) -> protocol::TaskResult {
    match result {
        Ok(output) => protocol::TaskResult {
            request_id,
            run_id,
            status: protocol::TaskStatus::Succeeded as i32,
            summary: output.summary,
            metrics: output
                .metrics
                .into_iter()
                .map(|(name, value)| protocol::Metric { name, value })
                .collect(),
            payload: output.payload,
            error: None,
        },
        Err(error) => protocol::TaskResult {
            request_id,
            run_id,
            status: match error {
                PlusTaskError::Cancelled(_) => protocol::TaskStatus::Cancelled as i32,
                _ => protocol::TaskStatus::Failed as i32,
            },
            summary: String::new(),
            metrics: Vec::new(),
            payload: Vec::new(),
            error: Some(error.to_string()),
        },
    }
}

impl From<protocol::AgentContextMessage> for crate::AgentContext {
    fn from(value: protocol::AgentContextMessage) -> Self {
        Self {
            agent_version: value.agent_version,
            operating_system: value.operating_system,
            architecture: value.architecture,
            agent_id: value.agent_id,
            data_dir: value.data_dir,
            config_dir: value.config_dir,
            plugin_dir: value.plugin_dir,
            plugin_data_dir: value.plugin_data_dir,
        }
    }
}
