//! Plus Worker 的 stdin/stdout 运行时。

use std::{collections::HashMap, panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;
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
        self.run_stream(BufReader::new(stdin()), stdout()).await
    }

    /// 在任意异步字节流上运行 Worker 核心协议；供 stdio Adapter 与内存流测试共用。
    pub async fn run_stream<R, W>(self, mut reader: R, writer: W) -> Result<(), PluginError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        if self.plugin_id.is_empty() || self.tasks.is_empty() {
            return Err(PluginError::ExecutionFailed(
                "Worker plugin_id and at least one Task are required".to_owned(),
            ));
        }
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
        if initialize.runtime_config_version == 0 {
            return Err(PluginError::ExecutionFailed(
                "Agent must initialize Worker with non-zero runtime_config_version".to_owned(),
            ));
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
        let mut initialized_kinds = Vec::with_capacity(task_kinds.len());
        for kind in &task_kinds {
            // 记录当前 Task 后再初始化；即使初始化只完成了一半，shutdown 也能让它释放资源。
            initialized_kinds.push(kind.clone());
            if let Err(error) = tasks[kind]
                .initialize(
                    &agent_context,
                    &initialize.runtime_config,
                    initialize.runtime_config_version,
                )
                .await
            {
                for initialized_kind in initialized_kinds.iter().rev() {
                    if let Err(shutdown_error) = tasks[initialized_kind]
                        .shutdown("Worker initialization failed")
                        .await
                    {
                        eprintln!(
                            "plus task {initialized_kind} failed during initialization cleanup: {shutdown_error}"
                        );
                    }
                }
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
        loop {
            let frame = match read_frame(&mut reader).await {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    shutdown_executions(
                        &cancellations,
                        &mut executions,
                        &tasks,
                        &task_kinds,
                        "Worker input frame failed",
                    )
                    .await;
                    return Err(error);
                }
            };
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
                    if execute.request_id.is_empty() {
                        send_response(
                            &writer,
                            error_frame(
                                "",
                                "invalid_request",
                                "Execute request_id must not be empty",
                            ),
                        )
                        .await?;
                        continue;
                    }
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
                    let mut active_cancellations = cancellations.lock().await;
                    if active_cancellations.contains_key(&request_id) {
                        drop(active_cancellations);
                        send_response(
                            &writer,
                            error_frame(
                                &request_id,
                                "duplicate_request",
                                "request_id is already running",
                            ),
                        )
                        .await?;
                        continue;
                    }
                    active_cancellations.insert(request_id.clone(), cancellation.clone());
                    drop(active_cancellations);
                    let task_config = execute.config;
                    let agent_context = agent_context.clone();
                    let deadline = deadline_duration(execute.deadline_unix_millis);
                    let semaphore = semaphore.clone();
                    let cancellations = cancellations.clone();
                    let writer = writer.clone();
                    executions.spawn(async move {
                        // 插件属于独立 Worker，但单个 Task panic 不应让 IPC 读循环失去
                        // request_id 关联；把 panic 转换成普通失败结果并继续服务后续请求。
                        let result = AssertUnwindSafe(async {
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
                        })
                        .catch_unwind();
                        let result = match deadline {
                            Some(duration) => match tokio::time::timeout(duration, result).await {
                                Ok(result) => result,
                                Err(_) => Ok(Err(PlusTaskError::Timeout)),
                            },
                            None => result.await,
                        }
                        .unwrap_or_else(|_| {
                            Err(PlusTaskError::Failed("Plus Task panicked".to_owned()))
                        });
                        cancellations.lock().await.remove(&request_id);
                        let response = task_result(request_id.clone(), run_id, result);
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
                            // 结果可能因为超过单帧上限而未写出；尝试发送一个很小的
                            // ErrorResponse，让 Agent 立即结束 pending，而不是等执行超时。
                            let _ = send_response(
                                &writer,
                                error_frame(
                                    &request_id,
                                    "result_delivery_failed",
                                    "Worker could not send the task result",
                                ),
                            )
                            .await;
                        }
                    });
                }
                Some(worker_request::Body::Cancel(cancel)) => {
                    let token = cancellations.lock().await.get(&cancel.request_id).cloned();
                    if let Some(token) = token {
                        // 这里只发出协作取消信号，不立即回复；真正的确认由执行任务
                        // 结束后发送 TaskResult(Cancelled)，这样 Agent 不会误判任务仍在运行。
                        token.cancel();
                    } else {
                        send_response(
                            &writer,
                            error_frame(
                                &cancel.request_id,
                                "unknown_request",
                                "request_id is not running",
                            ),
                        )
                        .await?;
                    }
                }
                Some(worker_request::Body::Shutdown(shutdown)) => {
                    shutdown_executions(
                        &cancellations,
                        &mut executions,
                        &tasks,
                        &task_kinds,
                        &shutdown.reason,
                    )
                    .await;
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
                    shutdown_executions(
                        &cancellations,
                        &mut executions,
                        &tasks,
                        &task_kinds,
                        "Worker protocol error",
                    )
                    .await;
                    return Err(PluginError::ExecutionFailed(
                        "Worker received an unexpected protocol frame".to_owned(),
                    ));
                }
            }
        }
        // stdin EOF 表示 Agent 或父进程已经断开。仍需执行和显式 Shutdown 相同的
        // 取消、等待和 Task 收尾，避免插件在正常管道关闭时丢失清理逻辑。
        shutdown_executions(
            &cancellations,
            &mut executions,
            &tasks,
            &task_kinds,
            "Worker input closed",
        )
        .await;
        Ok(())
    }
}

/// 统一回收 Worker 中的运行任务和插件级资源。
///
/// Task 必须协作响应 `CancellationToken`；如果插件不响应，Agent 外层仍会在自己的
/// shutdown timeout 到期后终止 Worker 进程，避免这里无限期阻塞 Agent 退出。
async fn shutdown_executions(
    cancellations: &Mutex<HashMap<String, CancellationToken>>,
    executions: &mut JoinSet<()>,
    tasks: &HashMap<String, Arc<dyn PlusTask>>,
    task_kinds: &[String],
    reason: &str,
) {
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
    for kind in task_kinds {
        if let Err(error) = tasks[kind].shutdown(reason).await {
            eprintln!("plus task {kind} failed during Worker shutdown: {error}");
        }
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

async fn send_response<W>(writer: &Arc<Mutex<W>>, frame: WorkerFrame) -> Result<(), PluginError>
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
            status: match &error {
                PlusTaskError::Cancelled(_) => protocol::TaskStatus::Cancelled as i32,
                PlusTaskError::InvalidConfig(_) => protocol::TaskStatus::InvalidConfig as i32,
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::{PlusWorker, task_result};
    use crate::{
        PlusTask, PlusTaskContext, PlusTaskError, PlusTaskOutput,
        framing::{read_frame, write_frame},
        protocol::{
            self, Hello, InitializeWorker, TaskStatus, WorkerFrame, WorkerRequest, WorkerResponse,
            worker_frame, worker_request, worker_response,
        },
    };
    use async_trait::async_trait;
    use tokio::io::AsyncWriteExt;
    use tokio::time::{Duration, timeout};

    #[test]
    fn task_result_preserves_success_output() {
        let result = task_result(
            "request".to_owned(),
            vec![1, 2, 3],
            Ok(PlusTaskOutput {
                summary: "ok".to_owned(),
                metrics: vec![("latency_ms".to_owned(), 12.5)],
                payload: vec![9],
            }),
        );
        assert_eq!(result.status, TaskStatus::Succeeded as i32);
        assert_eq!(result.summary, "ok");
        assert_eq!(result.metrics.len(), 1);
        assert_eq!(result.payload, vec![9]);
    }

    #[test]
    fn task_result_marks_cancellation_only_after_task_returns() {
        let result = task_result(
            "request".to_owned(),
            Vec::new(),
            Err(PlusTaskError::Cancelled("shutdown".to_owned())),
        );
        assert_eq!(result.status, TaskStatus::Cancelled as i32);
        assert!(result.error.is_some());
    }

    #[test]
    fn task_result_marks_invalid_configuration_as_non_retryable_status() {
        let result = task_result(
            "request".to_owned(),
            Vec::new(),
            Err(PlusTaskError::InvalidConfig("bad field".to_owned())),
        );
        assert_eq!(result.status, TaskStatus::InvalidConfig as i32);
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("bad field"))
        );
    }

    struct ShutdownProbe(Arc<AtomicBool>);

    #[async_trait]
    impl PlusTask for ShutdownProbe {
        fn kind(&self) -> &'static str {
            "smalux.plus.test.a_shutdown.v1"
        }

        async fn shutdown(&self, _reason: &str) -> Result<(), PlusTaskError> {
            self.0.store(true, Ordering::Release);
            Ok(())
        }

        async fn execute(
            &self,
            _context: PlusTaskContext,
            _config: &[u8],
        ) -> Result<PlusTaskOutput, PlusTaskError> {
            Ok(PlusTaskOutput::default())
        }
    }

    fn request(body: worker_request::Body) -> WorkerFrame {
        WorkerFrame {
            body: Some(worker_frame::Body::Request(WorkerRequest {
                body: Some(body),
            })),
        }
    }

    #[tokio::test]
    async fn input_eof_runs_task_shutdown_before_worker_exit() {
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let worker = PlusWorker::builder("smalux.plus.test")
            .register(ShutdownProbe(Arc::clone(&shutdown_called)))
            .build();
        let (worker_stream, mut client_stream) = tokio::io::duplex(4096);
        let (worker_reader, worker_writer) = tokio::io::split(worker_stream);
        let worker_task = tokio::spawn(worker.run_stream(worker_reader, worker_writer));

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Hello(Hello {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                expected_plugin_id: "smalux.plus.test".to_owned(),
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Initialize(InitializeWorker {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                plugin_id: "smalux.plus.test".to_owned(),
                config_revision: 1,
                runtime_config_version: 1,
                runtime_config: Vec::new(),
                max_concurrency: 1,
                agent_context: None,
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();

        // 空 request_id 不能进入活动执行表，否则后续结果无法可靠关联到调用方。
        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Execute(
                crate::protocol::ExecuteTask {
                    request_id: String::new(),
                    ..Default::default()
                },
            )),
        )
        .await
        .unwrap();
        let invalid = read_frame(&mut client_stream).await.unwrap().unwrap();
        let Some(worker_frame::Body::Response(WorkerResponse {
            body: Some(worker_response::Body::Error(error)),
        })) = invalid.body
        else {
            panic!("expected invalid request response")
        };
        assert_eq!(error.code, "invalid_request");

        client_stream.shutdown().await.unwrap();
        timeout(Duration::from_secs(1), worker_task)
            .await
            .expect("Worker must exit after input EOF")
            .unwrap()
            .unwrap();
        assert!(shutdown_called.load(Ordering::Acquire));
    }

    struct InitializationFailure;

    #[async_trait]
    impl PlusTask for InitializationFailure {
        fn kind(&self) -> &'static str {
            "smalux.plus.test.z_failure.v1"
        }

        async fn initialize(
            &self,
            _agent: &crate::AgentContext,
            _runtime_config: &[u8],
            _runtime_config_version: u32,
        ) -> Result<(), PlusTaskError> {
            Err(PlusTaskError::InvalidConfig(
                "expected test failure".to_owned(),
            ))
        }

        async fn execute(
            &self,
            _context: PlusTaskContext,
            _config: &[u8],
        ) -> Result<PlusTaskOutput, PlusTaskError> {
            Ok(PlusTaskOutput::default())
        }
    }

    #[tokio::test]
    async fn failed_initialization_shuts_down_previously_initialized_tasks() {
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let worker = PlusWorker::builder("smalux.plus.test")
            // a_shutdown 在排序上先于 z_failure，确保它已经初始化后才触发失败。
            .register(ShutdownProbe(Arc::clone(&shutdown_called)))
            .register(InitializationFailure)
            .build();
        let (worker_stream, mut client_stream) = tokio::io::duplex(4096);
        let (worker_reader, worker_writer) = tokio::io::split(worker_stream);
        let worker_task = tokio::spawn(worker.run_stream(worker_reader, worker_writer));

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Hello(Hello {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                expected_plugin_id: "smalux.plus.test".to_owned(),
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Initialize(InitializeWorker {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                plugin_id: "smalux.plus.test".to_owned(),
                config_revision: 1,
                runtime_config_version: 1,
                runtime_config: Vec::new(),
                max_concurrency: 1,
                agent_context: None,
            })),
        )
        .await
        .unwrap();
        let response = read_frame(&mut client_stream).await.unwrap().unwrap();
        let Some(worker_frame::Body::Response(WorkerResponse {
            body: Some(worker_response::Body::Error(error)),
        })) = response.body
        else {
            panic!("expected initialization failure response")
        };
        assert_eq!(error.code, "initialization_failed");
        assert!(worker_task.await.unwrap().is_err());
        assert!(shutdown_called.load(Ordering::Acquire));
    }

    struct RuntimeVersionProbe(Arc<std::sync::atomic::AtomicU32>);

    #[async_trait]
    impl PlusTask for RuntimeVersionProbe {
        fn kind(&self) -> &'static str {
            "smalux.plus.test.runtime_version.v1"
        }

        async fn initialize(
            &self,
            _agent: &crate::AgentContext,
            _runtime_config: &[u8],
            runtime_config_version: u32,
        ) -> Result<(), PlusTaskError> {
            self.0.store(runtime_config_version, Ordering::Release);
            Ok(())
        }

        async fn execute(
            &self,
            _context: PlusTaskContext,
            _config: &[u8],
        ) -> Result<PlusTaskOutput, PlusTaskError> {
            Ok(PlusTaskOutput::default())
        }
    }

    #[tokio::test]
    async fn initialize_receives_runtime_configuration_version() {
        let received_version = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker = PlusWorker::builder("smalux.plus.test")
            .register(RuntimeVersionProbe(Arc::clone(&received_version)))
            .build();
        let (worker_stream, mut client_stream) = tokio::io::duplex(4096);
        let (worker_reader, worker_writer) = tokio::io::split(worker_stream);
        let worker_task = tokio::spawn(worker.run_stream(worker_reader, worker_writer));

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Hello(Hello {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                expected_plugin_id: "smalux.plus.test".to_owned(),
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Initialize(InitializeWorker {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                plugin_id: "smalux.plus.test".to_owned(),
                config_revision: 1,
                runtime_config_version: 7,
                runtime_config: vec![7],
                max_concurrency: 1,
                agent_context: None,
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        assert_eq!(received_version.load(Ordering::Acquire), 7);

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Shutdown(crate::protocol::Shutdown {
                reason: "test".to_owned(),
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        assert!(worker_task.await.unwrap().is_ok());
    }

    struct PanicTask;

    #[async_trait]
    impl PlusTask for PanicTask {
        fn kind(&self) -> &'static str {
            "smalux.plus.test.panic.v1"
        }

        async fn execute(
            &self,
            _context: PlusTaskContext,
            _config: &[u8],
        ) -> Result<PlusTaskOutput, PlusTaskError> {
            panic!("test plugin panic");
        }
    }

    #[tokio::test]
    async fn task_panic_returns_failure_and_worker_keeps_serving_requests() {
        let worker = PlusWorker::builder("smalux.plus.test")
            .register(PanicTask)
            .build();
        let (worker_stream, mut client_stream) = tokio::io::duplex(4096);
        let (worker_reader, worker_writer) = tokio::io::split(worker_stream);
        let worker_task = tokio::spawn(worker.run_stream(worker_reader, worker_writer));

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Hello(Hello {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                expected_plugin_id: "smalux.plus.test".to_owned(),
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Initialize(InitializeWorker {
                protocol_version: protocol::WORKER_PROTOCOL_VERSION,
                plugin_id: "smalux.plus.test".to_owned(),
                config_revision: 1,
                runtime_config_version: 1,
                runtime_config: Vec::new(),
                max_concurrency: 1,
                agent_context: None,
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Execute(
                crate::protocol::ExecuteTask {
                    request_id: "panic-request".to_owned(),
                    task_kind: "smalux.plus.test.panic.v1".to_owned(),
                    ..Default::default()
                },
            )),
        )
        .await
        .unwrap();
        let failed = read_frame(&mut client_stream).await.unwrap().unwrap();
        let Some(worker_frame::Body::Response(WorkerResponse {
            body: Some(worker_response::Body::Result(result)),
        })) = failed.body
        else {
            panic!("expected failed task result")
        };
        assert_eq!(result.status, TaskStatus::Failed as i32);
        assert_eq!(
            result.error.as_deref(),
            Some("task failed: Plus Task panicked")
        );

        write_frame(
            &mut client_stream,
            &request(worker_request::Body::Shutdown(crate::protocol::Shutdown {
                reason: "test".to_owned(),
            })),
        )
        .await
        .unwrap();
        read_frame(&mut client_stream).await.unwrap().unwrap();
        assert!(worker_task.await.unwrap().is_ok());
    }
}
