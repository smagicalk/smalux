//! 远程 shell 会话管理和 PTY 管道。
//!
//! PTY 读写是阻塞 API，因此本模块把 PTY reader/writer/child wait 隔离在
//! 专用线程中，再通过有界 channel 接回 Tokio WebSocket 任务。

use super::message::{
    RemoteShellOpenRequest, RemoteShellStreamCommand, RemoteShellStreamEvent, decode_input_bytes,
    encode_stream_event, output_event, parse_stream_command,
};
use super::options::RemoteShellOptions;
use crate::config::model::{ExportConfig, ExportFormat, RemoteShellConfig};
use crate::export::ws::{WebSocketClient, WebSocketConfig};
use crate::export::{
    EncodedExportMessage, ExportInboundMessage, ExportMessageListener, ExportTransport,
    inbound_message_into_string,
};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::future::Future;
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::thread::{self, JoinHandle as StdJoinHandle};
use std::time::Duration as StdDuration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{Duration, Instant, sleep};

/// shell stream 输入队列大小。
const SHELL_INPUT_CHANNEL_CAPACITY: usize = 128;
/// PTY 输出队列大小。
const SHELL_OUTPUT_CHANNEL_CAPACITY: usize = 128;
/// PTY 单次读取缓冲大小。
const PTY_OUTPUT_BUFFER_SIZE: usize = 8192;
/// child wait 线程轮询间隔。
const CHILD_WAIT_POLL_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// 远程 shell 会话管理器。
#[derive(Debug, Clone)]
pub(crate) struct RemoteShellManager {
    /// CLI-only 静态选项。
    options: RemoteShellOptions,
    /// 当前活跃会话数。
    active_sessions: Arc<AtomicUsize>,
}

impl RemoteShellManager {
    /// 使用指定静态选项创建 manager。
    pub(crate) fn new(options: RemoteShellOptions) -> Self {
        Self {
            options,
            active_sessions: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 处理主控制通道下发的 shell 打开请求。
    pub(crate) fn open(
        &self,
        request: RemoteShellOpenRequest,
        export_config: &ExportConfig,
        shell_config: &RemoteShellConfig,
    ) -> anyhow::Result<()> {
        if !self.options.enabled {
            anyhow::bail!("remote shell is disabled");
        }

        request.validate()?;
        let permit = self.acquire_session_permit(shell_config.max_sessions)?;
        let session_id = request.session_id.clone();
        let stream_config = shell_stream_config(export_config, &request.stream_url)?;
        let stream_encoding = RemoteShellStreamEncoding::from_export_format(export_config.format);
        let session = RemoteShellSession {
            request,
            shell_config: shell_config.clone(),
            stream_config,
            stream_encoding,
            _permit: permit,
        };

        tokio::spawn(async move {
            tracing::info!(session_id = %session_id, "remote shell session starting");
            if let Err(err) = session.run().await {
                tracing::warn!(
                    session_id = %session_id,
                    error = ?err,
                    "remote shell session stopped with error"
                );
            } else {
                tracing::info!(session_id = %session_id, "remote shell session stopped");
            }
        });

        Ok(())
    }

    /// 尝试获取一个会话名额。
    fn acquire_session_permit(
        &self,
        max_sessions: usize,
    ) -> anyhow::Result<RemoteShellSessionPermit> {
        loop {
            let current = self.active_sessions.load(Ordering::SeqCst);
            if current >= max_sessions {
                anyhow::bail!("remote shell session limit reached");
            }

            if self
                .active_sessions
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(RemoteShellSessionPermit {
                    active_sessions: self.active_sessions.clone(),
                });
            }
        }
    }
}

/// 会话名额 guard，确保异常退出时也能释放计数。
struct RemoteShellSessionPermit {
    /// 活跃会话计数。
    active_sessions: Arc<AtomicUsize>,
}

impl Drop for RemoteShellSessionPermit {
    /// 会话结束时释放名额。
    fn drop(&mut self) {
        self.active_sessions.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 单个远程 shell 会话。
struct RemoteShellSession {
    /// 打开请求。
    request: RemoteShellOpenRequest,
    /// 会话启动时捕获的动态运行限制。
    shell_config: RemoteShellConfig,
    /// 临时 stream WebSocket 配置。
    stream_config: WebSocketConfig,
    /// stream 消息编码方式。
    stream_encoding: RemoteShellStreamEncoding,
    /// 会话名额 guard。
    _permit: RemoteShellSessionPermit,
}

/// 远程 shell stream 消息编码。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RemoteShellStreamEncoding {
    /// 兼容第三方服务的 WebSocket text。
    Text,
    /// Smalux 自有 binary wire。
    SmaluxWire,
}

impl RemoteShellStreamEncoding {
    /// 根据导出格式选择 stream 编码。
    fn from_export_format(format: ExportFormat) -> Self {
        match format {
            ExportFormat::SmaluxJson => Self::SmaluxWire,
            ExportFormat::Komari => Self::Text,
        }
    }
}

/// shell stream 发送端，负责给 binary wire 分配本 stream 内序号。
#[derive(Clone)]
struct ShellStreamSender {
    /// 共享 WebSocket stream。
    stream: SharedShellStream,
    /// 编码方式。
    encoding: RemoteShellStreamEncoding,
    /// 下一条 stream 事件序号。
    next_sequence: Arc<AtomicU64>,
}

impl ShellStreamSender {
    /// 创建发送端。
    fn new(stream: SharedShellStream, encoding: RemoteShellStreamEncoding) -> Self {
        Self {
            stream,
            encoding,
            next_sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 发送 shell stream event。
    async fn send(&self, event: RemoteShellStreamEvent) -> anyhow::Result<()> {
        let text = encode_stream_event(&event)?;
        let mut stream = self.stream.lock().await;
        match self.encoding {
            RemoteShellStreamEncoding::Text => stream.send_text_message(&text).await,
            RemoteShellStreamEncoding::SmaluxWire => {
                let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
                stream
                    .send_encoded_export_message(EncodedExportMessage::Binary {
                        sequence,
                        body: text.into_bytes(),
                    })
                    .await
            }
        }
    }
}

impl RemoteShellSession {
    /// 运行 shell 会话。
    async fn run(self) -> anyhow::Result<()> {
        let session_id = self.request.session_id.clone();
        let (input_tx, input_rx) = mpsc::channel(SHELL_INPUT_CHANNEL_CAPACITY);
        let mut client = WebSocketClient::new_with_config(self.stream_config.clone());
        client
            .set_listener(Box::new(RemoteShellStreamListener { input_tx }))
            .await?;
        client.connect().await?;
        let stream = Arc::new(Mutex::new(client));
        let sender = ShellStreamSender::new(stream.clone(), self.stream_encoding);

        let result = self.run_connected(sender.clone(), input_rx).await;
        if let Err(err) = &result {
            let _ = sender
                .send(RemoteShellStreamEvent::Error {
                    session_id: session_id.clone(),
                    message: err.to_string(),
                })
                .await;
        }

        stream.lock().await.close().await?;
        result
    }

    /// 在 stream 已连接后启动本地 PTY shell 并桥接 IO。
    async fn run_connected(
        self,
        sender: ShellStreamSender,
        input_rx: mpsc::Receiver<RemoteShellInput>,
    ) -> anyhow::Result<()> {
        let session_id = self.request.session_id.clone();
        let (cols, rows) = self.request.initial_size();
        let mut shell = spawn_pty_shell(&self.shell_config, cols, rows)?;

        sender
            .send(RemoteShellStreamEvent::Opened {
                session_id: session_id.clone(),
            })
            .await?;

        let result = run_shell_loop(
            &session_id,
            &self.shell_config,
            &sender,
            &mut shell,
            input_rx,
        )
        .await;
        shell.shutdown().await;
        let exit_code = result?;

        sender
            .send(RemoteShellStreamEvent::Exit {
                session_id,
                code: exit_code,
            })
            .await
    }
}

/// 临时 shell stream 连接。
type SharedShellStream = Arc<Mutex<WebSocketClient>>;

/// stream 中收到的输入事件。
enum RemoteShellInput {
    /// 写入 PTY 输入。
    Input(Vec<u8>),
    /// 调整终端尺寸。
    Resize { cols: u16, rows: u16 },
    /// 关闭会话。
    Close,
}

/// shell stream listener，把 server 文本消息转换为输入事件。
struct RemoteShellStreamListener {
    /// 输入事件发送端。
    input_tx: mpsc::Sender<RemoteShellInput>,
}

impl ExportMessageListener for RemoteShellStreamListener {
    /// 收到 stream 消息后写入 shell 输入队列。
    fn on_message(
        &self,
        msg: ExportInboundMessage,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let input_tx = self.input_tx.clone();
        Box::pin(async move {
            let msg = inbound_message_into_string(msg)?;
            let command = parse_stream_command(&msg)?;
            let input = match command {
                RemoteShellStreamCommand::Resize { cols, rows } => {
                    RemoteShellInput::Resize { cols, rows }
                }
                RemoteShellStreamCommand::Close => RemoteShellInput::Close,
                command => RemoteShellInput::Input(decode_input_bytes(command)?),
            };
            input_tx.send(input).await?;
            Ok(())
        })
    }
}

/// 已启动的 PTY shell。
struct PtyShell {
    /// PTY master，用于 resize。
    master: Box<dyn MasterPty + Send>,
    /// 写入 PTY 的阻塞线程入口。
    input_tx: std_mpsc::Sender<Vec<u8>>,
    /// PTY 输出异步接收端。
    output_rx: mpsc::Receiver<Vec<u8>>,
    /// child wait 结果。
    exit_rx: oneshot::Receiver<anyhow::Result<Option<i32>>>,
    /// child kill 请求。
    kill_tx: std_mpsc::Sender<()>,
    /// PTY reader 线程。
    reader_thread: StdJoinHandle<()>,
    /// PTY writer 线程。
    writer_thread: StdJoinHandle<()>,
    /// child wait 线程。
    child_thread: StdJoinHandle<()>,
}

impl PtyShell {
    /// 调整 PTY 尺寸。
    fn resize(&mut self, cols: u16, rows: u16) -> anyhow::Result<()> {
        self.master.resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        Ok(())
    }

    /// 写入 PTY 输入。
    fn send_input(&self, input: Vec<u8>) -> anyhow::Result<()> {
        self.input_tx.send(input)?;
        Ok(())
    }

    /// 请求停止 child。
    fn request_stop(&self) {
        let _ = self.kill_tx.send(());
    }

    /// 关闭 PTY shell 并等待阻塞线程退出。
    async fn shutdown(self) {
        self.request_stop();
        let PtyShell {
            master,
            input_tx,
            output_rx,
            exit_rx,
            kill_tx,
            reader_thread,
            writer_thread,
            child_thread,
        } = self;
        drop(master);
        drop(input_tx);
        drop(output_rx);
        drop(exit_rx);
        drop(kill_tx);
        join_shell_thread(reader_thread, "remote shell pty reader").await;
        join_shell_thread(writer_thread, "remote shell pty writer").await;
        join_shell_thread(child_thread, "remote shell child waiter").await;
    }
}

/// 根据主导出配置构造 shell stream WebSocket 配置。
fn shell_stream_config(
    export_config: &ExportConfig,
    stream_url: &str,
) -> anyhow::Result<WebSocketConfig> {
    let mut stream_export = export_config.clone();
    stream_export.server_url = stream_url.to_string();
    WebSocketConfig::try_from(&stream_export)
}

/// 启动本地 PTY shell。
fn spawn_pty_shell(
    shell_config: &RemoteShellConfig,
    cols: u16,
    rows: u16,
) -> anyhow::Result<PtyShell> {
    let program = shell_config.program_or_default();
    tracing::info!(
        program = %program,
        cols,
        rows,
        "remote shell pty spawning"
    );

    let pty_system = native_pty_system();
    let pair = pty_system.openpty(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let command = CommandBuilder::new(program);
    let child = pair.slave.spawn_command(command)?;
    drop(pair.slave);
    let reader = pair.master.try_clone_reader()?;
    let writer = pair.master.take_writer()?;
    let (input_tx, input_rx) = std_mpsc::channel::<Vec<u8>>();
    let (output_tx, output_rx) = mpsc::channel::<Vec<u8>>(SHELL_OUTPUT_CHANNEL_CAPACITY);
    let (kill_tx, kill_rx) = std_mpsc::channel::<()>();
    let (exit_tx, exit_rx) = oneshot::channel::<anyhow::Result<Option<i32>>>();

    let reader_thread = spawn_pty_reader(reader, output_tx);
    let writer_thread = spawn_pty_writer(writer, input_rx);
    let child_thread = spawn_child_waiter(child, kill_rx, exit_tx);

    Ok(PtyShell {
        master: pair.master,
        input_tx,
        output_rx,
        exit_rx,
        kill_tx,
        reader_thread,
        writer_thread,
        child_thread,
    })
}

/// shell 主循环，负责输入、输出、关闭、超时和进程退出。
async fn run_shell_loop(
    session_id: &str,
    shell_config: &RemoteShellConfig,
    sender: &ShellStreamSender,
    shell: &mut PtyShell,
    mut input_rx: mpsc::Receiver<RemoteShellInput>,
) -> anyhow::Result<Option<i32>> {
    let session_timeout = sleep(shell_config.session_timeout);
    let idle_timeout = sleep(shell_config.idle_timeout);
    tokio::pin!(session_timeout);
    tokio::pin!(idle_timeout);

    loop {
        tokio::select! {
            exit = &mut shell.exit_rx => {
                return exit.unwrap_or_else(|_| Err(anyhow::anyhow!("remote shell child waiter stopped")) );
            }
            input = input_rx.recv() => {
                match input {
                    Some(RemoteShellInput::Input(data)) => {
                        shell.send_input(data)?;
                        reset_idle_timeout(idle_timeout.as_mut(), shell_config.idle_timeout);
                    }
                    Some(RemoteShellInput::Resize { cols, rows }) => {
                        shell.resize(cols, rows)?;
                        tracing::debug!(
                            session_id = %session_id,
                            cols,
                            rows,
                            "remote shell pty resized"
                        );
                        reset_idle_timeout(idle_timeout.as_mut(), shell_config.idle_timeout);
                    }
                    Some(RemoteShellInput::Close) => {
                        tracing::info!(session_id = %session_id, "remote shell close requested");
                        shell.request_stop();
                        return Ok(None);
                    }
                    None => {
                        tracing::info!(session_id = %session_id, "remote shell stream input closed");
                        shell.request_stop();
                        return Ok(None);
                    }
                }
            }
            output = shell.output_rx.recv() => {
                let Some(output) = output else {
                    continue;
                };
                sender.send(output_event(session_id.to_string(), &output)).await?;
                reset_idle_timeout(idle_timeout.as_mut(), shell_config.idle_timeout);
            }
            _ = &mut idle_timeout => {
                tracing::warn!(
                    session_id = %session_id,
                    timeout_ms = shell_config.idle_timeout.as_millis(),
                    "remote shell idle timeout reached"
                );
                shell.request_stop();
                return Ok(None);
            }
            _ = &mut session_timeout => {
                tracing::warn!(
                    session_id = %session_id,
                    timeout_ms = shell_config.session_timeout.as_millis(),
                    "remote shell session timeout reached"
                );
                shell.request_stop();
                return Ok(None);
            }
        }
    }
}

/// 重置空闲超时。
fn reset_idle_timeout(mut idle_timeout: Pin<&mut tokio::time::Sleep>, duration: Duration) {
    idle_timeout.as_mut().reset(
        Instant::now()
            .checked_add(duration)
            .unwrap_or_else(Instant::now),
    );
}

/// 启动 PTY reader 阻塞线程。
fn spawn_pty_reader(
    mut reader: Box<dyn Read + Send>,
    output_tx: mpsc::Sender<Vec<u8>>,
) -> StdJoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = [0_u8; PTY_OUTPUT_BUFFER_SIZE];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    if output_tx.blocking_send(buffer[..read].to_vec()).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "remote shell pty reader stopped");
                    break;
                }
            }
        }
    })
}

/// 启动 PTY writer 阻塞线程。
fn spawn_pty_writer(
    mut writer: Box<dyn Write + Send>,
    input_rx: std_mpsc::Receiver<Vec<u8>>,
) -> StdJoinHandle<()> {
    thread::spawn(move || {
        while let Ok(input) = input_rx.recv() {
            if let Err(err) = writer.write_all(&input).and_then(|_| writer.flush()) {
                tracing::debug!(error = %err, "remote shell pty writer stopped");
                break;
            }
        }
    })
}

/// 启动 child wait 阻塞线程。
fn spawn_child_waiter(
    mut child: Box<dyn Child + Send + Sync>,
    kill_rx: std_mpsc::Receiver<()>,
    exit_tx: oneshot::Sender<anyhow::Result<Option<i32>>>,
) -> StdJoinHandle<()> {
    thread::spawn(move || {
        let result = loop {
            if kill_rx.try_recv().is_ok()
                && let Err(err) = child.kill()
            {
                break Err(anyhow::anyhow!(err));
            }

            match child.try_wait() {
                Ok(Some(status)) => break Ok(Some(status.exit_code() as i32)),
                Ok(None) => thread::sleep(CHILD_WAIT_POLL_INTERVAL),
                Err(err) => break Err(anyhow::anyhow!(err)),
            }
        };

        let _ = exit_tx.send(result);
    })
}

/// 等待阻塞线程退出。
async fn join_shell_thread(handle: StdJoinHandle<()>, name: &'static str) {
    match tokio::task::spawn_blocking(move || handle.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(_panic)) => {
            tracing::warn!(thread = name, "remote shell thread panicked");
        }
        Err(err) => {
            tracing::warn!(thread = name, error = %err, "remote shell thread join failed");
        }
    }
}

#[cfg(test)]
mod tests {
    //! 远程 shell manager 测试。

    use super::*;
    use crate::config::model::{ExportConfig, ExportFormat, RemoteShellConfig};
    use base64::Engine;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::protocol::Message;

    /// 构造测试打开请求。
    fn open_request() -> RemoteShellOpenRequest {
        RemoteShellOpenRequest {
            session_id: "shell-1".to_string(),
            stream_url: "ws://127.0.0.1:1/shell".to_string(),
            cols: None,
            rows: None,
        }
    }

    /// 验证默认关闭时拒绝打开 shell。
    #[test]
    fn manager_rejects_open_when_disabled() {
        let manager = RemoteShellManager::new(RemoteShellOptions::default());
        let error = manager
            .open(
                open_request(),
                &ExportConfig::default(),
                &RemoteShellConfig::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("disabled"));
    }

    /// 验证会话上限会拒绝新的打开请求。
    #[test]
    fn manager_rejects_open_when_session_limit_is_reached() {
        let manager = RemoteShellManager::new(RemoteShellOptions { enabled: true });
        let _permit = manager.acquire_session_permit(1).unwrap();

        let error = manager
            .open(
                open_request(),
                &ExportConfig::default(),
                &RemoteShellConfig::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("limit"));
    }

    /// 验证 stream 配置会复用主导出认证和 TLS 选项。
    #[test]
    fn shell_stream_config_reuses_export_options() {
        let mut export = ExportConfig::default();
        export.server_url = "ws://127.0.0.1/main".to_string();
        export.unsafe_cert = true;
        export.heartbeat = Duration::from_secs(9);

        let config = shell_stream_config(&export, "ws://127.0.0.1/shell").unwrap();

        assert_eq!(config.url, "ws://127.0.0.1/shell");
        assert!(config.unsafe_cert);
        assert_eq!(config.heartbeat, 9);
    }

    /// 验证 Smalux 自有格式的 shell stream 会走 binary wire，Komari 保持 text。
    #[test]
    fn shell_stream_encoding_follows_export_format() {
        assert_eq!(
            RemoteShellStreamEncoding::from_export_format(ExportFormat::SmaluxJson),
            RemoteShellStreamEncoding::SmaluxWire
        );
        assert_eq!(
            RemoteShellStreamEncoding::from_export_format(ExportFormat::Komari),
            RemoteShellStreamEncoding::Text
        );
    }

    /// 验证 manager 能打开临时 stream，并转发真实 PTY 输出。
    #[tokio::test]
    async fn manager_opens_shell_stream_and_forwards_output() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream_url = format!("ws://{}", listener.local_addr().unwrap());
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut opened = false;
            let mut output_text = String::new();
            let mut input_sent = false;
            let mut output_seen = false;
            let mut exited = false;

            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        match value["type"].as_str().unwrap() {
                            "opened" => {
                                opened = true;
                                if !cfg!(windows) {
                                    websocket
                                        .send(Message::Text(
                                            r#"{ "type": "input", "data": "echo smalux-shell-test\r\nexit\r\n" }"#
                                                .to_string()
                                                .into(),
                                        ))
                                        .await
                                        .unwrap();
                                    input_sent = true;
                                }
                            }
                            "output" => {
                                let data = value["data"].as_str().unwrap();
                                let decoded = base64::engine::general_purpose::STANDARD
                                    .decode(data)
                                    .unwrap();
                                output_text.push_str(&String::from_utf8_lossy(&decoded));
                                if cfg!(windows) && !input_sent && output_text.contains("\u{1b}[6n")
                                {
                                    // Windows PowerShell 可能先请求终端光标位置；测试 server 模拟一个最小终端响应。
                                    websocket
                                        .send(Message::Text(
                                            r#"{ "type": "input", "data": "\u001b[24;1Recho smalux-shell-test\r\nexit\r\n" }"#
                                                .to_string()
                                                .into(),
                                        ))
                                        .await
                                        .unwrap();
                                    input_sent = true;
                                }
                                output_seen |= output_text.contains("smalux-shell-test");
                            }
                            "exit" => {
                                exited = true;
                                let _ = websocket.close(None).await;
                                break;
                            }
                            other => panic!("unexpected shell event: {other}"),
                        }
                    }
                    Message::Ping(payload) => {
                        websocket.send(Message::Pong(payload)).await.unwrap();
                    }
                    Message::Close(_frame) => break,
                    _ => {}
                }
            }

            (opened, output_seen, exited, output_text)
        });

        let manager = RemoteShellManager::new(RemoteShellOptions { enabled: true });

        let mut export = ExportConfig::default();
        export.format = ExportFormat::Komari;

        manager
            .open(
                RemoteShellOpenRequest {
                    session_id: "shell-test".to_string(),
                    stream_url,
                    cols: Some(100),
                    rows: Some(30),
                },
                &export,
                &RemoteShellConfig {
                    idle_timeout: Duration::from_secs(5),
                    session_timeout: Duration::from_secs(10),
                    ..RemoteShellConfig::default()
                },
            )
            .unwrap();

        let (opened, output_seen, exited, output_text) =
            timeout(Duration::from_secs(12), server_task)
                .await
                .expect("timed out waiting for shell stream")
                .unwrap();

        assert!(opened);
        assert!(output_seen, "shell output was: {output_text:?}");
        assert!(exited);
    }
}
