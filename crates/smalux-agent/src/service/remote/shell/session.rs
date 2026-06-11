//! 单个远程 shell 会话和 stream 桥接。
//!
//! 本模块负责把独立 WebSocket stream、统一 shell codec 和本地 PTY shell 连接起来。
//! manager 只启动会话；PTY 阻塞细节继续下沉到 `pty` 模块。

use super::message::{
    RemoteShellOpenRequest, RemoteShellStreamEvent, open_request_initial_size, output_event,
};
use super::pty::{PtyShell, spawn_pty_shell};
use super::stream::{RemoteShellFrame, RemoteShellInput, RemoteShellStreamCodecRef};
use crate::config::model::{ExportConfig, RemoteShellConfig};
use crate::export::ws::{WebSocketClient, WebSocketConfig};
use crate::export::{
    EncodedTransportMessage, ExportTransport, InboundProtocolHandler, TransportInboundMessage,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, Instant, sleep};

/// shell stream 输入队列大小。
const SHELL_INPUT_CHANNEL_CAPACITY: usize = 128;

/// 会话名额 guard，确保异常退出时也能释放计数。
pub(super) struct RemoteShellSessionPermit {
    /// 活跃会话计数。
    pub(super) active_sessions: Arc<AtomicUsize>,
}

impl Drop for RemoteShellSessionPermit {
    /// 会话结束时释放名额。
    fn drop(&mut self) {
        self.active_sessions.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 单个远程 shell 会话。
pub(super) struct RemoteShellSession {
    /// 打开请求。
    request: RemoteShellOpenRequest,
    /// 会话启动时捕获的动态运行限制。
    shell_config: RemoteShellConfig,
    /// 临时 stream WebSocket 配置。
    stream_config: WebSocketConfig,
    /// stream codec。
    stream_codec: RemoteShellStreamCodecRef,
    /// 会话名额 guard。
    _permit: RemoteShellSessionPermit,
}

impl RemoteShellSession {
    /// 根据打开请求和当前配置创建 shell 会话。
    pub(super) fn new(
        request: RemoteShellOpenRequest,
        export_config: &ExportConfig,
        shell_config: &RemoteShellConfig,
        stream_codec: RemoteShellStreamCodecRef,
        permit: RemoteShellSessionPermit,
    ) -> anyhow::Result<Self> {
        let stream_config =
            shell_stream_config(export_config, &request.stream_url, stream_codec.as_ref())?;
        Ok(Self {
            request,
            shell_config: shell_config.clone(),
            stream_config,
            stream_codec,
            _permit: permit,
        })
    }

    /// 执行最小就绪检查：stream 建连、PTY 启动并发送 opened 事件。
    pub(super) async fn establish_ready(self) -> anyhow::Result<RemoteShellSessionReady> {
        let session_id = self.request.session_id.clone();
        let (input_tx, input_rx) = mpsc::channel(SHELL_INPUT_CHANNEL_CAPACITY);
        let mut client = WebSocketClient::new_with_config(self.stream_config.clone());
        client
            .set_inbound_handler(Box::new(RemoteShellStreamHandler {
                input_tx,
                codec: self.stream_codec.clone(),
            }))
            .await?;
        client.connect().await?;
        let stream = Arc::new(Mutex::new(client));
        let sender = ShellStreamSender::new(stream.clone(), self.stream_codec.clone());
        let (cols, rows) = open_request_initial_size(&self.request);
        let shell = spawn_pty_shell(&self.shell_config, cols, rows)?;

        if let Err(error) = sender
            .send_event(RemoteShellStreamEvent::Opened {
                session_id: session_id.clone(),
            })
            .await
        {
            let _ = stream.lock().await.close().await;
            shell.shutdown().await;
            return Err(error);
        }

        Ok(RemoteShellSessionReady {
            session_id,
            shell_config: self.shell_config,
            sender,
            input_rx,
            shell,
            stream,
            _permit: self._permit,
        })
    }
}

/// 已完成 stream/PTY ready 阶段的 shell 会话。
pub(super) struct RemoteShellSessionReady {
    /// 会话 ID。
    session_id: String,
    /// 会话启动时捕获的动态运行限制。
    shell_config: RemoteShellConfig,
    /// shell stream 发送端。
    sender: ShellStreamSender,
    /// shell stream 输入接收端。
    input_rx: mpsc::Receiver<RemoteShellInput>,
    /// 已启动的 PTY shell。
    shell: PtyShell,
    /// 已连接的 stream。
    stream: SharedShellStream,
    /// 会话名额 guard。
    _permit: RemoteShellSessionPermit,
}

impl RemoteShellSessionReady {
    /// 在 ready 之后运行 shell 主循环。
    pub(super) async fn run(mut self) -> anyhow::Result<()> {
        let result = run_shell_loop(
            &self.session_id,
            &self.shell_config,
            &self.sender,
            &mut self.shell,
            self.input_rx,
        )
        .await;
        if let Err(err) = &result {
            let _ = self
                .sender
                .send_event(RemoteShellStreamEvent::Error {
                    session_id: self.session_id.clone(),
                    message: err.to_string(),
                })
                .await;
        }

        self.shell.shutdown().await;
        let exit_code = result?;
        self.sender
            .send_event(RemoteShellStreamEvent::Exit {
                session_id: self.session_id,
                code: exit_code,
            })
            .await?;
        self.stream.lock().await.close().await
    }
}

/// 临时 shell stream 连接。
type SharedShellStream = Arc<Mutex<WebSocketClient>>;

/// shell stream 发送端，负责调用 codec 并给 Smalux wire 分配 stream 内序号。
#[derive(Clone)]
struct ShellStreamSender {
    /// 共享 WebSocket stream。
    stream: SharedShellStream,
    /// stream codec。
    codec: RemoteShellStreamCodecRef,
    /// 下一条 stream 事件序号。
    next_sequence: Arc<AtomicU64>,
}

impl ShellStreamSender {
    /// 创建发送端。
    fn new(stream: SharedShellStream, codec: RemoteShellStreamCodecRef) -> Self {
        Self {
            stream,
            codec,
            next_sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 发送 shell stream 控制事件。
    async fn send_event(&self, event: RemoteShellStreamEvent) -> anyhow::Result<()> {
        let Some(frame) = self.codec.encode_event(event)? else {
            return Ok(());
        };
        self.send_frame(frame).await
    }

    /// 发送 codec 已编码的 frame。
    async fn send_frame(&self, frame: RemoteShellFrame) -> anyhow::Result<()> {
        match frame {
            RemoteShellFrame::Text(text) => {
                let mut stream = self.stream.lock().await;
                stream.send_text_message(&text).await
            }
            RemoteShellFrame::RawBinary(bytes) => {
                let mut stream = self.stream.lock().await;
                stream.send_raw_binary_message(bytes).await
            }
            RemoteShellFrame::SmaluxWire(body) => {
                let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
                let mut stream = self.stream.lock().await;
                stream
                    .send_encoded_transport_message(EncodedTransportMessage::Binary {
                        sequence,
                        body,
                    })
                    .await
            }
        }
    }

    /// 发送 PTY 输出。
    async fn send_output(&self, session_id: &str, output: &[u8]) -> anyhow::Result<()> {
        self.send_event(output_event(session_id.to_string(), output))
            .await
    }
}

/// shell stream 入站处理器，把 server 消息转换为输入事件。
struct RemoteShellStreamHandler {
    /// 输入事件发送端。
    input_tx: mpsc::Sender<RemoteShellInput>,
    /// stream codec。
    codec: RemoteShellStreamCodecRef,
}

impl InboundProtocolHandler for RemoteShellStreamHandler {
    /// 收到 stream 消息后写入 shell 输入队列。
    fn on_message(
        &self,
        msg: TransportInboundMessage,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let input_tx = self.input_tx.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let Some(input) = codec.decode_inbound(msg)? else {
                return Ok(());
            };
            input_tx.send(input).await?;
            Ok(())
        })
    }
}

/// 根据主导出配置构造 shell stream WebSocket 配置。
pub(super) fn shell_stream_config(
    export_config: &ExportConfig,
    stream_url: &str,
    codec: &dyn super::stream::RemoteShellStreamCodec,
) -> anyhow::Result<WebSocketConfig> {
    let config = WebSocketConfig::from_export_endpoint(export_config, stream_url.to_string())?;
    Ok(codec.configure_websocket(config))
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
                sender.send_output(session_id, &output).await?;
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
