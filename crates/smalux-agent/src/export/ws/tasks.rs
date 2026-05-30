//! WebSocket 后台任务编排。

mod handler;

use crate::config::model::ExportWireMode;
use crate::export::{ExportInboundMessage, ExportMessageListener};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Interval, timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};

/// 监听器消息队列的缓冲大小。
pub(crate) const LISTENER_QUEUE_CAPACITY: usize = 128;
/// 主动关闭后等待对端 close frame 的最长时间。
pub(super) const CLOSE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// 后台任务退出的等待时间。
pub(crate) const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(6);
/// 心跳 ping 负载。
const PING_PAYLOAD: &str = "ping";

/// 写任务接收的内部控制消息。
pub(crate) enum WebSocketCommand {
    /// 发送一条文本消息。
    SendText(String),
    /// 发送一条二进制消息。
    SendBinary {
        /// 业务序号。
        sequence: u64,
        /// 业务 payload。
        bytes: Vec<u8>,
    },
    /// 主动关闭连接。
    Close,
}

/// WebSocket binary wire 状态。
pub(crate) enum WebSocketWireState {
    /// 明文 binary wire。
    BinaryPlain {
        /// 当前连接 session id。
        session_id: [u8; 16],
    },
    /// Noise PSK 安全 wire。
    SecurePsk {
        /// 当前连接 session id。
        session_id: [u8; 16],
        /// Noise transport 状态。
        transport: snow::TransportState,
    },
}

impl WebSocketWireState {
    /// 返回 wire 模式。
    pub(super) fn mode(&self) -> ExportWireMode {
        match self {
            Self::BinaryPlain { .. } => ExportWireMode::BinaryPlain,
            Self::SecurePsk { .. } => ExportWireMode::SecurePsk,
        }
    }
}

/// 发送到监听器 worker 的服务端消息。
pub(super) struct InboundMessageEvent {
    /// 入站消息内容。
    pub(super) payload: ExportInboundMessage,
    /// 处理该消息时使用的 listener 快照。
    pub(super) listener: Arc<dyn ExportMessageListener>,
}

/// WebSocket 写半边类型别名，避免处理函数签名过长。
pub(super) type WebSocketWriter = futures_util::stream::SplitSink<
    WebSocketStream<MaybeTlsStream<TcpStream>>,
    tungstenite::protocol::Message,
>;

/// 启动 WebSocket 后台任务。
pub(crate) fn spawn_websocket_tasks(
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    commands: mpsc::Receiver<WebSocketCommand>,
    heartbeat: u64,
    wire_state: WebSocketWireState,
    listener_lock: Arc<RwLock<Option<Arc<dyn ExportMessageListener>>>>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let (listener_tx, listener_rx) = mpsc::channel::<InboundMessageEvent>(LISTENER_QUEUE_CAPACITY);
    let listener_task = tokio::spawn(listener_worker(listener_rx));
    let websocket_task = tokio::spawn(websocket_loop(
        websocket,
        commands,
        heartbeat,
        wire_state,
        listener_lock,
        listener_tx,
    ));

    (websocket_task, listener_task)
}

/// 串行执行 listener 回调，避免业务处理阻塞 WebSocket 读循环。
async fn listener_worker(mut listener_rx: mpsc::Receiver<InboundMessageEvent>) {
    tracing::debug!(
        queue_capacity = LISTENER_QUEUE_CAPACITY,
        "listener worker started"
    );

    while let Some(event) = listener_rx.recv().await {
        if let Err(e) = event.listener.on_message(event.payload).await {
            tracing::error!(error = %e, "listener worker failed to process message");
        }
    }

    tracing::debug!("listener worker stopped");
}

/// WebSocket 读写循环。
async fn websocket_loop(
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    mut commands: mpsc::Receiver<WebSocketCommand>,
    heartbeat: u64,
    mut wire_state: WebSocketWireState,
    listener_lock: Arc<RwLock<Option<Arc<dyn ExportMessageListener>>>>,
    listener_tx: mpsc::Sender<InboundMessageEvent>,
) {
    let (mut write, mut read) = websocket.split();
    let mut ping_interval = heartbeat_interval(heartbeat);
    let close_timeout = tokio::time::sleep(CLOSE_HANDSHAKE_TIMEOUT);
    tokio::pin!(close_timeout);
    let mut close_requested = false;

    tracing::debug!(
        heartbeat_secs = heartbeat,
        wire_mode = wire_state.mode().as_str(),
        "websocket background task started"
    );

    loop {
        tokio::select! {
            _ = next_ping(&mut ping_interval), if !close_requested && ping_interval.is_some() => {
                if let Err(e) = write
                    .send(tungstenite::protocol::Message::Ping(bytes::Bytes::from(PING_PAYLOAD)))
                    .await
                {
                    tracing::error!(error = %e, "websocket ping failed; stopping background task");
                    break;
                }
                tracing::trace!("websocket ping sent");
            }
            msg = read.next() => {
                if !handler::handle_incoming_message(
                    msg,
                    &mut write,
                    &mut wire_state,
                    &listener_lock,
                    &listener_tx,
                    &mut close_requested,
                    close_timeout.as_mut(),
                )
                .await
                {
                    break;
                }
            }
            command = commands.recv(), if !close_requested => {
                if !handler::handle_command(
                    command,
                    &mut write,
                    &mut wire_state,
                    &mut close_requested,
                    close_timeout.as_mut(),
                )
                .await
                {
                    break;
                }
            }
            _ = &mut close_timeout, if close_requested => {
                tracing::warn!(
                    timeout_ms = CLOSE_HANDSHAKE_TIMEOUT.as_millis(),
                    "websocket close acknowledgement timed out; stopping background task"
                );
                break;
            }
        }
    }

    tracing::debug!("websocket background task stopped");
}

/// 根据心跳配置创建定时器；0 表示禁用心跳。
fn heartbeat_interval(heartbeat_secs: u64) -> Option<Interval> {
    (heartbeat_secs > 0).then(|| tokio::time::interval(Duration::from_secs(heartbeat_secs)))
}

/// 等待下一次心跳；心跳禁用时返回一个永远不完成的 future。
async fn next_ping(ping_interval: &mut Option<Interval>) {
    if let Some(ping_interval) = ping_interval {
        ping_interval.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}

/// 等待任务退出，超时后强制 abort。
pub(crate) async fn wait_for_task(
    mut task: JoinHandle<()>,
    timeout_dur: Duration,
    task_name: &'static str,
) {
    match timeout(timeout_dur, &mut task).await {
        Ok(Ok(())) => {
            tracing::debug!(task = task_name, "task stopped");
        }
        Ok(Err(e)) => {
            tracing::warn!(task = task_name, error = %e, "task failed during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                task = task_name,
                timeout_ms = timeout_dur.as_millis(),
                "task shutdown timed out; aborting"
            );
            task.abort();
            let _ = task.await;
        }
    }
}
