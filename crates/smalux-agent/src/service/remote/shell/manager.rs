//! 远程 shell 会话管理。
//!
//! manager 只负责全局开关、会话上限和启动单个 shell 会话任务；stream 连接和
//! PTY 阻塞线程分别放在 `session` / `pty` 模块中，避免入口模块承担过多细节。

use super::message::{RemoteShellOpenRequest, validate_open_request};
use super::options::RemoteShellOptions;
use super::session::{RemoteShellSession, RemoteShellSessionPermit};
use super::stream::RemoteShellStreamCodecRef;
use crate::config::model::{ExportConfig, RemoteShellConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
        stream_codec: RemoteShellStreamCodecRef,
    ) -> anyhow::Result<()> {
        if !self.options.enabled {
            anyhow::bail!("remote shell is disabled");
        }

        validate_open_request(&request)?;
        let permit = self.acquire_session_permit(shell_config.max_sessions)?;
        let session_id = request.session_id.clone();
        let codec_name = stream_codec.name();
        let session =
            RemoteShellSession::new(request, export_config, shell_config, stream_codec, permit)?;

        tokio::spawn(async move {
            tracing::info!(
                session_id = %session_id,
                codec = codec_name,
                "remote shell session starting"
            );
            if let Err(err) = session.run().await {
                tracing::warn!(
                    session_id = %session_id,
                    codec = codec_name,
                    error = ?err,
                    "remote shell session stopped with error"
                );
            } else {
                tracing::info!(
                    session_id = %session_id,
                    codec = codec_name,
                    "remote shell session stopped"
                );
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

#[cfg(test)]
mod tests {
    //! 远程 shell manager 测试。

    use super::*;
    use crate::config::model::{ExportConfig, RemoteShellConfig};
    use crate::export::TransportInboundMessage;
    use crate::export::ws::WebSocketConfig;
    use crate::service::remote::shell::session::shell_stream_config;
    use crate::service::remote::shell::{
        RemoteShellFrame, RemoteShellInput, RemoteShellStreamCodec, RemoteShellStreamEvent,
        SmaluxShellCodec,
    };
    use futures_util::{SinkExt, StreamExt};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::protocol::Message;

    /// 构造 Smalux 默认 codec。
    fn smalux_codec() -> RemoteShellStreamCodecRef {
        Arc::new(SmaluxShellCodec)
    }

    /// 测试用 raw terminal codec，避免 manager 测试依赖第三方兼容模块。
    #[derive(Debug)]
    struct RawTestCodec;

    impl RemoteShellStreamCodec for RawTestCodec {
        /// 返回 codec 名称。
        fn name(&self) -> &'static str {
            "raw_test"
        }

        /// 测试 raw codec 需要启用裸二进制帧。
        fn configure_websocket(&self, config: WebSocketConfig) -> WebSocketConfig {
            config.with_raw_binary_frames(true)
        }

        /// 测试 raw codec 直接把 binary 输入写入 PTY。
        fn decode_inbound(
            &self,
            msg: TransportInboundMessage,
        ) -> anyhow::Result<Option<RemoteShellInput>> {
            match msg {
                TransportInboundMessage::Binary(bytes) => Ok(Some(RemoteShellInput::Input(bytes))),
                TransportInboundMessage::Text(text) => {
                    Ok(Some(RemoteShellInput::Input(text.into_bytes())))
                }
            }
        }

        /// 测试 raw codec 只把 PTY 输出转为 raw binary。
        fn encode_event(
            &self,
            event: RemoteShellStreamEvent,
        ) -> anyhow::Result<Option<RemoteShellFrame>> {
            match event {
                RemoteShellStreamEvent::Output { data, .. } => {
                    let bytes =
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)?;
                    Ok(Some(RemoteShellFrame::RawBinary(bytes)))
                }
                RemoteShellStreamEvent::Error { message, .. } => {
                    Ok(Some(RemoteShellFrame::Text(message)))
                }
                RemoteShellStreamEvent::Opened { .. } | RemoteShellStreamEvent::Exit { .. } => {
                    Ok(None)
                }
            }
        }
    }

    /// 构造测试 raw codec。
    fn raw_test_codec() -> RemoteShellStreamCodecRef {
        Arc::new(RawTestCodec)
    }

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
                smalux_codec(),
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
                smalux_codec(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("limit"));
    }

    /// 验证 stream 配置会复用主导出认证和 TLS 选项。
    #[test]
    fn shell_stream_config_reuses_export_options() {
        let export = ExportConfig {
            base_url: "http://127.0.0.1".to_string(),
            unsafe_cert: true,
            heartbeat: Duration::from_secs(9),
            ..ExportConfig::default()
        };

        let codec = smalux_codec();
        let config = shell_stream_config(&export, "ws://127.0.0.1/shell", codec.as_ref()).unwrap();

        assert_eq!(config.url, "ws://127.0.0.1/shell");
        assert!(config.unsafe_cert);
        assert_eq!(config.heartbeat, Duration::from_secs(9));
        assert!(!config.raw_binary_frames);
    }

    /// 验证 raw codec 可以启用裸 binary 帧。
    #[test]
    fn shell_stream_config_enables_raw_binary_for_raw_codec() {
        let export = ExportConfig::default();
        let codec = raw_test_codec();
        let config = shell_stream_config(&export, "ws://127.0.0.1/shell", codec.as_ref()).unwrap();

        assert!(config.raw_binary_frames);
    }

    /// 验证 raw terminal stream 会转发裸二进制 PTY 输出。
    #[tokio::test]
    async fn manager_opens_raw_shell_stream_and_forwards_raw_output() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream_url = format!("ws://{}", listener.local_addr().unwrap());
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut output_text = String::new();
            let mut input_sent = false;
            let mut output_seen = false;

            if !cfg!(windows) {
                websocket
                    .send(Message::Binary(bytes::Bytes::from_static(
                        b"echo smalux-shell-test\r\nexit\r\n",
                    )))
                    .await
                    .unwrap();
                input_sent = true;
            }

            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Text(text) => {
                        panic!("raw shell stream output must not be JSON text: {text}");
                    }
                    Message::Binary(bytes) => {
                        output_text.push_str(&String::from_utf8_lossy(&bytes));
                        if cfg!(windows) && !input_sent && output_text.contains("\u{1b}[6n") {
                            // Windows PowerShell 可能先请求终端光标位置；测试 server 模拟一个最小终端响应。
                            websocket
                                .send(Message::Binary(bytes::Bytes::from_static(
                                    b"\x1b[24;1Recho smalux-shell-test\r\nexit\r\n",
                                )))
                                .await
                                .unwrap();
                            input_sent = true;
                        }
                        output_seen |= output_text.contains("smalux-shell-test");
                    }
                    Message::Ping(payload) => {
                        websocket.send(Message::Pong(payload)).await.unwrap();
                    }
                    Message::Close(_frame) => break,
                    _ => {}
                }
            }

            (input_sent, output_seen, output_text)
        });

        let manager = RemoteShellManager::new(RemoteShellOptions { enabled: true });

        let export = ExportConfig::default();

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
                raw_test_codec(),
            )
            .unwrap();

        let (input_sent, output_seen, output_text) = timeout(Duration::from_secs(12), server_task)
            .await
            .expect("timed out waiting for shell stream")
            .unwrap();

        assert!(input_sent);
        assert!(output_seen, "shell output was: {output_text:?}");
    }
}
