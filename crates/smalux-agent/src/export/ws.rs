//! WebSocket 导出模块入口。
//!
//! 这里只负责组织 WebSocket 子模块并暴露必要类型；具体职责拆到独立文件。

/// WebSocket 认证模型。
pub(crate) mod auth;
/// WebSocket 连接状态机。
mod client;
/// WebSocket 客户端配置。
mod config;
/// WebSocket 握手 request 和 URL 工具。
mod request;
/// WebSocket 后台收发任务。
mod tasks;

pub(crate) use client::WebSocketClient;
pub(crate) use config::WebSocketConfig;

#[cfg(test)]
mod tests {
    //! WebSocket 配置、握手和收发行为测试。

    use super::auth::WebSocketAuth;
    use super::client::WebSocketClient;
    use super::config::WebSocketConfig;
    use super::request::{build_connect_request, build_connect_url, redact_url};
    use crate::config::model::{ExportAuthMode, ExportConfig, ExportFormat, ExportWireMode};
    use crate::export::security;
    use crate::export::wire;
    use crate::export::{
        EncodedExportMessage, ExportInboundMessage, ExportMessageListener, ExportTransport,
    };
    use base64::Engine;
    use futures_util::{SinkExt, StreamExt};
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
    use tokio_tungstenite::tungstenite::protocol::Message;

    /// 测试接收超时时间。
    const TEST_RECV_TIMEOUT: Duration = Duration::from_secs(3);

    /// 用 channel 收集 listener 收到的文本消息。
    struct ChannelMessageListener {
        /// listener 名称，方便区分替换前后的消息来源。
        name: &'static str,
        /// 测试用消息发送端。
        sender: mpsc::Sender<String>,
    }

    impl ChannelMessageListener {
        /// 创建 channel listener。
        fn new(name: &'static str, sender: mpsc::Sender<String>) -> Self {
            Self { name, sender }
        }
    }

    impl ExportMessageListener for ChannelMessageListener {
        /// 将收到的消息写入测试 channel。
        fn on_message(
            &self,
            msg: ExportInboundMessage,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
            let msg = match msg {
                ExportInboundMessage::Text(msg) => msg,
                ExportInboundMessage::Binary(bytes) => format!("binary:{}", bytes.len()),
            };
            let msg = format!("{}:{msg}", self.name);
            let sender = self.sender.clone();
            Box::pin(async move {
                sender.send(msg).await?;
                Ok(())
            })
        }
    }

    /// 握手时捕获到的信息。
    #[derive(Debug)]
    struct HandshakeCapture {
        /// request URI。
        uri: String,
        /// Authorization header。
        authorization: Option<String>,
    }

    /// 构建默认测试 client。
    fn client_with_config(config: WebSocketConfig) -> WebSocketClient {
        WebSocketClient::new_with_config(config)
    }

    /// 构建 query token 认证配置。
    fn query_token_config(url: String) -> WebSocketConfig {
        WebSocketConfig::new(url).with_auth(WebSocketAuth::QueryToken {
            param: "token".to_string(),
            token: "secret-token".to_string(),
        })
    }

    /// 等待 mock server 传回一条消息。
    async fn recv_with_timeout(receiver: &mut mpsc::Receiver<String>) -> String {
        tokio::time::timeout(TEST_RECV_TIMEOUT, receiver.recv())
            .await
            .expect("timed out waiting for channel message")
            .expect("channel closed before message")
    }

    /// 接收握手捕获信息。
    async fn recv_handshake(capture_rx: oneshot::Receiver<HandshakeCapture>) -> HandshakeCapture {
        timeout(Duration::from_secs(3), capture_rx)
            .await
            .expect("timed out waiting for handshake capture")
            .expect("handshake capture sender dropped")
    }

    /// 启动一个 echo WebSocket server。
    async fn spawn_echo_server() -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = server_url(listener.local_addr().unwrap());
        let (received_tx, received_rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Text(text) => {
                        received_tx.send(text.to_string()).await.unwrap();
                        websocket.send(Message::Text(text)).await.unwrap();
                    }
                    Message::Binary(bytes) => {
                        let label = wire::decode_wire_packet(&bytes)
                            .map(|packet| {
                                format!("wire:{}:{}", packet.kind.as_str(), packet.payload.len())
                            })
                            .unwrap_or_else(|_| format!("binary:{}", bytes.len()));
                        received_tx.send(label).await.unwrap();
                        websocket.send(Message::Binary(bytes)).await.unwrap();
                    }
                    Message::Close(frame) => {
                        websocket.send(Message::Close(frame)).await.unwrap();
                        break;
                    }
                    Message::Ping(payload) => {
                        websocket.send(Message::Pong(payload)).await.unwrap();
                    }
                    _ => {}
                }
            }
        });

        (url, received_rx)
    }

    /// 启动只检查握手信息的 WebSocket server。
    async fn spawn_handshake_server() -> (String, oneshot::Receiver<HandshakeCapture>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = server_url(listener.local_addr().unwrap());
        let (capture_tx, capture_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let capture_tx = std::sync::Mutex::new(Some(capture_tx));
            let mut websocket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &Request, response: Response| {
                    let authorization = request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned);
                    let capture = HandshakeCapture {
                        uri: request.uri().to_string(),
                        authorization,
                    };

                    if let Some(sender) = capture_tx.lock().unwrap().take() {
                        let _ = sender.send(capture);
                    }

                    Ok(response)
                },
            )
            .await
            .unwrap();

            let _ = websocket.close(None).await;
        });

        (url, capture_rx)
    }

    /// 启动连接后立即由服务端主动关闭的 WebSocket server。
    async fn spawn_server_close_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = server_url(listener.local_addr().unwrap());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = websocket.close(None).await;
        });

        url
    }

    /// 启动支持 Smalux secure_psk 的 echo WebSocket server。
    async fn spawn_secure_echo_server(token: String) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = server_url(listener.local_addr().unwrap());
        let (received_tx, received_rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let key = security::parse_secure_token(&token).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

            let hello = read_test_wire_packet(&mut websocket).await;
            assert_eq!(hello.kind, wire::WirePacketKind::Hello);
            let decoded_hello = security::decode_secure_hello(&hello.payload).unwrap();
            assert_eq!(decoded_hello.key_id, key.key_id);
            assert_eq!(decoded_hello.pattern, security::NOISE_PATTERN);

            let first = read_test_wire_packet(&mut websocket).await;
            assert_eq!(first.kind, wire::WirePacketKind::Handshake);
            assert_eq!(first.session_id, hello.session_id);

            let mut responder = security::build_noise_responder(&key.psk).unwrap();
            security::read_handshake_message(&mut responder, &first.payload).unwrap();
            let response = security::write_handshake_message(&mut responder, b"").unwrap();
            send_test_wire_packet(
                &mut websocket,
                wire::WirePacket::new(
                    wire::WirePacketKind::Handshake,
                    hello.session_id,
                    2,
                    response,
                ),
            )
            .await;
            let mut transport = responder.into_transport_mode().unwrap();

            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Binary(bytes) => {
                        let packet = wire::decode_wire_packet(&bytes).unwrap();
                        assert_eq!(packet.kind, wire::WirePacketKind::SecureData);
                        let plain =
                            security::decrypt_payload(&mut transport, &packet.payload).unwrap();
                        received_tx
                            .send(String::from_utf8(plain.clone()).unwrap())
                            .await
                            .unwrap();
                        let encrypted = security::encrypt_payload(&mut transport, &plain).unwrap();
                        send_test_wire_packet(
                            &mut websocket,
                            wire::WirePacket::secure_data(
                                packet.session_id,
                                packet.sequence,
                                encrypted,
                            ),
                        )
                        .await;
                    }
                    Message::Close(frame) => {
                        websocket.send(Message::Close(frame)).await.unwrap();
                        break;
                    }
                    Message::Ping(payload) => {
                        websocket.send(Message::Pong(payload)).await.unwrap();
                    }
                    _ => {}
                }
            }
        });

        (url, received_rx)
    }

    /// 测试用 secure token。
    fn secure_test_token(key_id: &str) -> String {
        let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([9u8; 32]);
        format!("smx1.{key_id}.{secret}")
    }

    /// 测试 server 读取一条 wire packet。
    async fn read_test_wire_packet(
        websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) -> wire::WirePacket {
        loop {
            match websocket.next().await.unwrap().unwrap() {
                Message::Binary(bytes) => return wire::decode_wire_packet(&bytes).unwrap(),
                Message::Ping(payload) => websocket.send(Message::Pong(payload)).await.unwrap(),
                other => panic!("unexpected websocket message during secure test: {other:?}"),
            }
        }
    }

    /// 测试 server 发送一条 wire packet。
    async fn send_test_wire_packet(
        websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        packet: wire::WirePacket,
    ) {
        let bytes = wire::encode_wire_packet(&packet).unwrap();
        websocket
            .send(Message::Binary(bytes::Bytes::from(bytes)))
            .await
            .unwrap();
    }

    /// 将本地监听地址转换为 ws URL。
    fn server_url(addr: SocketAddr) -> String {
        format!("ws://{addr}")
    }

    /// 未连接时发送应返回明确错误。
    #[tokio::test]
    async fn send_without_connection_returns_error() {
        let mut client = client_with_config(WebSocketConfig::new("ws://127.0.0.1:1".to_string()));

        let error = client.send_text_message("hello").await.unwrap_err();

        assert!(error.to_string().contains("client not connected"));
    }

    /// Debug 输出必须脱敏 token。
    #[test]
    fn debug_output_masks_token() {
        let config = query_token_config("ws://127.0.0.1:8080/ws?access_token=from-url".to_string());
        let debug = format!("{config:?}");

        assert!(debug.contains("token_set"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("from-url"));
        assert!(debug.contains("<redacted>"));
    }

    /// URL 构建会合并基础 URL query、额外 query 和认证 query。
    #[test]
    fn build_connect_url_merges_multiple_query_params() {
        let config = query_token_config("ws://127.0.0.1:8080/ws?client=agent".to_string())
            .with_query_param("feature", "metrics v1");

        let url = build_connect_url(&config).unwrap();

        assert_eq!(
            url,
            "ws://127.0.0.1:8080/ws?client=agent&feature=metrics+v1&token=secret-token"
        );
    }

    /// URL 缺少 path 时使用根路径。
    #[test]
    fn build_connect_url_adds_root_path_when_missing() {
        let config =
            WebSocketConfig::new("ws://127.0.0.1:8080".to_string()).with_query_param("agent", "a1");

        let url = build_connect_url(&config).unwrap();

        assert_eq!(url, "ws://127.0.0.1:8080/?agent=a1");
    }

    /// Bearer token 会写入 Authorization header。
    #[test]
    fn bearer_token_sets_authorization_header() {
        let config = WebSocketConfig::new("ws://127.0.0.1:8080/ws".to_string()).with_auth(
            WebSocketAuth::BearerToken {
                token: "secret-token".to_string(),
            },
        );

        let request = build_connect_request(&config).unwrap();

        assert_eq!(
            request.headers().get(AUTHORIZATION).unwrap(),
            "Bearer secret-token"
        );
    }

    /// 重复敏感 query 参数会报错，避免服务端解析歧义。
    #[test]
    fn duplicate_sensitive_query_param_returns_error() {
        let config = query_token_config("ws://127.0.0.1:8080/ws?token=from-url".to_string());

        let error = build_connect_url(&config).unwrap_err();

        assert!(error.to_string().contains("duplicate sensitive query"));
    }

    /// URL 解码后重复的敏感参数也会报错。
    #[test]
    fn duplicate_sensitive_query_param_after_decoding_returns_error() {
        let config =
            WebSocketConfig::new("ws://127.0.0.1:8080/ws?access%5Ftoken=from-url".to_string())
                .with_query_param("access_token", "from-query");

        let error = build_connect_url(&config).unwrap_err();

        assert!(error.to_string().contains("duplicate sensitive query"));
    }

    /// URL 脱敏保留非敏感 query 和 fragment。
    #[test]
    fn redact_url_masks_sensitive_query_values() {
        let redacted = redact_url("ws://127.0.0.1/ws?token=abc&client=agent&api_key=key#fragment");

        assert_eq!(
            redacted,
            "ws://127.0.0.1/ws?token=<redacted>&client=agent&api_key=<redacted>#fragment"
        );
    }

    /// ExportConfig 可以转换为 query token WebSocketConfig。
    #[test]
    fn export_config_converts_to_query_token_websocket_config() {
        let mut export = ExportConfig {
            server_url: "ws://127.0.0.1:8080/ws".to_string(),
            format: ExportFormat::SmaluxJson,
            wire_mode: crate::config::model::ExportWireMode::BinaryPlain,
            secure_required: false,
            token: Some("secret-token".to_string()),
            auth_mode: ExportAuthMode::Query,
            query_token_param: "access_token".to_string(),
            query: BTreeMap::new(),
            unsafe_cert: true,
            heartbeat: Duration::from_secs(7),
            reconnect_interval: Duration::from_secs(5),
        };
        export
            .query
            .insert("agent_id".to_string(), "agent-1".to_string());

        let config = WebSocketConfig::try_from(&export).unwrap();
        let url = build_connect_url(&config).unwrap();

        assert!(url.contains("agent_id=agent-1"));
        assert!(url.contains("access_token=secret-token"));
        assert!(config.unsafe_cert);
        assert_eq!(config.heartbeat, 7);
    }

    /// Bearer 模式缺少 token 时必须快速失败。
    #[test]
    fn export_config_missing_token_returns_error_for_bearer_auth() {
        let export = ExportConfig {
            auth_mode: ExportAuthMode::Bearer,
            ..ExportConfig::default()
        };

        let error = WebSocketConfig::try_from(&export).unwrap_err();

        assert!(error.to_string().contains("export.token is required"));
    }

    /// Komari 不使用 Smalux wire 安全模式，避免第三方兼容配置被 secure_psk 残留字段影响。
    #[test]
    fn komari_websocket_config_ignores_smalux_wire_mode() {
        let export = ExportConfig {
            server_url: "wss://example.com/api/clients/report".to_string(),
            format: ExportFormat::Komari,
            wire_mode: ExportWireMode::SecurePsk,
            token: Some("komari-token".to_string()),
            auth_mode: ExportAuthMode::Query,
            ..ExportConfig::default()
        };

        let config = WebSocketConfig::try_from(&export).unwrap();

        assert_eq!(config.wire_mode, ExportWireMode::BinaryPlain);
        assert!(config.secure_key.is_none());
    }

    /// 本地 WebSocket server 验证连接、发送、接收和关闭流程。
    #[tokio::test]
    async fn connect_send_receive_and_close_with_local_server() {
        let (url, mut server_rx) = spawn_echo_server().await;
        let (listener_tx, mut listener_rx) = mpsc::channel(8);
        let mut client = client_with_config(WebSocketConfig::new(url).with_heartbeat(0));
        client
            .set_listener(Box::new(ChannelMessageListener::new(
                "listener",
                listener_tx,
            )))
            .await
            .unwrap();

        client.connect().await.unwrap();
        client.send_text_message("hello").await.unwrap();

        assert_eq!(recv_with_timeout(&mut server_rx).await, "hello");
        assert_eq!(recv_with_timeout(&mut listener_rx).await, "listener:hello");

        client.close().await.unwrap();
    }

    /// 本地 WebSocket server 验证二进制收发。
    #[tokio::test]
    async fn connect_send_receive_binary_with_local_server() {
        let (url, mut server_rx) = spawn_echo_server().await;
        let (listener_tx, mut listener_rx) = mpsc::channel(8);
        let mut client = client_with_config(WebSocketConfig::new(url).with_heartbeat(0));
        client
            .set_listener(Box::new(ChannelMessageListener::new(
                "listener",
                listener_tx,
            )))
            .await
            .unwrap();

        client.connect().await.unwrap();
        client
            .send_encoded_export_message(EncodedExportMessage::Binary {
                sequence: 1,
                body: vec![1, 2, 3, 4],
            })
            .await
            .unwrap();

        assert_eq!(recv_with_timeout(&mut server_rx).await, "wire:plain_data:4");
        assert_eq!(
            recv_with_timeout(&mut listener_rx).await,
            "listener:binary:4"
        );

        client.close().await.unwrap();
    }

    /// 本地 WebSocket server 验证 secure_psk 握手、加密发送和解密接收。
    #[tokio::test]
    async fn connect_secure_psk_send_receive_with_local_server() {
        let token = secure_test_token("agent-key");
        let (url, mut server_rx) = spawn_secure_echo_server(token.clone()).await;
        let (listener_tx, mut listener_rx) = mpsc::channel(8);
        let config = WebSocketConfig::new(url)
            .with_heartbeat(0)
            .with_wire_security(
                ExportWireMode::SecurePsk,
                Some(security::parse_secure_token(&token).unwrap()),
            );
        let mut client = client_with_config(config);
        client
            .set_listener(Box::new(ChannelMessageListener::new(
                "listener",
                listener_tx,
            )))
            .await
            .unwrap();

        client.connect().await.unwrap();
        client
            .send_encoded_export_message(EncodedExportMessage::Binary {
                sequence: 7,
                body: b"secure-payload".to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(recv_with_timeout(&mut server_rx).await, "secure-payload");
        assert_eq!(
            recv_with_timeout(&mut listener_rx).await,
            format!("listener:binary:{}", b"secure-payload".len())
        );

        client.close().await.unwrap();
    }

    /// query token 会通过 URI 传给服务端。
    #[tokio::test]
    async fn query_token_is_sent_as_query_param() {
        let (url, capture_rx) = spawn_handshake_server().await;
        let mut client = client_with_config(query_token_config(url).with_heartbeat(0));

        client.connect().await.unwrap();
        let capture = recv_handshake(capture_rx).await;
        client.close().await.unwrap();

        assert!(capture.uri.contains("token=secret-token"));
        assert!(capture.authorization.is_none());
    }

    /// Bearer token 会通过 Authorization header 传给服务端。
    #[tokio::test]
    async fn bearer_token_is_sent_as_authorization_header() {
        let (url, capture_rx) = spawn_handshake_server().await;
        let config =
            WebSocketConfig::new(url)
                .with_heartbeat(0)
                .with_auth(WebSocketAuth::BearerToken {
                    token: "secret-token".to_string(),
                });
        let mut client = client_with_config(config);

        client.connect().await.unwrap();
        let capture = recv_handshake(capture_rx).await;
        client.close().await.unwrap();

        assert_eq!(
            capture.authorization.as_deref(),
            Some("Bearer secret-token")
        );
        assert!(!capture.uri.contains("secret-token"));
    }

    /// 已连接 client 不能重复 connect。
    #[tokio::test]
    async fn duplicate_connect_returns_error() {
        let (url, _server_rx) = spawn_echo_server().await;
        let mut client = client_with_config(WebSocketConfig::new(url).with_heartbeat(0));

        client.connect().await.unwrap();
        let error = client.connect().await.unwrap_err();
        client.close().await.unwrap();

        assert!(error.to_string().contains("already connected"));
    }

    /// 替换 listener 后，新消息应交给新 listener。
    #[tokio::test]
    async fn listener_update_applies_to_subsequent_messages() {
        let (url, mut server_rx) = spawn_echo_server().await;
        let (first_tx, mut first_rx) = mpsc::channel(8);
        let (second_tx, mut second_rx) = mpsc::channel(8);
        let mut client = client_with_config(WebSocketConfig::new(url).with_heartbeat(0));

        client
            .set_listener(Box::new(ChannelMessageListener::new("first", first_tx)))
            .await
            .unwrap();
        client.connect().await.unwrap();
        client.send_text_message("one").await.unwrap();
        assert_eq!(recv_with_timeout(&mut server_rx).await, "one");
        assert_eq!(recv_with_timeout(&mut first_rx).await, "first:one");

        client
            .set_listener(Box::new(ChannelMessageListener::new("second", second_tx)))
            .await
            .unwrap();
        client.send_text_message("two").await.unwrap();
        assert_eq!(recv_with_timeout(&mut server_rx).await, "two");
        assert_eq!(recv_with_timeout(&mut second_rx).await, "second:two");

        client.close().await.unwrap();
    }

    /// 心跳为 0 时不会主动发送 ping，普通收发仍可用。
    #[tokio::test]
    async fn heartbeat_zero_disables_ping() {
        let (url, mut server_rx) = spawn_echo_server().await;
        let mut client = client_with_config(WebSocketConfig::new(url).with_heartbeat(0));

        client.connect().await.unwrap();
        client.send_text_message("no-ping").await.unwrap();
        assert_eq!(recv_with_timeout(&mut server_rx).await, "no-ping");
        client.close().await.unwrap();
    }

    /// 服务端先关闭后，client close 应该能完成清理。
    #[tokio::test]
    async fn server_close_can_be_cleaned_up_by_client_close() {
        let url = spawn_server_close_server().await;
        let mut client = client_with_config(WebSocketConfig::new(url).with_heartbeat(0));

        client.connect().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.close().await.unwrap();
    }
}
