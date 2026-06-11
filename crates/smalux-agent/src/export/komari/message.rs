//! Komari server 消息入站处理。
//!
//! terminal 和 exec 消息会转换为内部入站命令；其它 server 消息会安全忽略，
//! 避免误解析成 smalux `config_patch`。

use crate::config::ConfigManager;
use crate::export::{
    InboundProtocolHandler, TransportInboundMessage, inbound_message_into_string, komari::terminal,
};
use crate::service::{
    InboundCommand, InboundCommandEnvelope, InboundCommandSender, RemoteProbeApply,
    RemoteProbeExecutionRequest, RemoteShellOpenRequest, RemoteTaskRunRequest, display_probe_id,
};
use serde::Deserialize;
use smalux_protocol::{RemoteProbeId, RemoteProbeResultSource};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// 构造 Komari server 消息入站处理器。
pub(super) fn inbound_handler(
    config_manager: ConfigManager,
    commands: InboundCommandSender,
) -> Box<dyn InboundProtocolHandler> {
    Box::new(KomariInboundHandler {
        config_manager,
        commands,
    })
}

/// Komari server 消息入站处理器。
///
/// terminal 消息转交 remote shell，exec 消息转交 remote task，其它 server 事件只记录并忽略。
#[derive(Debug)]
struct KomariInboundHandler {
    /// 动态配置管理器，用于读取当前 export 配置。
    config_manager: ConfigManager,
    /// 入站命令发送端。
    commands: InboundCommandSender,
}

impl InboundProtocolHandler for KomariInboundHandler {
    /// 解析 Komari server 文本消息。
    fn on_message(
        &self,
        msg: TransportInboundMessage,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let config_manager = self.config_manager.clone();
        let commands = self.commands.clone();
        Box::pin(async move {
            let msg = inbound_message_into_string(msg)?;
            let bytes = msg.len();
            if let Ok(event) = terminal::parse_terminal_request(&msg) {
                let current_config = config_manager.current();
                let export_config = current_config.export;
                let stream_url =
                    super::url::komari_terminal_url(&export_config, &event.request_id)?;
                let stream_export_config = terminal::terminal_stream_export_config(export_config);
                let session_id = event.request_id;
                commands
                    .send(InboundCommandEnvelope::without_response(
                        InboundCommand::RemoteShellOpen {
                            request: RemoteShellOpenRequest {
                                session_id: session_id.clone(),
                                stream_url,
                                cols: None,
                                rows: None,
                            },
                            stream_export_config: Some(stream_export_config),
                            stream_codec: Arc::new(terminal::KomariTerminalCodec),
                        },
                    ))
                    .await
                    .map_err(|_| anyhow::anyhow!("inbound command channel is closed"))?;
                tracing::info!(
                    session_id = %session_id,
                    "komari terminal command queued"
                );
                return Ok(());
            }

            if let Ok(event) = parse_exec_request(&msg) {
                let task_id = event.task_id.clone();
                let command_len = event.command.len();
                commands
                    .send(InboundCommandEnvelope::without_response(
                        InboundCommand::RemoteTaskRun {
                            request: event.into_remote_task_request(),
                        },
                    ))
                    .await
                    .map_err(|_| anyhow::anyhow!("inbound command channel is closed"))?;
                tracing::info!(
                    task_id = %task_id,
                    command_len,
                    "komari exec command queued"
                );
                return Ok(());
            }

            if let Ok(event) = parse_ping_request(&msg) {
                let task_id = event.ping_task_id.clone();
                let probe_type = event.ping_type;
                let target = event.ping_target.clone();
                commands
                    .send(InboundCommandEnvelope::without_response(
                        InboundCommand::RemoteProbeApply {
                            request: RemoteProbeApply::Once {
                                runs: vec![event.into_remote_probe_request()],
                            },
                        },
                    ))
                    .await
                    .map_err(|_| anyhow::anyhow!("inbound command channel is closed"))?;
                tracing::info!(
                    probe_id = %display_probe_id(&task_id),
                    probe_type = probe_type.as_str(),
                    target = %target,
                    "komari ping command queued"
                );
                return Ok(());
            }

            tracing::debug!(bytes, "komari server message ignored");
            Ok(())
        })
    }
}

/// Komari exec 控制消息。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
struct KomariExecRequest {
    /// 消息类型，必须是 exec。
    message: String,
    /// Komari server 生成的任务 ID。
    task_id: String,
    /// 要交给平台 shell 执行的命令字符串。
    command: String,
}

/// Komari ping 控制消息。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
struct KomariPingRequest {
    /// 消息类型，必须是 ping。
    message: String,
    /// Komari server 生成的 ping 任务 ID。
    ping_task_id: RemoteProbeId,
    /// 探测类型。
    ping_type: crate::service::RemoteProbeType,
    /// 探测目标。
    ping_target: String,
}

impl KomariPingRequest {
    /// 转换为内部远程探测请求，继续复用 remote probe 的动态开关和频率保护。
    fn into_remote_probe_request(self) -> RemoteProbeExecutionRequest {
        let point_id = self.ping_task_id.clone();
        RemoteProbeExecutionRequest {
            source: RemoteProbeResultSource::Once,
            point_id: Some(point_id),
            request_id: Some(self.ping_task_id),
            job_id: None,
            probe_type: self.ping_type,
            target: self.ping_target,
            timeout: None,
        }
    }
}

impl KomariExecRequest {
    /// 转换为内部远程任务请求，继续复用 remote task 的权限、并发和超时限制。
    fn into_remote_task_request(self) -> RemoteTaskRunRequest {
        let (program, args) = shell_command(self.command);
        RemoteTaskRunRequest {
            task_id: self.task_id,
            program,
            args,
            timeout: None,
        }
    }
}

/// 解析 Komari exec 消息。
fn parse_exec_request(message: &str) -> anyhow::Result<KomariExecRequest> {
    let request: KomariExecRequest = serde_json::from_str(message)?;
    if request.message != "exec" {
        anyhow::bail!("not a komari exec message");
    }
    if request.task_id.trim().is_empty() {
        anyhow::bail!("komari exec task_id cannot be empty");
    }
    if request.command.trim().is_empty() {
        anyhow::bail!("komari exec command cannot be empty");
    }
    Ok(request)
}

/// 解析 Komari ping 消息。
fn parse_ping_request(message: &str) -> anyhow::Result<KomariPingRequest> {
    let request: KomariPingRequest = serde_json::from_str(message)?;
    if request.message != "ping" {
        anyhow::bail!("not a komari ping message");
    }
    if matches!(&request.ping_task_id, RemoteProbeId::String(value) if value.trim().is_empty()) {
        anyhow::bail!("komari ping_task_id cannot be empty");
    }
    if request.ping_target.trim().is_empty() {
        anyhow::bail!("komari ping_target cannot be empty");
    }
    Ok(request)
}

/// 按平台选择 shell，把 Komari 的 command 字符串转换成内部 program + args。
fn shell_command(command: String) -> (String, Vec<String>) {
    if cfg!(windows) {
        (
            "powershell.exe".to_string(),
            vec!["-NoProfile".to_string(), "-Command".to_string(), command],
        )
    } else {
        ("/bin/sh".to_string(), vec!["-c".to_string(), command])
    }
}

#[cfg(test)]
mod tests {
    //! Komari 消息入站处理器测试。

    use super::*;
    use crate::config::AgentConfig;
    use crate::config::model::{ExportAuthMode, RemoteShellConfig};
    use crate::service::shell::{RemoteShellManager, RemoteShellOptions};
    use crate::service::{InboundCommand, inbound_command_channel};
    use futures_util::{SinkExt, StreamExt};
    use smalux_protocol::{RemoteShellOpenRequest, ServerFrame, encode_server_frame};
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};
    use tokio_tungstenite::tungstenite::protocol::Message;

    /// 验证 terminal 消息会转换为内部远程 shell 命令。
    #[tokio::test]
    async fn inbound_handler_enqueues_terminal_command() {
        let mut config = AgentConfig::default();
        config.export.base_url = "http://127.0.0.1:3000".to_string();
        config.export.auth_mode = ExportAuthMode::Query;
        config.export.token = Some("secret-token".to_string());
        let manager = ConfigManager::new(config).unwrap();
        let (commands, mut command_rx) = inbound_command_channel();
        let handler = inbound_handler(manager, commands);

        handler
            .on_message(TransportInboundMessage::Text(
                r#"{ "message": "terminal", "request_id": "term-1" }"#.to_string(),
            ))
            .await
            .unwrap();

        let command = command_rx.try_recv().unwrap().command;
        let InboundCommand::RemoteShellOpen {
            request,
            stream_export_config,
            stream_codec,
        } = command
        else {
            panic!("expected remote shell open command");
        };

        assert_eq!(request.session_id, "term-1");
        assert!(request.stream_url.contains("/api/clients/terminal"));
        assert!(stream_export_config.unwrap().token.is_none());
        assert_eq!(stream_codec.name(), "komari_terminal");
    }

    /// 验证 Komari terminal 消息可以打开真实 raw binary terminal stream。
    #[tokio::test]
    async fn inbound_handler_terminal_command_opens_raw_terminal_stream() {
        let terminal_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", terminal_listener.local_addr().unwrap());
        let terminal_task = tokio::spawn(async move {
            let (stream, _) = terminal_listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut output_text = String::new();
            let mut input_sent = false;

            if !cfg!(windows) {
                websocket
                    .send(Message::Binary(bytes::Bytes::from_static(
                        b"echo komari-terminal-e2e\r\nexit\r\n",
                    )))
                    .await
                    .unwrap();
                input_sent = true;
            }

            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Binary(bytes) => {
                        output_text.push_str(&String::from_utf8_lossy(&bytes));
                        if cfg!(windows) && !input_sent && output_text.contains("\u{1b}[6n") {
                            websocket
                                .send(Message::Binary(bytes::Bytes::from_static(
                                    b"\x1b[24;1Recho komari-terminal-e2e\r\nexit\r\n",
                                )))
                                .await
                                .unwrap();
                            input_sent = true;
                        }
                        if output_text.contains("komari-terminal-e2e") {
                            break;
                        }
                    }
                    Message::Text(text) => {
                        panic!("komari terminal output must be raw binary, got text: {text}");
                    }
                    Message::Ping(payload) => {
                        websocket.send(Message::Pong(payload)).await.unwrap();
                    }
                    Message::Close(_frame) => break,
                    _ => {}
                }
            }

            output_text
        });

        let mut config = AgentConfig::default();
        config.export.base_url = base_url;
        config.export.auth_mode = ExportAuthMode::Query;
        config.export.token = Some("secret-token".to_string());
        let manager = ConfigManager::new(config).unwrap();
        let (commands, mut command_rx) = inbound_command_channel();
        let handler = inbound_handler(manager, commands);

        handler
            .on_message(TransportInboundMessage::Text(
                r#"{ "message": "terminal", "request_id": "term-e2e" }"#.to_string(),
            ))
            .await
            .unwrap();

        let command = command_rx.try_recv().unwrap().command;
        let InboundCommand::RemoteShellOpen {
            request,
            stream_export_config,
            stream_codec,
        } = command
        else {
            panic!("expected remote shell open command");
        };

        assert!(request.stream_url.contains("/api/clients/terminal"));
        assert!(request.stream_url.contains("id=term-e2e"));
        assert!(request.stream_url.contains("token=secret-token"));

        RemoteShellManager::new(RemoteShellOptions { enabled: true })
            .open(
                request,
                &stream_export_config.unwrap(),
                &RemoteShellConfig {
                    idle_timeout: Duration::from_secs(5),
                    session_timeout: Duration::from_secs(10),
                    program: Some(if cfg!(windows) {
                        "cmd.exe".to_string()
                    } else {
                        "/bin/sh".to_string()
                    }),
                    ..RemoteShellConfig::default()
                },
                stream_codec,
            )
            .await
            .unwrap();

        let output_text = timeout(Duration::from_secs(12), terminal_task)
            .await
            .expect("timed out waiting for komari terminal stream")
            .unwrap();

        assert!(
            output_text.contains("komari-terminal-e2e"),
            "terminal output was: {output_text:?}"
        );
    }

    /// 验证 exec 消息会转换为内部远程任务命令。
    #[tokio::test]
    async fn inbound_handler_enqueues_exec_command() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();
        let (commands, mut command_rx) = inbound_command_channel();
        let handler = inbound_handler(manager, commands);

        handler
            .on_message(TransportInboundMessage::Text(
                r#"{ "message": "exec", "task_id": "task-1", "command": "echo ok" }"#.to_string(),
            ))
            .await
            .unwrap();

        let command = command_rx.try_recv().unwrap().command;
        let InboundCommand::RemoteTaskRun { request } = command else {
            panic!("expected remote task run command");
        };

        assert_eq!(request.task_id, "task-1");
        assert_eq!(request.timeout, None);
        if cfg!(windows) {
            assert_eq!(request.program, "powershell.exe");
            assert_eq!(
                request.args,
                vec![
                    "-NoProfile".to_string(),
                    "-Command".to_string(),
                    "echo ok".to_string()
                ]
            );
        } else {
            assert_eq!(request.program, "/bin/sh");
            assert_eq!(request.args, vec!["-c".to_string(), "echo ok".to_string()]);
        }
    }

    /// 验证 ping 消息会转换为内部远程探测命令。
    #[tokio::test]
    async fn inbound_handler_enqueues_ping_command() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();
        let (commands, mut command_rx) = inbound_command_channel();
        let handler = inbound_handler(manager, commands);

        handler
            .on_message(TransportInboundMessage::Text(
                r#"{ "message": "ping", "ping_task_id": 123, "ping_type": "tcp", "ping_target": "example.com:443" }"#.to_string(),
            ))
            .await
            .unwrap();

        let command = command_rx.try_recv().unwrap().command;
        let InboundCommand::RemoteProbeApply { request } = command else {
            panic!("expected remote probe apply command");
        };
        let RemoteProbeApply::Once { runs } = request else {
            panic!("expected remote probe once command");
        };
        let request = runs.into_iter().next().unwrap();

        assert_eq!(request.source, RemoteProbeResultSource::Once);
        assert_eq!(request.request_id, Some(RemoteProbeId::from(123)));
        assert_eq!(request.job_id, None);
        assert_eq!(request.probe_type, crate::service::RemoteProbeType::Tcp);
        assert_eq!(request.target, "example.com:443");
    }

    /// 验证空 exec 字段会被拒绝，避免把无效任务送入执行器。
    #[test]
    fn parse_exec_request_rejects_blank_fields() {
        let error =
            parse_exec_request(r#"{ "message": "exec", "task_id": "task-1", "command": " " }"#)
                .unwrap_err();

        assert!(error.to_string().contains("command"));
    }

    /// 验证空 ping 目标会被拒绝，避免无效探测进入执行器。
    #[test]
    fn parse_ping_request_rejects_blank_target() {
        let error = parse_ping_request(
            r#"{ "message": "ping", "ping_task_id": 1, "ping_type": "tcp", "ping_target": " " }"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("ping_target"));
    }

    /// 验证 Komari handler 不会把 Smalux ServerFrame 误当成第三方控制消息。
    #[tokio::test]
    async fn inbound_handler_ignores_smalux_server_frame() {
        let manager = ConfigManager::new(AgentConfig::default()).unwrap();
        let (commands, mut command_rx) = inbound_command_channel();
        let handler = inbound_handler(manager, commands);
        let message = encode_server_frame(&ServerFrame::remote_shell_open(
            42,
            100,
            RemoteShellOpenRequest {
                session_id: "shell-1".to_string(),
                stream_url: "wss://example.com/shell/shell-1".to_string(),
                cols: Some(120),
                rows: Some(30),
            },
        ))
        .unwrap();

        handler
            .on_message(TransportInboundMessage::Text(message))
            .await
            .unwrap();

        assert!(command_rx.try_recv().is_err());
    }
}
