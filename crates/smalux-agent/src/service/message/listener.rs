//! Server 控制消息处理。
//!
//! 自有协议只接受稳定的 `smalux_protocol::ServerFrame`。第三方兼容协议需要在各自
//! adapter/listener 中先转换为统一内部命令，不能绕回这里解析 raw JSON。

use crate::config::ConfigManager;
use crate::config::model::AgentConfigPatch;
use crate::export::{ExportInboundMessage, ExportMessageListener, inbound_message_into_string};
use crate::service::{shell::SmaluxShellCodec, task::RemoteTaskRunRequest};
use smalux_protocol::{ServerPayload, decode_server_frame};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::inbound::{InboundCommand, InboundCommandEnvelope, InboundCommandSender};

/// 导出消息 listener，负责把 server 控制消息转换成内部命令。
#[derive(Debug, Clone)]
pub(crate) struct ServiceControlListener {
    /// 动态配置管理器，用于读取当前 agent ID 做目标过滤。
    config_manager: ConfigManager,
    /// 入站命令发送端。
    commands: InboundCommandSender,
}

impl ServiceControlListener {
    /// 创建控制消息监听器。
    pub(crate) fn new(config_manager: ConfigManager, commands: InboundCommandSender) -> Self {
        Self {
            config_manager,
            commands,
        }
    }

    /// 解析一条 server 控制消息。
    ///
    /// WebSocket transport 已经在进入 listener 前完成 wire 解包和可选解密，这里收到的
    /// 始终是 UTF-8 JSON 字符串。解析失败表示不是当前自有协议消息，直接丢弃；
    /// 已识别 frame 的 payload 转换失败会返回错误，后续调度器可以据此回 protocol error。
    pub(crate) fn decode_message(
        &self,
        msg: &str,
    ) -> anyhow::Result<Option<InboundCommandEnvelope>> {
        let frame = match decode_server_frame(msg) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(error = %error, "unknown server frame dropped");
                return Ok(None);
            }
        };

        if !self.target_matches(frame.target_agent_id.as_deref()) {
            tracing::warn!(
                target_agent_id = frame.target_agent_id.as_deref(),
                current_agent_id = %self.config_manager.current().agent_id,
                sequence = frame.sequence,
                "server frame target mismatch; dropped"
            );
            return Ok(None);
        }

        let command = match frame.payload {
            ServerPayload::SnapshotRequest { request } => InboundCommand::SnapshotRequest {
                reason: request.reason,
            },
            ServerPayload::Ack { ack } => {
                tracing::debug!(sequence = ack.sequence, "server ack ignored");
                return Ok(None);
            }
            ServerPayload::Error { error } => {
                tracing::warn!(
                    sequence = error.sequence,
                    code = %error.code,
                    message = %error.message,
                    "server protocol error received"
                );
                return Ok(None);
            }
            ServerPayload::ConfigPatch { patch } => {
                let patch = serde_json::from_value::<AgentConfigPatch>(patch)?;
                InboundCommand::ConfigPatch {
                    patch: Box::new(patch),
                }
            }
            ServerPayload::CollectProcessesOnce { request } => {
                InboundCommand::CollectProcessesOnce {
                    level: request.level,
                    limit: request.limit,
                }
            }
            ServerPayload::CollectSocketsOnce { request } => InboundCommand::CollectSocketsOnce {
                level: request.level,
                limit: request.limit,
            },
            ServerPayload::RemoteTaskRun { request } => {
                let request: RemoteTaskRunRequest = request.into();
                InboundCommand::RemoteTaskRun { request }
            }
            ServerPayload::RemoteProbeRun { request } => InboundCommand::RemoteProbeRun {
                request: request.into(),
            },
            ServerPayload::RemoteShellOpen { request } => InboundCommand::RemoteShellOpen {
                request,
                stream_export_config: None,
                stream_codec: Arc::new(SmaluxShellCodec),
            },
        };

        Ok(Some(InboundCommandEnvelope::with_response(
            command,
            frame.sequence,
        )))
    }

    /// 解析并投递一条 server 文本消息。
    pub(crate) async fn handle_message(&self, msg: &str) -> anyhow::Result<()> {
        if let Some(command) = self.decode_message(msg)? {
            self.commands
                .send(command)
                .await
                .map_err(|_| anyhow::anyhow!("inbound command channel is closed"))?;

            tracing::debug!("server control command queued");
        }
        Ok(())
    }

    /// 检查 server frame 的目标 agent 是否匹配当前 agent。
    fn target_matches(&self, target_agent_id: Option<&str>) -> bool {
        let Some(target_agent_id) = target_agent_id else {
            return true;
        };
        target_agent_id == self.config_manager.current().agent_id
    }
}

impl ExportMessageListener for ServiceControlListener {
    /// 收到 server 消息后解析控制命令并投递到 service 调度器。
    fn on_message(
        &self,
        msg: ExportInboundMessage,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let msg = inbound_message_into_string(msg)?;
            self.handle_message(&msg).await
        })
    }
}

#[cfg(test)]
mod tests {
    //! service control listener 测试。

    use super::*;
    use crate::config::{AgentConfig, ConfigManager};
    use crate::service::inbound::inbound_command_channel;
    use smalux_protocol::{RemoteShellOpenRequest, ServerFrame, encode_server_frame};

    /// 构造测试 listener。
    fn listener(agent_id: &str) -> ServiceControlListener {
        let config = AgentConfig {
            agent_id: agent_id.to_string(),
            ..AgentConfig::default()
        };
        let config_manager = ConfigManager::new(config).unwrap();
        let (commands, _command_rx) = inbound_command_channel();
        ServiceControlListener::new(config_manager, commands)
    }

    /// 验证稳定 ServerFrame remote_shell_open 会保留 sequence 并进入内部命令。
    #[test]
    fn decode_message_accepts_server_frame_remote_shell_open() {
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

        let envelope = listener("agent-1")
            .decode_message(&message)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.meta.unwrap().sequence, 42);
        let InboundCommand::RemoteShellOpen {
            request,
            stream_export_config,
            stream_codec,
        } = envelope.command
        else {
            panic!("expected remote shell open command");
        };

        assert_eq!(request.session_id, "shell-1");
        assert_eq!(request.cols, Some(120));
        assert!(stream_export_config.is_none());
        assert_eq!(stream_codec.name(), "smalux_shell");
    }

    /// 验证 raw remote_shell_open 会被丢弃，不再作为自有协议入口。
    #[test]
    fn decode_message_drops_raw_remote_shell_open() {
        let decoded = listener("agent-1")
            .decode_message(
                r#"{
                "type": "remote_shell_open",
                "session_id": "shell-raw",
                "stream_url": "ws://127.0.0.1:1/shell"
            }"#,
            )
            .unwrap();

        assert!(decoded.is_none());
    }

    /// 验证 raw remote_probe_run 会被丢弃，不再作为自有协议入口。
    #[test]
    fn decode_message_drops_raw_remote_probe_run() {
        let decoded = listener("agent-1")
            .decode_message(
                r#"{
                "type": "remote_probe_run",
                "task_id": "probe-raw",
                "probe_type": "tcp",
                "target": "127.0.0.1:80"
            }"#,
            )
            .unwrap();

        assert!(decoded.is_none());
    }

    /// 验证目标 agent 不匹配时直接丢弃。
    #[test]
    fn decode_message_drops_mismatched_target_agent_id() {
        let message = encode_server_frame(
            &ServerFrame::remote_shell_open(
                42,
                100,
                RemoteShellOpenRequest {
                    session_id: "shell-1".to_string(),
                    stream_url: "wss://example.com/shell/shell-1".to_string(),
                    cols: Some(120),
                    rows: Some(30),
                },
            )
            .with_target_agent_id("agent-other"),
        )
        .unwrap();

        let decoded = listener("agent-1").decode_message(&message).unwrap();

        assert!(decoded.is_none());
    }
}
