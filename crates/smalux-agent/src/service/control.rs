//! Server 控制消息处理。

use super::probe::RemoteProbeRunRequest;
use crate::config::model::AgentConfigPatch;
use crate::export::{ExportInboundMessage, ExportMessageListener, inbound_message_into_string};
use crate::service::inbound::{InboundCommand, InboundCommandEnvelope, InboundCommandSender};
use crate::service::shell::RemoteShellOpenRequest;
use crate::service::task::RemoteTaskRunRequest;
use serde::Deserialize;
use smalux_core::model::info::MetricLevel;
use smalux_protocol::{ServerPayload, decode_server_frame};
use std::future::Future;
use std::pin::Pin;

/// 服务端下发给 agent 的控制消息。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerControlMessage {
    /// 服务配置增量更新。
    ConfigPatch {
        /// 需要应用的配置 patch。
        patch: AgentConfigPatch,
    },
    /// 立即采集一次进程信息。
    CollectProcessesOnce {
        /// 本次采集级别；缺省使用当前配置。
        level: Option<MetricLevel>,
        /// 本次返回条数上限；缺省使用当前配置。
        limit: Option<usize>,
    },
    /// 立即采集一次 Socket 信息。
    CollectSocketsOnce {
        /// 本次采集级别；缺省使用当前配置。
        level: Option<MetricLevel>,
        /// 本次返回条数上限；缺省使用当前配置。
        limit: Option<usize>,
    },
    /// 打开远程 shell 会话。
    RemoteShellOpen {
        /// 远程 shell 打开请求。
        #[serde(flatten)]
        request: RemoteShellOpenRequest,
    },
    /// 执行远程非交互任务。
    RemoteTaskRun {
        /// 远程任务请求。
        #[serde(flatten)]
        request: RemoteTaskRunRequest,
    },
    /// 执行远程网络探测。
    RemoteProbeRun {
        /// 远程探测请求。
        #[serde(flatten)]
        request: RemoteProbeRunRequest,
    },
}

/// 导出消息 listener，负责把 server 控制消息转换成内部命令。
#[derive(Debug, Clone)]
pub(crate) struct ServiceControlListener {
    /// 入站命令发送端。
    commands: InboundCommandSender,
}

impl ServiceControlListener {
    /// 创建控制消息监听器。
    pub(crate) fn new(commands: InboundCommandSender) -> Self {
        Self { commands }
    }

    /// 解析一条 server 文本消息。
    pub(crate) fn decode_message(msg: &str) -> anyhow::Result<Option<InboundCommandEnvelope>> {
        if let Ok(frame) = decode_server_frame(msg) {
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
                ServerPayload::RemoteProbeRun { request } => InboundCommand::RemoteProbeRun {
                    request: request.into(),
                },
            };
            return Ok(Some(InboundCommandEnvelope::with_response(
                command,
                frame.sequence,
            )));
        }

        let message: ServerControlMessage = serde_json::from_str(msg)?;

        let command = match message {
            ServerControlMessage::ConfigPatch { patch } => InboundCommand::ConfigPatch { patch },
            ServerControlMessage::CollectProcessesOnce { level, limit } => {
                InboundCommand::CollectProcessesOnce { level, limit }
            }
            ServerControlMessage::CollectSocketsOnce { level, limit } => {
                InboundCommand::CollectSocketsOnce { level, limit }
            }
            ServerControlMessage::RemoteShellOpen { request } => InboundCommand::RemoteShellOpen {
                request,
                stream_export_config: None,
            },
            ServerControlMessage::RemoteTaskRun { request } => {
                InboundCommand::RemoteTaskRun { request }
            }
            ServerControlMessage::RemoteProbeRun { request } => {
                InboundCommand::RemoteProbeRun { request }
            }
        };

        Ok(Some(InboundCommandEnvelope::without_response(command)))
    }

    /// 解析并投递一条 server 文本消息。
    pub(crate) async fn handle_message(&self, msg: &str) -> anyhow::Result<()> {
        if let Some(command) = Self::decode_message(msg)? {
            self.commands
                .send(command)
                .await
                .map_err(|_| anyhow::anyhow!("inbound command channel is closed"))?;

            tracing::debug!("server control command queued");
        }
        Ok(())
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
