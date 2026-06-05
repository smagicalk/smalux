//! Server 控制消息处理。
//!
//! 入站控制消息有两条路径：优先解析稳定的 `smalux_protocol::ServerFrame`，这样可以
//! 保留 server `sequence` 并回传 `ack/error`；如果不是 `ServerFrame`，再按 agent
//! 当前兼容的 raw control JSON 解析，用于 `config_patch`、远程 task 等还没有提升到
//! `smalux-protocol` 的命令。

use crate::config::model::AgentConfigPatch;
use crate::export::{ExportInboundMessage, ExportMessageListener, inbound_message_into_string};
use crate::service::{
    probe::RemoteProbeRunRequest, shell::RemoteShellOpenRequest, task::RemoteTaskRunRequest,
};
use serde::Deserialize;
use smalux_core::model::info::MetricLevel;
use smalux_protocol::{ServerPayload, decode_server_frame};
use std::future::Future;
use std::pin::Pin;

use super::inbound::{InboundCommand, InboundCommandEnvelope, InboundCommandSender};

/// 服务端下发给 agent 的 raw 控制消息。
///
/// 这些消息可以放在 `binary_plain` 的 `PlainData` payload、`secure_psk` 解密后的
/// `SecureData` payload，或本地调试用 WebSocket text 中。它们没有协议级
/// `sequence`，因此调度后不会自动产生控制层 `ack/error`。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerControlMessage {
    /// 服务配置增量更新。
    ConfigPatch {
        /// 需要应用的配置 patch。
        patch: Box<AgentConfigPatch>,
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

    /// 解析一条 server 控制消息。
    ///
    /// WebSocket transport 已经在进入 listener 前完成 wire 解包和可选解密，这里收到的
    /// 始终是 UTF-8 JSON 字符串。先尝试 `ServerFrame` 是为了保留 sequence；fallback
    /// raw JSON 主要用于当前还没进入 `smalux-protocol` 的兼容命令。
    pub(crate) fn decode_message(msg: &str) -> anyhow::Result<Option<InboundCommandEnvelope>> {
        // 标准 ServerFrame 成功解析时，保留 server sequence，后续调度器会据此回 ack/error。
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

        // 兼容 raw control JSON 没有 sequence，只能执行本地调度和日志，不回控制 ack。
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
