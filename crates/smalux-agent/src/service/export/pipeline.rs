//! export pipeline 构建和重建。

use super::super::control::SmaluxControlHandler;
use super::super::inbound::InboundCommandSender;
use super::delivery::{DeliveryState, delivery_states_from_plan};
use crate::config::ConfigManager;
use crate::config::model::{ExportConfig, ExportFormat, OutboundConfig};
use crate::export::{
    ExportLogOptions, ExportRouter, InboundProtocolHandler, TransportEventReceiver, TransportHub,
    build_komari_inbound_handler, build_protocol_adapter, transport_event_channel,
};
use tokio::time::sleep;

/// 已连接导出 pipeline 的运行时状态。
///
/// 这个结构只在 export supervisor 内部流转，代表“当前 adapter + transport hub +
/// delivery 调度状态”的一整套连接。export 配置变更时整体重建，单纯 outbound 变更时
/// 只重建 `DeliveryState`，避免不必要断开 WebSocket。
pub(super) struct ConnectedExportPipeline {
    /// 当前 transport 连接集合。
    pub(super) transport_hub: TransportHub,
    /// transport worker 回传的发送结果事件。
    pub(super) transport_events: TransportEventReceiver,
    /// 当前导出 adapter router。
    pub(super) router: ExportRouter,
    /// 当前生效的 export 配置。
    pub(super) export_config: ExportConfig,
    /// 当前生效的 outbound 配置。
    pub(super) outbound_config: OutboundConfig,
    /// 当前 delivery 调度状态。
    pub(super) deliveries: Vec<DeliveryState>,
}

/// 使用当前配置创建 adapter、transport plan，并连接导出 transport。
pub(super) async fn connect_export_pipeline(
    config_manager: ConfigManager,
    inbound_commands: InboundCommandSender,
) -> anyhow::Result<ConnectedExportPipeline> {
    loop {
        let config = config_manager.current();
        let export_config = config.export.clone();
        let outbound_config = config.outbound.clone();
        let reconnect_interval = export_config.reconnect_interval;
        let format = export_config.format.as_str();
        let log_options = ExportLogOptions::new(config.log_payload, config.log_payload_max_bytes);
        let mut router =
            ExportRouter::new(build_protocol_adapter(export_config.format), log_options);
        let mut transport_plan = router.transport_plan(&export_config)?;
        transport_plan.apply_outbound_config(&outbound_config);
        let deliveries = delivery_states_from_plan(&transport_plan);
        let (transport_event_tx, transport_events) = transport_event_channel();
        let mut transport_hub = TransportHub::from_plan(transport_plan, transport_event_tx)?;
        let transport_summary = transport_hub.summary();

        // handler 绑定在 realtime report transport 上；server 控制消息从主长连接进入，
        // 再转换为统一入站命令队列。Komari 和 Smalux 自有协议只在 handler 层分叉。
        transport_hub
            .set_realtime_report_handler(build_export_inbound_handler(
                export_config.format,
                config_manager.clone(),
                inbound_commands.clone(),
            ))
            .await?;

        match transport_hub.connect_all().await {
            Ok(()) => {
                return Ok(ConnectedExportPipeline {
                    transport_hub,
                    transport_events,
                    router,
                    export_config,
                    outbound_config,
                    deliveries,
                });
            }
            Err(err) => {
                tracing::warn!(
                    transports = %transport_summary,
                    format = format,
                    error = %err,
                    reconnect_interval_ms = reconnect_interval.as_millis(),
                    "export transport connect failed; retrying"
                );
                close_transport_hub(&mut transport_hub).await;
                sleep(reconnect_interval).await;
            }
        }
    }
}

/// 重新读取 adapter delivery plan 并应用运行时 outbound 配置。
pub(super) fn rebuild_delivery_states(
    router: &mut ExportRouter,
    export_config: &ExportConfig,
    outbound_config: &OutboundConfig,
) -> anyhow::Result<Vec<DeliveryState>> {
    let mut transport_plan = router.transport_plan(export_config)?;
    transport_plan.apply_outbound_config(outbound_config);
    Ok(delivery_states_from_plan(&transport_plan))
}

/// 根据导出格式创建服务端入站协议处理器。
fn build_export_inbound_handler(
    format: ExportFormat,
    config_manager: ConfigManager,
    inbound_commands: InboundCommandSender,
) -> Box<dyn InboundProtocolHandler> {
    match format {
        ExportFormat::SmaluxJson => {
            Box::new(SmaluxControlHandler::new(config_manager, inbound_commands))
        }
        ExportFormat::Komari => build_komari_inbound_handler(config_manager, inbound_commands),
    }
}

/// 关闭导出 transport hub；关闭失败只记录日志，避免掩盖真正的 supervisor 退出原因。
pub(super) async fn close_transport_hub(transport_hub: &mut TransportHub) {
    if let Err(err) = transport_hub.close_all().await {
        tracing::warn!(error = ?err, "export transport hub close failed");
    }
}
