//! Service 内部消息层。
//!
//! 协议 listener、入站命令和出站事件都集中在这里。外部 transport 只需要把
//! server 消息转换成入站命令，业务执行结果统一进入出站事件队列。

use crate::telemetry::TelemetryUpdate;
use tokio::sync::mpsc;

pub(crate) mod inbound;
pub(crate) mod listener;
pub(crate) mod outbound;

/// telemetry update 队列容量。
const TELEMETRY_UPDATE_QUEUE_CAPACITY: usize = 256;

/// telemetry 更新发送端。
pub(crate) type TelemetryUpdateSender = mpsc::Sender<TelemetryUpdate>;
/// telemetry 更新接收端。
pub(crate) type TelemetryUpdateReceiver = mpsc::Receiver<TelemetryUpdate>;

/// 创建 telemetry 更新队列。
pub(crate) fn telemetry_update_channel() -> (TelemetryUpdateSender, TelemetryUpdateReceiver) {
    mpsc::channel(TELEMETRY_UPDATE_QUEUE_CAPACITY)
}
