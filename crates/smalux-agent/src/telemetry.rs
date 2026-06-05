//! Agent 监控数据状态和上报聚合模块。
//!
//! 这里保存 agent 内部统一的 telemetry 模型。采集层只负责产生数据，导出层只负责发送数据，
//! 中间的状态维护、snapshot/delta/heartbeat 决策都集中在这里。

mod aggregator;
mod event;
mod state;

pub(crate) use aggregator::TelemetryAggregator;
pub(crate) use event::{ReportEvent, TelemetryUpdate};
pub(crate) use state::LatestTelemetry;
