//! 导出 plan、job 和 transport 请求模型。

use super::{http, ws};
use crate::config::model::{JobConfig, JobsConfig};
use std::time::Duration;

/// 导出 transport 标识。
///
/// 当前实时上报和低频基础信息各自有稳定 transport ID。
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum TransportId {
    /// 实时上报通道。
    RealtimeReport,
    /// 低频基础信息通道。
    BasicInfo,
}

impl TransportId {
    /// 返回稳定日志名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeReport => "realtime_report",
            Self::BasicInfo => "basic_info",
        }
    }
}

/// 单个 transport 的启动规格。
pub(crate) enum TransportSpec {
    /// WebSocket 长连接规格。
    WebSocket {
        /// transport ID。
        id: TransportId,
        /// WebSocket 配置。
        config: ws::WebSocketConfig,
        /// 是否在 pipeline 连接阶段立即建立 WebSocket。
        connect_on_start: bool,
    },
    /// HTTP 短请求规格。
    Http {
        /// transport ID。
        id: TransportId,
        /// HTTP transport 配置。
        config: http::HttpConfig,
    },
}

impl TransportSpec {
    /// 返回 transport ID。
    pub(crate) fn id(&self) -> TransportId {
        match self {
            Self::WebSocket { id, .. } => *id,
            Self::Http { id, .. } => *id,
        }
    }
}

/// 导出 job ID。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportJobId {
    /// 实时上报 job。
    RealtimeReport,
    /// Komari basic info 低频上报 job。
    BasicInfo,
    /// 远程任务结果即时回传。
    RemoteTaskResult,
    /// 远程网络探测结果即时回传。
    RemoteProbeResult,
    /// 控制命令确认即时回传。
    ControlAck,
    /// 控制命令错误即时回传。
    ControlError,
}

impl ExportJobId {
    /// 返回日志使用的 job 名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeReport => "realtime_report",
            Self::BasicInfo => "basic_info",
            Self::RemoteTaskResult => "remote_task_result",
            Self::RemoteProbeResult => "remote_probe_result",
            Self::ControlAck => "control_ack",
            Self::ControlError => "control_error",
        }
    }
}

/// 导出 job 触发方式。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportJobTrigger {
    /// 最新 report 更新后触发。
    OnLatestReport,
    /// 按固定间隔触发。
    Interval(Duration),
}

/// 导出 job 失败处理策略。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportJobFailurePolicy {
    /// 失败后重建导出 pipeline，适合长连接实时上报。
    ReconnectPipeline,
    /// 失败只记录日志，等待下一次调度，适合低频辅助 HTTP 请求。
    LogAndContinue,
}

/// adapter 需要运行的导出 job。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ExportJobSpec {
    /// job ID。
    pub(crate) id: ExportJobId,
    /// job 触发方式。
    pub(crate) trigger: ExportJobTrigger,
    /// 是否在拿到第一份 report 后立即运行。
    pub(crate) run_on_start: bool,
    /// 失败处理策略。
    pub(crate) failure_policy: ExportJobFailurePolicy,
}

impl ExportJobSpec {
    /// 创建跟随最新 report 的实时上报 job。
    pub(crate) fn on_latest_report(id: ExportJobId) -> Self {
        Self {
            id,
            trigger: ExportJobTrigger::OnLatestReport,
            run_on_start: true,
            failure_policy: ExportJobFailurePolicy::ReconnectPipeline,
        }
    }

    /// 创建固定间隔上报 job。
    pub(crate) fn interval(
        id: ExportJobId,
        interval: Duration,
        failure_policy: ExportJobFailurePolicy,
    ) -> Self {
        Self {
            id,
            trigger: ExportJobTrigger::Interval(interval),
            run_on_start: true,
            failure_policy,
        }
    }
}

/// adapter 需要启动的 transport 和 job 集合。
///
/// 这个 plan 是导出层的扩展点：同一份内部 report 可以被不同 adapter 拆成不同 transport
/// 和 job，例如 Smalux 默认只用实时 WebSocket，而 Komari 同时需要 WebSocket report 和
/// HTTP basic info。
pub(crate) struct TransportPlan {
    /// 所有 transport 规格。
    pub(crate) transports: Vec<TransportSpec>,
    /// 所有导出 job 规格。
    pub(crate) jobs: Vec<ExportJobSpec>,
}

impl TransportPlan {
    /// 创建默认只有实时上报 job 的 plan。
    pub(crate) fn new(transports: Vec<TransportSpec>) -> Self {
        Self {
            transports,
            jobs: vec![ExportJobSpec::on_latest_report(ExportJobId::RealtimeReport)],
        }
    }

    /// 创建带自定义 job 的 plan。
    pub(crate) fn with_jobs(transports: Vec<TransportSpec>, jobs: Vec<ExportJobSpec>) -> Self {
        Self { transports, jobs }
    }

    /// 返回 job 列表。
    pub(crate) fn jobs(&self) -> &[ExportJobSpec] {
        &self.jobs
    }

    /// 消费 plan，返回 transport 规格。
    pub(crate) fn into_transports(self) -> Vec<TransportSpec> {
        self.transports
    }

    /// 应用运行时 job 配置，禁用的 job 会从 plan 中移除。
    pub(crate) fn apply_job_config(&mut self, config: &JobsConfig) {
        self.jobs.retain_mut(|job| {
            let Some(job_config) = match_job_config(config, job.id) else {
                return true;
            };
            if !job_config.enabled {
                tracing::info!(job = job.id.as_str(), "export job disabled");
                return false;
            }

            job.trigger = ExportJobTrigger::Interval(job_config.interval);
            job.run_on_start = job_config.run_on_start;
            true
        });
    }
}

/// 根据 job ID 读取对应配置。
fn match_job_config(config: &JobsConfig, job_id: ExportJobId) -> Option<&JobConfig> {
    match job_id {
        ExportJobId::RealtimeReport => Some(&config.realtime_report),
        ExportJobId::BasicInfo => Some(&config.basic_info),
        ExportJobId::RemoteTaskResult
        | ExportJobId::RemoteProbeResult
        | ExportJobId::ControlAck
        | ExportJobId::ControlError => None,
    }
}

/// adapter 编码后的单次发送请求。
#[derive(Debug)]
pub(crate) enum TransportRequest {
    /// 发送 WebSocket 文本消息。
    WebSocketText {
        /// 目标 transport。
        transport: TransportId,
        /// 文本内容。
        body: String,
    },
    /// 发送 WebSocket 二进制消息。
    WebSocketBinary {
        /// 目标 transport。
        transport: TransportId,
        /// 业务序号。
        sequence: u64,
        /// 二进制内容。
        body: Vec<u8>,
    },
    /// 发送 HTTP JSON 请求。
    HttpJson {
        /// 目标 transport。
        transport: TransportId,
        /// HTTP method。
        method: http::HttpMethod,
        /// 完整请求 URL。
        url: String,
        /// JSON body。
        body: serde_json::Value,
    },
}

impl TransportRequest {
    /// 返回目标 transport。
    pub(crate) fn transport_id(&self) -> TransportId {
        match self {
            Self::WebSocketText { transport, .. } | Self::WebSocketBinary { transport, .. } => {
                *transport
            }
            Self::HttpJson { transport, .. } => *transport,
        }
    }
}
