//! 通用远程 job 协议模型。
//!
//! `job_apply` 是 server 管理 agent 侧长期/周期任务的统一入口。首版只实现
//! `kind=probe`，但外层 operation、generation、job_id、interval 和 result 语义会被后续
//! backup、health_check 等 job kind 复用。

use super::probe::{RemoteProbeId, RemoteProbeResult, RemoteProbeType};
use serde::{Deserialize, Serialize};

/// 通用远程 job 类型。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteJobKind {
    /// 网络探测 job。
    Probe,
}

impl RemoteJobKind {
    /// 返回稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Probe => "probe",
        }
    }
}

/// 通用远程 job 操作。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteJobOperation {
    /// 立即运行一次，不修改 agent 本地 job 表。
    Once,
    /// 用 server 给出的列表整组替换 agent 本地 job 表。
    Replace,
    /// 增量新增、修改或删除 agent 本地 job。
    Patch,
}

/// 通用一次性 job 运行请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteJobRunRequest {
    /// 一次性网络探测。
    Probe {
        /// server 侧生成的请求 ID。
        request_id: RemoteProbeId,
        /// server 侧业务探测点 ID；用于把结果关联回固定探测点。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        point_id: Option<RemoteProbeId>,
        /// 探测类型。
        probe_type: RemoteProbeType,
        /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
        target: String,
        /// 本次请求的超时；缺省时使用 agent 默认值。
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "humantime_serde::option"
        )]
        timeout: Option<std::time::Duration>,
    },
}

impl RemoteJobRunRequest {
    /// 返回 job 类型。
    pub fn kind(&self) -> RemoteJobKind {
        match self {
            Self::Probe { .. } => RemoteJobKind::Probe,
        }
    }
}

/// 通用持续 job 定义。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteJobSpec {
    /// 持续网络探测 job。
    Probe {
        /// server 侧生成的 job ID。
        job_id: String,
        /// server 侧业务探测点 ID；用于把持续任务结果关联回固定探测点。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        point_id: Option<RemoteProbeId>,
        /// 是否启用该 job。
        enabled: bool,
        /// 探测类型。
        probe_type: RemoteProbeType,
        /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
        target: String,
        /// 持续运行间隔。
        #[serde(with = "humantime_serde")]
        interval: std::time::Duration,
        /// 本次 job 的超时；缺省时使用 agent 默认值。
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "humantime_serde::option"
        )]
        timeout: Option<std::time::Duration>,
    },
}

impl RemoteJobSpec {
    /// 返回 job 类型。
    pub fn kind(&self) -> RemoteJobKind {
        match self {
            Self::Probe { .. } => RemoteJobKind::Probe,
        }
    }

    /// 返回 job ID。
    pub fn job_id(&self) -> &str {
        match self {
            Self::Probe { job_id, .. } => job_id,
        }
    }
}

/// 通用远程 job 应用请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteJobApplyRequest {
    /// 操作类型。
    pub operation: RemoteJobOperation,
    /// 请求代际，用于 server 乱序保护。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    /// 一次性运行请求。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<RemoteJobRunRequest>,
    /// 持续 job 整组替换列表。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<RemoteJobSpec>,
    /// 持续 job 增量 upsert 列表。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upsert_jobs: Vec<RemoteJobSpec>,
    /// 持续 job 删除列表。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_job_ids: Vec<String>,
}

/// 通用远程 job 结果。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteJobResult {
    /// 网络探测 job 结果。
    Probe {
        /// 探测执行结果。
        result: RemoteProbeResult,
    },
}

impl RemoteJobResult {
    /// 从网络探测结果构造通用 job 结果。
    pub fn probe(result: RemoteProbeResult) -> Self {
        Self::Probe { result }
    }

    /// 返回 job 类型。
    pub fn kind(&self) -> RemoteJobKind {
        match self {
            Self::Probe { .. } => RemoteJobKind::Probe,
        }
    }

    /// 如果当前结果是 probe，返回内部探测结果。
    pub fn as_probe(&self) -> Option<&RemoteProbeResult> {
        match self {
            Self::Probe { result } => Some(result),
        }
    }

    /// 返回日志友好的结果关联 ID。
    pub fn display_id(&self) -> String {
        match self {
            Self::Probe { result } => result.display_id(),
        }
    }
}
