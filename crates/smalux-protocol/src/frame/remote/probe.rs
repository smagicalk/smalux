//! 远程网络探测协议模型。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 远程探测关联 ID，只接受字符串或整数，避免协议层出现任意 JSON 结构。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RemoteProbeId {
    /// 字符串 ID。
    String(String),
    /// 整数 ID。
    Integer(i64),
}

impl RemoteProbeId {
    /// 返回日志友好的稳定字符串。
    pub fn display(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
        }
    }
}

impl std::fmt::Display for RemoteProbeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => formatter.write_str(value),
            Self::Integer(value) => write!(formatter, "{value}"),
        }
    }
}

impl From<String> for RemoteProbeId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for RemoteProbeId {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for RemoteProbeId {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<i32> for RemoteProbeId {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl TryFrom<u64> for RemoteProbeId {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let value = i64::try_from(value).map_err(|_error| {
            anyhow::anyhow!("remote probe id u64 must fit into signed 64-bit integer")
        })?;
        Ok(Self::Integer(value))
    }
}

impl From<u32> for RemoteProbeId {
    fn from(value: u32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl TryFrom<Value> for RemoteProbeId {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(value) => Ok(Self::String(value)),
            Value::Number(value) => value.as_i64().map(Self::Integer).ok_or_else(|| {
                anyhow::anyhow!("remote probe id number must fit into signed 64-bit integer")
            }),
            _ => anyhow::bail!("remote probe id must be a string or integer"),
        }
    }
}

impl From<RemoteProbeId> for Value {
    fn from(value: RemoteProbeId) -> Self {
        match value {
            RemoteProbeId::String(value) => Value::String(value),
            RemoteProbeId::Integer(value) => Value::from(value),
        }
    }
}

/// 远程网络探测类型。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProbeType {
    /// TCP 连接耗时探测。
    Tcp,
    /// HTTP/HTTPS 请求耗时探测。
    Http,
    /// ICMP 探测。
    Icmp,
}

impl RemoteProbeType {
    /// 返回稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Http => "http",
            Self::Icmp => "icmp",
        }
    }
}

/// 远程探测一次性请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeOnceRequest {
    /// server 侧生成的请求 ID。
    #[serde(alias = "task_id")]
    pub request_id: RemoteProbeId,
    /// server 侧业务探测点 ID；用于把一次执行结果关联回固定探测点。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<RemoteProbeId>,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
    pub target: String,
    /// 本次请求的超时；缺省时使用 agent 默认值。
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde::option"
    )]
    pub timeout: Option<std::time::Duration>,
}

/// 远程探测持续任务。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeJob {
    /// server 侧生成的任务 ID。
    pub job_id: String,
    /// server 侧业务探测点 ID；用于把持续任务结果关联回固定探测点。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<RemoteProbeId>,
    /// 是否启用该任务。
    pub enabled: bool,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
    pub target: String,
    /// 持续探测间隔。
    #[serde(with = "humantime_serde")]
    pub interval: std::time::Duration,
    /// 本次任务的超时；缺省时使用 agent 默认值。
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde::option"
    )]
    pub timeout: Option<std::time::Duration>,
}

/// 远程探测操作。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProbeOperation {
    /// 立即执行一次。
    Once,
    /// 整组替换持续任务。
    Replace,
    /// 增量更新持续任务。
    Patch,
}

/// 远程探测应用请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeApplyRequest {
    /// 操作类型。
    pub operation: RemoteProbeOperation,
    /// 请求代际，用于 server 乱序保护。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    /// 一次性探测请求。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<RemoteProbeOnceRequest>,
    /// 持续任务整组替换列表。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<RemoteProbeJob>,
    /// 持续任务增量 upsert 列表。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upsert_jobs: Vec<RemoteProbeJob>,
    /// 持续任务删除列表。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_job_ids: Vec<String>,
}

/// 远程探测结果来源。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProbeResultSource {
    /// 一次性探测请求。
    Once,
    /// 持续探测任务。
    Job,
}

/// 远程探测结果状态。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProbeResultStatus {
    /// 探测成功。
    Success,
    /// 已执行但探测失败、超时或类型未实现。
    Failed,
    /// agent 本地策略拒绝执行，例如未启用或限频。
    Rejected,
}

/// 远程探测结果。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeResult {
    /// agent 为本次探测运行生成的唯一 ID。
    pub run_id: String,
    /// 结果来源，用于区分一次性请求和持续任务。
    pub source: RemoteProbeResultSource,
    /// server 侧业务探测点 ID；由下发请求原样带回。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_id: Option<RemoteProbeId>,
    /// 一次性请求 ID；`source=once` 时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RemoteProbeId>,
    /// 持续任务 ID；`source=job` 时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标。
    pub target: String,
    /// 探测结果状态。
    pub status: RemoteProbeResultStatus,
    /// 成功探测的延迟，单位毫秒；失败或拒绝时为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// 开始时间，Unix 时间戳，单位秒。
    pub started_at: u64,
    /// 完成时间，Unix 时间戳，单位秒。
    pub finished_at: u64,
    /// 执行耗时，单位毫秒。
    pub duration_ms: u64,
    /// 失败、禁用或限频原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl RemoteProbeResult {
    /// 返回日志和兼容协议使用的结果关联 ID。
    pub fn display_id(&self) -> String {
        if let Some(point_id) = &self.point_id {
            return point_id.display();
        }
        if let Some(request_id) = &self.request_id {
            return request_id.display();
        }
        self.job_id.clone().unwrap_or_default()
    }

    /// 返回 Komari ping_result 需要的 task_id。
    pub fn komari_task_id(&self) -> RemoteProbeId {
        // 正常结果必须携带 request_id 或 job_id；空字符串只作为异常数据的兼容兜底，
        // 避免第三方适配层在处理脏数据时 panic。
        self.request_id
            .clone()
            .or_else(|| self.job_id.clone().map(RemoteProbeId::from))
            .unwrap_or_else(|| RemoteProbeId::from(""))
    }

    /// 返回 Komari ping_result 的 value 语义，成功为延迟，失败或拒绝为 -1。
    pub fn komari_value(&self) -> i64 {
        self.latency_ms
            .map(|value| value.min(i64::MAX as u64) as i64)
            .unwrap_or(-1)
    }
}

#[cfg(test)]
mod tests {
    //! 远程探测协议模型测试。

    use super::*;

    /// 验证 u64 在 i64 范围内可以无损转换为整数任务 ID。
    #[test]
    fn remote_probe_id_accepts_u64_inside_i64_range() {
        let probe_id = RemoteProbeId::try_from(i64::MAX as u64).unwrap();

        assert_eq!(probe_id, RemoteProbeId::Integer(i64::MAX));
    }

    /// 验证 u64 超出 i64 范围时不会被静默截断。
    #[test]
    fn remote_probe_id_rejects_u64_above_i64_max() {
        let error = RemoteProbeId::try_from(i64::MAX as u64 + 1).unwrap_err();

        assert!(error.to_string().contains("signed 64-bit"));
    }
}
