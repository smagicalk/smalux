//! Read-only CPU/memory projections shared by HTTP snapshots and WebSocket streams.
use std::{
    collections::{BTreeMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure};
use prost::Message;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, Order, QueryFilter, QueryOrder, QuerySelect,
    sea_query::NullOrdering,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smalux_protocol::agent::v1::{JobDefinition, TaskReport, task_definition, task_result};
use uuid::Uuid;

use crate::database::{
    ServerDatabase,
    entity::{agent, agent_job, task_report},
};

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_AGENTS: usize = 100;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Binding {
    agent_id: String,
    cpu_job_id: Option<String>,
    memory_job_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsConfig {
    bindings: BTreeMap<String, Binding>,
    revision: String,
    stale_ms: i64,
}

impl MetricsConfig {
    pub(crate) fn parse(json: &str, stale_seconds: u64) -> anyhow::Result<Self> {
        ensure!(json.len() <= 256 * 1024, "metric bindings exceed 256 KiB");
        ensure!(
            (1..=86400).contains(&stale_seconds),
            "metric stale threshold must be 1..86400 seconds"
        );
        let values: Vec<Binding> = serde_json::from_str(json)?;
        ensure!(values.len() <= 1000, "too many metric bindings");
        let mut bindings = BTreeMap::new();
        for binding in values {
            ensure!(valid_agent_id(&binding.agent_id), "invalid metric Agent id");
            for id in [&binding.cpu_job_id, &binding.memory_job_id]
                .into_iter()
                .flatten()
            {
                let uuid = Uuid::parse_str(id)?;
                ensure!(
                    uuid.to_string() == *id,
                    "metric Job id must be a canonical UUID"
                );
            }
            ensure!(
                binding.cpu_job_id.is_none() || binding.cpu_job_id != binding.memory_job_id,
                "CPU and memory must use distinct Jobs"
            );
            if bindings.insert(binding.agent_id.clone(), binding).is_some() {
                bail!("duplicate metric Agent binding");
            }
        }
        // Ordering and whitespace in the environment must not change the binding revision.
        let digest = Sha256::digest(serde_json::to_vec(&bindings)?);
        let revision =
            u128::from_be_bytes(digest[..16].try_into().expect("SHA256 prefix")).to_string();
        Ok(Self {
            bindings,
            revision,
            stale_ms: (stale_seconds * 1000) as i64,
        })
    }
}

fn valid_agent_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_graphic())
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MetricName {
    Cpu,
    Memory,
}
impl MetricName {
    fn result_kind(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
        }
    }
    fn task_kind(self) -> &'static str {
        match self {
            Self::Cpu => "smalux.collect.cpu.v1",
            Self::Memory => "smalux.collect.memory.v1",
        }
    }
    fn matches_task(self, definition: &JobDefinition) -> bool {
        matches!(
            (self, definition.task.as_ref().and_then(|t| t.task.as_ref())),
            (Self::Cpu, Some(task_definition::Task::Cpu(_)))
                | (Self::Memory, Some(task_definition::Task::Memory(_)))
        )
    }
}

fn all_metrics() -> Vec<MetricName> {
    vec![MetricName::Cpu, MetricName::Memory]
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LatestParams {
    pub agent_ids: Vec<String>,
    #[serde(default = "all_metrics")]
    pub metrics: Vec<MetricName>,
}
impl LatestParams {
    pub(crate) fn validate(&self) -> Result<(), MetricsError> {
        if self.agent_ids.is_empty()
            || self.agent_ids.len() > MAX_AGENTS
            || self.agent_ids.iter().any(|id| !valid_agent_id(id))
            || self.agent_ids.iter().collect::<HashSet<_>>().len() != self.agent_ids.len()
            || self.metrics.is_empty()
            || self.metrics.len() > 2
            || (self.metrics.len() == 2 && self.metrics[0] == self.metrics[1])
        {
            return Err(MetricsError::InvalidParams);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetricsError {
    InvalidParams,
    Forbidden,
    Internal,
}
impl From<sea_orm::DbErr> for MetricsError {
    fn from(_: sea_orm::DbErr) -> Self {
        Self::Internal
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MetricsLatest {
    agent_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu: Option<MetricSample<CpuMetrics>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory: Option<MetricSample<MemoryMetrics>>,
    binding_revision: String,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MetricSample<T> {
    value: Option<T>,
    sampled_at_ms: Option<i64>,
    received_at_ms: Option<i64>,
    quality: MetricQuality,
    source_job_id: Option<String>,
    source_job_revision: Option<String>,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
struct MetricQuality {
    state: &'static str,
    reason: Option<&'static str>,
}
impl MetricQuality {
    fn new(state: &'static str, reason: &'static str) -> Self {
        Self {
            state,
            reason: Some(reason),
        }
    }
}
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct CpuMetrics {
    cpu_usage_percent: f64,
    logical_cores: Option<u32>,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MemoryMetrics {
    used_bytes: Option<u64>,
    total_bytes: Option<u64>,
    used_percent: Option<f64>,
}

pub(crate) async fn authorize_agents(
    db: &ServerDatabase,
    config: &MetricsConfig,
    ids: &[String],
) -> Result<(), MetricsError> {
    authorize_on(db.connection(), config, ids).await
}
async fn authorize_on<C: ConnectionTrait>(
    conn: &C,
    config: &MetricsConfig,
    ids: &[String],
) -> Result<(), MetricsError> {
    if ids.is_empty()
        || ids.len() > MAX_AGENTS
        || ids.iter().any(|id| !config.bindings.contains_key(id))
    {
        return Err(MetricsError::Forbidden);
    }
    let rows = agent::Entity::find()
        .filter(agent::Column::AgentId.is_in(ids.iter().cloned()))
        .filter(agent::Column::Status.eq("active"))
        .filter(agent::Column::RevokedAt.is_null())
        .all(conn)
        .await?;
    if rows.len() != ids.len() {
        return Err(MetricsError::Forbidden);
    }
    Ok(())
}

pub(crate) async fn latest(
    db: &ServerDatabase,
    config: &MetricsConfig,
    params: &LatestParams,
) -> Result<Vec<MetricsLatest>, MetricsError> {
    params.validate()?;
    // All groups and the current catalog are read from one database snapshot.
    let txn = db
        .begin_snapshot_read()
        .await
        .map_err(|_| MetricsError::Internal)?;
    authorize_on(&txn, config, &params.agent_ids).await?;
    let mut items = Vec::with_capacity(params.agent_ids.len());
    for id in &params.agent_ids {
        let binding = config.bindings.get(id).ok_or(MetricsError::Forbidden)?;
        let cpu = if params.metrics.contains(&MetricName::Cpu) {
            Some(
                load_sample(
                    &txn,
                    id,
                    binding.cpu_job_id.as_deref(),
                    MetricName::Cpu,
                    config.stale_ms,
                    decode_cpu,
                )
                .await?,
            )
        } else {
            None
        };
        let memory = if params.metrics.contains(&MetricName::Memory) {
            Some(
                load_sample(
                    &txn,
                    id,
                    binding.memory_job_id.as_deref(),
                    MetricName::Memory,
                    config.stale_ms,
                    decode_memory,
                )
                .await?,
            )
        } else {
            None
        };
        items.push(MetricsLatest {
            agent_id: id.clone(),
            cpu,
            memory,
            binding_revision: config.revision.clone(),
        });
    }
    txn.commit().await?;
    Ok(items)
}

async fn load_sample<C: ConnectionTrait, T>(
    conn: &C,
    agent_id: &str,
    job_id: Option<&str>,
    metric: MetricName,
    stale_ms: i64,
    decode: fn(task_result::Result) -> Result<T, MetricQuality>,
) -> Result<MetricSample<T>, MetricsError> {
    let mut sample = MetricSample {
        value: None,
        sampled_at_ms: None,
        received_at_ms: None,
        quality: MetricQuality::new("unknown", "notConfigured"),
        source_job_id: job_id.map(str::to_owned),
        source_job_revision: None,
    };
    let Some(job_id) = job_id else {
        return Ok(sample);
    };
    let job_bytes = Uuid::parse_str(job_id)
        .map_err(|_| MetricsError::Internal)?
        .as_bytes()
        .to_vec();
    sample.quality = MetricQuality::new("unavailable", "sourceUnavailable");
    let Some(job) = agent_job::Entity::find()
        .filter(agent_job::Column::AgentId.eq(agent_id))
        .filter(agent_job::Column::JobId.eq(job_bytes.clone()))
        .one(conn)
        .await?
    else {
        return Ok(sample);
    };
    if job.revision > 0 {
        sample.source_job_revision = Some(job.revision.to_string());
    }
    let definition = JobDefinition::decode(job.definition.as_slice()).ok();
    if !job.enabled
        || job.revision <= 0
        || job.task_kind != metric.task_kind()
        || !definition.is_some_and(|d| {
            d.enabled
                && d.job_id == job_bytes
                && d.revision == job.revision as u64
                && metric.matches_task(&d)
        })
    {
        return Ok(sample);
    }
    sample.quality = MetricQuality::new("unknown", "noSamples");
    // Select by sample time, not receipt time: a late old run must never win.
    let Some(row) = task_report::Entity::find()
        .filter(task_report::Column::AgentId.eq(agent_id))
        .filter(task_report::Column::JobId.eq(job_bytes))
        .filter(task_report::Column::JobRevision.eq(job.revision))
        .filter(task_report::Column::ResultKind.eq(metric.result_kind()))
        .order_by_with_nulls(
            task_report::Column::StartedAt,
            Order::Desc,
            NullOrdering::Last,
        )
        .order_by_desc(task_report::Column::ReceivedAt)
        .order_by_desc(task_report::Column::ReportId)
        .limit(1)
        .one(conn)
        .await?
    else {
        return Ok(sample);
    };
    // Observe the clock after the read: a report committed while awaiting SQL must
    // not be classified as future merely because the request started earlier.
    let now_us = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| MetricsError::Internal)?
            .as_micros(),
    )
    .map_err(|_| MetricsError::Internal)?;
    sample.sampled_at_ms = row.started_at.and_then(safe_millis);
    sample.received_at_ms = safe_millis(row.received_at);
    sample.quality = MetricQuality::new("unavailable", "invalidReport");
    let report = match TaskReport::decode(row.payload.as_slice()) {
        Ok(v) => v,
        Err(_) => return Ok(sample),
    };
    if report.job_id != row.job_id
        || report.job_revision != job.revision as u64
        || report.run_id != row.run_id
        || report.attempt as i64 != row.attempt as i64
        || timestamp_us(report.started_at.as_ref()) != Some(row.started_at)
    {
        return Ok(sample);
    }
    if row.received_at > now_us
        || sample.received_at_ms.is_none()
        || row
            .started_at
            .is_some_and(|t| t > now_us || safe_millis(t).is_none())
    {
        sample.quality = MetricQuality::new("unavailable", "invalidTimestamp");
        return Ok(sample);
    }
    let Some(started_at) = row.started_at else {
        sample.quality = MetricQuality::new("unknown", "missingSampleTime");
        return Ok(sample);
    };
    let Some(result) = report.result.and_then(|r| r.result) else {
        return Ok(sample);
    };
    match decode(result) {
        Ok(value) => {
            sample.value = Some(value);
            sample.quality = if (now_us - started_at) / 1000 > stale_ms {
                MetricQuality::new("stale", "sampleExpired")
            } else {
                MetricQuality {
                    state: "valid",
                    reason: None,
                }
            };
        }
        Err(quality) => sample.quality = quality,
    }
    Ok(sample)
}

fn safe_millis(us: i64) -> Option<i64> {
    (us >= 0 && us / 1000 <= MAX_SAFE_INTEGER as i64).then_some(us / 1000)
}
fn timestamp_us(ts: Option<&prost_types::Timestamp>) -> Option<Option<i64>> {
    let Some(ts) = ts else { return Some(None) };
    if !(0..1_000_000_000).contains(&ts.nanos) {
        return None;
    }
    ts.seconds
        .checked_mul(1_000_000)?
        .checked_add(i64::from(ts.nanos) / 1000)
        .map(Some)
}
fn percent(value: f64) -> bool {
    value.is_finite() && (0.0..=100.0).contains(&value)
}
fn decode_cpu(result: task_result::Result) -> Result<CpuMetrics, MetricQuality> {
    let task_result::Result::Cpu(cpu) = result else {
        return Err(MetricQuality::new("unavailable", "invalidReport"));
    };
    if !percent(f64::from(cpu.global_usage_percent)) {
        return Err(MetricQuality::new("unavailable", "invalidValue"));
    }
    if !cpu.warmed_up {
        return Err(MetricQuality::new("warmingUp", "firstSample"));
    }
    Ok(CpuMetrics {
        cpu_usage_percent: f64::from(cpu.global_usage_percent),
        logical_cores: (cpu.logical_cpu_count > 0).then_some(cpu.logical_cpu_count),
    })
}
fn decode_memory(result: task_result::Result) -> Result<MemoryMetrics, MetricQuality> {
    let task_result::Result::Memory(memory) = result else {
        return Err(MetricQuality::new("unavailable", "invalidReport"));
    };
    if memory.total_bytes == 0
        || memory.used_bytes > memory.total_bytes
        || memory.total_bytes > MAX_SAFE_INTEGER
        || !percent(memory.usage_percent)
    {
        return Err(MetricQuality::new("unavailable", "invalidValue"));
    }
    Ok(MemoryMetrics {
        used_bytes: Some(memory.used_bytes),
        total_bytes: Some(memory.total_bytes),
        used_percent: Some(memory.usage_percent),
    })
}

#[cfg(test)]
mod tests;
