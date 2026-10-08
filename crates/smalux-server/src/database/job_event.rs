//! Agent Scheduler 事件的数据库 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use super::{
    DatabaseError, ServerDatabase,
    entity::{agent_job_version, job_event},
};
use prost::Message;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    sea_query::OnConflict,
};
use smalux_protocol::agent::v1::{JobEvent, JobEventKind};

/// 用于管理查询的 JobEvent 元数据与原始 Proto payload。
#[derive(Clone, Debug)]
pub struct JobEventRecord {
    pub event_id: String,
    pub agent_id: String,
    /// Agent 进程实例 UUID；同一实例重连不变，重启后变化。
    pub instance_id: Vec<u8>,
    pub sequence: i64,
    pub kind: i32,
    pub job_id: Vec<u8>,
    pub revision: i64,
    pub run_id: Vec<u8>,
    pub attempt: i32,
    pub emitted_at: i64,
    pub message: String,
    pub will_retry: bool,
    /// 该事件到达时是否发现同一进程实例的序号缺口。
    pub gap_detected: bool,
    pub payload: Vec<u8>,
}

/// JobEvent 分页游标；`emitted_at` 使用数据库存储单位微秒。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobEventCursor {
    pub emitted_at: i64,
    pub event_id: String,
}

/// Server 接收并保存一条 Agent Scheduler 事件。
impl ServerDatabase {
    pub async fn append_job_event(
        &self,
        agent_id: &str,
        event: &JobEvent,
    ) -> Result<String, DatabaseError> {
        if agent_id.is_empty() {
            return Err(DatabaseError::InvalidAgentRegistration(
                "agent id is required for Job event".to_owned(),
            ));
        }
        if agent_id.len() > 64 {
            return Err(DatabaseError::InvalidJobEvent(
                "agent id is too long for Job event identity".to_owned(),
            ));
        }
        if event.instance_id.len() != 16 || event.sequence == 0 {
            return Err(DatabaseError::InvalidJobEvent(
                "Job event instance id must contain 16 bytes and sequence must be positive"
                    .to_owned(),
            ));
        }
        let sequence = i64::try_from(event.sequence).map_err(|_| {
            DatabaseError::InvalidJobEvent("Job event sequence exceeds i64".to_owned())
        })?;
        let revision = i64::try_from(event.revision).map_err(|_| {
            DatabaseError::InvalidJobEvent("Job event revision exceeds i64".to_owned())
        })?;
        let attempt = i32::try_from(event.attempt).map_err(|_| {
            DatabaseError::InvalidJobEvent("Job event attempt exceeds i32".to_owned())
        })?;
        let duration_ms = i64::try_from(event.duration_ms).map_err(|_| {
            DatabaseError::InvalidJobEvent("Job event duration exceeds i64".to_owned())
        })?;
        let pending_count = i32::try_from(event.pending_count).map_err(|_| {
            DatabaseError::InvalidJobEvent("Job event pending count exceeds i32".to_owned())
        })?;
        validate_job_event_attribution(self, agent_id, event).await?;
        let existing = job_event::Entity::find()
            .filter(job_event::Column::AgentId.eq(agent_id))
            .filter(job_event::Column::InstanceId.eq(event.instance_id.clone()))
            .filter(job_event::Column::Sequence.eq(sequence))
            .one(self.connection())
            .await?;
        // 事件 ID 直接由幂等身份派生；并发重复插入时，即使数据库返回“未插入”，
        // 调用方仍能得到与已存在行相同的 ID，不依赖随机 UUID 或竞态查询结果。
        let event_id = format!(
            "{agent_id}:{}:{}",
            encode_hex(&event.instance_id),
            event.sequence
        );
        let payload = event.encode_to_vec();
        if let Some(existing) = existing {
            if existing.payload != payload {
                return Err(DatabaseError::InvalidJobEvent(
                    "duplicate Job event identity has different payload".to_owned(),
                ));
            }
            return Ok(existing.event_id);
        }
        let previous = job_event::Entity::find()
            .filter(job_event::Column::AgentId.eq(agent_id))
            .filter(job_event::Column::InstanceId.eq(event.instance_id.clone()))
            .order_by_desc(job_event::Column::Sequence)
            .one(self.connection())
            .await?;
        let gap_detected = previous
            .as_ref()
            .map(|value| sequence > value.sequence.saturating_add(1))
            .unwrap_or(event.sequence > 1);
        if gap_detected {
            tracing::warn!(
                agent_id,
                sequence = event.sequence,
                previous_sequence = previous.as_ref().map(|value| value.sequence),
                "Agent Job event stream has a sequence gap"
            );
        }
        let emitted_at = event
            .emitted_at
            .as_ref()
            .map(|value| value.seconds.saturating_mul(1_000_000) + i64::from(value.nanos) / 1_000)
            .unwrap_or_else(unix_micros);
        let run_at = event
            .run_at
            .as_ref()
            .map(|value| value.seconds.saturating_mul(1_000_000) + i64::from(value.nanos) / 1_000);
        let insert = job_event::Entity::insert(job_event::ActiveModel {
            event_id: Set(event_id.clone()),
            agent_id: Set(agent_id.to_owned()),
            instance_id: Set(event.instance_id.clone()),
            sequence: Set(sequence),
            kind: Set(event.kind),
            job_id: Set(event.job_id.clone()),
            revision: Set(revision),
            run_id: Set(event.run_id.clone()),
            attempt: Set(attempt),
            emitted_at: Set(emitted_at),
            run_at: Set(run_at),
            duration_ms: Set(duration_ms),
            message: Set(event.message.clone()),
            will_retry: Set(event.will_retry),
            pending_count: Set(pending_count),
            gap_detected: Set(gap_detected),
            payload: Set(payload),
        })
        .on_conflict(
            OnConflict::columns([
                job_event::Column::AgentId,
                job_event::Column::InstanceId,
                job_event::Column::Sequence,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec(self.connection())
        .await;
        match insert {
            Ok(_) => Ok(event_id),
            Err(sea_orm::DbErr::RecordNotInserted) => {
                let Some(existing) = job_event::Entity::find()
                    .filter(job_event::Column::AgentId.eq(agent_id))
                    .filter(job_event::Column::InstanceId.eq(event.instance_id.clone()))
                    .filter(job_event::Column::Sequence.eq(sequence))
                    .one(self.connection())
                    .await?
                else {
                    return Err(DatabaseError::InvalidJobEvent(
                        "duplicate Job event insert did not produce a row".to_owned(),
                    ));
                };
                if existing.payload != event.encode_to_vec() {
                    return Err(DatabaseError::InvalidJobEvent(
                        "duplicate Job event identity has different payload".to_owned(),
                    ));
                }
                Ok(existing.event_id)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// 按接收时间倒序读取事件摘要；页面/CLI 可在此基础上增加过滤条件。
    pub async fn list_job_events(
        &self,
        agent_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<JobEventRecord>, DatabaseError> {
        let mut query = job_event::Entity::find();
        if let Some(agent_id) = agent_id {
            query = query.filter(job_event::Column::AgentId.eq(agent_id));
        }
        Ok(query
            .order_by_desc(job_event::Column::EmittedAt)
            .limit(limit.clamp(1, 500))
            .all(self.connection())
            .await?
            .into_iter()
            .map(|row| JobEventRecord {
                event_id: row.event_id,
                agent_id: row.agent_id,
                instance_id: row.instance_id,
                sequence: row.sequence,
                kind: row.kind,
                job_id: row.job_id,
                revision: row.revision,
                run_id: row.run_id,
                attempt: row.attempt,
                emitted_at: row.emitted_at,
                message: row.message,
                will_retry: row.will_retry,
                gap_detected: row.gap_detected,
                payload: row.payload,
            })
            .collect())
    }

    /// 按事件时间与 ID 稳定倒序分页；from/to 为毫秒时间戳，区间为 [from, to)。
    /// `after` 使用记录返回的微秒时间戳与 ID，查询结果只包含游标之后的更旧记录。
    pub async fn query_job_events(
        &self,
        agent_id: Option<&str>,
        job_id: Option<&[u8]>,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        after: Option<&JobEventCursor>,
        limit: u64,
    ) -> Result<Vec<JobEventRecord>, DatabaseError> {
        let mut query = job_event::Entity::find();
        if let Some(agent_id) = agent_id {
            query = query.filter(job_event::Column::AgentId.eq(agent_id));
        }
        if let Some(job_id) = job_id {
            query = query.filter(job_event::Column::JobId.eq(job_id.to_vec()));
        }
        if let Some(from_ms) = from_ms {
            query =
                query.filter(job_event::Column::EmittedAt.gte(query_timestamp_micros(from_ms)?));
        }
        if let Some(to_ms) = to_ms {
            query = query.filter(job_event::Column::EmittedAt.lt(query_timestamp_micros(to_ms)?));
        }
        if let Some(after) = after {
            query = query.filter(
                Condition::any()
                    .add(job_event::Column::EmittedAt.lt(after.emitted_at))
                    .add(
                        Condition::all()
                            .add(job_event::Column::EmittedAt.eq(after.emitted_at))
                            .add(job_event::Column::EventId.lt(after.event_id.as_str())),
                    ),
            );
        }
        Ok(query
            .order_by_desc(job_event::Column::EmittedAt)
            .order_by_desc(job_event::Column::EventId)
            .limit(limit.clamp(1, 101))
            .all(self.connection())
            .await?
            .into_iter()
            .map(|row| JobEventRecord {
                event_id: row.event_id,
                agent_id: row.agent_id,
                instance_id: row.instance_id,
                sequence: row.sequence,
                kind: row.kind,
                job_id: row.job_id,
                revision: row.revision,
                run_id: row.run_id,
                attempt: row.attempt,
                emitted_at: row.emitted_at,
                message: row.message,
                will_retry: row.will_retry,
                gap_detected: row.gap_detected,
                payload: row.payload,
            })
            .collect())
    }
}

fn query_timestamp_micros(timestamp_ms: i64) -> Result<i64, DatabaseError> {
    timestamp_ms.checked_mul(1_000).ok_or_else(|| {
        DatabaseError::InvalidJobEvent(
            "query timestamp in milliseconds exceeds i64 microseconds".to_owned(),
        )
    })
}

/// 把 16 字节事件实例编码成固定长度的小写十六进制文本。
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

/// Job 相关事件必须引用该 Agent 曾经拥有的历史 Job revision；Scheduler 全局事件允许为空。
async fn validate_job_event_attribution(
    database: &ServerDatabase,
    agent_id: &str,
    event: &JobEvent,
) -> Result<(), DatabaseError> {
    let kind = JobEventKind::try_from(event.kind)
        .map_err(|_| DatabaseError::InvalidJobEvent("Job event kind is unknown".to_owned()))?;
    if kind == JobEventKind::Unspecified {
        return Err(DatabaseError::InvalidJobEvent(
            "Job event kind is unspecified".to_owned(),
        ));
    }
    if event.job_id.is_empty() && event.revision == 0 {
        if is_global_event(kind) {
            return Ok(());
        }
        return Err(DatabaseError::InvalidJobEvent(
            "Job event kind requires Job attribution".to_owned(),
        ));
    }
    if event.job_id.len() != 16 || event.revision == 0 {
        return Err(DatabaseError::InvalidJobEvent(
            "Job event Job attribution is incomplete".to_owned(),
        ));
    }
    if requires_execution_identity(kind) && (event.run_id.len() != 16 || event.attempt == 0) {
        return Err(DatabaseError::InvalidJobEvent(
            "execution Job event requires a 16-byte run id and positive attempt".to_owned(),
        ));
    }
    let revision = i64::try_from(event.revision)
        .map_err(|_| DatabaseError::InvalidJobEvent("Job event revision exceeds i64".to_owned()))?;
    if agent_job_version::Entity::find()
        .filter(agent_job_version::Column::AgentId.eq(agent_id))
        .filter(agent_job_version::Column::JobId.eq(event.job_id.clone()))
        .filter(agent_job_version::Column::Revision.eq(revision))
        .one(database.connection())
        .await?
        .is_none()
    {
        return Err(DatabaseError::InvalidJobEvent(
            "Job event revision does not belong to this Agent".to_owned(),
        ));
    }
    Ok(())
}

/// 这些事件描述一次具体执行，必须带有 run_id 和 attempt；Job 配置事件只需 Job revision。
fn requires_execution_identity(kind: JobEventKind) -> bool {
    matches!(
        kind,
        JobEventKind::TriggerScheduled
            | JobEventKind::TriggerSkipped
            | JobEventKind::ExecutionQueued
            | JobEventKind::ExecutionStarted
            | JobEventKind::ExecutionSucceeded
            | JobEventKind::ExecutionFailed
            | JobEventKind::ExecutionTimedOut
            | JobEventKind::ExecutionPanicked
            | JobEventKind::ExecutionCancelled
            | JobEventKind::RetryScheduled
            | JobEventKind::ReportDelivered
            | JobEventKind::ReportDeliveryFailed
            | JobEventKind::PendingReplaced
            | JobEventKind::BackpressureApplied
    )
}

/// 只有 Scheduler 自身生命周期事件可以不引用具体 Job。
fn is_global_event(kind: JobEventKind) -> bool {
    matches!(
        kind,
        JobEventKind::SchedulerStarted
            | JobEventKind::SchedulerConfigUpdated
            | JobEventKind::SchedulerStopping
            | JobEventKind::SchedulerStopped
            | JobEventKind::SchedulerFailed
    )
}

fn unix_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseConfig;
    use smalux_protocol::agent::v1::{JobEvent, JobEventKind};

    #[tokio::test]
    async fn appends_and_lists_job_event_payload() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("database should connect");
        let event = JobEvent {
            instance_id: vec![3; 16],
            sequence: 4,
            kind: JobEventKind::SchedulerStarted as i32,
            ..Default::default()
        };
        let id = database.append_job_event("agent-a", &event).await.unwrap();
        let rows = database.list_job_events(Some("agent-a"), 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, id);
        assert_eq!(JobEvent::decode(rows[0].payload.as_slice()).unwrap(), event);
        assert!(rows[0].gap_detected);
    }

    #[tokio::test]
    async fn duplicate_event_is_idempotent_and_payload_conflict_is_rejected() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let event = JobEvent {
            instance_id: vec![5; 16],
            sequence: 1,
            kind: JobEventKind::SchedulerStarted as i32,
            ..Default::default()
        };
        let first = database.append_job_event("agent-a", &event).await.unwrap();
        let second = database.append_job_event("agent-a", &event).await.unwrap();
        assert_eq!(first, second);
        let mut changed = event.clone();
        changed.message = "different".to_owned();
        assert!(matches!(
            database.append_job_event("agent-a", &changed).await,
            Err(DatabaseError::InvalidJobEvent(_))
        ));
    }

    #[tokio::test]
    async fn execution_event_requires_run_identity() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let event = JobEvent {
            instance_id: vec![6; 16],
            sequence: 1,
            kind: JobEventKind::ExecutionFailed as i32,
            job_id: vec![1; 16],
            revision: 1,
            ..Default::default()
        };
        assert!(matches!(
            database.append_job_event("agent-a", &event).await,
            Err(DatabaseError::InvalidJobEvent(_))
        ));
    }

    #[tokio::test]
    async fn non_global_event_cannot_omit_job_attribution() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let event = JobEvent {
            instance_id: vec![7; 16],
            sequence: 1,
            kind: JobEventKind::JobAdded as i32,
            ..Default::default()
        };
        assert!(matches!(
            database.append_job_event("agent-a", &event).await,
            Err(DatabaseError::InvalidJobEvent(_))
        ));
    }

    #[tokio::test]
    async fn query_job_events_filters_and_pages_by_timestamp_then_id() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let job_id: &[u8] = b"job-a";
        for (event_id, agent_id, row_job_id, emitted_at) in [
            ("z", "agent-a", job_id, 2_500),
            ("m", "agent-a", job_id, 2_500),
            ("a", "agent-a", job_id, 2_500),
            ("q", "agent-a", job_id, 2_499),
            ("from-inclusive", "agent-a", job_id, 2_000),
            ("before-from", "agent-a", job_id, 1_999),
            ("at-to", "agent-a", job_id, 3_000),
            ("other-agent", "agent-b", job_id, 2_600),
            ("other-job", "agent-a", &b"job-b"[..], 2_600),
        ] {
            insert_job_event(&database, event_id, agent_id, row_job_id, emitted_at).await;
        }

        let page = database
            .query_job_events(Some("agent-a"), Some(job_id), Some(2), Some(3), None, 100)
            .await
            .unwrap();
        assert_eq!(
            page.iter()
                .map(|row| row.event_id.as_str())
                .collect::<Vec<_>>(),
            ["z", "m", "a", "q", "from-inclusive"]
        );

        let next_page = database
            .query_job_events(
                Some("agent-a"),
                Some(job_id),
                Some(2),
                Some(3),
                Some(&JobEventCursor {
                    emitted_at: 2_500,
                    event_id: "m".to_owned(),
                }),
                100,
            )
            .await
            .unwrap();
        assert_eq!(
            next_page
                .iter()
                .map(|row| row.event_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "q", "from-inclusive"]
        );
    }

    #[tokio::test]
    async fn query_job_events_clamps_limit_to_one_through_one_hundred() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        for index in 0..101 {
            insert_job_event(
                &database,
                &format!("event-{index:03}"),
                "agent-a",
                b"job-a",
                i64::from(index),
            )
            .await;
        }
        let minimum = database
            .query_job_events(None, None, None, None, None, 0)
            .await
            .unwrap();
        let maximum = database
            .query_job_events(None, None, None, None, None, 500)
            .await
            .unwrap();
        assert_eq!(minimum.len(), 1);
        assert_eq!(maximum.len(), 101);
    }

    async fn insert_job_event(
        database: &ServerDatabase,
        event_id: &str,
        agent_id: &str,
        job_id: &[u8],
        emitted_at: i64,
    ) {
        job_event::Entity::insert(job_event::ActiveModel {
            event_id: Set(event_id.to_owned()),
            agent_id: Set(agent_id.to_owned()),
            instance_id: Set(event_id.as_bytes().to_vec()),
            sequence: Set(1),
            kind: Set(JobEventKind::SchedulerStarted as i32),
            job_id: Set(job_id.to_vec()),
            revision: Set(0),
            run_id: Set(Vec::new()),
            attempt: Set(0),
            emitted_at: Set(emitted_at),
            run_at: Set(None),
            duration_ms: Set(0),
            message: Set(String::new()),
            will_retry: Set(false),
            pending_count: Set(0),
            gap_detected: Set(false),
            payload: Set(Vec::new()),
        })
        .exec(database.connection())
        .await
        .unwrap();
    }
}
