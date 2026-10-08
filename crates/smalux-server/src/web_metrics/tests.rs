use super::*;
use crate::database::DatabaseConfig;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, IntoActiveModel};
use serde_json::{Value, json};
use smalux_protocol::agent::v1::{CpuSnapshot, MemorySnapshot, TaskDefinition, TaskResult};

fn config(cpu: Uuid, memory: Uuid) -> MetricsConfig {
    MetricsConfig::parse(
        &json!([{"agentId":"agent-a","cpuJobId":cpu,"memoryJobId":memory}]).to_string(),
        60,
    )
    .unwrap()
}
fn params() -> LatestParams {
    LatestParams {
        agent_ids: vec!["agent-a".into()],
        metrics: all_metrics(),
    }
}
fn cpu(value: f32) -> task_result::Result {
    task_result::Result::Cpu(CpuSnapshot {
        warmed_up: true,
        global_usage_percent: value,
        logical_cpu_count: 8,
        ..Default::default()
    })
}
fn memory(used: u64, total: u64, percent: f64) -> task_result::Result {
    task_result::Result::Memory(MemorySnapshot {
        used_bytes: used,
        total_bytes: total,
        usage_percent: percent,
        ..Default::default()
    })
}
fn job(id: Uuid, revision: u64, enabled: bool, metric: MetricName) -> JobDefinition {
    JobDefinition {
        job_id: id.as_bytes().to_vec(),
        revision,
        enabled,
        task: Some(TaskDefinition {
            task: Some(match metric {
                MetricName::Cpu => task_definition::Task::Cpu(Default::default()),
                MetricName::Memory => task_definition::Task::Memory(Default::default()),
            }),
        }),
        ..Default::default()
    }
}
fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}
async fn fixture() -> (ServerDatabase, MetricsConfig, Uuid, Uuid) {
    let db = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
        .await
        .unwrap();
    agent::Entity::insert(agent::ActiveModel {
        agent_id: Set("agent-a".into()),
        name: Set("A".into()),
        public_key: Set(vec![17; 32]),
        status: Set("active".into()),
        created_at: Set(now_us()),
        updated_at: Set(now_us()),
        revoked_at: Set(None),
    })
    .exec(db.connection())
    .await
    .unwrap();
    let c = Uuid::new_v4();
    let m = Uuid::new_v4();
    db.replace_agent_job_catalog(
        "agent-a",
        vec![
            job(c, 1, true, MetricName::Cpu),
            job(m, 1, true, MetricName::Memory),
        ],
    )
    .await
    .unwrap();
    (db, config(c, m), c, m)
}
async fn report(
    db: &ServerDatabase,
    id: Uuid,
    revision: u64,
    at: Option<i64>,
    result: task_result::Result,
) {
    db.append_task_report(
        "agent-a",
        &TaskReport {
            job_id: id.as_bytes().to_vec(),
            job_revision: revision,
            run_id: Uuid::new_v4().as_bytes().to_vec(),
            attempt: 1,
            started_at: at.map(|us| prost_types::Timestamp {
                seconds: us.div_euclid(1_000_000),
                nanos: (us.rem_euclid(1_000_000) * 1000) as i32,
            }),
            result: Some(TaskResult {
                result: Some(result),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}
async fn snapshot(db: &ServerDatabase, config: &MetricsConfig) -> Value {
    serde_json::to_value(latest(db, config, &params()).await.unwrap()).unwrap()
}

#[test]
fn binding_revision_is_order_independent_and_config_is_strict() {
    let a = r#"[{"agentId":"a"},{"agentId":"b"}]"#;
    let b = r#"[ { "agentId": "b" }, { "agentId": "a" } ]"#;
    assert_eq!(
        MetricsConfig::parse(a, 60).unwrap().revision,
        MetricsConfig::parse(b, 60).unwrap().revision
    );
    assert_ne!(
        MetricsConfig::parse(a, 60).unwrap().revision,
        MetricsConfig::parse("[]", 60).unwrap().revision
    );
    for bad in [
        r#"[{"agentId":"a"},{"agentId":"a"}]"#,
        r#"[{"agentId":"a","unknown":1}]"#,
        r#"[{"agentId":"a","cpuJobId":"not-uuid"}]"#,
        r#"[{"agentId":"a b"}]"#,
        r#"[{"agentId":""}]"#,
    ] {
        assert!(MetricsConfig::parse(bad, 60).is_err(), "{bad}");
    }
    let id = Uuid::new_v4();
    assert!(
        MetricsConfig::parse(
            &json!([{"agentId":"a","cpuJobId":id,"memoryJobId":id}]).to_string(),
            60
        )
        .is_err()
    );
    assert!(MetricsConfig::parse("[]", 0).is_err());
    assert!(MetricsConfig::parse("[]", 86401).is_err());
    assert!(MetricsConfig::parse(&" ".repeat(256 * 1024 + 1), 60).is_err());
    let too_many: Vec<_> = (0..1001)
        .map(|n| json!({"agentId":n.to_string()}))
        .collect();
    assert!(MetricsConfig::parse(&serde_json::to_string(&too_many).unwrap(), 60).is_err());
}
#[test]
fn latest_params_reject_duplicates_unknown_fields_and_unsupported_metrics() {
    let defaults: LatestParams = serde_json::from_value(json!({"agentIds":["agent-a"]})).unwrap();
    assert_eq!(defaults.metrics, all_metrics());
    assert!(defaults.validate().is_ok());
    for bad in [
        json!({"agentIds":["a"],"metrics":["network"]}),
        json!({"agentIds":["a"],"extra":true}),
    ] {
        assert!(serde_json::from_value::<LatestParams>(bad).is_err());
    }
    for bad in [
        json!({"agentIds":[]}),
        json!({"agentIds":["a","a"]}),
        json!({"agentIds":["a"],"metrics":[]}),
        json!({"agentIds":["a"],"metrics":["cpu","cpu"]}),
        json!({"agentIds":(0..101).map(|n|n.to_string()).collect::<Vec<_>>()}),
    ] {
        assert_eq!(
            serde_json::from_value::<LatestParams>(bad)
                .unwrap()
                .validate(),
            Err(MetricsError::InvalidParams)
        );
    }
}
#[test]
fn value_validation_preserves_zero_and_rejects_nan_range_and_unsafe_integers() {
    assert_eq!(decode_cpu(cpu(0.0)).unwrap().cpu_usage_percent, 0.0);
    for value in [f32::NAN, f32::INFINITY, -1.0, 101.0] {
        assert_eq!(decode_cpu(cpu(value)).unwrap_err().state, "unavailable");
    }
    assert_eq!(
        decode_cpu(task_result::Result::Cpu(CpuSnapshot::default()))
            .unwrap_err()
            .state,
        "warmingUp"
    );
    assert_eq!(
        decode_memory(memory(0, 1024, 0.0)).unwrap().used_bytes,
        Some(0)
    );
    assert!(decode_memory(memory(MAX_SAFE_INTEGER, MAX_SAFE_INTEGER, 100.0)).is_ok());
    for result in [
        memory(1, 0, 0.0),
        memory(2, 1, 1.0),
        memory(0, MAX_SAFE_INTEGER + 1, 0.0),
        memory(1, 2, f64::NAN),
        memory(1, 2, 101.0),
    ] {
        assert_eq!(decode_memory(result).unwrap_err().state, "unavailable");
    }
    assert!(safe_millis(-1).is_none());
    assert!(safe_millis(i64::MAX).is_none());
}
#[tokio::test]
async fn persisted_zero_reports_have_independent_timestamps_and_requested_groups_only() {
    let (db, cfg, c, m) = fixture().await;
    let at = now_us() - 1_000_000;
    report(&db, c, 1, Some(at), cpu(0.0)).await;
    report(&db, m, 1, Some(at - 1_000_000), memory(0, 1024, 0.0)).await;
    let result = snapshot(&db, &cfg).await;
    assert_eq!(result[0]["cpu"]["value"]["cpuUsagePercent"], 0.0);
    assert_eq!(result[0]["memory"]["value"]["usedBytes"], 0);
    assert_eq!(result[0]["cpu"]["quality"]["state"], "valid");
    assert_eq!(result[0]["cpu"]["sampledAtMs"], at / 1000);
    assert_eq!(result[0]["memory"]["sampledAtMs"], (at - 1_000_000) / 1000);
    assert_eq!(result[0]["cpu"]["sourceJobId"], c.to_string());
    assert_eq!(result[0]["cpu"]["sourceJobRevision"], "1");
    let only = serde_json::to_value(
        latest(
            &db,
            &cfg,
            &LatestParams {
                metrics: vec![MetricName::Cpu],
                ..params()
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(only[0].get("memory").is_none());
    assert!(only[0].get("payload").is_none());
}
#[tokio::test]
async fn no_binding_no_sample_missing_time_stale_and_future_are_not_fake_zero() {
    let (db, cfg, c, m) = fixture().await;
    let value = snapshot(&db, &cfg).await;
    assert!(value[0]["cpu"]["value"].is_null());
    assert_eq!(value[0]["cpu"]["quality"]["reason"], "noSamples");
    let unbound = MetricsConfig::parse(r#"[{"agentId":"agent-a"}]"#, 60).unwrap();
    assert_eq!(
        snapshot(&db, &unbound).await[0]["cpu"]["quality"]["reason"],
        "notConfigured"
    );
    report(&db, c, 1, None, cpu(10.0)).await;
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["quality"]["reason"],
        "missingSampleTime"
    );
    report(&db, c, 1, Some(now_us() - 120_000_000), cpu(20.0)).await;
    let stale = snapshot(&db, &cfg).await;
    assert_eq!(stale[0]["cpu"]["quality"]["state"], "stale");
    assert_eq!(stale[0]["cpu"]["value"]["cpuUsagePercent"], 20.0);
    assert_eq!(
        stale,
        snapshot(&db, &cfg).await,
        "age alone must not create a changed snapshot"
    );
    report(&db, m, 1, Some(now_us() + 120_000_000), memory(1, 2, 50.0)).await;
    let future = snapshot(&db, &cfg).await;
    assert!(future[0]["memory"]["value"].is_null());
    assert_eq!(future[0]["memory"]["quality"]["reason"], "invalidTimestamp");
}
#[tokio::test]
async fn latest_uses_current_revision_and_sample_time_not_late_receipt() {
    let (db, cfg, c, m) = fixture().await;
    let at = now_us() - 5_000_000;
    report(&db, c, 1, Some(at), cpu(10.0)).await;
    report(&db, c, 1, Some(at - 1_000_000), cpu(99.0)).await;
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["value"]["cpuUsagePercent"],
        10.0
    );
    db.replace_agent_job_catalog(
        "agent-a",
        vec![
            job(c, 2, true, MetricName::Cpu),
            job(m, 1, true, MetricName::Memory),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["quality"]["reason"],
        "noSamples"
    );
    report(&db, c, 2, Some(at), cpu(20.0)).await;
    report(&db, c, 1, Some(at + 1_000_000), cpu(99.0)).await;
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["value"]["cpuUsagePercent"],
        20.0
    );
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["sourceJobRevision"],
        "2"
    );
    report(&db, c, 2, Some(at), cpu(30.0)).await;
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["value"]["cpuUsagePercent"],
        30.0,
        "receipt breaks tied sample times"
    );
}
#[tokio::test]
async fn removed_disabled_and_incompatible_sources_are_unavailable() {
    let (db, cfg, c, m) = fixture().await;
    report(&db, c, 1, Some(now_us() - 1000), cpu(10.0)).await;
    db.replace_agent_job_catalog(
        "agent-a",
        vec![
            job(c, 2, false, MetricName::Cpu),
            job(m, 1, true, MetricName::Memory),
        ],
    )
    .await
    .unwrap();
    let value = snapshot(&db, &cfg).await;
    assert!(value[0]["cpu"]["value"].is_null());
    assert_eq!(value[0]["cpu"]["quality"]["reason"], "sourceUnavailable");
    db.replace_agent_job_catalog("agent-a", vec![job(c, 3, true, MetricName::Memory)])
        .await
        .unwrap();
    assert_eq!(
        snapshot(&db, &cfg).await[0]["cpu"]["quality"]["reason"],
        "sourceUnavailable"
    );
    assert_eq!(
        snapshot(&db, &cfg).await[0]["memory"]["quality"]["reason"],
        "sourceUnavailable"
    );
}
#[tokio::test]
async fn corrupted_payload_and_wrong_result_cannot_leak_or_reuse_old_values() {
    let (db, cfg, c, _) = fixture().await;
    report(&db, c, 1, Some(now_us() - 1000), cpu(10.0)).await;
    let original = task_report::Entity::find()
        .one(db.connection())
        .await
        .unwrap()
        .unwrap();
    for payload in [vec![255], {
        let mut p = TaskReport::decode(original.payload.as_slice()).unwrap();
        p.result.as_mut().unwrap().result = Some(memory(1, 2, 50.0));
        p.encode_to_vec()
    }] {
        let mut row = original.clone().into_active_model();
        row.payload = Set(payload);
        row.update(db.connection()).await.unwrap();
        let value = snapshot(&db, &cfg).await;
        assert!(value[0]["cpu"]["value"].is_null());
        assert_eq!(value[0]["cpu"]["quality"]["reason"], "invalidReport");
    }
}
#[tokio::test]
async fn agent_scope_rejects_absent_unconfigured_or_revoked_as_one_forbidden() {
    let (db, cfg, _, _) = fixture().await;
    assert_eq!(
        authorize_agents(&db, &cfg, &["absent".into()]).await,
        Err(MetricsError::Forbidden)
    );
    assert_eq!(
        authorize_agents(
            &db,
            &MetricsConfig::parse("[]", 60).unwrap(),
            &params().agent_ids
        )
        .await,
        Err(MetricsError::Forbidden)
    );
    let mut row = agent::Entity::find_by_id("agent-a")
        .one(db.connection())
        .await
        .unwrap()
        .unwrap()
        .into_active_model();
    row.status = Set("revoked".into());
    row.update(db.connection()).await.unwrap();
    assert_eq!(
        latest(&db, &cfg, &params()).await.unwrap_err(),
        MetricsError::Forbidden
    );
}
