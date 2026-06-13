//! 远程网络探测执行器和持续任务调度。
//!
//! 本模块统一处理一次性探测和持续任务同步。通用 job 层负责把 `job_apply(kind=probe)`
//! 转换成内部 `RemoteProbeApply`；具体 TCP/HTTP 探测、频率保护、持续任务 worker 和结果投递都收口在这里。

use crate::collect::unix_timestamp_secs;
use crate::config::ConfigManager;
use crate::service::message::outbound::{
    OutboundEvent, OutboundSender, OutboundSequence, RemoteJobResultEnvelope,
};
use serde::Deserialize;
use smalux_core::utils::validate::ensure_non_empty;
use smalux_protocol::{
    RemoteJobResult, RemoteProbeId, RemoteProbeResult, RemoteProbeResultSource,
    RemoteProbeResultStatus, RemoteProbeType,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// 远程探测目标缓存上限，避免恶意 server 用大量唯一目标撑大内存。
const RATE_STATE_MAX_TARGETS: usize = 1024;

/// 协议无关的远程探测执行请求。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub(crate) struct RemoteProbeExecutionRequest {
    /// 结果来源。
    pub(crate) source: RemoteProbeResultSource,
    /// server 侧业务探测点 ID。
    pub(crate) point_id: Option<RemoteProbeId>,
    /// 一次性请求 ID。
    pub(crate) request_id: Option<RemoteProbeId>,
    /// 持续任务 ID。
    pub(crate) job_id: Option<String>,
    /// 探测类型。
    pub(crate) probe_type: RemoteProbeType,
    /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
    pub(crate) target: String,
    /// 本次请求的超时；缺省时使用当前 agent 配置。
    #[serde(default)]
    pub(crate) timeout: Option<Duration>,
}

impl RemoteProbeExecutionRequest {
    /// 校验并返回自身，便于通用 job adapter 复用 probe 的校验规则。
    pub(crate) fn validated(self) -> anyhow::Result<Self> {
        self.validate()?;
        Ok(self)
    }

    /// 校验探测请求最小字段。
    fn validate(&self) -> anyhow::Result<()> {
        validate_optional_probe_id("remote_probe.point_id", &self.point_id)?;
        match self.source {
            RemoteProbeResultSource::Once => {
                if self.job_id.is_some() {
                    anyhow::bail!("remote_probe.once cannot contain job_id");
                }
                match &self.request_id {
                    Some(RemoteProbeId::String(value)) if value.trim().is_empty() => {
                        anyhow::bail!("remote_probe.request_id cannot be empty");
                    }
                    Some(_) => {}
                    None => anyhow::bail!("remote_probe.once requires request_id"),
                }
            }
            RemoteProbeResultSource::Job => {
                if self.request_id.is_some() {
                    anyhow::bail!("remote_probe.job cannot contain request_id");
                }
                ensure_non_empty(
                    "remote_probe.job_id",
                    self.job_id.as_deref().unwrap_or_default(),
                )?;
            }
        }
        ensure_non_empty("remote_probe.target", &self.target)?;
        Ok(())
    }

    /// 返回日志和兼容协议使用的关联 ID。
    fn display_id(&self) -> String {
        self.point_id
            .as_ref()
            .map(RemoteProbeId::display)
            .or_else(|| self.request_id.as_ref().map(RemoteProbeId::display))
            .or_else(|| self.job_id.clone())
            .unwrap_or_default()
    }

    /// 返回本次执行请求对应的业务探测点 ID，日志中没有时输出空字符串。
    fn point_display_id(&self) -> String {
        self.point_id
            .as_ref()
            .map(RemoteProbeId::display)
            .unwrap_or_default()
    }

    /// 返回本次执行请求对应的请求或任务 ID，便于日志区分执行和业务探测点。
    fn execution_display_id(&self) -> String {
        self.request_id
            .as_ref()
            .map(RemoteProbeId::display)
            .or_else(|| self.job_id.clone())
            .unwrap_or_default()
    }

    /// 用于同目标限频的稳定 key。
    fn target_key(&self) -> String {
        format!(
            "{}:{}",
            self.probe_type.as_str(),
            self.target.trim().to_ascii_lowercase()
        )
    }

    /// 把一次执行请求收口转换成最终上报结果。
    ///
    /// 结果字段集中在这里映射，后续协议新增字段时不需要分别修改执行成功、
    /// 执行失败和本地拒绝三条路径。
    fn into_result(self, parts: RemoteProbeResultParts) -> RemoteProbeResult {
        RemoteProbeResult {
            run_id: parts.run_id,
            source: self.source,
            point_id: self.point_id,
            request_id: self.request_id,
            job_id: self.job_id,
            probe_type: self.probe_type,
            target: self.target,
            status: parts.status,
            latency_ms: parts.latency_ms,
            started_at: parts.started_at,
            finished_at: parts.finished_at,
            duration_ms: parts.duration_ms,
            error: parts.error,
        }
    }
}

/// 远程探测结果的执行侧字段。
///
/// 这些字段由执行器本地生成，和 server 下发的探测目标字段分开，避免构造结果时
/// 出现过长参数列表。
#[derive(Debug)]
struct RemoteProbeResultParts {
    /// agent 生成的单次运行唯一 ID。
    run_id: String,
    /// 结果状态。
    status: RemoteProbeResultStatus,
    /// 成功时的延迟毫秒数。
    latency_ms: Option<u64>,
    /// 开始时间，Unix 秒。
    started_at: u64,
    /// 完成时间，Unix 秒。
    finished_at: u64,
    /// 实际耗时，毫秒。
    duration_ms: u64,
    /// 失败或拒绝原因。
    error: Option<String>,
}

/// 持续远程探测任务定义。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RemoteProbeJobSpec {
    /// server 侧生成的持续任务 ID。
    pub(crate) job_id: String,
    /// server 侧业务探测点 ID。
    pub(crate) point_id: Option<RemoteProbeId>,
    /// 是否启用该任务。
    pub(crate) enabled: bool,
    /// 探测类型。
    pub(crate) probe_type: RemoteProbeType,
    /// 探测目标。
    pub(crate) target: String,
    /// 持续探测间隔。
    pub(crate) interval: Duration,
    /// 任务级超时；缺省时使用 agent 默认值。
    pub(crate) timeout: Option<Duration>,
}

impl RemoteProbeJobSpec {
    /// 校验并返回自身，便于通用 job adapter 复用 probe 的校验规则。
    pub(crate) fn validated(self) -> anyhow::Result<Self> {
        self.validate()?;
        Ok(self)
    }

    /// 校验持续任务最小字段。
    fn validate(&self) -> anyhow::Result<()> {
        ensure_non_empty("remote_probe.job_id", &self.job_id)?;
        validate_optional_probe_id("remote_probe.point_id", &self.point_id)?;
        ensure_non_empty("remote_probe.target", &self.target)?;
        if self.interval.is_zero() {
            anyhow::bail!("remote_probe.interval must be greater than zero");
        }
        Ok(())
    }

    /// 转成一次执行请求。
    fn to_execution_request(&self) -> RemoteProbeExecutionRequest {
        RemoteProbeExecutionRequest {
            source: RemoteProbeResultSource::Job,
            point_id: self.point_id.clone(),
            request_id: None,
            job_id: Some(self.job_id.clone()),
            probe_type: self.probe_type,
            target: self.target.clone(),
            timeout: self.timeout,
        }
    }
}

/// 统一远程探测请求。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RemoteProbeApply {
    /// 立即执行一批一次性探测。
    Once {
        /// 一次性探测列表。
        runs: Vec<RemoteProbeExecutionRequest>,
    },
    /// 用新任务列表整组替换当前持续任务。
    Replace {
        /// 任务代际。
        generation: u64,
        /// 新任务列表。
        jobs: Vec<RemoteProbeJobSpec>,
    },
    /// 增量更新当前持续任务。
    Patch {
        /// 任务代际。
        generation: u64,
        /// 需要新增或更新的任务。
        upsert_jobs: Vec<RemoteProbeJobSpec>,
        /// 需要删除的任务 ID。
        remove_job_ids: Vec<String>,
    },
}

/// 远程探测管理器。
#[derive(Debug, Clone)]
pub(crate) struct RemoteProbeManager {
    /// 动态配置管理器。
    config_manager: ConfigManager,
    /// 出站事件发送端。
    outbound_tx: OutboundSender,
    /// 全局出站序号。
    sequence: OutboundSequence,
    /// 本地频率保护状态。
    rate_state: Arc<Mutex<ProbeRateState>>,
    /// 持续任务运行状态。
    scheduler_state: Arc<Mutex<ProbeSchedulerState>>,
    /// HTTP 探测复用连接池。
    http_client: reqwest::Client,
}

impl RemoteProbeManager {
    /// 创建远程探测管理器。
    pub(crate) fn new(
        config_manager: ConfigManager,
        outbound_tx: OutboundSender,
        sequence: OutboundSequence,
    ) -> Self {
        Self {
            config_manager,
            outbound_tx,
            sequence,
            rate_state: Arc::new(Mutex::new(ProbeRateState::default())),
            scheduler_state: Arc::new(Mutex::new(ProbeSchedulerState::default())),
            http_client: reqwest::Client::new(),
        }
    }

    /// 应用一次性探测或持续任务更新。
    pub(crate) async fn apply(&self, request: RemoteProbeApply) -> anyhow::Result<()> {
        match request {
            RemoteProbeApply::Once { runs } => {
                for run in runs {
                    self.start_once(run).await?;
                }
                Ok(())
            }
            RemoteProbeApply::Replace { generation, jobs } => self.apply_replace(generation, jobs),
            RemoteProbeApply::Patch {
                generation,
                upsert_jobs,
                remove_job_ids,
            } => self.apply_patch(generation, upsert_jobs, remove_job_ids),
        }
    }

    /// 启动一次性探测；禁用或限频时立即回传 `status=rejected`。
    async fn start_once(&self, request: RemoteProbeExecutionRequest) -> anyhow::Result<()> {
        request.validate()?;
        let config = self.config_manager.current();
        let agent_id = config.agent_id.clone();
        let probe_config = config.remote_probe;

        if !probe_config.enabled {
            self.queue_immediate_result(agent_id, request, "remote probe is disabled")
                .await?;
            return Ok(());
        }

        if let Some(reason) = self.try_mark_started(&request, &probe_config) {
            self.queue_immediate_result(agent_id, request, &reason)
                .await?;
            return Ok(());
        }

        let timeout = request
            .timeout
            .map(|requested| requested.min(probe_config.timeout))
            .unwrap_or(probe_config.timeout);
        self.spawn_probe_task(agent_id, request, timeout);
        Ok(())
    }

    /// 应用整组持续任务替换。
    fn apply_replace(&self, generation: u64, jobs: Vec<RemoteProbeJobSpec>) -> anyhow::Result<()> {
        let mut state = self
            .scheduler_state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if state.generation.is_some_and(|current| generation < current) {
            tracing::warn!(
                generation,
                current_generation = state.generation.unwrap_or(0),
                "stale remote probe replace ignored"
            );
            return Ok(());
        }

        let mut old_jobs = std::mem::take(&mut state.jobs);
        let mut rebuilt = HashMap::with_capacity(jobs.len());
        for spec in jobs {
            let key = spec.job_id.clone();
            let control = match old_jobs.remove(&key) {
                Some(existing) if existing.spec == spec => existing.control,
                Some(existing) => {
                    stop_job_worker(existing.control);
                    None
                }
                None => None,
            };
            rebuilt.insert(key, ScheduledProbeJob { spec, control });
        }

        for existing in old_jobs.into_values() {
            stop_job_worker(existing.control);
        }

        state.jobs = rebuilt;
        state.generation = Some(generation);
        drop(state);
        self.reconcile_job_workers();
        tracing::info!(generation, "remote probe jobs replaced");
        Ok(())
    }

    /// 应用持续任务增量 patch。
    fn apply_patch(
        &self,
        generation: u64,
        upsert_jobs: Vec<RemoteProbeJobSpec>,
        remove_job_ids: Vec<String>,
    ) -> anyhow::Result<()> {
        let mut state = self
            .scheduler_state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if state.generation.is_some_and(|current| generation < current) {
            tracing::warn!(
                generation,
                current_generation = state.generation.unwrap_or(0),
                "stale remote probe patch ignored"
            );
            return Ok(());
        }

        for job_id in remove_job_ids {
            if let Some(existing) = state.jobs.remove(&job_id) {
                stop_job_worker(existing.control);
            }
        }

        for spec in upsert_jobs {
            match state.jobs.remove(&spec.job_id) {
                Some(existing) if existing.spec == spec => {
                    state.jobs.insert(spec.job_id.clone(), existing);
                }
                Some(existing) => {
                    stop_job_worker(existing.control);
                    state.jobs.insert(
                        spec.job_id.clone(),
                        ScheduledProbeJob {
                            spec,
                            control: None,
                        },
                    );
                }
                None => {
                    state.jobs.insert(
                        spec.job_id.clone(),
                        ScheduledProbeJob {
                            spec,
                            control: None,
                        },
                    );
                }
            }
        }

        state.generation = Some(generation);
        drop(state);
        self.reconcile_job_workers();
        tracing::info!(generation, "remote probe jobs patched");
        Ok(())
    }

    /// 对齐 worker 运行状态和当前任务表。
    fn reconcile_job_workers(&self) {
        let mut state = self
            .scheduler_state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        for job in state.jobs.values_mut() {
            if job.spec.enabled {
                if job.control.is_none() {
                    job.control = Some(self.spawn_job_worker(job.spec.clone()));
                }
            } else if job.control.is_some() {
                let control = job.control.take();
                stop_job_worker(control);
            }
        }
    }

    /// 为单个持续任务启动 worker。
    fn spawn_job_worker(&self, spec: RemoteProbeJobSpec) -> JobWorkerControl {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let manager = self.clone();
        let job_id = spec.job_id.clone();
        let interval = spec.interval;
        let handle = tokio::spawn(async move {
            tracing::info!(
                job_id = %job_id,
                point_id = %spec.point_id.as_ref().map(RemoteProbeId::display).unwrap_or_default(),
                probe_type = spec.probe_type.as_str(),
                interval_ms = interval.as_millis(),
                "remote probe job started"
            );
            run_job_loop(manager, spec, shutdown_rx).await;
        });
        JobWorkerControl {
            shutdown_tx,
            handle,
        }
    }

    /// 单次执行持续任务；禁用或限频时只记录日志，不上报 rejected。
    async fn start_job(&self, spec: &RemoteProbeJobSpec) -> anyhow::Result<()> {
        let request = spec.to_execution_request();
        let config = self.config_manager.current();
        let probe_config = config.remote_probe;
        if !probe_config.enabled {
            tracing::debug!(
                job_id = %spec.job_id,
                "remote probe job skipped because remote probe is disabled"
            );
            return Ok(());
        }

        if let Some(reason) = self.try_mark_started(&request, &probe_config) {
            tracing::debug!(job_id = %spec.job_id, reason, "remote probe job skipped");
            return Ok(());
        }

        let timeout = spec
            .timeout
            .map(|requested| requested.min(probe_config.timeout))
            .unwrap_or(probe_config.timeout);
        let agent_id = config.agent_id.clone();
        self.spawn_probe_task(agent_id, request, timeout);
        Ok(())
    }

    /// 启动真正的探测任务。
    fn spawn_probe_task(
        &self,
        agent_id: String,
        request: RemoteProbeExecutionRequest,
        timeout: Duration,
    ) {
        let manager = self.clone();
        let run_id = new_run_id();
        tracing::info!(
            run_id = %run_id,
            probe_id = %request.display_id(),
            point_id = %request.point_display_id(),
            execution_id = %request.execution_display_id(),
            probe_type = request.probe_type.as_str(),
            target_len = request.target.len(),
            timeout_ms = timeout.as_millis(),
            "remote probe accepted"
        );

        tokio::spawn(async move {
            let probe_id = request.display_id();
            let point_id = request.point_display_id();
            let execution_id = request.execution_display_id();
            let result =
                execute_remote_probe(run_id, request, timeout, manager.http_client.clone()).await;
            let run_id = result.run_id.clone();
            let probe_type = result.probe_type.as_str();
            let target_len = result.target.len();
            let status = result.status;
            let latency_ms = result.latency_ms;
            let duration_ms = result.duration_ms;
            let errored = result.error.is_some();
            manager.send_result(agent_id, result).await;
            tracing::info!(
                run_id = %run_id,
                probe_id = %probe_id,
                point_id = %point_id,
                execution_id = %execution_id,
                probe_type,
                target_len,
                status = ?status,
                latency_ms,
                duration_ms,
                errored,
                "remote probe finished"
            );
        });
    }

    /// 检查并记录本次探测启动时间；返回 Some 表示被限频拒绝。
    fn try_mark_started(
        &self,
        request: &RemoteProbeExecutionRequest,
        config: &crate::config::model::RemoteProbeConfig,
    ) -> Option<String> {
        let now = Instant::now();
        let target_key = request.target_key();
        let mut state = self
            .rate_state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        state.prune(now, config.target_min_interval);

        if state
            .last_global_started_at
            .is_some_and(|started| now.duration_since(started) < config.global_min_interval)
        {
            return Some("remote probe global rate limit reached".to_string());
        }
        if state
            .last_target_started_at
            .get(&target_key)
            .is_some_and(|started| now.duration_since(*started) < config.target_min_interval)
        {
            return Some("remote probe target rate limit reached".to_string());
        }

        state.last_global_started_at = Some(now);
        state.insert_target(target_key, now);
        None
    }

    /// 立即投递失败结果。
    async fn queue_immediate_result(
        &self,
        agent_id: String,
        request: RemoteProbeExecutionRequest,
        reason: &str,
    ) -> anyhow::Result<()> {
        let now = unix_timestamp_secs();
        let run_id = new_run_id();
        let probe_id_for_log = request.display_id();
        let point_id_for_log = request.point_display_id();
        let execution_id_for_log = request.execution_display_id();
        let probe_type_for_log = request.probe_type;
        let target_len_for_log = request.target.len();
        let result = request.into_result(RemoteProbeResultParts {
            run_id: run_id.clone(),
            status: RemoteProbeResultStatus::Rejected,
            latency_ms: None,
            started_at: now,
            finished_at: now,
            duration_ms: 0,
            error: Some(reason.to_string()),
        });
        let event = self.result_event(agent_id, result);
        self.outbound_tx
            .send(event)
            .await
            .map_err(|error| anyhow::anyhow!("outbound event send failed: {error}"))?;
        tracing::warn!(
            run_id = %run_id,
            probe_id = %probe_id_for_log,
            point_id = %point_id_for_log,
            execution_id = %execution_id_for_log,
            probe_type = probe_type_for_log.as_str(),
            target_len = target_len_for_log,
            reason,
            "remote probe rejected"
        );
        Ok(())
    }

    /// 异步投递探测结果。
    async fn send_result(&self, agent_id: String, result: RemoteProbeResult) {
        let run_id = result.run_id.clone();
        let probe_id = result.display_id();
        let event = self.result_event(agent_id, result);
        if let Err(error) = self.outbound_tx.send(event).await {
            tracing::warn!(
                run_id = %run_id,
                probe_id = %probe_id,
                error = %error,
                "remote probe result dropped"
            );
        }
    }

    /// 把探测结果包装成出站事件。
    fn result_event(&self, agent_id: String, result: RemoteProbeResult) -> OutboundEvent {
        let sequence = self.sequence.next();
        OutboundEvent::RemoteJobResult(RemoteJobResultEnvelope::new(
            agent_id,
            sequence,
            RemoteJobResult::probe(result),
        ))
    }
}

/// 持续任务运行状态。
#[derive(Debug, Default)]
struct ProbeSchedulerState {
    /// 当前持续任务代际。
    generation: Option<u64>,
    /// 当前持续任务列表。
    jobs: HashMap<String, ScheduledProbeJob>,
}

/// 单个持续任务的运行状态。
#[derive(Debug)]
struct ScheduledProbeJob {
    /// 当前任务定义。
    spec: RemoteProbeJobSpec,
    /// 可选 worker 控制句柄。
    control: Option<JobWorkerControl>,
}

/// 单个持续任务 worker 控制句柄。
#[derive(Debug)]
struct JobWorkerControl {
    /// worker 停止信号。
    shutdown_tx: watch::Sender<bool>,
    /// worker 任务句柄。
    handle: JoinHandle<()>,
}

/// 停止一个持续任务 worker。
fn stop_job_worker(control: Option<JobWorkerControl>) {
    let Some(control) = control else {
        return;
    };
    let _ = control.shutdown_tx.send_replace(true);
    drop(control.handle);
}

/// 周期运行单个持续任务。
async fn run_job_loop(
    manager: RemoteProbeManager,
    spec: RemoteProbeJobSpec,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let job_id = spec.job_id.clone();
    let mut first_run = true;
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        if !first_run {
            tokio::select! {
                _ = tokio::time::sleep(spec.interval) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
            }
        }
        first_run = false;

        if *shutdown_rx.borrow() {
            break;
        }

        if let Err(error) = manager.start_job(&spec).await {
            tracing::warn!(job_id = %job_id, error = ?error, "remote probe job execution failed");
        }
    }

    tracing::info!(job_id = %job_id, "remote probe job stopped");
}

/// 校验可选探测 ID，避免空字符串进入结果回传。
fn validate_optional_probe_id(field: &str, value: &Option<RemoteProbeId>) -> anyhow::Result<()> {
    if matches!(value, Some(RemoteProbeId::String(value)) if value.trim().is_empty()) {
        anyhow::bail!("{field} cannot be empty");
    }
    Ok(())
}

/// 频率保护状态。
#[derive(Debug, Default)]
struct ProbeRateState {
    /// 最近一次探测启动时间。
    last_global_started_at: Option<Instant>,
    /// 每个目标最近一次探测启动时间。
    last_target_started_at: HashMap<String, Instant>,
}

impl ProbeRateState {
    /// 插入目标探测时间，超过上限时移除最旧目标。
    fn insert_target(&mut self, key: String, started_at: Instant) {
        if !self.last_target_started_at.contains_key(&key)
            && self.last_target_started_at.len() >= RATE_STATE_MAX_TARGETS
            && let Some(oldest_key) = self
                .last_target_started_at
                .iter()
                .min_by_key(|(_key, started_at)| *started_at)
                .map(|(key, _started_at)| key.clone())
        {
            self.last_target_started_at.remove(&oldest_key);
        }
        self.last_target_started_at.insert(key, started_at);
    }

    /// 清理已超过目标限频窗口的旧记录。
    fn prune(&mut self, now: Instant, target_min_interval: Duration) {
        self.last_target_started_at
            .retain(|_key, started_at| now.duration_since(*started_at) < target_min_interval);
    }
}

/// 执行远程探测并构建结果。
async fn execute_remote_probe(
    run_id: String,
    request: RemoteProbeExecutionRequest,
    timeout: Duration,
    http_client: reqwest::Client,
) -> RemoteProbeResult {
    let started_at = unix_timestamp_secs();
    let started = Instant::now();
    let result = match request.probe_type {
        RemoteProbeType::Tcp => probe_tcp(&request.target, timeout).await,
        RemoteProbeType::Http => probe_http(&http_client, &request.target, timeout).await,
        RemoteProbeType::Icmp => Err(anyhow::anyhow!("icmp probe is not implemented")),
    };
    let duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let (status, latency_ms, error) = match result {
        Ok(()) => (RemoteProbeResultStatus::Success, Some(duration_ms), None),
        Err(error) => (
            RemoteProbeResultStatus::Failed,
            None,
            Some(error.to_string()),
        ),
    };

    request.into_result(RemoteProbeResultParts {
        run_id,
        status,
        latency_ms,
        started_at,
        finished_at: unix_timestamp_secs(),
        duration_ms,
        error,
    })
}

/// 生成单次探测运行 ID。
fn new_run_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// 执行 TCP 连接探测。
async fn probe_tcp(target: &str, timeout: Duration) -> anyhow::Result<()> {
    let address = tcp_target_address(target)?;
    tokio::time::timeout(timeout, TcpStream::connect(&address))
        .await
        .map_err(|_| anyhow::anyhow!("tcp probe timed out"))??;
    Ok(())
}

/// 执行 HTTP/HTTPS 探测，发送 GET 请求但不读取响应 body。
async fn probe_http(
    client: &reqwest::Client,
    target: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let url = http_target_url(target)?;
    tokio::time::timeout(timeout, client.get(url).timeout(timeout).send())
        .await
        .map_err(|_| anyhow::anyhow!("http probe timed out"))??;
    Ok(())
}

/// 解析 TCP 目标，兼容 `host:port` 和 `tcp://host:port`。
fn tcp_target_address(target: &str) -> anyhow::Result<String> {
    let target = target.trim();
    if let Ok(url) = reqwest::Url::parse(target) {
        if url.scheme() != "tcp" {
            anyhow::bail!("tcp probe target must use tcp:// or host:port");
        }
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("tcp probe target host is missing"))?;
        let port = url
            .port()
            .ok_or_else(|| anyhow::anyhow!("tcp probe target port is missing"))?;
        return Ok(format!("{host}:{port}"));
    }
    Ok(target.to_string())
}

/// 解析 HTTP 目标，缺少 scheme 时默认按 HTTP 处理。
fn http_target_url(target: &str) -> anyhow::Result<reqwest::Url> {
    let target = target.trim();
    let normalized = if target.contains("://") {
        target.to_string()
    } else {
        format!("http://{target}")
    };
    let url = reqwest::Url::parse(&normalized)?;
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("http probe target must use http or https");
    }
    Ok(url)
}

/// 把兼容协议的 probe id 转成日志友好的字符串。
pub(crate) fn display_probe_id(probe_id: &RemoteProbeId) -> String {
    probe_id.display()
}

#[cfg(test)]
mod tests {
    //! 远程网络探测测试。

    use super::*;
    use crate::config::{AgentConfig, ConfigManager};
    use crate::service::message::outbound::{OutboundEvent, OutboundSequence, outbound_channel};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 从通用 job 出站事件中取出 probe 结果。
    fn unwrap_probe_result(event: OutboundEvent) -> (u64, RemoteProbeResult) {
        let OutboundEvent::RemoteJobResult(envelope) = event else {
            panic!("expected remote job result");
        };
        let Some(result) = envelope.result.as_probe() else {
            panic!("expected probe job result");
        };
        (envelope.sequence, result.clone())
    }

    /// 构造测试探测管理器。
    fn probe_manager(
        mut config: AgentConfig,
    ) -> (
        RemoteProbeManager,
        crate::service::message::outbound::OutboundReceiver,
    ) {
        config.agent_id = "agent-probe".to_string();
        let manager = ConfigManager::new(config).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let probe_manager =
            RemoteProbeManager::new(manager, outbound_tx, OutboundSequence::default());

        (probe_manager, outbound_rx)
    }

    /// 构造 TCP 一次性探测请求。
    fn tcp_request(
        request_id: impl Into<RemoteProbeId>,
        target: String,
    ) -> RemoteProbeExecutionRequest {
        let request_id = request_id.into();
        RemoteProbeExecutionRequest {
            source: RemoteProbeResultSource::Once,
            point_id: Some(request_id.clone()),
            request_id: Some(request_id),
            job_id: None,
            probe_type: RemoteProbeType::Tcp,
            target,
            timeout: None,
        }
    }

    /// 验证默认关闭时不会发起网络探测，而是直接回传 rejected。
    #[tokio::test]
    async fn disabled_remote_probe_returns_rejected_result() {
        let (manager, mut outbound_rx) = probe_manager(AgentConfig::default());

        manager
            .apply(RemoteProbeApply::Once {
                runs: vec![tcp_request("probe-disabled", "127.0.0.1:1".to_string())],
            })
            .await
            .unwrap();

        let event = outbound_rx.recv().await.unwrap();
        let (sequence, result) = unwrap_probe_result(event);

        assert_eq!(sequence, 1);
        assert_eq!(result.source, RemoteProbeResultSource::Once);
        assert_eq!(result.point_id, Some(RemoteProbeId::from("probe-disabled")));
        assert_eq!(
            result.request_id,
            Some(RemoteProbeId::from("probe-disabled"))
        );
        assert_eq!(result.status, RemoteProbeResultStatus::Rejected);
        assert_eq!(result.latency_ms, None);
        assert!(result.error.as_deref().unwrap().contains("disabled"));
    }

    /// 验证全局限频会直接返回 rejected，不排队延迟执行。
    #[tokio::test]
    async fn remote_probe_global_rate_limit_returns_rejected_result() {
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);
        {
            let mut state = manager.rate_state.lock().unwrap();
            state.last_global_started_at = Some(Instant::now());
        }

        manager
            .apply(RemoteProbeApply::Once {
                runs: vec![tcp_request("probe-rate", "127.0.0.1:1".to_string())],
            })
            .await
            .unwrap();

        let event = outbound_rx.recv().await.unwrap();
        let (_sequence, result) = unwrap_probe_result(event);

        assert_eq!(result.status, RemoteProbeResultStatus::Rejected);
        assert_eq!(result.latency_ms, None);
        assert!(
            result
                .error
                .as_deref()
                .unwrap()
                .contains("global rate limit")
        );
    }

    /// 验证 TCP 探测成功时会回传非负耗时。
    #[tokio::test]
    async fn remote_probe_tcp_success_returns_latency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
        });
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);

        manager
            .apply(RemoteProbeApply::Once {
                runs: vec![tcp_request("probe-ok", target)],
            })
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_sequence, result) = unwrap_probe_result(event);
        let _ = server.await;

        assert_eq!(result.source, RemoteProbeResultSource::Once);
        assert_eq!(result.request_id, Some(RemoteProbeId::from("probe-ok")));
        assert_eq!(result.status, RemoteProbeResultStatus::Success);
        assert!(result.latency_ms.is_some());
        assert_eq!(result.error, None);
    }

    /// 验证 HTTP 探测使用 GET 请求并回传非负耗时。
    #[tokio::test]
    async fn remote_probe_http_success_returns_latency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 512];
            let read = stream.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.starts_with("GET "));
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);

        manager
            .apply(RemoteProbeApply::Once {
                runs: vec![RemoteProbeExecutionRequest {
                    source: RemoteProbeResultSource::Once,
                    point_id: Some(RemoteProbeId::from("point-7")),
                    request_id: Some(RemoteProbeId::from(7)),
                    job_id: None,
                    probe_type: RemoteProbeType::Http,
                    target,
                    timeout: None,
                }],
            })
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_sequence, result) = unwrap_probe_result(event);
        let _ = server.await;

        assert_eq!(result.source, RemoteProbeResultSource::Once);
        assert_eq!(result.point_id, Some(RemoteProbeId::from("point-7")));
        assert_eq!(result.request_id, Some(RemoteProbeId::from(7)));
        assert_eq!(result.status, RemoteProbeResultStatus::Success);
        assert!(result.latency_ms.is_some());
        assert_eq!(result.error, None);
    }

    /// 验证 replace 持续任务会启动 worker 并上报结果。
    #[tokio::test]
    async fn remote_probe_replace_jobs_starts_worker() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
        });
        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);

        manager
            .apply(RemoteProbeApply::Replace {
                generation: 1,
                jobs: vec![RemoteProbeJobSpec {
                    job_id: "job-main".to_string(),
                    point_id: Some(RemoteProbeId::from("point-main")),
                    enabled: true,
                    probe_type: RemoteProbeType::Tcp,
                    target,
                    interval: Duration::from_millis(50),
                    timeout: None,
                }],
            })
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_sequence, result) = unwrap_probe_result(event);
        let _ = server.await;

        assert_eq!(result.source, RemoteProbeResultSource::Job);
        assert_eq!(result.point_id, Some(RemoteProbeId::from("point-main")));
        assert_eq!(result.job_id.as_deref(), Some("job-main"));
        assert_eq!(result.status, RemoteProbeResultStatus::Success);
        assert!(result.latency_ms.is_some());
    }

    /// 验证 patch 可以删除旧任务并启动新任务。
    #[tokio::test]
    async fn remote_probe_patch_replaces_running_job() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_target = first_listener.local_addr().unwrap().to_string();
        let first_server = tokio::spawn(async move {
            let (_stream, _) = first_listener.accept().await.unwrap();
        });
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_target = second_listener.local_addr().unwrap().to_string();
        let second_server = tokio::spawn(async move {
            let (_stream, _) = second_listener.accept().await.unwrap();
        });

        let mut config = AgentConfig::default();
        config.remote_probe.enabled = true;
        let (manager, mut outbound_rx) = probe_manager(config);

        manager
            .apply(RemoteProbeApply::Replace {
                generation: 1,
                jobs: vec![RemoteProbeJobSpec {
                    job_id: "job-old".to_string(),
                    point_id: Some(RemoteProbeId::from("point-old")),
                    enabled: true,
                    probe_type: RemoteProbeType::Tcp,
                    target: first_target,
                    interval: Duration::from_secs(30),
                    timeout: None,
                }],
            })
            .await
            .unwrap();

        let first_event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_sequence, first_result) = unwrap_probe_result(first_event);
        assert_eq!(first_result.source, RemoteProbeResultSource::Job);
        assert_eq!(first_result.job_id.as_deref(), Some("job-old"));

        manager
            .apply(RemoteProbeApply::Patch {
                generation: 2,
                upsert_jobs: vec![RemoteProbeJobSpec {
                    job_id: "job-new".to_string(),
                    point_id: Some(RemoteProbeId::from("point-new")),
                    enabled: true,
                    probe_type: RemoteProbeType::Tcp,
                    target: second_target,
                    interval: Duration::from_millis(50),
                    timeout: None,
                }],
                remove_job_ids: vec!["job-old".to_string()],
            })
            .await
            .unwrap();

        let second_event = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let (_sequence, second_result) = unwrap_probe_result(second_event);

        let _ = first_server.await;
        let _ = second_server.await;

        assert_eq!(second_result.source, RemoteProbeResultSource::Job);
        assert_eq!(
            second_result.point_id,
            Some(RemoteProbeId::from("point-new"))
        );
        assert_eq!(second_result.job_id.as_deref(), Some("job-new"));
        assert_eq!(second_result.status, RemoteProbeResultStatus::Success);
        assert!(second_result.latency_ms.is_some());
    }
}
