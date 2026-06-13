//! 通用远程 job 管理器。
//!
//! 本模块只处理 `job_apply` 的通用外壳：operation、generation、job_id 去重和 kind
//! 分发。具体执行能力仍由各自 executor 负责，首版只有 `kind=probe`，复用现有
//! `RemoteProbeManager` 的限频、worker 和结果投递逻辑。

use super::probe::{RemoteProbeApply, RemoteProbeExecutionRequest, RemoteProbeJobSpec};
use smalux_protocol::{
    RemoteJobApplyRequest, RemoteJobOperation, RemoteJobRunRequest, RemoteJobSpec, RemoteProbeId,
    RemoteProbeResultSource,
};
use std::collections::HashSet;

/// 通用远程 job 管理器。
#[derive(Debug, Clone)]
pub(crate) struct RemoteJobManager {
    /// 网络探测 job executor。
    probe: super::probe::RemoteProbeManager,
}

impl RemoteJobManager {
    /// 创建通用 job 管理器。
    pub(crate) const fn new(probe: super::probe::RemoteProbeManager) -> Self {
        Self { probe }
    }

    /// 应用一次性 job 或持续 job 更新。
    pub(crate) async fn apply(&self, request: RemoteJobApply) -> anyhow::Result<()> {
        match request {
            RemoteJobApply::Once { runs } => {
                self.probe.apply(RemoteProbeApply::Once { runs }).await
            }
            RemoteJobApply::Replace { generation, jobs } => {
                self.probe
                    .apply(RemoteProbeApply::Replace { generation, jobs })
                    .await
            }
            RemoteJobApply::Patch {
                generation,
                upsert_jobs,
                remove_job_ids,
            } => {
                self.probe
                    .apply(RemoteProbeApply::Patch {
                        generation,
                        upsert_jobs,
                        remove_job_ids,
                    })
                    .await
            }
        }
    }
}

/// agent 内部的通用 job 请求。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RemoteJobApply {
    /// 立即运行一批一次性 job。
    Once {
        /// 一次性 probe 运行列表。
        runs: Vec<RemoteProbeExecutionRequest>,
    },
    /// 用新 job 列表整组替换当前持续 job。
    Replace {
        /// job 代际。
        generation: u64,
        /// probe job 列表。
        jobs: Vec<RemoteProbeJobSpec>,
    },
    /// 增量更新当前持续 job。
    Patch {
        /// job 代际。
        generation: u64,
        /// 需要新增或更新的 probe job。
        upsert_jobs: Vec<RemoteProbeJobSpec>,
        /// 需要删除的 job ID。
        remove_job_ids: Vec<String>,
    },
}

impl TryFrom<RemoteJobApplyRequest> for RemoteJobApply {
    type Error = anyhow::Error;

    fn try_from(request: RemoteJobApplyRequest) -> Result<Self, Self::Error> {
        match request.operation {
            RemoteJobOperation::Once => parse_once_jobs(request),
            RemoteJobOperation::Replace => parse_replace_jobs(request),
            RemoteJobOperation::Patch => parse_patch_jobs(request),
        }
    }
}

/// 解析一次性 job 请求。
fn parse_once_jobs(request: RemoteJobApplyRequest) -> anyhow::Result<RemoteJobApply> {
    if request.runs.is_empty() {
        anyhow::bail!("job_apply.once requires at least one run");
    }
    if !request.jobs.is_empty()
        || !request.upsert_jobs.is_empty()
        || !request.remove_job_ids.is_empty()
    {
        anyhow::bail!("job_apply.once only accepts runs");
    }

    let mut request_ids = HashSet::new();
    let mut runs = Vec::with_capacity(request.runs.len());
    for run in request.runs {
        let run = probe_run_request(run)?;
        let display = run
            .request_id
            .as_ref()
            .map(RemoteProbeId::display)
            .unwrap_or_default();
        if !request_ids.insert(display.clone()) {
            anyhow::bail!("duplicate job run request_id: {display}");
        }
        runs.push(run);
    }

    Ok(RemoteJobApply::Once { runs })
}

/// 解析整组替换 job 请求。
fn parse_replace_jobs(request: RemoteJobApplyRequest) -> anyhow::Result<RemoteJobApply> {
    let generation = request
        .generation
        .ok_or_else(|| anyhow::anyhow!("job_apply.replace requires generation"))?;
    if !request.runs.is_empty()
        || !request.upsert_jobs.is_empty()
        || !request.remove_job_ids.is_empty()
    {
        anyhow::bail!("job_apply.replace only accepts jobs");
    }

    Ok(RemoteJobApply::Replace {
        generation,
        jobs: probe_job_specs(request.jobs)?,
    })
}

/// 解析增量 patch job 请求。
fn parse_patch_jobs(request: RemoteJobApplyRequest) -> anyhow::Result<RemoteJobApply> {
    let generation = request
        .generation
        .ok_or_else(|| anyhow::anyhow!("job_apply.patch requires generation"))?;
    if !request.runs.is_empty() || !request.jobs.is_empty() {
        anyhow::bail!("job_apply.patch only accepts upsert_jobs/remove_job_ids");
    }

    Ok(RemoteJobApply::Patch {
        generation,
        upsert_jobs: probe_job_specs(request.upsert_jobs)?,
        remove_job_ids: normalize_job_ids(request.remove_job_ids)?,
    })
}

/// 转换一次性 probe job。
fn probe_run_request(request: RemoteJobRunRequest) -> anyhow::Result<RemoteProbeExecutionRequest> {
    match request {
        RemoteJobRunRequest::Probe {
            request_id,
            point_id,
            probe_type,
            target,
            timeout,
        } => RemoteProbeExecutionRequest {
            source: RemoteProbeResultSource::Once,
            point_id,
            request_id: Some(request_id),
            job_id: None,
            probe_type,
            target,
            timeout,
        }
        .validated(),
    }
}

/// 转换持续 probe job 列表。
fn probe_job_specs(jobs: Vec<RemoteJobSpec>) -> anyhow::Result<Vec<RemoteProbeJobSpec>> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::with_capacity(jobs.len());
    for job in jobs {
        let spec = match job {
            RemoteJobSpec::Probe {
                job_id,
                point_id,
                enabled,
                probe_type,
                target,
                interval,
                timeout,
            } => RemoteProbeJobSpec {
                job_id,
                point_id,
                enabled,
                probe_type,
                target,
                interval,
                timeout,
            }
            .validated()?,
        };
        if !seen.insert(spec.job_id.clone()) {
            anyhow::bail!("duplicate job_id: {}", spec.job_id);
        }
        parsed.push(spec);
    }
    Ok(parsed)
}

/// 归一化删除 job ID 列表。
fn normalize_job_ids(job_ids: Vec<String>) -> anyhow::Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(job_ids.len());
    for job_id in job_ids {
        let job_id = job_id.trim().to_string();
        if job_id.is_empty() {
            anyhow::bail!("job_id cannot be empty");
        }
        if seen.insert(job_id.clone()) {
            normalized.push(job_id);
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    //! 通用 job 请求解析测试。

    use super::*;
    use smalux_protocol::{RemoteJobKind, RemoteProbeType};
    use std::time::Duration;

    /// 验证一次性 probe job 会转换为 probe 执行请求。
    #[test]
    fn job_once_probe_converts_to_probe_run() {
        let apply: RemoteJobApply = RemoteJobApplyRequest {
            operation: RemoteJobOperation::Once,
            generation: None,
            runs: vec![RemoteJobRunRequest::Probe {
                request_id: RemoteProbeId::from("run-1"),
                point_id: Some(RemoteProbeId::from("point-1")),
                probe_type: RemoteProbeType::Tcp,
                target: "example.com:443".to_string(),
                timeout: None,
            }],
            jobs: Vec::new(),
            upsert_jobs: Vec::new(),
            remove_job_ids: Vec::new(),
        }
        .try_into()
        .unwrap();

        let RemoteJobApply::Once { runs } = apply else {
            panic!("expected once job");
        };
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].request_id, Some(RemoteProbeId::from("run-1")));
        assert_eq!(
            RemoteJobRunRequest::Probe {
                request_id: RemoteProbeId::from("x"),
                point_id: None,
                probe_type: RemoteProbeType::Tcp,
                target: "localhost:80".to_string(),
                timeout: None,
            }
            .kind(),
            RemoteJobKind::Probe
        );
    }

    /// 验证持续 probe job 需要 generation。
    #[test]
    fn job_replace_requires_generation() {
        let error = RemoteJobApply::try_from(RemoteJobApplyRequest {
            operation: RemoteJobOperation::Replace,
            generation: None,
            runs: Vec::new(),
            jobs: Vec::new(),
            upsert_jobs: Vec::new(),
            remove_job_ids: Vec::new(),
        })
        .unwrap_err();

        assert!(error.to_string().contains("requires generation"));
    }

    /// 验证 patch 会去重删除 ID。
    #[test]
    fn job_patch_normalizes_remove_ids() {
        let apply: RemoteJobApply = RemoteJobApplyRequest {
            operation: RemoteJobOperation::Patch,
            generation: Some(7),
            runs: Vec::new(),
            jobs: Vec::new(),
            upsert_jobs: Vec::new(),
            remove_job_ids: vec![" a ".to_string(), "a".to_string(), "b".to_string()],
        }
        .try_into()
        .unwrap();

        let RemoteJobApply::Patch { remove_job_ids, .. } = apply else {
            panic!("expected patch job");
        };
        assert_eq!(remove_job_ids, vec!["a".to_string(), "b".to_string()]);
    }

    /// 验证持续 probe job 会保留通用 interval。
    #[test]
    fn job_replace_probe_keeps_interval() {
        let apply: RemoteJobApply = RemoteJobApplyRequest {
            operation: RemoteJobOperation::Replace,
            generation: Some(3),
            runs: Vec::new(),
            jobs: vec![RemoteJobSpec::Probe {
                job_id: "probe-1".to_string(),
                point_id: None,
                enabled: true,
                probe_type: RemoteProbeType::Http,
                target: "https://example.com".to_string(),
                interval: Duration::from_secs(30),
                timeout: None,
            }],
            upsert_jobs: Vec::new(),
            remove_job_ids: Vec::new(),
        }
        .try_into()
        .unwrap();

        let RemoteJobApply::Replace { generation, jobs } = apply else {
            panic!("expected replace job");
        };
        assert_eq!(generation, 3);
        assert_eq!(jobs[0].job_id, "probe-1");
        assert_eq!(jobs[0].interval, Duration::from_secs(30));
    }
}
