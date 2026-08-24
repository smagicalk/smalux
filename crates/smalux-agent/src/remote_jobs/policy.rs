//! Agent 对 Server 下发 Job 的本地安全策略。
//!
//! 策略只作用于 [`RemoteJobController`](super::RemoteJobController) 管理的 Job，不进入
//! Scheduler 通用层，因此 Agent 内部代码创建的本地维护任务不会被本地黑名单误伤。

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use smalux_protocol::agent::v1::{
    AgentJobPolicySnapshot, AgentJobPolicySync, JobDefinition, agent_job_policy_sync,
};
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};

use crate::tasks::task_kind;

/// 一次远程 Job 策略拒绝的稳定原因。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDenial {
    /// 当前 Agent 禁止执行任何远程 Job。
    AllRemoteJobs,
    /// Job 引用的稳定 Task 标识位于拒绝集合中。
    TaskKind(String),
}

impl std::fmt::Display for PolicyDenial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllRemoteJobs => formatter.write_str("all remote jobs are disabled"),
            Self::TaskKind(kind) => {
                write!(formatter, "task kind `{kind}` is denied by local policy")
            }
        }
    }
}

/// Agent 本地拥有、可在运行时修改的远程 Job 策略。
#[derive(Clone, Debug, Default)]
pub struct RemoteJobPolicy {
    revision: u64,
    deny_all: bool,
    denied_task_kinds: BTreeSet<String>,
}

/// 可安全通过 IPC 和日志展示的策略快照。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteJobPolicySnapshot {
    pub revision: u64,
    pub deny_all: bool,
    pub denied_task_kinds: Vec<String>,
}

impl RemoteJobPolicySnapshot {
    /// 判断某个外层 Task kind 是否被当前快照拒绝。
    pub fn denies(&self, task_kind: &str) -> bool {
        self.deny_all
            || self
                .denied_task_kinds
                .binary_search_by(|kind| kind.as_str().cmp(task_kind))
                .is_ok()
    }

    /// 转换成会话内完整快照；本地排序结果会原样保留。
    pub fn to_protocol_message(&self) -> AgentJobPolicySync {
        AgentJobPolicySync {
            body: Some(agent_job_policy_sync::Body::Snapshot(
                AgentJobPolicySnapshot {
                    revision: self.revision,
                    deny_all: self.deny_all,
                    denied_task_kinds: self.denied_task_kinds.clone(),
                },
            )),
        }
    }
}

/// 本地 CLI 支持的策略变化；每个值都可以安全重复执行。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteJobPolicyChange {
    AddTask(String),
    RemoveTask(String),
    DenyAll,
    AllowAll,
}

/// 一次持久化策略修改的结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteJobPolicyUpdate {
    pub changed: bool,
    pub snapshot: RemoteJobPolicySnapshot,
}

const POLICY_FORMAT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct PolicyDocument {
    format_version: u32,
    policy: RemoteJobPolicySnapshot,
}

enum PolicyPersistence {
    Memory,
    File(PathBuf),
}

/// 串行修改并持久化 Agent 本地 Job 策略。
pub struct RemoteJobPolicyManager {
    persistence: PolicyPersistence,
    policy: Mutex<RemoteJobPolicy>,
}

impl RemoteJobPolicy {
    /// 构造已完成 CLI 格式校验的不可变策略。
    pub fn new(deny_all: bool, denied_task_kinds: impl IntoIterator<Item = String>) -> Self {
        Self {
            revision: 0,
            deny_all,
            denied_task_kinds: denied_task_kinds.into_iter().collect(),
        }
    }

    /// 在构造 Task 或修改 Scheduler 前检查一份完整定义。
    pub fn check_definition(&self, definition: &JobDefinition) -> Result<(), PolicyDenial> {
        if self.deny_all {
            return Err(PolicyDenial::AllRemoteJobs);
        }
        if let Some(kind) = definition.task.as_ref().and_then(task_kind)
            && self.denied_task_kinds.contains(&kind)
        {
            return Err(PolicyDenial::TaskKind(kind.to_owned()));
        }
        Ok(())
    }

    /// 检查已安装 Job 的立即运行请求。
    pub fn check_installed(&self, task_kind: &str) -> Result<(), PolicyDenial> {
        if self.deny_all {
            return Err(PolicyDenial::AllRemoteJobs);
        }
        if self.denied_task_kinds.contains(task_kind) {
            return Err(PolicyDenial::TaskKind(task_kind.to_owned()));
        }
        Ok(())
    }

    /// 返回排序稳定、不会包含秘密的策略快照。
    pub fn snapshot(&self) -> RemoteJobPolicySnapshot {
        RemoteJobPolicySnapshot {
            revision: self.revision,
            deny_all: self.deny_all,
            denied_task_kinds: self.denied_task_kinds.iter().cloned().collect(),
        }
    }

    fn from_snapshot(snapshot: RemoteJobPolicySnapshot) -> anyhow::Result<Self> {
        let denied_task_kinds = snapshot
            .denied_task_kinds
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            denied_task_kinds.len() == snapshot.denied_task_kinds.len(),
            "Agent Job policy contains duplicate Task kinds"
        );
        anyhow::ensure!(
            denied_task_kinds.iter().all(|kind| !kind.trim().is_empty()),
            "Agent Job policy contains an empty Task kind"
        );
        Ok(Self {
            revision: snapshot.revision,
            deny_all: snapshot.deny_all,
            denied_task_kinds,
        })
    }
}

impl RemoteJobPolicyManager {
    /// 创建不写磁盘的 Manager，供测试和嵌入式调用方使用。
    pub fn in_memory(policy: RemoteJobPolicy) -> Self {
        Self {
            persistence: PolicyPersistence::Memory,
            policy: Mutex::new(policy),
        }
    }

    /// 从版本化 JSON 文件加载策略；文件不存在时返回 revision 0 的空策略。
    pub async fn load_file(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let policy = match fs::read(&path).await {
            Ok(bytes) => decode_policy_document(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                RemoteJobPolicy::default()
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            persistence: PolicyPersistence::File(path),
            policy: Mutex::new(policy),
        })
    }

    /// 离线备份无法解析的策略文件并创建空策略；返回备份路径。
    pub async fn repair_file(path: impl Into<PathBuf>) -> anyhow::Result<Option<PathBuf>> {
        let path = path.into();
        let backup = if fs::try_exists(&path).await? {
            let backup = sidecar_path(&path, &format!("corrupt-{}", unix_millis()));
            fs::rename(&path, &backup).await?;
            Some(backup)
        } else {
            None
        };
        let manager = Self::load_file(&path).await?;
        manager.persist(&RemoteJobPolicy::default()).await?;
        Ok(backup)
    }

    /// 返回排序稳定、可安全展示和同步的当前快照。
    pub async fn snapshot(&self) -> RemoteJobPolicySnapshot {
        self.policy.lock().await.snapshot()
    }

    /// 原子应用一次策略变化；幂等操作不会写文件或推进 revision。
    pub async fn apply(
        &self,
        change: RemoteJobPolicyChange,
    ) -> anyhow::Result<RemoteJobPolicyUpdate> {
        let mut current = self.policy.lock().await;
        let mut candidate = current.clone();
        let changed = match change {
            RemoteJobPolicyChange::AddTask(kind) => candidate.denied_task_kinds.insert(kind),
            RemoteJobPolicyChange::RemoveTask(kind) => candidate.denied_task_kinds.remove(&kind),
            RemoteJobPolicyChange::DenyAll => !std::mem::replace(&mut candidate.deny_all, true),
            RemoteJobPolicyChange::AllowAll => std::mem::replace(&mut candidate.deny_all, false),
        };
        if !changed {
            return Ok(RemoteJobPolicyUpdate {
                changed: false,
                snapshot: current.snapshot(),
            });
        }
        candidate.revision = candidate
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Agent Job policy revision overflow"))?;
        self.persist(&candidate).await?;
        *current = candidate;
        Ok(RemoteJobPolicyUpdate {
            changed: true,
            snapshot: current.snapshot(),
        })
    }

    /// 在当前锁外提供策略检查，供远程命令执行前读取一致快照。
    pub async fn check_definition(&self, definition: &JobDefinition) -> Result<(), PolicyDenial> {
        self.policy.lock().await.check_definition(definition)
    }

    /// 检查已经安装的 Job 是否仍允许立即运行。
    pub async fn check_installed(&self, task_kind: &str) -> Result<(), PolicyDenial> {
        self.policy.lock().await.check_installed(task_kind)
    }

    async fn persist(&self, policy: &RemoteJobPolicy) -> anyhow::Result<()> {
        let PolicyPersistence::File(path) = &self.persistence else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let bytes = serde_json::to_vec_pretty(&PolicyDocument {
            format_version: POLICY_FORMAT_VERSION,
            policy: policy.snapshot(),
        })?;
        let new_path = sidecar_path(path, "new");
        let backup_path = sidecar_path(path, "bak");
        let mut file = fs::File::create(&new_path).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);

        let _ = fs::remove_file(&backup_path).await;
        if fs::try_exists(path).await? {
            fs::rename(path, &backup_path).await?;
        }
        if let Err(error) = fs::rename(&new_path, path).await {
            if fs::try_exists(&backup_path).await? {
                let _ = fs::rename(&backup_path, path).await;
            }
            return Err(error.into());
        }
        let _ = fs::remove_file(backup_path).await;
        Ok(())
    }
}

fn decode_policy_document(bytes: &[u8]) -> anyhow::Result<RemoteJobPolicy> {
    let document: PolicyDocument = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        document.format_version == POLICY_FORMAT_VERSION,
        "unsupported Agent Job policy format version {}",
        document.format_version
    );
    RemoteJobPolicy::from_snapshot(document.policy)
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(format!(".{suffix}"));
    PathBuf::from(value)
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{RemoteJobPolicyChange, RemoteJobPolicyManager};

    #[tokio::test]
    async fn file_policy_manager_persists_a_real_change_and_revision() {
        let directory =
            std::env::temp_dir().join(format!("smalux-agent-policy-{}", Uuid::new_v4()));
        let path = directory.join("job-policy.json");
        let manager = RemoteJobPolicyManager::load_file(&path).await.unwrap();

        let update = manager
            .apply(RemoteJobPolicyChange::AddTask(
                "smalux.collect.process.v1".to_owned(),
            ))
            .await
            .unwrap();
        assert!(update.changed);
        assert_eq!(update.snapshot.revision, 1);
        drop(manager);

        let reloaded = RemoteJobPolicyManager::load_file(&path).await.unwrap();
        let snapshot = reloaded.snapshot().await;
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.denied_task_kinds, ["smalux.collect.process.v1"]);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn allow_all_keeps_task_specific_denials() {
        let manager = RemoteJobPolicyManager::in_memory(Default::default());
        manager
            .apply(RemoteJobPolicyChange::AddTask(
                "smalux.collect.cpu.v1".into(),
            ))
            .await
            .unwrap();
        manager.apply(RemoteJobPolicyChange::DenyAll).await.unwrap();
        let update = manager
            .apply(RemoteJobPolicyChange::AllowAll)
            .await
            .unwrap();
        assert!(!update.snapshot.deny_all);
        assert_eq!(update.snapshot.denied_task_kinds, ["smalux.collect.cpu.v1"]);
    }

    #[tokio::test]
    async fn repair_backs_up_corrupt_file_and_creates_empty_policy() {
        let directory =
            std::env::temp_dir().join(format!("smalux-agent-policy-repair-{}", Uuid::new_v4()));
        let path = directory.join("job-policy.json");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        tokio::fs::write(&path, b"not-json").await.unwrap();

        let backup = RemoteJobPolicyManager::repair_file(&path)
            .await
            .unwrap()
            .expect("corrupt policy is backed up");
        assert_eq!(tokio::fs::read(&backup).await.unwrap(), b"not-json");
        let manager = RemoteJobPolicyManager::load_file(&path).await.unwrap();
        assert_eq!(manager.snapshot().await, Default::default());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
