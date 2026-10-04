//! 当前进程内 Agent Session 的实时目录。
//!
//! 数据库保存长期 Agent 身份，这里只保存随连接存在的诊断状态和独立取消令牌。记录不含
//! Noise 密钥、注册 Token 或业务明文；Server 重启后目录自然清空。

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::SystemTime,
};

use smalux_protocol::agent::v1::AgentReconcileSummary;
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Session 在 Server 端认证状态机中的粗粒度阶段。
pub(crate) enum SessionStage {
    /// gRPC 流已接受，但尚未完成 Noise 模式识别和握手。
    Handshaking,
    /// 已选择 XXpsk3，正在使用一次性 Token 完成首次注册。
    Registering,
    /// 已通过 IK 验证持久化 Agent 身份，可以处理业务消息。
    Authenticated,
}

impl SessionStage {
    /// 返回 IPC 使用的稳定小写名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Handshaking => "handshaking",
            Self::Registering => "registering",
            Self::Authenticated => "authenticated",
        }
    }
}

#[derive(Clone, Debug)]
/// 供管理接口读取的 Session 快照，不包含流、密钥或业务载荷。
pub(crate) struct SessionSnapshot {
    /// 当前 Server 进程内分配的临时 ID。
    pub session_id: u64,
    /// 完成身份认证后写入的稳定 Agent ID。
    pub agent_id: Option<String>,
    /// 当前连接选择的 Noise 握手模式。
    pub authentication_mode: Option<String>,
    /// 当前认证阶段。
    pub stage: SessionStage,
    /// 接受连接的时间，Unix epoch 微秒。
    pub connected_at_unix_micros: i64,
    /// 最近状态变化或业务活动时间，Unix epoch 微秒。
    pub last_activity_at_unix_micros: i64,
}

/// 内部可取消句柄与对外安全快照的组合。
struct SessionEntry {
    snapshot: SessionSnapshot,
    cancellation: CancellationToken,
    /// 权威 Job catalog 发生变更时唤醒业务循环；watch 会合并高频连续更新。
    catalog_change: watch::Sender<u64>,
    /// Plus Worker runtime 发生变更时唤醒业务循环。
    ///
    /// 目录和 runtime 使用两个独立信号，修改普通 Job 时不会无条件重启 Worker。
    runtime_change: watch::Sender<u64>,
}

#[derive(Clone, Default)]
/// 当前进程共享的实时 Session 目录。
///
/// `RwLock<BTreeMap<...>>` 让查询获得稳定的 ID 顺序；所有公开方法都在 `.await` 返回前
/// 释放锁，不会把锁守卫带入数据库或网络操作。
pub(crate) struct SessionRegistry {
    entries: Arc<RwLock<BTreeMap<u64, SessionEntry>>>,
    /// 当前 Server 进程最近见到的每个 Agent 进程实例 ID。
    ///
    /// 该缓存只用于重连优化，不是持久化授权状态；Server 重启后第一次连接自然执行
    /// 完整同步，新的 Agent 进程实例也必须执行完整同步。
    reconcile_instances: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SessionRegistry {
    /// 登记一个刚被 gRPC handler 接受的 Session，并返回其独立取消令牌。
    ///
    /// worker 必须在结束时调用 [`Self::remove`]，否则管理状态会保留陈旧连接。
    pub(crate) async fn register(&self, session_id: u64) -> CancellationToken {
        let cancellation = CancellationToken::new();
        let (catalog_change, _) = watch::channel(0u64);
        let (runtime_change, _) = watch::channel(0u64);
        let now = unix_timestamp_micros();
        self.entries.write().await.insert(
            session_id,
            SessionEntry {
                snapshot: SessionSnapshot {
                    session_id,
                    agent_id: None,
                    authentication_mode: None,
                    stage: SessionStage::Handshaking,
                    connected_at_unix_micros: now,
                    last_activity_at_unix_micros: now,
                },
                cancellation: cancellation.clone(),
                catalog_change,
                runtime_change,
            },
        );
        cancellation
    }

    /// 标记 Session 已选择首次注册使用的 XXpsk3 模式。
    pub(crate) async fn mark_registering(&self, session_id: u64) {
        self.update(session_id, SessionStage::Registering, None, Some("xxpsk3"))
            .await;
    }

    /// 在 IK 授权成功后绑定稳定 Agent ID 和实际认证模式。
    pub(crate) async fn mark_authenticated(
        &self,
        session_id: u64,
        agent_id: &str,
        authentication_mode: &str,
    ) {
        self.update(
            session_id,
            SessionStage::Authenticated,
            Some(agent_id),
            Some(authentication_mode),
        )
        .await;
    }

    /// 在一次写锁内更新阶段、可选身份和最后活动时间。
    async fn update(
        &self,
        session_id: u64,
        stage: SessionStage,
        agent_id: Option<&str>,
        authentication_mode: Option<&str>,
    ) {
        if let Some(entry) = self.entries.write().await.get_mut(&session_id) {
            entry.snapshot.stage = stage;
            if let Some(agent_id) = agent_id {
                entry.snapshot.agent_id = Some(agent_id.to_owned());
            }
            if let Some(authentication_mode) = authentication_mode {
                entry.snapshot.authentication_mode = Some(authentication_mode.to_owned());
            }
            entry.snapshot.last_activity_at_unix_micros = unix_timestamp_micros();
        }
    }

    /// 业务循环每处理一条有效消息后刷新活动时间。
    pub(crate) async fn touch(&self, session_id: u64) {
        if let Some(entry) = self.entries.write().await.get_mut(&session_id) {
            entry.snapshot.last_activity_at_unix_micros = unix_timestamp_micros();
        }
    }

    /// 复制全部安全快照；返回后调用者不再持有目录锁。
    pub(crate) async fn list(&self) -> Vec<SessionSnapshot> {
        self.entries
            .read()
            .await
            .values()
            .map(|entry| entry.snapshot.clone())
            .collect()
    }

    /// 查询一个 Session 的安全快照。
    pub(crate) async fn get(&self, session_id: u64) -> Option<SessionSnapshot> {
        self.entries
            .read()
            .await
            .get(&session_id)
            .map(|entry| entry.snapshot.clone())
    }

    /// 向一个 Session 发出取消信号。
    ///
    /// 返回 `true` 只表示已找到并触发令牌；worker 完成清理前记录仍可能短暂可见。
    pub(crate) async fn disconnect(&self, session_id: u64) -> bool {
        let entries = self.entries.read().await;
        let Some(entry) = entries.get(&session_id) else {
            return false;
        };
        entry.cancellation.cancel();
        true
    }

    /// 取消属于指定 Agent 的全部 Session，返回匹配数量。
    pub(crate) async fn disconnect_agent(&self, agent_id: &str) -> usize {
        let entries = self.entries.read().await;
        let matches = entries
            .values()
            .filter(|entry| entry.snapshot.agent_id.as_deref() == Some(agent_id))
            .collect::<Vec<_>>();
        for entry in &matches {
            entry.cancellation.cancel();
        }
        matches.len()
    }

    /// 订阅指定 Session 的权威目录变化；Session 已结束时返回 None。
    pub(crate) async fn subscribe_catalog_changes(
        &self,
        session_id: u64,
    ) -> Option<watch::Receiver<u64>> {
        self.entries
            .read()
            .await
            .get(&session_id)
            .map(|entry| entry.catalog_change.subscribe())
    }

    /// 订阅指定 Session 的 Plus runtime 变化；Session 已结束时返回 None。
    pub(crate) async fn subscribe_runtime_changes(
        &self,
        session_id: u64,
    ) -> Option<watch::Receiver<u64>> {
        self.entries
            .read()
            .await
            .get(&session_id)
            .map(|entry| entry.runtime_change.subscribe())
    }

    /// 通知指定 Agent 的全部在线会话在安全边界重新读取并下发完整目录。
    pub(crate) async fn notify_catalog_changed(&self, agent_id: &str) -> usize {
        let entries = self.entries.read().await;
        let mut notified = 0;
        for entry in entries.values() {
            if entry.snapshot.agent_id.as_deref() == Some(agent_id) {
                entry.catalog_change.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
                notified += 1;
            }
        }
        notified
    }

    /// 通知指定 Agent 的全部在线会话重新读取并下发 runtime 快照。
    pub(crate) async fn notify_runtime_changed(&self, agent_id: &str) -> usize {
        let entries = self.entries.read().await;
        let mut notified = 0;
        for entry in entries.values() {
            if entry.snapshot.agent_id.as_deref() == Some(agent_id) {
                entry.runtime_change.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
                notified += 1;
            }
        }
        notified
    }

    /// 记录 Agent 对账摘要的进程实例，并返回是否是该 Agent 的新进程实例。
    pub(crate) async fn observe_reconcile_instance(
        &self,
        agent_id: &str,
        summary: &AgentReconcileSummary,
    ) -> bool {
        let mut instances = self.reconcile_instances.write().await;
        let changed = instances
            .get(agent_id)
            .is_none_or(|current| current.as_slice() != summary.instance_id.as_slice());
        instances.insert(agent_id.to_owned(), summary.instance_id.clone());
        changed
    }

    /// worker 退出后删除实时记录；删除不存在的 ID 保持幂等。
    pub(crate) async fn remove(&self, session_id: u64) {
        self.entries.write().await.remove(&session_id);
    }

    /// 返回包括握手中、注册中和已认证连接在内的当前记录数。
    pub(crate) async fn len(&self) -> usize {
        self.entries.read().await.len()
    }
}

/// 获取用于诊断显示的 Unix epoch 微秒时间；系统时钟早于 epoch 时回退为 0。
fn unix_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_micros().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{SessionRegistry, SessionStage};
    use smalux_protocol::agent::v1::AgentReconcileSummary;

    #[tokio::test]
    async fn disconnecting_an_agent_cancels_only_its_sessions() {
        let registry = SessionRegistry::default();
        let first = registry.register(1).await;
        let second = registry.register(2).await;
        registry.mark_authenticated(1, "agent-a", "ik").await;
        registry.mark_authenticated(2, "agent-b", "ik").await;

        assert_eq!(registry.disconnect_agent("agent-a").await, 1);
        assert!(first.is_cancelled());
        assert!(!second.is_cancelled());
        assert_eq!(
            registry.get(2).await.unwrap().stage,
            SessionStage::Authenticated
        );
    }

    #[tokio::test]
    async fn removing_a_finished_session_removes_its_snapshot() {
        let registry = SessionRegistry::default();
        registry.register(7).await;
        registry.remove(7).await;
        assert!(registry.get(7).await.is_none());
    }

    #[tokio::test]
    async fn catalog_and_runtime_notifications_are_independent() {
        let registry = SessionRegistry::default();
        registry.register(1).await;
        registry.mark_authenticated(1, "agent-a", "ik").await;
        let mut catalog = registry.subscribe_catalog_changes(1).await.unwrap();
        let mut runtime = registry.subscribe_runtime_changes(1).await.unwrap();

        assert_eq!(registry.notify_catalog_changed("agent-a").await, 1);
        assert!(catalog.changed().await.is_ok());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), runtime.changed())
                .await
                .is_err()
        );

        assert_eq!(registry.notify_runtime_changed("agent-a").await, 1);
        assert!(runtime.changed().await.is_ok());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), catalog.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn new_agent_process_instance_is_detected_across_reconnects() {
        let registry = SessionRegistry::default();
        let first = AgentReconcileSummary {
            instance_id: vec![1; 16],
            ..Default::default()
        };
        let same = first.clone();
        let second = AgentReconcileSummary {
            instance_id: vec![2; 16],
            ..Default::default()
        };
        assert!(registry.observe_reconcile_instance("agent-a", &first).await);
        assert!(!registry.observe_reconcile_instance("agent-a", &same).await);
        assert!(
            registry
                .observe_reconcile_instance("agent-a", &second)
                .await
        );
    }
}
