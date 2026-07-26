//! Agent 与 Server 长期 Noise 静态密钥轮换状态机。
//!
//! 本模块不执行 I/O。每次改变状态后，调用方必须先取得 `snapshot()` 并原子持久化，
//! 再发送网络确认或进入下一阶段。这样进程崩溃后可以从明确阶段恢复，而不是猜测密钥状态。

use crate::agent::v1::{AgentKeyRotationRequest, ServerKeyAnnouncement};

use super::{KeyId, NoiseError, NoiseIdentity, NoisePublicKey, RotationId};

/// Agent 私钥状态的可持久化快照；调用方决定使用数据库还是本地文件。
#[derive(Clone)]
pub struct AgentKeySetSnapshot {
    /// 当前稳定使用的 Agent 长期身份。
    pub current: NoiseIdentity,
    /// 已生成但尚未最终确认的新 Agent 身份。
    pub pending: Option<NoiseIdentity>,
    /// 与 pending 一一对应的轮换事务 ID。
    pub rotation_id: Option<RotationId>,
}

/// Agent 本地持有的长期私钥状态机。
pub struct AgentKeySet {
    /// 所有可恢复状态集中在一个可克隆 snapshot 中。
    state: AgentKeySetSnapshot,
}

/// `prepare_rotation` 的返回值，包含本地新身份和可直接发送的公开请求。
pub struct AgentRotationPrepared {
    /// 本次轮换事务 ID。
    pub rotation_id: RotationId,
    /// 新生成的完整身份；含私钥，必须按敏感数据处理。
    pub new_identity: NoiseIdentity,
    /// 只包含 rotation ID、新公钥和 key ID 的 Protobuf 请求。
    pub request: AgentKeyRotationRequest,
}

impl AgentKeySet {
    /// 用已持久化的稳定身份创建没有 pending 的初始状态。
    pub fn new(current: NoiseIdentity) -> Self {
        Self {
            state: AgentKeySetSnapshot {
                current,
                pending: None,
                rotation_id: None,
            },
        }
    }

    /// 从数据库或本地文件恢复状态，并验证 pending 与 rotation ID 同时存在或同时缺失。
    pub fn from_snapshot(snapshot: AgentKeySetSnapshot) -> Result<Self, NoiseError> {
        if snapshot.pending.is_some() != snapshot.rotation_id.is_some() {
            return Err(NoiseError::NoPendingRotation);
        }
        Ok(Self { state: snapshot })
    }

    /// 生成 pending 身份和事务 ID，并返回可发送给 Server 的轮换请求。
    ///
    /// 方法成功后内存状态已经改变；发送请求前必须保存 `snapshot()`。
    pub fn prepare_rotation(&mut self) -> Result<AgentRotationPrepared, NoiseError> {
        // 同一 Agent 同时只允许一笔轮换，避免确认消息匹配到错误私钥。
        if self.state.pending.is_some() {
            return Err(NoiseError::RotationAlreadyInProgress);
        }
        let identity = NoiseIdentity::generate()?;
        let rotation_id = RotationId::generate()?;
        self.state.pending = Some(identity.clone());
        self.state.rotation_id = Some(rotation_id);
        Ok(AgentRotationPrepared {
            rotation_id,
            new_identity: identity.clone(),
            request: AgentKeyRotationRequest {
                rotation_id: rotation_id.as_bytes().to_vec(),
                new_public_key: identity.public_key().as_bytes().to_vec(),
                new_key_id: identity.key_id().as_bytes().to_vec(),
            },
        })
    }

    /// 返回 IK 尝试身份，顺序固定为 pending 优先、current 回退。
    pub fn connection_candidates(&self) -> Vec<&NoiseIdentity> {
        self.state
            .pending
            .iter()
            .chain(std::iter::once(&self.state.current))
            .collect()
    }

    /// 收到 Server 接受消息后，把指定 pending 提升为唯一 current。
    ///
    /// Agent 旧身份不会保留 previous；需要回滚时应在 promote 前使用候选连接机制。
    pub fn promote_pending(&mut self, rotation_id: RotationId) -> Result<(), NoiseError> {
        // ID 不匹配通常表示过期确认或数据库状态被另一个事务更新。
        if self.state.rotation_id != Some(rotation_id) {
            return Err(NoiseError::RotationIdMismatch);
        }
        self.state.current = self
            .state
            .pending
            .take()
            .ok_or(NoiseError::NoPendingRotation)?;
        self.state.rotation_id = None;
        Ok(())
    }

    /// Server 拒绝轮换或事务超时后，丢弃尚未生效的 pending 私钥。
    pub fn cancel_rotation(&mut self) -> Result<(), NoiseError> {
        if self.state.pending.take().is_none() {
            return Err(NoiseError::NoPendingRotation);
        }
        self.state.rotation_id = None;
        Ok(())
    }

    /// 克隆完整 Agent 私钥状态，供调用方原子写入数据库或本地文件。
    pub fn snapshot(&self) -> AgentKeySetSnapshot {
        self.state.clone()
    }
}

#[derive(Clone, Debug)]
/// Server 为单个 Agent 保存的授权公钥 snapshot。
pub struct AgentPublicKeySetSnapshot {
    /// 当前稳定授权的 Agent 公钥。
    pub current: NoisePublicKey,
    /// 轮换期间额外允许的新 Agent 公钥。
    pub pending: Option<NoisePublicKey>,
    /// 与 pending 对应的轮换事务 ID。
    pub rotation_id: Option<RotationId>,
}

/// Server 侧单个 Agent 的授权公钥状态机。
pub struct AgentPublicKeySet {
    /// 可原子持久化的全部授权状态。
    state: AgentPublicKeySetSnapshot,
}

impl AgentPublicKeySet {
    /// 用首次注册得到的 Agent 公钥创建稳定状态。
    pub fn new(current: NoisePublicKey) -> Self {
        Self {
            state: AgentPublicKeySetSnapshot {
                current,
                pending: None,
                rotation_id: None,
            },
        }
    }

    /// 从持久化 snapshot 恢复授权集合，并校验 pending 与事务 ID 的一致性。
    pub fn from_snapshot(snapshot: AgentPublicKeySetSnapshot) -> Result<Self, NoiseError> {
        if snapshot.pending.is_some() != snapshot.rotation_id.is_some() {
            return Err(NoiseError::NoPendingRotation);
        }
        Ok(Self { state: snapshot })
    }

    /// 校验 Agent 轮换请求，并把新公钥加入临时授权窗口。
    ///
    /// Server 只接收公钥；重新计算 key ID 可防止请求内的冗余字段互相矛盾。
    pub fn stage(&mut self, request: &AgentKeyRotationRequest) -> Result<(), NoiseError> {
        if self.state.pending.is_some() {
            return Err(NoiseError::RotationAlreadyInProgress);
        }
        let rotation_id = RotationId::from_bytes(&request.rotation_id)?;
        let key = NoisePublicKey::from_bytes(&request.new_public_key)?;
        if key.key_id() != KeyId::from_bytes(&request.new_key_id)? {
            return Err(NoiseError::AuthenticationFailed);
        }
        self.state.pending = Some(key);
        self.state.rotation_id = Some(rotation_id);
        Ok(())
    }

    /// 判断握手认证出的 Agent 静态公钥是否属于 current 或 pending。
    pub fn authorize(&self, key: NoisePublicKey) -> bool {
        self.state.current == key || self.state.pending == Some(key)
    }

    /// 新 Agent 身份完成 IK 后，把 pending 提升为唯一 current 授权公钥。
    pub fn promote_pending(&mut self, rotation_id: RotationId) -> Result<(), NoiseError> {
        if self.state.rotation_id != Some(rotation_id) {
            return Err(NoiseError::RotationIdMismatch);
        }
        self.state.current = self
            .state
            .pending
            .take()
            .ok_or(NoiseError::NoPendingRotation)?;
        self.state.rotation_id = None;
        Ok(())
    }

    /// Agent 换钥被拒绝或超时后，撤销 pending 公钥授权。
    pub fn cancel_rotation(&mut self) -> Result<(), NoiseError> {
        if self.state.pending.take().is_none() {
            return Err(NoiseError::NoPendingRotation);
        }
        self.state.rotation_id = None;
        Ok(())
    }

    /// 克隆 Server 保存的单 Agent 授权状态，供调用方持久化。
    pub fn snapshot(&self) -> AgentPublicKeySetSnapshot {
        self.state.clone()
    }
}

#[derive(Clone)]
/// Server 长期私钥环的可持久化 snapshot。
pub struct ServerKeyRingSnapshot {
    /// 新连接默认使用的 Server 身份。
    pub current: NoiseIdentity,
    /// 已生成并公告、尚未 promote 的下一把身份。
    pub next: Option<NoiseIdentity>,
    /// promote 后暂时保留的上一把身份，用于迁移期旧 Agent。
    pub previous: Option<NoiseIdentity>,
    /// 与 next 对应的轮换事务 ID；promote 后清空。
    pub rotation_id: Option<RotationId>,
}

/// `ServerKeyRing::prepare_rotation` 的完整返回值。
pub struct ServerRotationPrepared {
    /// 本次 Server 换钥事务 ID。
    pub rotation_id: RotationId,
    /// 新生成的完整 Server 身份；含私钥。
    pub next_identity: NoiseIdentity,
    /// 可通过已认证旧会话发送给 Agent 的公开公告。
    pub announcement: ServerKeyAnnouncement,
}

/// Server 同时接受 current/next/previous 的长期私钥环。
pub struct ServerKeyRing {
    /// 所有恢复所需状态。
    state: ServerKeyRingSnapshot,
}

impl ServerKeyRing {
    /// 用稳定 Server 身份创建没有轮换窗口的 keyring。
    pub fn new(current: NoiseIdentity) -> Self {
        Self {
            state: ServerKeyRingSnapshot {
                current,
                next: None,
                previous: None,
                rotation_id: None,
            },
        }
    }

    /// 从持久化 snapshot 恢复，并校验 next 与 rotation ID 同时存在或同时缺失。
    pub fn from_snapshot(snapshot: ServerKeyRingSnapshot) -> Result<Self, NoiseError> {
        if snapshot.next.is_some() != snapshot.rotation_id.is_some() {
            return Err(NoiseError::NoPendingRotation);
        }
        Ok(Self { state: snapshot })
    }

    /// 生成 next 身份与公开公告，开启一笔 Server 长期密钥轮换。
    ///
    /// 成功后应先持久化 `snapshot()`，再向 Agent 发送 `announcement`。
    pub fn prepare_rotation(&mut self) -> Result<ServerRotationPrepared, NoiseError> {
        if self.state.next.is_some() || self.state.previous.is_some() {
            return Err(NoiseError::RotationAlreadyInProgress);
        }
        let next = NoiseIdentity::generate()?;
        let rotation_id = RotationId::generate()?;
        self.state.next = Some(next.clone());
        self.state.rotation_id = Some(rotation_id);
        Ok(ServerRotationPrepared {
            rotation_id,
            next_identity: next.clone(),
            announcement: ServerKeyAnnouncement {
                rotation_id: rotation_id.as_bytes().to_vec(),
                new_public_key: next.public_key().as_bytes().to_vec(),
                new_key_id: next.key_id().as_bytes().to_vec(),
            },
        })
    }

    /// 返回当前可用于接受 IK 的全部身份，顺序为 current、next、previous。
    pub fn active_keys(&self) -> Vec<&NoiseIdentity> {
        std::iter::once(&self.state.current)
            .chain(self.state.next.iter())
            .chain(self.state.previous.iter())
            .collect()
    }

    /// 按 Client 首帧携带的 key ID 查找对应的活跃 Server 私钥。
    pub fn find_active(&self, key_id: KeyId) -> Option<&NoiseIdentity> {
        self.active_keys()
            .into_iter()
            .find(|identity| identity.key_id() == key_id)
    }

    /// Agent 已保存新公钥后，把 next 提升为 current，并暂时保留旧 current。
    pub fn promote_next(&mut self, rotation_id: RotationId) -> Result<(), NoiseError> {
        if self.state.rotation_id != Some(rotation_id) {
            return Err(NoiseError::RotationIdMismatch);
        }
        let next = self
            .state
            .next
            .take()
            .ok_or(NoiseError::NoPendingRotation)?;
        self.state.previous = Some(std::mem::replace(&mut self.state.current, next));
        self.state.rotation_id = None;
        Ok(())
    }

    /// 迁移观察期结束后永久移除 previous 私钥，使旧 key ID 不再可连接。
    pub fn retire_previous(&mut self) -> Result<(), NoiseError> {
        if self.state.previous.take().is_none() {
            return Err(NoiseError::NoPendingRotation);
        }
        Ok(())
    }

    /// 在 promote 前撤销 Server 轮换并丢弃 next 私钥。
    pub fn cancel_rotation(&mut self) -> Result<(), NoiseError> {
        if self.state.next.take().is_none() {
            return Err(NoiseError::NoPendingRotation);
        }
        self.state.rotation_id = None;
        Ok(())
    }

    /// 克隆包含私钥的完整 keyring 状态，调用方必须按敏感数据保存。
    pub fn snapshot(&self) -> ServerKeyRingSnapshot {
        self.state.clone()
    }
}

#[derive(Clone, Debug)]
/// Agent 固定的 Server 公钥集合 snapshot。
pub struct PinnedServerKeysSnapshot {
    /// 当前稳定信任的 Server 公钥。
    pub current: NoisePublicKey,
    /// 已收到公告、准备优先尝试的新 Server 公钥。
    pub pending: Option<NoisePublicKey>,
    /// promote 后暂时保留的旧 Server 公钥。
    pub previous: Option<NoisePublicKey>,
    /// 与 pending 对应的 Server 换钥事务 ID。
    pub rotation_id: Option<RotationId>,
}

/// Agent 侧用于 IK 的 Server 公钥信任集合。
pub struct PinnedServerKeys {
    /// 可由数据库或本地文件恢复的完整状态。
    state: PinnedServerKeysSnapshot,
}

impl PinnedServerKeys {
    /// 用首次 XXpsk3 注册认证得到的 Server 公钥创建稳定状态。
    pub fn new(current: NoisePublicKey) -> Self {
        Self {
            state: PinnedServerKeysSnapshot {
                current,
                pending: None,
                previous: None,
                rotation_id: None,
            },
        }
    }

    /// 从持久化 snapshot 恢复，并校验 pending 与 rotation ID 的一致性。
    pub fn from_snapshot(snapshot: PinnedServerKeysSnapshot) -> Result<Self, NoiseError> {
        if snapshot.pending.is_some() != snapshot.rotation_id.is_some() {
            return Err(NoiseError::NoPendingRotation);
        }
        Ok(Self { state: snapshot })
    }

    /// 校验 Server 公告并加入 pending 信任公钥。
    ///
    /// 成功后应先保存 `snapshot()`，再通过旧加密会话发送确认。
    pub fn stage(&mut self, announcement: &ServerKeyAnnouncement) -> Result<(), NoiseError> {
        if self.state.pending.is_some() {
            return Err(NoiseError::RotationAlreadyInProgress);
        }
        let rotation_id = RotationId::from_bytes(&announcement.rotation_id)?;
        let key = NoisePublicKey::from_bytes(&announcement.new_public_key)?;
        if key.key_id() != KeyId::from_bytes(&announcement.new_key_id)? {
            return Err(NoiseError::AuthenticationFailed);
        }
        self.state.pending = Some(key);
        self.state.rotation_id = Some(rotation_id);
        Ok(())
    }

    /// 返回 IK 连接候选，依次尝试 pending、current、previous。
    pub fn connection_candidates(&self) -> Vec<NoisePublicKey> {
        self.state
            .pending
            .iter()
            .copied()
            .chain(std::iter::once(self.state.current))
            .chain(self.state.previous.iter().copied())
            .collect()
    }

    /// 使用 pending 公钥成功建立 IK 后，将其提升为 current 并保留旧 current。
    pub fn promote_pending(&mut self, rotation_id: RotationId) -> Result<(), NoiseError> {
        if self.state.rotation_id != Some(rotation_id) {
            return Err(NoiseError::RotationIdMismatch);
        }
        let next = self
            .state
            .pending
            .take()
            .ok_or(NoiseError::NoPendingRotation)?;
        self.state.previous = Some(std::mem::replace(&mut self.state.current, next));
        self.state.rotation_id = None;
        Ok(())
    }

    /// 观察期结束后移除 previous，之后不再信任旧 Server 公钥。
    pub fn retire_previous(&mut self) -> Result<(), NoiseError> {
        if self.state.previous.take().is_none() {
            return Err(NoiseError::NoPendingRotation);
        }
        Ok(())
    }

    /// 新公钥无法连接或轮换被撤销时，丢弃 pending 信任项。
    pub fn cancel_pending(&mut self) -> Result<(), NoiseError> {
        if self.state.pending.take().is_none() {
            return Err(NoiseError::NoPendingRotation);
        }
        self.state.rotation_id = None;
        Ok(())
    }

    /// 克隆完整 Server 公钥信任状态，供 Agent 自行持久化。
    pub fn snapshot(&self) -> PinnedServerKeysSnapshot {
        self.state.clone()
    }
}
