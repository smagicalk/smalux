//! Agent 长期认证状态模型及其阶段转换不变量。

use smalux_protocol::{
    agent::v1::ServerKeyAnnouncement,
    noise::{
        NoiseError, NoiseIdentity, NoisePublicKey, PinnedServerKeys, PinnedServerKeysSnapshot,
        RotationId,
    },
};

/// 本地认证状态所处的持久化阶段。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationStage {
    IdentityPrepared,
    RegistrationPending,
    Registered,
}

/// 可以跨进程恢复的 Agent 身份状态；实时 gRPC 流和 Noise nonce 不在其中。
#[derive(Clone)]
pub enum PersistedAgentState {
    IdentityPrepared {
        identity: NoiseIdentity,
    },
    RegistrationPending {
        agent_id: String,
        identity: NoiseIdentity,
        server_keys: PinnedServerKeysSnapshot,
        registration_id: [u8; 16],
    },
    Registered {
        agent_id: String,
        identity: NoiseIdentity,
        server_keys: PinnedServerKeysSnapshot,
        registration_id: [u8; 16],
    },
}

impl PersistedAgentState {
    pub fn identity_prepared(identity: NoiseIdentity) -> Self {
        Self::IdentityPrepared { identity }
    }

    pub fn registration_pending(
        agent_id: String,
        identity: NoiseIdentity,
        server_public_key: NoisePublicKey,
        registration_id: [u8; 16],
    ) -> Self {
        let server_keys = PinnedServerKeys::new(server_public_key).snapshot();
        Self::RegistrationPending {
            agent_id,
            identity,
            server_keys,
            registration_id,
        }
    }

    pub fn registered_from_pending(state: &Self) -> anyhow::Result<Self> {
        let Self::RegistrationPending {
            agent_id,
            identity,
            server_keys,
            registration_id,
        } = state
        else {
            anyhow::bail!("only a pending Agent state can be marked registered");
        };
        Ok(Self::Registered {
            agent_id: agent_id.clone(),
            identity: identity.clone(),
            server_keys: server_keys.clone(),
            registration_id: *registration_id,
        })
    }

    pub fn stage(&self) -> RegistrationStage {
        match self {
            Self::IdentityPrepared { .. } => RegistrationStage::IdentityPrepared,
            Self::RegistrationPending { .. } => RegistrationStage::RegistrationPending,
            Self::Registered { .. } => RegistrationStage::Registered,
        }
    }

    pub fn identity(&self) -> &NoiseIdentity {
        match self {
            Self::IdentityPrepared { identity }
            | Self::RegistrationPending { identity, .. }
            | Self::Registered { identity, .. } => identity,
        }
    }

    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::IdentityPrepared { .. } => None,
            Self::RegistrationPending { agent_id, .. } | Self::Registered { agent_id, .. } => {
                Some(agent_id)
            }
        }
    }

    /// 返回当前、pending 和 previous Server 公钥，按连接优先级排列。
    pub fn server_key_candidates(&self) -> Vec<NoisePublicKey> {
        match self {
            Self::IdentityPrepared { .. } => Vec::new(),
            Self::RegistrationPending { server_keys, .. }
            | Self::Registered { server_keys, .. } => server_key_candidates(server_keys),
        }
    }

    pub fn registration_id(&self) -> Option<[u8; 16]> {
        match self {
            Self::IdentityPrepared { .. } => None,
            Self::RegistrationPending {
                registration_id, ..
            }
            | Self::Registered {
                registration_id, ..
            } => Some(*registration_id),
        }
    }

    /// 返回当前持久化的 Server 公钥轮换快照；身份尚未注册时返回 `None`。
    pub fn server_key_snapshot(&self) -> Option<&PinnedServerKeysSnapshot> {
        match self {
            Self::IdentityPrepared { .. } => None,
            Self::RegistrationPending { server_keys, .. }
            | Self::Registered { server_keys, .. } => Some(server_keys),
        }
    }

    /// 校验当前会话收到的 Server 公钥公告，并写入 pending 状态。
    ///
    /// 调用方必须在发送 acknowledgement 前持久化返回的新状态。
    pub fn stage_server_key(
        &self,
        announcement: &ServerKeyAnnouncement,
    ) -> Result<Self, NoiseError> {
        let (agent_id, identity, server_keys, registration_id, pending) = match self {
            Self::IdentityPrepared { .. } => {
                return Err(NoiseError::NoPendingRotation);
            }
            Self::RegistrationPending {
                agent_id,
                identity,
                server_keys,
                registration_id,
            } => (agent_id, identity, server_keys, registration_id, true),
            Self::Registered {
                agent_id,
                identity,
                server_keys,
                registration_id,
            } => (agent_id, identity, server_keys, registration_id, false),
        };
        let mut pinned = PinnedServerKeys::from_snapshot(server_keys.clone())?;
        pinned.stage(announcement)?;
        let server_keys = pinned.snapshot();
        Ok(if pending {
            Self::RegistrationPending {
                agent_id: agent_id.clone(),
                identity: identity.clone(),
                server_keys,
                registration_id: *registration_id,
            }
        } else {
            Self::Registered {
                agent_id: agent_id.clone(),
                identity: identity.clone(),
                server_keys,
                registration_id: *registration_id,
            }
        })
    }

    /// 在使用 pending Server 公钥成功建立 IK 后提升它为 current。
    pub fn promote_server_key(&self, rotation_id: RotationId) -> Result<Self, NoiseError> {
        let (agent_id, identity, server_keys, registration_id, pending) = match self {
            Self::IdentityPrepared { .. } => return Err(NoiseError::NoPendingRotation),
            Self::RegistrationPending {
                agent_id,
                identity,
                server_keys,
                registration_id,
            } => (agent_id, identity, server_keys, registration_id, true),
            Self::Registered {
                agent_id,
                identity,
                server_keys,
                registration_id,
            } => (agent_id, identity, server_keys, registration_id, false),
        };
        let mut pinned = PinnedServerKeys::from_snapshot(server_keys.clone())?;
        pinned.promote_pending(rotation_id)?;
        let server_keys = pinned.snapshot();
        Ok(if pending {
            Self::RegistrationPending {
                agent_id: agent_id.clone(),
                identity: identity.clone(),
                server_keys,
                registration_id: *registration_id,
            }
        } else {
            Self::Registered {
                agent_id: agent_id.clone(),
                identity: identity.clone(),
                server_keys,
                registration_id: *registration_id,
            }
        })
    }
}

fn server_key_candidates(snapshot: &PinnedServerKeysSnapshot) -> Vec<NoisePublicKey> {
    snapshot
        .pending
        .iter()
        .copied()
        .chain(std::iter::once(snapshot.current))
        .chain(snapshot.previous.iter().copied())
        .collect()
}

#[cfg(test)]
mod tests {
    use smalux_protocol::noise::{NoiseIdentity, PinnedServerKeys};

    use super::PersistedAgentState;

    #[test]
    fn server_key_rotation_prepends_new_key_and_deduplicates_replays() {
        let agent = NoiseIdentity::generate().unwrap();
        let old_server = NoiseIdentity::generate().unwrap();
        let new_server = NoiseIdentity::generate().unwrap();
        let pending = PersistedAgentState::registration_pending(
            "agent-1".to_owned(),
            agent,
            old_server.public_key(),
            [9; 16],
        );
        let registered = PersistedAgentState::registered_from_pending(&pending).unwrap();

        let prepared = PinnedServerKeys::new(old_server.public_key()).snapshot();
        let announcement = smalux_protocol::agent::v1::ServerKeyAnnouncement {
            rotation_id: [3; 16].to_vec(),
            new_public_key: new_server.public_key().as_bytes().to_vec(),
            new_key_id: new_server.key_id().as_bytes().to_vec(),
        };
        let pending = PersistedAgentState::Registered {
            agent_id: "agent-1".to_owned(),
            identity: registered.identity().clone(),
            server_keys: prepared,
            registration_id: [9; 16],
        }
        .stage_server_key(&announcement)
        .unwrap();
        let replayed = pending.stage_server_key(&announcement).unwrap();

        assert_eq!(
            replayed.server_key_candidates(),
            vec![new_server.public_key(), old_server.public_key()]
        );
    }
}
