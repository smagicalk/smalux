//! Agent 长期认证状态模型及其阶段转换不变量。

use smalux_protocol::noise::{NoiseIdentity, NoisePublicKey};

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
        server_public_keys: Vec<NoisePublicKey>,
        registration_id: [u8; 16],
    },
    Registered {
        agent_id: String,
        identity: NoiseIdentity,
        server_public_keys: Vec<NoisePublicKey>,
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
        Self::RegistrationPending {
            agent_id,
            identity,
            server_public_keys: vec![server_public_key],
            registration_id,
        }
    }

    pub fn registered_from_pending(state: &Self) -> anyhow::Result<Self> {
        let Self::RegistrationPending {
            agent_id,
            identity,
            server_public_keys,
            registration_id,
        } = state
        else {
            anyhow::bail!("only a pending Agent state can be marked registered");
        };
        Ok(Self::Registered {
            agent_id: agent_id.clone(),
            identity: identity.clone(),
            server_public_keys: server_public_keys.clone(),
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

    pub fn server_public_keys(&self) -> &[NoisePublicKey] {
        match self {
            Self::IdentityPrepared { .. } => &[],
            Self::RegistrationPending {
                server_public_keys, ..
            }
            | Self::Registered {
                server_public_keys, ..
            } => server_public_keys,
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

    /// 把已通过当前加密会话认证的 Server 新公钥加入后续 IK 候选。
    ///
    /// 新公钥放在最前面，使下一次连接优先尝试轮换后的 key；重复公告保持幂等。
    pub fn with_server_public_key(&self, key: NoisePublicKey) -> anyhow::Result<Self> {
        let (agent_id, identity, server_public_keys, registration_id, pending) = match self {
            Self::IdentityPrepared { .. } => {
                anyhow::bail!("Server key rotation requires a pending or registered Agent state")
            }
            Self::RegistrationPending {
                agent_id,
                identity,
                server_public_keys,
                registration_id,
            } => (
                agent_id,
                identity,
                server_public_keys,
                registration_id,
                true,
            ),
            Self::Registered {
                agent_id,
                identity,
                server_public_keys,
                registration_id,
            } => (
                agent_id,
                identity,
                server_public_keys,
                registration_id,
                false,
            ),
        };
        let mut keys = server_public_keys.clone();
        if let Some(index) = keys.iter().position(|candidate| *candidate == key) {
            keys.remove(index);
        }
        keys.insert(0, key);
        Ok(if pending {
            Self::RegistrationPending {
                agent_id: agent_id.clone(),
                identity: identity.clone(),
                server_public_keys: keys,
                registration_id: *registration_id,
            }
        } else {
            Self::Registered {
                agent_id: agent_id.clone(),
                identity: identity.clone(),
                server_public_keys: keys,
                registration_id: *registration_id,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use smalux_protocol::noise::NoiseIdentity;

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

        let rotated = registered
            .with_server_public_key(new_server.public_key())
            .unwrap();
        let replayed = rotated
            .with_server_public_key(new_server.public_key())
            .unwrap();

        assert_eq!(
            replayed.server_public_keys(),
            &[new_server.public_key(), old_server.public_key()]
        );
    }
}
