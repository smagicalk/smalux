//! Noise 单端口示例的共享状态。
//!
//! 这里的文件只会被 `examples/noise_shared_port_*` 编译，不会进入正式协议库。

// Client 与 Server 共用此文件，但两端使用的辅助项不同。
#![allow(dead_code)]

use std::{
    collections::HashMap,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Mutex,
};

use snow::{Builder, params::NoiseParams};

/// 首次注册使用 XXpsk3：Client 不预置 Server 公钥，双方改用注册 Token 认证握手。
pub const NOISE_XX_PSK3: &str = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s";
/// 已注册 Agent 的连接使用 IK，以 Server 和 Agent 的静态公钥互相认证。
pub const NOISE_IK: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";
/// 32 字节是 Noise 25519 静态公钥的固定长度。
pub const NOISE_PUBLIC_KEY_LEN: usize = 32;

/// 示例内层错误统一使用的结果类型。
pub type ExampleResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Server 或 Agent 持久化的 Noise 静态密钥对。
#[derive(Clone, Debug)]
pub struct NoiseIdentity {
    pub private_key: Vec<u8>,
    pub public_key: Vec<u8>,
}

/// 加载静态密钥对；首次启动时生成并写入指定目录。
pub fn load_or_generate_identity(directory: &Path) -> ExampleResult<NoiseIdentity> {
    fs::create_dir_all(directory)?;
    let private_path = directory.join("static-private.bin");
    let public_path = directory.join("static-public.bin");

    match (private_path.exists(), public_path.exists()) {
        (true, true) => {
            let identity = NoiseIdentity {
                private_key: fs::read(private_path)?,
                public_key: fs::read(public_path)?,
            };
            validate_public_key(&identity.public_key)?;
            Ok(identity)
        }
        (false, false) => {
            // 生成静态密钥只依赖 DH 算法；使用 XXpsk3 的参数可确保与首次握手算法一致。
            let params: NoiseParams = NOISE_XX_PSK3.parse()?;
            let keypair = Builder::new(params).generate_keypair()?;
            let identity = NoiseIdentity {
                private_key: keypair.private,
                public_key: keypair.public,
            };
            fs::write(private_path, &identity.private_key)?;
            fs::write(public_path, &identity.public_key)?;
            Ok(identity)
        }
        _ => Err("incomplete Noise identity directory".into()),
    }
}

/// 将 32 字节公钥编码为适合复制到环境变量的十六进制字符串。
pub fn public_key_hex(public_key: &[u8]) -> String {
    public_key
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 把 64 位十六进制注册 Token 解码成 Noise 要求的 32 字节 PSK。
pub fn parse_registration_psk(value: &str) -> Result<[u8; 32], RegistrationTokenError> {
    if value.len() != NOISE_PUBLIC_KEY_LEN * 2 {
        return Err(RegistrationTokenError);
    }
    let mut key = [0_u8; NOISE_PUBLIC_KEY_LEN];
    for index in (0..value.len()).step_by(2) {
        key[index / 2] =
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| RegistrationTokenError)?;
    }
    Ok(key)
}

/// Server 为首次注册临时发放的 Token 与已授权 Agent 的映射。
#[derive(Debug)]
pub struct AgentRegistry {
    data: Mutex<RegistryData>,
}

#[derive(Debug)]
struct RegistryData {
    tokens: HashMap<String, String>,
    /// Noise 公钥到稳定 Agent 身份的授权映射；名称只存作展示字段。
    agents: HashMap<Vec<u8>, AgentRecord>,
    registrations: HashMap<Vec<u8>, RegistrationRecord>,
    agents_dir: PathBuf,
    registrations_dir: PathBuf,
    tokens_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AgentRecord {
    agent_id: String,
    name: String,
}

#[derive(Clone, Debug)]
struct RegistrationRecord {
    registration_id: [u8; 16],
    token: String,
    agent_id: String,
    agent_name: String,
    public_key: Vec<u8>,
    committed: bool,
}

/// `prepare` 返回给网络层的稳定注册事务信息。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedRegistration {
    pub registration_id: [u8; 16],
    pub agent_id: String,
    pub already_committed: bool,
}

/// 控制台可展示的 Agent 摘要；业务身份始终使用 `agent_id`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredAgent {
    pub agent_id: String,
    pub name: String,
}

impl AgentRegistry {
    /// 从 `agents/*.key` 恢复 Agent 公钥，并预置一条 Token；仅供测试或受控启动使用。
    pub fn load(token: String, data_dir: &Path) -> ExampleResult<Self> {
        Self::load_optional_token(Some(token), data_dir)
    }

    /// 从磁盘恢复 Agent、注册事务与此前签发的 Token，不自动创建新 Token。
    pub fn load_without_token(data_dir: &Path) -> ExampleResult<Self> {
        Self::load_optional_token(None, data_dir)
    }

    fn load_optional_token(token: Option<String>, data_dir: &Path) -> ExampleResult<Self> {
        let agents_dir = data_dir.join("agents");
        let registrations_dir = data_dir.join("registrations");
        let tokens_dir = data_dir.join("registration-tokens");
        fs::create_dir_all(&agents_dir)?;
        fs::create_dir_all(&registrations_dir)?;
        fs::create_dir_all(&tokens_dir)?;
        let mut agents = HashMap::new();

        for entry in fs::read_dir(&agents_dir)? {
            let directory = entry?.path();
            if !directory.is_dir() {
                continue;
            }
            let Some(agent_id) = directory
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
            else {
                continue;
            };
            if !valid_agent_id(&agent_id) {
                return Err("invalid persisted Agent ID".into());
            }
            let name = fs::read_to_string(directory.join("name.txt"))?;
            if !valid_agent_name(&name) {
                return Err("invalid persisted Agent name".into());
            }
            let key = fs::read(directory.join("public.key"))?;
            validate_public_key(&key)?;
            if agents.insert(key, AgentRecord { agent_id, name }).is_some() {
                return Err("duplicated persisted Agent public key".into());
            }
        }

        let mut registrations = HashMap::new();
        for entry in fs::read_dir(&registrations_dir)? {
            let directory = entry?.path();
            if !directory.is_dir() {
                continue;
            }
            let agent_id = fs::read_to_string(directory.join("agent-id.txt"))?;
            if !valid_agent_id(&agent_id) {
                return Err("invalid pending Agent ID".into());
            }
            let agent_name = fs::read_to_string(directory.join("agent-name.txt"))?;
            if !valid_agent_name(&agent_name) {
                return Err("invalid pending Agent name".into());
            }
            let token = fs::read_to_string(directory.join("token.txt"))?;
            let public_key = fs::read(directory.join("public.key"))?;
            validate_public_key(&public_key)?;
            let registration_id: [u8; 16] = fs::read(directory.join("registration-id.bin"))?
                .try_into()
                .map_err(|_| "registration ID must contain 16 bytes")?;
            let committed = directory.join("committed").exists();
            let record = RegistrationRecord {
                registration_id,
                token,
                agent_id,
                agent_name,
                public_key: public_key.clone(),
                committed,
            };
            if registrations.insert(public_key, record).is_some() {
                return Err("duplicated persisted registration public key".into());
            }
        }

        let mut tokens = HashMap::new();
        for entry in fs::read_dir(&tokens_dir)? {
            let token = fs::read_to_string(entry?.path())?;
            tokens.insert(registration_token_id(&token).to_owned(), token);
        }
        if let Some(token) = token {
            let initial_id = registration_token_id(&token).to_owned();
            fs::write(tokens_dir.join(format!("{initial_id}.token")), &token)?;
            tokens.insert(initial_id, token);
        }
        Ok(Self {
            data: Mutex::new(RegistryData {
                tokens,
                agents,
                registrations,
                agents_dir,
                registrations_dir,
                tokens_dir,
            }),
        })
    }

    /// 验证请求并持久化 pending 注册；完全相同的重试返回同一事务 ID。
    pub fn prepare(
        &self,
        token: &str,
        agent_name: &str,
        public_key: &[u8],
    ) -> Result<PreparedRegistration, RegistrationError> {
        if !valid_agent_name(agent_name) {
            return Err(RegistrationError::InvalidAgentName);
        }
        validate_public_key(public_key).map_err(|_| RegistrationError::InvalidPublicKey)?;
        let mut data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        if let Some(existing) = data.registrations.get(public_key) {
            if existing.token == token {
                return Ok(PreparedRegistration {
                    registration_id: existing.registration_id,
                    agent_id: existing.agent_id.clone(),
                    already_committed: existing.committed,
                });
            }
            return Err(RegistrationError::AgentAlreadyRegistered);
        }
        if !data.tokens.values().any(|issued| issued == token) {
            return Err(RegistrationError::InvalidToken);
        }
        if data
            .registrations
            .values()
            .any(|record| record.token == token)
        {
            return Err(RegistrationError::TokenAlreadyUsed);
        }
        let mut registration_id = [0_u8; 16];
        getrandom::fill(&mut registration_id).map_err(|_| RegistrationError::Storage)?;
        let mut agent_id_bytes = [0_u8; 16];
        getrandom::fill(&mut agent_id_bytes).map_err(|_| RegistrationError::Storage)?;
        let agent_id = public_key_hex(&agent_id_bytes);
        let record = RegistrationRecord {
            registration_id,
            token: token.to_owned(),
            agent_id: agent_id.clone(),
            agent_name: agent_name.to_owned(),
            public_key: public_key.to_vec(),
            committed: false,
        };
        persist_registration(&data.registrations_dir, &record)
            .map_err(|_| RegistrationError::Storage)?;
        data.registrations.insert(public_key.to_vec(), record);
        Ok(PreparedRegistration {
            registration_id,
            agent_id,
            already_committed: false,
        })
    }

    /// 把匹配事务从 pending 激活为可用于 IK 的 Agent；重复提交保持成功。
    pub fn commit(
        &self,
        registration_id: [u8; 16],
        public_key: &[u8],
    ) -> Result<String, RegistrationError> {
        let mut data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        let record = data
            .registrations
            .get(public_key)
            .cloned()
            .ok_or(RegistrationError::UnknownRegistration)?;
        if record.registration_id != registration_id {
            return Err(RegistrationError::InvalidRegistrationId);
        }
        if !record.committed {
            let agent_directory = data.agents_dir.join(&record.agent_id);
            fs::create_dir_all(&agent_directory).map_err(|_| RegistrationError::Storage)?;
            fs::write(agent_directory.join("public.key"), public_key)
                .map_err(|_| RegistrationError::Storage)?;
            fs::write(agent_directory.join("name.txt"), &record.agent_name)
                .map_err(|_| RegistrationError::Storage)?;
            let directory = data.registrations_dir.join(&record.agent_id);
            fs::write(directory.join("committed"), b"committed")
                .map_err(|_| RegistrationError::Storage)?;
            data.agents.insert(
                public_key.to_vec(),
                AgentRecord {
                    agent_id: record.agent_id.clone(),
                    name: record.agent_name.clone(),
                },
            );
            if let Some(stored) = data.registrations.get_mut(public_key) {
                stored.committed = true;
            }
        }
        Ok(record.agent_id)
    }

    /// 验证 Token 和 XX 公钥后分配稳定 ID；名称只作为允许重复的展示字段保存。
    pub fn register(
        &self,
        token: &str,
        agent_name: &str,
        public_key: &[u8],
    ) -> Result<String, RegistrationError> {
        let prepared = self.prepare(token, agent_name, public_key)?;
        self.commit(prepared.registration_id, public_key)
    }

    /// IK 握手后用 Client 静态公钥查找业务身份。
    pub fn authenticate(&self, public_key: &[u8]) -> Result<String, RegistrationError> {
        self.data
            .lock()
            .map_err(|_| RegistrationError::Storage)?
            .agents
            .get(public_key)
            .map(|agent| agent.agent_id.clone())
            .ok_or(RegistrationError::UnknownAgent)
    }

    /// 返回当前已登记的稳定 ID 和展示名称，供示例控制台观察注册表状态。
    pub fn registered_agents(&self) -> Result<Vec<RegisteredAgent>, RegistrationError> {
        let mut agents = self
            .data
            .lock()
            .map_err(|_| RegistrationError::Storage)?
            .agents
            .values()
            .map(|agent| RegisteredAgent {
                agent_id: agent.agent_id.clone(),
                name: agent.name.clone(),
            })
            .collect::<Vec<_>>();
        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        Ok(agents)
    }

    /// 签发一条只允许绑定一台 Agent 的随机注册凭据。
    pub fn issue_registration_token(&self) -> Result<String, RegistrationError> {
        let token = random_registration_token().map_err(|_| RegistrationError::Storage)?;
        let mut data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        let token_id = registration_token_id(&token).to_owned();
        fs::write(data.tokens_dir.join(format!("{token_id}.token")), &token)
            .map_err(|_| RegistrationError::Storage)?;
        data.tokens.insert(token_id, token.clone());
        Ok(token)
    }

    /// 返回可安全展示的公开 Token ID，不输出秘密 PSK。
    pub fn registration_token_ids(&self) -> Result<Vec<String>, RegistrationError> {
        let mut ids = self
            .data
            .lock()
            .map_err(|_| RegistrationError::Storage)?
            .tokens
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    /// 吊销未完成注册或已经泄露的 Token。
    pub fn revoke_registration_token(&self, token_id: &str) -> Result<bool, RegistrationError> {
        if !valid_registration_token_id(token_id) {
            return Err(RegistrationError::InvalidToken);
        }
        let mut data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        let path = data.tokens_dir.join(format!("{token_id}.token"));
        if path.exists() {
            fs::remove_file(path).map_err(|_| RegistrationError::Storage)?;
        }
        let removed = data.tokens.remove(token_id).is_some();
        Ok(removed)
    }

    /// 根据公开 Token ID 返回对应的 XXpsk3 PSK。
    pub fn resolve_registration_psk(&self, token_id: &str) -> Result<[u8; 32], RegistrationError> {
        let data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        let token = data
            .tokens
            .get(token_id)
            .ok_or(RegistrationError::InvalidToken)?;
        parse_registration_credential(token)
            .map(|credential| credential.psk)
            .map_err(|_| RegistrationError::InvalidToken)
    }

    /// 删除内存和磁盘内的 Agent 公钥，下一次 IK 会话会被拒绝。
    pub fn revoke(&self, agent_id: &str) -> Result<bool, RegistrationError> {
        let mut data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        let before = data.agents.len();
        let token_ids = data
            .registrations
            .values()
            .filter(|record| record.agent_id == agent_id)
            .map(|record| registration_token_id(&record.token).to_owned())
            .collect::<Vec<_>>();
        data.agents.retain(|_, agent| agent.agent_id != agent_id);
        let path = data.agents_dir.join(agent_id);
        if path.exists() {
            fs::remove_dir_all(path).map_err(|_| RegistrationError::Storage)?;
        }
        let registration_path = data.registrations_dir.join(agent_id);
        if registration_path.exists() {
            fs::remove_dir_all(registration_path).map_err(|_| RegistrationError::Storage)?;
        }
        data.registrations
            .retain(|_, record| record.agent_id != agent_id);
        for token_id in token_ids {
            data.tokens.remove(&token_id);
            let token_path = data.tokens_dir.join(format!("{token_id}.token"));
            if token_path.exists() {
                fs::remove_file(token_path).map_err(|_| RegistrationError::Storage)?;
            }
        }
        Ok(before != data.agents.len())
    }
}

/// 注册表给网络层的稳定错误分类。
#[derive(Debug, PartialEq, Eq)]
pub enum RegistrationError {
    InvalidToken,
    TokenAlreadyUsed,
    InvalidAgentName,
    InvalidPublicKey,
    AgentAlreadyRegistered,
    UnknownRegistration,
    InvalidRegistrationId,
    UnknownAgent,
    Storage,
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidToken => "invalid registration token",
            Self::TokenAlreadyUsed => "registration token already used",
            Self::InvalidAgentName => "invalid Agent name",
            Self::InvalidPublicKey => "invalid Noise public key",
            Self::AgentAlreadyRegistered => "Agent is already registered",
            Self::UnknownRegistration => "registration transaction does not exist",
            Self::InvalidRegistrationId => "registration transaction ID does not match",
            Self::UnknownAgent => "Agent Noise public key is not registered",
            Self::Storage => "registration storage failed",
        })
    }
}

impl std::error::Error for RegistrationError {}

/// 注册 Token 不能转换为 Noise PSK。
#[derive(Debug, Clone, Copy)]
pub struct RegistrationTokenError;

impl fmt::Display for RegistrationTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("registration token must be <32 hex ID>.<64 hex PSK>")
    }
}

impl std::error::Error for RegistrationTokenError {}

/// 解析 `公开ID.秘密PSK` 形式的注册凭据。
pub struct RegistrationCredential {
    pub token_id: String,
    pub psk: [u8; 32],
}

pub fn parse_registration_credential(
    token: &str,
) -> Result<RegistrationCredential, RegistrationTokenError> {
    let (token_id, secret) = token.split_once('.').ok_or(RegistrationTokenError)?;
    if !valid_registration_token_id(token_id) {
        return Err(RegistrationTokenError);
    }
    Ok(RegistrationCredential {
        token_id: token_id.to_owned(),
        psk: parse_registration_psk(secret)?,
    })
}

fn registration_token_id(token: &str) -> &str {
    token.split_once('.').map(|(id, _)| id).unwrap_or(token)
}

fn valid_registration_token_id(token_id: &str) -> bool {
    token_id.len() == 32 && token_id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// 生成 128 位公开 ID 和 256 位秘密 PSK 组成的一次性注册凭据。
pub fn random_registration_token() -> io::Result<String> {
    let mut id = [0_u8; 16];
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut id).map_err(|error| io::Error::other(error.to_string()))?;
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(format!(
        "{}.{}",
        public_key_hex(&id),
        public_key_hex(&bytes)
    ))
}

fn validate_public_key(public_key: &[u8]) -> ExampleResult<()> {
    if public_key.len() == NOISE_PUBLIC_KEY_LEN {
        Ok(())
    } else {
        Err("Noise public key must contain 32 bytes".into())
    }
}

fn valid_agent_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// Example 使用 16 字节随机值的 32 位十六进制文本作为稳定 Agent ID。
fn valid_agent_id(agent_id: &str) -> bool {
    agent_id.len() == 32 && agent_id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// 用一个目录保存注册事务的独立字段，避免示例引入额外序列化依赖。
fn persist_registration(directory: &Path, record: &RegistrationRecord) -> io::Result<()> {
    let transaction = directory.join(&record.agent_id);
    fs::create_dir_all(&transaction)?;
    fs::write(
        transaction.join("registration-id.bin"),
        record.registration_id,
    )?;
    fs::write(transaction.join("token.txt"), &record.token)?;
    fs::write(transaction.join("agent-id.txt"), &record.agent_id)?;
    fs::write(transaction.join("agent-name.txt"), &record.agent_name)?;
    fs::write(transaction.join("public.key"), &record.public_key)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        AgentRegistry, NOISE_XX_PSK3, RegistrationError, parse_registration_credential,
        parse_registration_psk, random_registration_token,
    };

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    fn temp_directory() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "smalux-noise-example-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&directory).expect("test directory should be created");
        directory
    }

    #[test]
    fn registration_is_persisted_and_available_after_restart() {
        let directory = temp_directory();
        let registry = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        let key = vec![7; 32];
        let agent_id = registry.register("token", "example-agent", &key).unwrap();

        let reloaded = AgentRegistry::load("next".to_owned(), &directory).unwrap();
        assert_eq!(reloaded.authenticate(&key), Ok(agent_id));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registration_prepare_and_commit_are_idempotent() {
        let directory = temp_directory();
        let registry = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        let key = vec![8; 32];

        let first = registry.prepare("token", "example-agent", &key).unwrap();
        let retried = registry.prepare("token", "example-agent", &key).unwrap();
        assert_eq!(first.registration_id, retried.registration_id);
        assert_eq!(
            registry.authenticate(&key),
            Err(RegistrationError::UnknownAgent)
        );

        registry.commit(first.registration_id, &key).unwrap();
        assert_eq!(registry.authenticate(&key), Ok(first.agent_id.clone()));
        let completed_retry = registry.prepare("token", "example-agent", &key).unwrap();
        assert_eq!(first.registration_id, completed_retry.registration_id);
        assert!(completed_retry.already_committed);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pending_registration_recovers_across_server_restarts() {
        let directory = temp_directory();
        let key = vec![9; 32];
        let first = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        let prepared = first.prepare("token", "restart-agent", &key).unwrap();
        drop(first);

        let reloaded = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        let recovered = reloaded.prepare("token", "restart-agent", &key).unwrap();
        assert_eq!(recovered.registration_id, prepared.registration_id);
        assert!(!recovered.already_committed);
        reloaded.commit(recovered.registration_id, &key).unwrap();
        drop(reloaded);

        let completed = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        let recovered = completed.prepare("token", "restart-agent", &key).unwrap();
        assert_eq!(recovered.registration_id, prepared.registration_id);
        assert!(recovered.already_committed);
        assert_eq!(completed.authenticate(&key), Ok(prepared.agent_id));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registration_token_cannot_be_rebound_to_another_public_key() {
        let directory = temp_directory();
        let registry = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        registry.prepare("token", "first-agent", &[1; 32]).unwrap();

        assert_eq!(
            registry.prepare("token", "second-agent", &[2; 32]),
            Err(RegistrationError::TokenAlreadyUsed)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_token_does_not_consume_the_valid_token() {
        let directory = temp_directory();
        let registry = AgentRegistry::load("token".to_owned(), &directory).unwrap();

        assert_eq!(
            registry.register("wrong", "example-agent", &[1; 32]),
            Err(RegistrationError::InvalidToken)
        );
        assert!(
            registry
                .register("token", "example-agent", &[1; 32])
                .is_ok()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn revocation_denies_the_next_ik_session() {
        let directory = temp_directory();
        let registry = AgentRegistry::load("token".to_owned(), &directory).unwrap();
        let key = vec![3; 32];
        let agent_id = registry.register("token", "example-agent", &key).unwrap();

        assert!(registry.revoke(&agent_id).unwrap());
        assert_eq!(
            registry.authenticate(&key),
            Err(RegistrationError::UnknownAgent)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registered_agents_are_identified_and_revoked_by_id() {
        let directory = temp_directory();
        let first_token = random_registration_token().unwrap();
        let registry = AgentRegistry::load(first_token.clone(), &directory).unwrap();
        let second_token = registry.issue_registration_token().unwrap();
        let first_id = registry
            .register(&first_token, "shared-name", &[4; 32])
            .unwrap();
        let second_id = registry
            .register(&second_token, "shared-name", &[5; 32])
            .unwrap();

        let agents = registry.registered_agents().unwrap();
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().all(|agent| agent.name == "shared-name"));
        assert!(agents.iter().any(|agent| agent.agent_id == first_id));
        assert!(agents.iter().any(|agent| agent.agent_id == second_id));

        assert!(registry.revoke(&first_id).unwrap());
        let remaining = registry.registered_agents().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].agent_id, second_id);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registration_token_parser_requires_exact_256_bit_hex() {
        assert_eq!(parse_registration_psk(&"00".repeat(32)).unwrap(), [0; 32]);
        assert!(parse_registration_psk("00").is_err());
        assert!(parse_registration_psk(&"zz".repeat(32)).is_err());
    }

    #[test]
    fn xxpsk3_pattern_is_supported_by_snow() {
        let params: snow::params::NoiseParams = NOISE_XX_PSK3
            .parse()
            .expect("XXpsk3 parameters should parse");
        let keypair = snow::Builder::new(params)
            .generate_keypair()
            .expect("XXpsk3 key generation should work");
        assert_eq!(keypair.public.len(), 32);
    }

    #[test]
    fn xxpsk3_rejects_a_client_that_has_a_different_token() {
        // 使用同一算法参数生成双方长期静态密钥，模拟真实 Client 与 Server。
        let params: snow::params::NoiseParams = NOISE_XX_PSK3
            .parse()
            .expect("XXpsk3 parameters should parse");
        let client_keypair = snow::Builder::new(params.clone())
            .generate_keypair()
            .expect("Client keypair should generate");
        let server_keypair = snow::Builder::new(params.clone())
            .generate_keypair()
            .expect("Server keypair should generate");
        // Client 与 Server 故意使用不同 Token，只有第三条消息会暴露该不匹配。
        let client_psk = [1_u8; 32];
        let server_psk = [2_u8; 32];
        let mut client = snow::Builder::new(params.clone())
            .local_private_key(&client_keypair.private)
            .expect("Client private key should be accepted")
            .psk(3, &client_psk)
            .expect("Client PSK should be accepted")
            .build_initiator()
            .expect("Client initiator should build");
        let mut server = snow::Builder::new(params)
            .local_private_key(&server_keypair.private)
            .expect("Server private key should be accepted")
            .psk(3, &server_psk)
            .expect("Server PSK should be accepted")
            .build_responder()
            .expect("Server responder should build");
        let mut message = [0_u8; 128];
        let mut payload = [0_u8; 128];
        // 前两条消息不报错，因为 psk3 设计为在第三条消息混入 PSK。
        let first_len = client.write_message(&[], &mut message).unwrap();
        server
            .read_message(&message[..first_len], &mut payload)
            .unwrap();
        let second_len = server.write_message(&[], &mut message).unwrap();
        client
            .read_message(&message[..second_len], &mut payload)
            .unwrap();
        // Client 用错误 PSK 写出第三条消息，Server 必须拒绝它。
        let third_len = client.write_message(&[], &mut message).unwrap();
        assert!(
            server
                .read_message(&message[..third_len], &mut payload)
                .is_err()
        );
    }

    #[test]
    fn registration_token_is_256_bit_hex() {
        let token = random_registration_token().unwrap();
        let credential = parse_registration_credential(&token).unwrap();
        assert_eq!(credential.token_id.len(), 32);
        assert_eq!(credential.psk.len(), 32);
    }

    #[test]
    fn independent_tokens_register_independent_agents() {
        let directory = temp_directory();
        let first_token = random_registration_token().unwrap();
        let registry = AgentRegistry::load(first_token.clone(), &directory).unwrap();
        let second_token = registry.issue_registration_token().unwrap();

        let first_id = registry
            .register(&first_token, "first-agent", &[11; 32])
            .unwrap();
        let second_id = registry
            .register(&second_token, "second-agent", &[12; 32])
            .unwrap();

        let agents = registry.registered_agents().unwrap();
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().any(|agent| agent.agent_id == first_id));
        assert!(agents.iter().any(|agent| agent.agent_id == second_id));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unused_issued_token_survives_server_restart() {
        let directory = temp_directory();
        let initial = random_registration_token().unwrap();
        let registry = AgentRegistry::load(initial, &directory).unwrap();
        let issued = registry.issue_registration_token().unwrap();
        let credential = parse_registration_credential(&issued).unwrap();
        drop(registry);

        let next_initial = random_registration_token().unwrap();
        let reloaded = AgentRegistry::load(next_initial, &directory).unwrap();
        assert_eq!(
            reloaded
                .resolve_registration_psk(&credential.token_id)
                .unwrap(),
            credential.psk
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn server_registry_starts_without_a_default_token() {
        let directory = temp_directory();
        let registry = AgentRegistry::load_without_token(&directory).unwrap();

        assert!(registry.registration_token_ids().unwrap().is_empty());
        assert_eq!(
            registry.resolve_registration_psk("missing"),
            Err(RegistrationError::InvalidToken)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn revoking_agent_also_prevents_its_token_from_being_reused() {
        let directory = temp_directory();
        let registry = AgentRegistry::load_without_token(&directory).unwrap();
        let token = registry.issue_registration_token().unwrap();
        let agent_id = registry.register(&token, "first-agent", &[21; 32]).unwrap();

        assert!(registry.revoke(&agent_id).unwrap());
        assert_eq!(
            registry.register(&token, "second-agent", &[22; 32]),
            Err(RegistrationError::InvalidToken)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn revoked_registration_token_stays_revoked_after_restart() {
        let directory = temp_directory();
        let registry = AgentRegistry::load_without_token(&directory).unwrap();
        let token = registry.issue_registration_token().unwrap();
        let credential = parse_registration_credential(&token).unwrap();
        registry
            .prepare(&token, "pending-agent", &[23; 32])
            .unwrap();

        assert!(
            registry
                .revoke_registration_token(&credential.token_id)
                .unwrap()
        );
        assert_eq!(
            registry.revoke_registration_token("../invalid"),
            Err(RegistrationError::InvalidToken)
        );
        drop(registry);

        let reloaded = AgentRegistry::load_without_token(&directory).unwrap();
        assert_eq!(
            reloaded.resolve_registration_psk(&credential.token_id),
            Err(RegistrationError::InvalidToken)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
