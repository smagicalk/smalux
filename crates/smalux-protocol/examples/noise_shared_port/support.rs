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
    token: String,
    agents: HashMap<Vec<u8>, String>,
    registrations: HashMap<Vec<u8>, RegistrationRecord>,
    agents_dir: PathBuf,
    registrations_dir: PathBuf,
}

#[derive(Clone, Debug)]
struct RegistrationRecord {
    registration_id: [u8; 16],
    token: String,
    agent_id: String,
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

impl AgentRegistry {
    /// 从 `agents/*.key` 恢复 Agent 公钥；本次启动提供一个新的单次注册 Token。
    pub fn load(token: String, data_dir: &Path) -> ExampleResult<Self> {
        let agents_dir = data_dir.join("agents");
        let registrations_dir = data_dir.join("registrations");
        fs::create_dir_all(&agents_dir)?;
        fs::create_dir_all(&registrations_dir)?;
        let mut agents = HashMap::new();

        for entry in fs::read_dir(&agents_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("key") {
                continue;
            }
            let Some(agent_id) = path
                .file_stem()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
            else {
                continue;
            };
            if !valid_agent_name(&agent_id) {
                return Err("invalid persisted Agent name".into());
            }
            let key = fs::read(path)?;
            validate_public_key(&key)?;
            if agents.insert(key, agent_id).is_some() {
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
            if !valid_agent_name(&agent_id) {
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
                public_key: public_key.clone(),
                committed,
            };
            if registrations.insert(public_key, record).is_some() {
                return Err("duplicated persisted registration public key".into());
            }
        }

        Ok(Self {
            data: Mutex::new(RegistryData {
                token,
                agents,
                registrations,
                agents_dir,
                registrations_dir,
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
            if existing.token == token && existing.agent_id == agent_name {
                return Ok(PreparedRegistration {
                    registration_id: existing.registration_id,
                    agent_id: existing.agent_id.clone(),
                    already_committed: existing.committed,
                });
            }
            return Err(RegistrationError::AgentAlreadyRegistered);
        }
        if data.token != token {
            return Err(RegistrationError::InvalidToken);
        }
        if data
            .registrations
            .values()
            .any(|record| record.token == token)
        {
            return Err(RegistrationError::TokenAlreadyUsed);
        }
        if data.agents.values().any(|name| name == agent_name)
            || data
                .registrations
                .values()
                .any(|record| record.agent_id == agent_name)
        {
            return Err(RegistrationError::AgentAlreadyRegistered);
        }

        let mut registration_id = [0_u8; 16];
        getrandom::fill(&mut registration_id).map_err(|_| RegistrationError::Storage)?;
        let record = RegistrationRecord {
            registration_id,
            token: token.to_owned(),
            agent_id: agent_name.to_owned(),
            public_key: public_key.to_vec(),
            committed: false,
        };
        persist_registration(&data.registrations_dir, &record)
            .map_err(|_| RegistrationError::Storage)?;
        data.registrations.insert(public_key.to_vec(), record);
        Ok(PreparedRegistration {
            registration_id,
            agent_id: agent_name.to_owned(),
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
            fs::write(
                data.agents_dir.join(format!("{}.key", record.agent_id)),
                public_key,
            )
            .map_err(|_| RegistrationError::Storage)?;
            let directory = data.registrations_dir.join(&record.agent_id);
            fs::write(directory.join("committed"), b"committed")
                .map_err(|_| RegistrationError::Storage)?;
            data.agents
                .insert(public_key.to_vec(), record.agent_id.clone());
            if let Some(stored) = data.registrations.get_mut(public_key) {
                stored.committed = true;
            }
        }
        Ok(record.agent_id)
    }

    /// 验证 Token 后绑定 Agent 名称和 XX 握手中得到的静态公钥。
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
            .cloned()
            .ok_or(RegistrationError::UnknownAgent)
    }

    /// 返回当前已登记的 Agent 名称，供示例控制台观察注册表状态。
    pub fn registered_agents(&self) -> Result<Vec<String>, RegistrationError> {
        let mut agents = self
            .data
            .lock()
            .map_err(|_| RegistrationError::Storage)?
            .agents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        agents.sort();
        Ok(agents)
    }

    /// 删除内存和磁盘内的 Agent 公钥，下一次 IK 会话会被拒绝。
    pub fn revoke(&self, agent_name: &str) -> Result<bool, RegistrationError> {
        let mut data = self.data.lock().map_err(|_| RegistrationError::Storage)?;
        let before = data.agents.len();
        data.agents.retain(|_, name| name != agent_name);
        let path = data.agents_dir.join(format!("{agent_name}.key"));
        if path.exists() {
            fs::remove_file(path).map_err(|_| RegistrationError::Storage)?;
        }
        let registration_path = data.registrations_dir.join(agent_name);
        if registration_path.exists() {
            fs::remove_dir_all(registration_path).map_err(|_| RegistrationError::Storage)?;
        }
        data.registrations
            .retain(|_, record| record.agent_id != agent_name);
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
        formatter.write_str("registration token must contain 32 bytes of hexadecimal text")
    }
}

impl std::error::Error for RegistrationTokenError {}

/// 生成 256 位随机 Token，供测试验证生产实现应采用的格式。
///
/// 当前可运行示例为了便于手工操作使用 `FIXED_REGISTRATION_TOKEN`，不会调用此函数。
pub fn random_registration_token() -> io::Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(public_key_hex(&bytes))
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
    fs::write(transaction.join("public.key"), &record.public_key)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        AgentRegistry, NOISE_XX_PSK3, RegistrationError, parse_registration_psk,
        random_registration_token,
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
        registry.register("token", "example-agent", &key).unwrap();

        let reloaded = AgentRegistry::load("next".to_owned(), &directory).unwrap();
        assert_eq!(reloaded.authenticate(&key), Ok("example-agent".to_owned()));
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
        assert_eq!(registry.authenticate(&key), Ok("example-agent".to_owned()));
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
        assert_eq!(completed.authenticate(&key), Ok("restart-agent".to_owned()));
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
        registry.register("token", "example-agent", &key).unwrap();

        assert!(registry.revoke("example-agent").unwrap());
        assert_eq!(
            registry.authenticate(&key),
            Err(RegistrationError::UnknownAgent)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registered_agents_are_sorted_and_revocation_removes_the_name() {
        let first_directory = temp_directory();
        let first = AgentRegistry::load("first-token".to_owned(), &first_directory).unwrap();
        first.register("first-token", "z-agent", &[4; 32]).unwrap();

        let second_directory = temp_directory();
        std::fs::create_dir_all(second_directory.join("agents")).unwrap();
        std::fs::copy(
            first_directory.join("agents/z-agent.key"),
            second_directory.join("agents/z-agent.key"),
        )
        .unwrap();
        std::fs::write(second_directory.join("agents/a-agent.key"), [5; 32]).unwrap();
        let second = AgentRegistry::load("second-token".to_owned(), &second_directory).unwrap();

        assert_eq!(
            second.registered_agents().unwrap(),
            vec!["a-agent".to_owned(), "z-agent".to_owned()]
        );
        assert!(second.revoke("a-agent").unwrap());
        assert_eq!(
            second.registered_agents().unwrap(),
            vec!["z-agent".to_owned()]
        );

        std::fs::remove_dir_all(first_directory).unwrap();
        std::fs::remove_dir_all(second_directory).unwrap();
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
        assert_eq!(token.len(), 64);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}
