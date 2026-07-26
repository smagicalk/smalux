//! Noise 单端口示例 Client。
//!
//! 首次运行只需要 Server 打印的一次性 Token，不需要预置 Server Noise 公钥。
//! XXpsk3 成功后保存双方公钥关系，后续运行直接使用 IK。
//! 外层可以是本地 h2c、Server 直接 TLS，或 Cloudflare/Nginx 终止 TLS；Noise 始终启用。

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use common::{
    AGENT_DATA_DIR_ENV, DEFAULT_AGENT_DATA_DIR, DEFAULT_ENDPOINT, ENDPOINT_ENV,
    ENROLLMENT_TOKEN_ENV, GRPC_PREFIX,
};
use smalux_protocol::{
    agent::v1::{
        Messages, MessagesRequest, SecureMessage, messages, messages_request, secure_message,
    },
    noise::{NoiseIdentity, NoisePublicKey},
    tonic_transport::AgentProtocolClient,
};
use support::{ExampleResult, load_or_generate_identity, parse_enrollment_psk};

#[path = "../common.rs"]
mod common;
#[path = "../support.rs"]
mod support;

/// Agent 重启后恢复后续 IK 所需的最小长期状态。
struct AgentIdentity {
    /// Agent 自己的长期 Noise 静态密钥对。
    noise: NoiseIdentity,
    /// 首次 XXpsk3 成功后学到并保存的 Server 静态公钥。
    server_public_key: NoisePublicKey,
    /// Server 返回并确认的业务身份。
    agent_id: String,
}

#[tokio::main]
/// 选择首次注册或已注册连接，然后运行一段 IK 加密业务流。
async fn main() -> ExampleResult<()> {
    // Endpoint 可以是本地 h2c，也可以是 Cloudflare/Nginx 暴露的 HTTPS 地址。
    let endpoint = env::var(ENDPOINT_ENV).unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
    // 每个 Agent 数据目录对应一套独立的长期 Noise 身份。
    let data_dir = PathBuf::from(
        env::var(AGENT_DATA_DIR_ENV).unwrap_or_else(|_| DEFAULT_AGENT_DATA_DIR.to_owned()),
    );

    // 本地材料完整表示已经注册；完全不存在则进入首次 XXpsk3 注册。
    let identity = match load_agent_identity(&data_dir)? {
        Some(identity) => {
            // 已注册 Agent 不再读取 Token，也不会再次运行首次信任流程。
            println!("[client] loaded Agent identity from {}", data_dir.display());
            identity
        }
        // 首次注册结束后会返回刚保存的完整身份。
        None => enroll(&endpoint, &data_dir).await?,
    };
    // 无论身份来自磁盘还是刚注册成功，业务会话统一使用 IK。
    run_ik_session(&endpoint, &identity).await
}

/// 使用一次性 Token 完成 XXpsk3 注册，并在成功确认后保存长期身份。
async fn enroll(endpoint: &str, data_dir: &Path) -> ExampleResult<AgentIdentity> {
    // 首次启动唯一需要带外取得的秘密是 Server 打印的一次性 Token。
    let token =
        env::var(ENROLLMENT_TOKEN_ENV).map_err(|_| format!("missing {ENROLLMENT_TOKEN_ENV}"))?;
    // Token 是 64 位十六进制文本；Noise PSK 必须是恰好 32 字节。
    let enrollment_psk = parse_enrollment_psk(&token)?;
    // Agent 在本地生成长期静态密钥；私钥从不通过 gRPC 发送。
    let stored = load_or_generate_identity(&data_dir.join("noise"))?;
    let noise = NoiseIdentity::from_parts(&stored.private_key, &stored.public_key)?;
    let mut client = AgentProtocolClient::new(endpoint);
    client.set_grpc_prefix(GRPC_PREFIX);
    // 正式协议方法完成 XXpsk3、加密 TokenRequest 和加密 TokenResponse。
    let outcome = client
        .enroll(
            noise.clone(),
            &enrollment_psk,
            token,
            "example-agent".to_owned(),
        )
        .await?;

    // 到这里 Server 已被 PSK 认证，才把刚学到的 Server 公钥加入长期身份。
    let identity = AgentIdentity {
        noise,
        server_public_key: outcome.server_public_key,
        agent_id: outcome.agent_id,
    };
    // 保存成功后，后续启动不再需要注册 Token。
    save_agent_identity(data_dir, &identity)?;
    println!(
        "[client][xxpsk3] learned Server key and enrolled agent={}",
        identity.agent_id
    );
    Ok(identity)
}

/// 使用已保存的双方静态身份建立 IK，并顺序发送三条示例监控数据。
async fn run_ik_session(endpoint: &str, identity: &AgentIdentity) -> ExampleResult<()> {
    // 每次连接创建独立 Client 配置，实际 Agent 可以长期复用配置对象。
    let mut client = AgentProtocolClient::new(endpoint);
    client.set_grpc_prefix(GRPC_PREFIX);
    let mut session = client
        .connect(&identity.noise, identity.server_public_key)
        .await?;
    println!("[client][ik] authenticated Server and Agent");

    for sequence in 1..=3 {
        // sequence 是业务幂等/确认标识，不是 Noise nonce；Noise nonce 由会话内部维护。
        let payload = format!("metric-batch-{sequence}");
        println!("[client][noise] -> sequence={sequence} payload={payload}");
        session
            .send(SecureMessage {
                body: Some(secure_message::Body::Messages(Messages {
                    body: Some(messages::Body::Request(MessagesRequest {
                        sequence,
                        payload: Some(messages_request::Payload::StringPayload(payload)),
                    })),
                })),
            })
            .await?;

        // Noise TransportState 的 nonce 有顺序要求，本示例逐条请求、逐条读取 ACK。
        let response = session
            .receive()
            .await?
            .ok_or("Server closed the session")?;
        let secure_message::Body::Messages(Messages {
            body: Some(messages::Body::Response(response)),
        }) = response.body.ok_or("Server returned an empty response")?
        else {
            return Err("Server returned an unexpected response".into());
        };
        if response.acknowledged_sequence != sequence
            || response.payload
                != Some(
                    smalux_protocol::agent::v1::messages_response::Payload::StringPayload(format!(
                        "metric-batch-{sequence}"
                    )),
                )
        {
            return Err("Server returned an invalid ACK".into());
        }
        println!(
            "[client][noise] <- acknowledged_sequence={} payload=metric-batch-{sequence}",
            response.acknowledged_sequence
        );
    }
    drop(session);
    println!("[client] IK encrypted bidirectional stream completed");
    Ok(())
}

/// 从示例数据目录恢复完整 Agent 状态；完全不存在返回 `None`，部分存在直接报错。
fn load_agent_identity(data_dir: &Path) -> ExampleResult<Option<AgentIdentity>> {
    // Server 公钥、业务 ID 和 Agent keypair 共同组成一条可用 IK 身份记录。
    let server_key_path = data_dir.join("server-public.bin");
    let agent_id_path = data_dir.join("agent-id.txt");
    let noise_directory = data_dir.join("noise");
    let any_exists = server_key_path.exists() || agent_id_path.exists() || noise_directory.exists();
    if !any_exists {
        return Ok(None);
    }
    if !server_key_path.exists() || !agent_id_path.exists() || !noise_directory.exists() {
        return Err("incomplete Agent Noise identity directory".into());
    }
    let server_public_key = NoisePublicKey::from_bytes(&fs::read(server_key_path)?)?;
    let stored = load_or_generate_identity(&noise_directory)?;
    Ok(Some(AgentIdentity {
        noise: NoiseIdentity::from_parts(&stored.private_key, &stored.public_key)?,
        server_public_key,
        agent_id: fs::read_to_string(agent_id_path)?,
    }))
}

/// 把 XXpsk3 成功结果写入示例目录。
///
/// 生产代码应使用临时文件 + 原子 rename 或数据库事务，并加密保护 Agent 私钥。
fn save_agent_identity(data_dir: &Path, identity: &AgentIdentity) -> ExampleResult<()> {
    // `noise` 私钥已由 load_or_generate_identity 在注册前写入；这里补齐 Server 信任和业务 ID。
    fs::create_dir_all(data_dir)?;
    fs::write(
        data_dir.join("server-public.bin"),
        identity.server_public_key.as_bytes(),
    )?;
    fs::write(data_dir.join("agent-id.txt"), &identity.agent_id)?;
    Ok(())
}
