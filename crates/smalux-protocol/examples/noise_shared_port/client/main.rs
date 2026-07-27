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
    AGENT_DATA_DIR_ENV, DEFAULT_AGENT_DATA_DIR, DEFAULT_ENDPOINT, ENDPOINT_ENV, ExampleMode,
    GRPC_PREFIX, REGISTRATION_TOKEN_ENV,
};
use smalux_protocol::{
    agent::v1::{
        Messages, MessagesRequest, SecureMessage, messages, messages_request, secure_message,
    },
    noise::{NoiseIdentity, NoisePublicKey},
    tonic_transport::{
        AgentPendingRegistration, AgentProtocolClient, SessionDriver, SessionDriverConfig,
        SessionEvent,
    },
};
use support::{ExampleResult, load_or_generate_identity, parse_registration_psk};

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
    /// 最后一次成功注册的幂等事务 ID。
    registration_id: [u8; 16],
}

#[tokio::main]
/// 选择首次注册 Session 或后续 IK Session，然后运行同一段加密业务流。
async fn main() -> ExampleResult<()> {
    // Endpoint 可以是本地 h2c，也可以是 Cloudflare/Nginx 暴露的 HTTPS 地址。
    let endpoint = env::var(ENDPOINT_ENV).unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
    let mode = ExampleMode::from_args()?;
    println!("[client] session mode={mode:?}");
    // 每个 Agent 数据目录对应一套独立的长期 Noise 身份。
    let data_dir = PathBuf::from(
        env::var(AGENT_DATA_DIR_ENV).unwrap_or_else(|_| DEFAULT_AGENT_DATA_DIR.to_owned()),
    );

    // 本地材料完整表示已经注册；完全不存在则进入首次 XXpsk3 注册。
    let (identity, session) = match load_agent_identity(&data_dir)? {
        Some(identity) => {
            // 已注册 Agent 不再读取 Token，也不会再次运行首次信任流程。
            println!("[client] loaded Agent identity from {}", data_dir.display());
            let session = open_ik_session(&endpoint, &identity).await?;
            (identity, session)
        }
        // 首次注册返回的 XX Session 已经完成授权，不需要为了业务消息立即重连 IK。
        None => register_agent(&endpoint, &data_dir).await?,
    };
    // XX 注册流和 IK 重连流都进入相同的业务消息阶段。
    run_messages(session, &identity.agent_id, mode).await
}

/// 使用一次性 Token 完成 XXpsk3 注册，保存身份并保留当前已授权 Session。
async fn register_agent(
    endpoint: &str,
    data_dir: &Path,
) -> ExampleResult<(
    AgentIdentity,
    smalux_protocol::tonic_transport::TonicNoiseSession,
)> {
    // 首次启动唯一需要带外取得的秘密是 Server 打印的一次性 Token。
    let token = env::var(REGISTRATION_TOKEN_ENV)
        .map_err(|_| format!("missing {REGISTRATION_TOKEN_ENV}"))?;
    // Token 是 64 位十六进制文本；Noise PSK 必须是恰好 32 字节。
    let registration_psk = parse_registration_psk(&token)?;
    // Agent 在本地生成长期静态密钥；私钥从不通过 gRPC 发送。
    let stored = load_or_generate_identity(&data_dir.join("noise"))?;
    let noise = NoiseIdentity::from_parts(&stored.private_key, &stored.public_key)?;
    let mut client = AgentProtocolClient::new(endpoint);
    client.set_grpc_prefix(GRPC_PREFIX);
    // prepare 只执行到 Server 保存 pending；此时还没有最终消费 Token。
    let pending = client
        .prepare_registration(
            noise.clone(),
            &registration_psk,
            token,
            "example-agent".to_owned(),
        )
        .await?;

    // 必须在 commit 前保存这些字段；进程在下一行之后崩溃时可用相同身份恢复同一事务。
    save_pending_registration(data_dir, &pending)?;
    println!(
        "[client][xxpsk3] saved pending registration agent={} transaction={}",
        pending.agent_id,
        hex_id(&pending.registration_id)
    );
    // commit 等待 Server 激活 Agent 并返回 RegistrationCommitted。
    let outcome = pending.commit().await?;
    mark_registration_committed(data_dir)?;

    // 到这里 Server 已被 PSK 认证，才把刚学到的 Server 公钥加入长期身份。
    let identity = AgentIdentity {
        noise: outcome.agent_identity,
        server_public_key: outcome.server_public_key,
        agent_id: outcome.agent_id,
        registration_id: outcome.registration_id,
    };
    // 保存成功后，后续启动不再需要注册 Token。
    save_agent_identity(data_dir, &identity)?;
    println!(
        "[client][xxpsk3] learned Server key and registered agent={}",
        identity.agent_id
    );
    println!("[client][xxpsk3] registration session remains active for business messages");
    Ok((identity, outcome.session))
}

/// 使用已保存的双方静态身份建立后续 IK Session。
async fn open_ik_session(
    endpoint: &str,
    identity: &AgentIdentity,
) -> ExampleResult<smalux_protocol::tonic_transport::TonicNoiseSession> {
    // 每次连接创建独立 Client 配置，实际 Agent 可以长期复用配置对象。
    let mut client = AgentProtocolClient::new(endpoint);
    client.set_grpc_prefix(GRPC_PREFIX);
    let session = client
        .connect(&identity.noise, identity.server_public_key)
        .await?;
    println!("[client][ik] authenticated Server and Agent");
    Ok(session)
}

/// 在已经完成注册或 IK 认证的 Session 上顺序发送三条示例监控数据。
async fn run_messages(
    session: smalux_protocol::tonic_transport::TonicNoiseSession,
    agent_id: &str,
    mode: ExampleMode,
) -> ExampleResult<()> {
    match mode {
        ExampleMode::Manual => run_messages_manual(session, agent_id).await,
        ExampleMode::Driver => run_messages_driver(session, agent_id).await,
    }
}

/// 逐步调用会话小方法，便于观察每一次发送和接收。
async fn run_messages_manual(
    mut session: smalux_protocol::tonic_transport::TonicNoiseSession,
    agent_id: &str,
) -> ExampleResult<()> {
    println!("[client][noise] business stream started agent={agent_id}");
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
            .receive_event()
            .await?
            .ok_or("Server closed the session")?;
        let SessionEvent::Messages(Messages {
            body: Some(messages::Body::Response(response)),
        }) = response
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
    println!("[client] encrypted bidirectional stream completed");
    Ok(())
}

/// 把会话交给可选 Driver，业务层只使用可克隆句柄和强类型事件。
async fn run_messages_driver(
    session: smalux_protocol::tonic_transport::TonicNoiseSession,
    agent_id: &str,
) -> ExampleResult<()> {
    println!("[client][driver] business stream started agent={agent_id}");
    let mut running = SessionDriver::spawn(session, SessionDriverConfig::default());
    for sequence in 1..=3 {
        let payload = format!("metric-batch-{sequence}");
        running
            .handle
            .send_messages(Messages {
                body: Some(messages::Body::Request(MessagesRequest {
                    sequence,
                    payload: Some(messages_request::Payload::StringPayload(payload)),
                })),
            })
            .await?;
        let event = running
            .events
            .recv()
            .await
            .ok_or("SessionDriver stopped before returning an event")??;
        let SessionEvent::Messages(Messages {
            body: Some(messages::Body::Response(response)),
        }) = event
        else {
            return Err("Server returned an unexpected Driver event".into());
        };
        if response.acknowledged_sequence != sequence {
            return Err("Server returned an invalid Driver ACK".into());
        }
        println!(
            "[client][driver] <- acknowledged_sequence={}",
            response.acknowledged_sequence
        );
    }
    running.handle.shutdown().await?;
    running.task.await?;
    println!("[client][driver] encrypted bidirectional stream completed");
    Ok(())
}

/// 从示例数据目录恢复完整 Agent 状态；完全不存在返回 `None`，部分存在直接报错。
fn load_agent_identity(data_dir: &Path) -> ExampleResult<Option<AgentIdentity>> {
    // Server 公钥、业务 ID 和 Agent keypair 共同组成一条可用 IK 身份记录。
    let server_key_path = data_dir.join("server-public.bin");
    let agent_id_path = data_dir.join("agent-id.txt");
    let registration_id_path = data_dir.join("registration-id.bin");
    let committed_path = data_dir.join("registration-committed");
    let noise_directory = data_dir.join("noise");
    let any_exists = server_key_path.exists()
        || agent_id_path.exists()
        || registration_id_path.exists()
        || committed_path.exists()
        || noise_directory.exists();
    if !any_exists {
        return Ok(None);
    }
    // 只有 Noise 身份存在表示注册尚未 prepare，可继续用原身份注册。
    if noise_directory.exists()
        && !server_key_path.exists()
        && !agent_id_path.exists()
        && !registration_id_path.exists()
        && !committed_path.exists()
    {
        return Ok(None);
    }
    // pending 状态包含全部身份材料但没有 committed 标记，重新执行注册可幂等恢复。
    if noise_directory.exists()
        && server_key_path.exists()
        && agent_id_path.exists()
        && registration_id_path.exists()
        && !committed_path.exists()
    {
        return Ok(None);
    }
    if !server_key_path.exists()
        || !agent_id_path.exists()
        || !registration_id_path.exists()
        || !committed_path.exists()
        || !noise_directory.exists()
    {
        return Err("incomplete Agent Noise identity directory".into());
    }
    let server_public_key = NoisePublicKey::from_bytes(&fs::read(server_key_path)?)?;
    let stored = load_or_generate_identity(&noise_directory)?;
    let registration_id = fs::read(registration_id_path)?
        .try_into()
        .map_err(|_| "registration ID must contain 16 bytes")?;
    Ok(Some(AgentIdentity {
        noise: NoiseIdentity::from_parts(&stored.private_key, &stored.public_key)?,
        server_public_key,
        agent_id: fs::read_to_string(agent_id_path)?,
        registration_id,
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
    fs::write(
        data_dir.join("registration-id.bin"),
        identity.registration_id,
    )?;
    fs::write(data_dir.join("registration-committed"), b"committed")?;
    Ok(())
}

/// 在发送 RegistrationCommit 前保存 Server 返回的 pending 注册材料。
fn save_pending_registration(
    data_dir: &Path,
    pending: &AgentPendingRegistration,
) -> ExampleResult<()> {
    fs::create_dir_all(data_dir)?;
    fs::write(
        data_dir.join("server-public.bin"),
        pending.server_public_key.as_bytes(),
    )?;
    fs::write(data_dir.join("agent-id.txt"), &pending.agent_id)?;
    fs::write(
        data_dir.join("registration-id.bin"),
        pending.registration_id,
    )?;
    let committed = data_dir.join("registration-committed");
    if committed.exists() {
        fs::remove_file(committed)?;
    }
    Ok(())
}

/// 收到 Server 最终确认后写入完成标记；下次启动只有存在该标记才进入 IK。
fn mark_registration_committed(data_dir: &Path) -> ExampleResult<()> {
    fs::write(data_dir.join("registration-committed"), b"committed")?;
    Ok(())
}

/// 把 16 字节事务 ID 格式化为仅用于示例日志的十六进制文本。
fn hex_id(value: &[u8; 16]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}
