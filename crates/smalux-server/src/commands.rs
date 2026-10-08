//! Server CLI 命令执行器。
//!
//! `run`、`config check` 和本地 `auth bootstrap` 在当前进程执行；其余命令通过本地 IPC
//! 操作运行中的 Server。危险操作在请求发出前统一确认。

use secrecy::{ExposeSecret, SecretString};
use std::{
    fs,
    fs::OpenOptions,
    io::{IsTerminal, Write},
    path::Path,
    time::Duration,
};

use crate::{
    cli::{
        AgentCommand, AuthCommand, Cli, CliCommand, ConfigCommand, KeyringCommand, OutputFormat,
        PluginCommand, RegistrationTokenCommand, ServerJobCommand, SessionCommand,
    },
    management::{ControlRequest, ControlResponse},
};

/// 分发顶层命令：启动/配置校验在本进程执行，其余命令转发给运行中的 Server。
pub(crate) async fn execute(cli: Cli) -> anyhow::Result<()> {
    let (endpoint, request_timeout, command) = cli.command_or_default();
    if let CliCommand::Auth { command } = &command {
        match command {
            AuthCommand::Bootstrap { username } => {
                anyhow::ensure!(
                    std::io::stdin().is_terminal(),
                    "auth bootstrap requires a local interactive terminal"
                );
                let config = crate::config::ServerConfig::from_env()?;
                let database = crate::database::ServerDatabase::connect(config.database).await?;
                let password = SecretString::from(rpassword::prompt_password("Admin password: ")?);
                let confirmation =
                    SecretString::from(rpassword::prompt_password("Confirm password: ")?);
                anyhow::ensure!(
                    password.expose_secret() == confirmation.expose_secret(),
                    "password confirmation did not match"
                );
                crate::web_auth::bootstrap(
                    std::sync::Arc::new(database),
                    &username,
                    password.expose_secret(),
                )
                .await?;
                println!("initial administrator created");
                return Ok(());
            }
        }
    }
    match command {
        CliCommand::Run(args) => {
            let config = args.resolve()?;
            config.validate_web()?;
            crate::bootstrap::run_server(config, endpoint).await
        }
        CliCommand::Config {
            command: ConfigCommand::Check(args),
        } => {
            let config = args.resolve()?;
            config.validate_web()?;
            println!(
                "configuration is valid: listen={}:{} database={}",
                config.address,
                config.port,
                config.database.backend_label()
            );
            Ok(())
        }
        command => execute_remote(&endpoint, request_timeout, command).await,
    }
}

/// 将强类型 CLI 子命令映射成一个 IPC 请求，并处理确认、输出和一次性凭据文件。
async fn execute_remote(
    endpoint: &Path,
    timeout: Duration,
    command: CliCommand,
) -> anyhow::Result<()> {
    let (request, output, credential_file) = match command {
        CliCommand::Status(args) => {
            if args.watch {
                return watch_status(endpoint, timeout, args.interval, args.output.output).await;
            }
            (ControlRequest::Status, args.output.output, None)
        }
        CliCommand::Config {
            command: ConfigCommand::Show(args),
        } => (ControlRequest::EffectiveConfig, args.output, None),
        CliCommand::RegistrationToken { command } => match command {
            RegistrationTokenCommand::Create(args) => {
                let valid_for_seconds = args.validity().map(|duration| duration.as_secs());
                (
                    ControlRequest::CreateRegistrationToken {
                        agent_name: args.agent_name,
                        valid_for_seconds,
                    },
                    args.output.output,
                    args.credential_file,
                )
            }
            RegistrationTokenCommand::List(args) => (
                ControlRequest::ListRegistrationTokens {
                    status: args
                        .status
                        .map(|value| format!("{value:?}").to_ascii_lowercase()),
                    agent_name: args.agent_name,
                    limit: args.limit,
                    after: args.after,
                },
                args.output.output,
                None,
            ),
            RegistrationTokenCommand::Show { token_id, output } => (
                ControlRequest::GetRegistrationToken { token_id },
                output.output,
                None,
            ),
            RegistrationTokenCommand::Revoke {
                token_id,
                confirmation,
                output,
            } => {
                if !confirm_dangerous(
                    confirmation.yes,
                    &format!("revoke registration Token {token_id}"),
                )? {
                    println!("operation cancelled");
                    return Ok(());
                }
                (
                    ControlRequest::RevokeRegistrationToken { token_id },
                    output.output,
                    None,
                )
            }
        },
        CliCommand::Agent { command } => match command {
            AgentCommand::List(args) => (
                ControlRequest::ListAgents {
                    status: args
                        .status
                        .map(|value| format!("{value:?}").to_ascii_lowercase()),
                    name: args.name,
                    online: if args.online {
                        Some(true)
                    } else if args.offline {
                        Some(false)
                    } else {
                        None
                    },
                    limit: args.limit,
                    after: args.after,
                },
                args.output.output,
                None,
            ),
            AgentCommand::Show { agent_id, output } => {
                (ControlRequest::GetAgent { agent_id }, output.output, None)
            }
            AgentCommand::Rename {
                agent_id,
                name,
                output,
            } => (
                ControlRequest::RenameAgent { agent_id, name },
                output.output,
                None,
            ),
            AgentCommand::Revoke {
                agent_id,
                confirmation,
                output,
            } => {
                if !confirm_dangerous(
                    confirmation.yes,
                    &format!("revoke Agent {agent_id} and disconnect its active sessions"),
                )? {
                    println!("operation cancelled");
                    return Ok(());
                }
                (
                    ControlRequest::RevokeAgent { agent_id },
                    output.output,
                    None,
                )
            }
        },
        CliCommand::Job { command } => match command {
            ServerJobCommand::Replace(args) => {
                let definitions = args
                    .definitions
                    .iter()
                    .map(|path| {
                        let bytes = fs::read(path).map_err(|error| {
                            anyhow::anyhow!(
                                "failed to read JobDefinition {}: {error}",
                                path.display()
                            )
                        })?;
                        anyhow::ensure!(
                            bytes.len() <= 1024 * 1024,
                            "JobDefinition file exceeds 1 MiB"
                        );
                        Ok(bytes)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                (
                    ControlRequest::ReplaceAgentJobCatalog {
                        agent_id: args.agent_id,
                        definitions,
                        expected_revision: args.expected_revision,
                    },
                    args.output.output,
                    None,
                )
            }
            ServerJobCommand::List { agent_id, output } => (
                ControlRequest::GetAgentJobCatalog { agent_id },
                output.output,
                None,
            ),
            ServerJobCommand::Clear {
                agent_id,
                expected_revision,
                confirmation,
                output,
            } => {
                if !confirm_dangerous(
                    confirmation.yes,
                    &format!("clear all remote Jobs for Agent {agent_id}"),
                )? {
                    println!("operation cancelled");
                    return Ok(());
                }
                (
                    ControlRequest::ReplaceAgentJobCatalog {
                        agent_id,
                        definitions: Vec::new(),
                        expected_revision,
                    },
                    output.output,
                    None,
                )
            }
        },
        CliCommand::Plugin { command } => match command {
            PluginCommand::RuntimeReplace {
                agent_id,
                file,
                expected_revision,
                output,
            } => {
                let bytes = fs::read(&file).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to read plugin runtime JSON {}: {error}",
                        file.display()
                    )
                })?;
                let runtimes = serde_json::from_slice(&bytes)
                    .map_err(|error| anyhow::anyhow!("plugin runtime JSON is invalid: {error}"))?;
                (
                    ControlRequest::ReplaceAgentPluginRuntime {
                        agent_id,
                        runtimes,
                        expected_revision,
                    },
                    output.output,
                    None,
                )
            }
            PluginCommand::RuntimeList { agent_id, output } => (
                ControlRequest::GetAgentPluginRuntime { agent_id },
                output.output,
                None,
            ),
            PluginCommand::RuntimeClear {
                agent_id,
                expected_revision,
                confirmation,
                output,
            } => {
                if !confirm_dangerous(
                    confirmation.yes,
                    &format!("clear Plus runtime for Agent {agent_id}"),
                )? {
                    println!("operation cancelled");
                    return Ok(());
                }
                (
                    ControlRequest::ReplaceAgentPluginRuntime {
                        agent_id,
                        runtimes: Vec::new(),
                        expected_revision,
                    },
                    output.output,
                    None,
                )
            }
        },
        CliCommand::Report(args) => (
            ControlRequest::ListTaskReports {
                agent_id: args.agent_id,
                limit: args.limit,
            },
            args.output.output,
            None,
        ),
        CliCommand::Event(args) => (
            ControlRequest::ListJobEvents {
                agent_id: args.agent_id,
                limit: args.limit,
            },
            args.output.output,
            None,
        ),
        CliCommand::Session { command } => match command {
            SessionCommand::List(args) => (
                ControlRequest::ListSessions {
                    agent_id: args.agent_id,
                    state: args
                        .state
                        .map(|value| format!("{value:?}").to_ascii_lowercase()),
                },
                args.output.output,
                None,
            ),
            SessionCommand::Show { session_id, output } => (
                ControlRequest::GetSession { session_id },
                output.output,
                None,
            ),
            SessionCommand::Disconnect {
                session_id,
                confirmation,
                output,
            } => {
                if !confirm_dangerous(
                    confirmation.yes,
                    &format!("disconnect Session {session_id}"),
                )? {
                    println!("operation cancelled");
                    return Ok(());
                }
                (
                    ControlRequest::DisconnectSession { session_id },
                    output.output,
                    None,
                )
            }
        },
        CliCommand::Keyring {
            command: KeyringCommand::Status(output),
        } => (ControlRequest::KeyringStatus, output.output, None),
        CliCommand::Shutdown(confirmation) => {
            if !confirm_dangerous(confirmation.yes, "gracefully stop the running Server")? {
                println!("operation cancelled");
                return Ok(());
            }
            (ControlRequest::Shutdown, OutputFormat::Table, None)
        }
        CliCommand::Run(_)
        | CliCommand::Config {
            command: ConfigCommand::Check(_),
        } => unreachable!(),
        CliCommand::Auth { .. } => unreachable!(),
    };

    // 先以 create_new 预留目标，防止请求成功后才发现文件已存在而丢失唯一一次的秘密。
    let mut credential_output = credential_file
        .as_deref()
        .map(reserve_credential)
        .transpose()?;
    let response = match crate::management::request(endpoint, timeout, request).await {
        Ok(response) => response,
        Err(error) => {
            drop(credential_output.take());
            if let Some(path) = credential_file.as_deref() {
                let _ = std::fs::remove_file(path);
            }
            return Err(anyhow::anyhow!(
                "failed to contact Server management endpoint {}: {error}",
                endpoint.display()
            ));
        }
    };
    if let Err(error) = reject_error(&response) {
        drop(credential_output.take());
        if let Some(path) = credential_file.as_deref() {
            let _ = std::fs::remove_file(path);
        }
        return Err(error);
    }
    if let (Some(path), Some(file)) = (credential_file, credential_output) {
        persist_credential(&path, file, &response)?;
        if let ControlResponse::RegistrationTokenCreated(token) = &response {
            println!(
                "token_id={} expires_at={}",
                token.token_id,
                token
                    .expires_at_unix_micros
                    .map_or_else(|| "never".to_owned(), |value| value.to_string())
            );
        }
        return Ok(());
    }
    print_response(&response, output)
}

/// 按固定间隔重复查询状态；Ctrl+C 只结束观察，不会关闭 Server。
async fn watch_status(
    endpoint: &Path,
    timeout: Duration,
    interval: Duration,
    output: OutputFormat,
) -> anyhow::Result<()> {
    loop {
        let response =
            crate::management::request(endpoint, timeout, ControlRequest::Status).await?;
        reject_error(&response)?;
        print_response(&response, output)?;
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            result = tokio::signal::ctrl_c() => { result?; return Ok(()); }
        }
    }
}

/// 把 Server 的结构化错误转成本地命令失败，保留稳定错误码供脚本诊断。
fn reject_error(response: &ControlResponse) -> anyhow::Result<()> {
    if let ControlResponse::Error { code, message } = response {
        anyhow::bail!("Server management error [{code}]: {message}");
    }
    Ok(())
}

/// 危险操作的统一确认门。
///
/// 非交互终端不能等待 stdin，必须显式传入 `--yes`，避免自动化任务挂起。
fn confirm_dangerous(yes: bool, action: &str) -> anyhow::Result<bool> {
    if yes {
        return Ok(true);
    }
    anyhow::ensure!(
        std::io::stdin().is_terminal(),
        "dangerous operation requires --yes in a non-interactive terminal"
    );
    print!("Confirm {action}? Type 'yes' to continue: ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("yes"))
}

/// 原子创建一个全新凭据文件；已存在路径绝不覆盖。
fn reserve_credential(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(|error| {
        anyhow::anyhow!(
            "failed to create credential file {}: {error}",
            path.display()
        )
    })
}

/// 校验响应类型后写入唯一一次返回的完整凭据并同步到存储设备。
fn write_credential(
    path: &Path,
    file: &mut std::fs::File,
    response: &ControlResponse,
) -> anyhow::Result<()> {
    let ControlResponse::RegistrationTokenCreated(token) = response else {
        anyhow::bail!("Server did not return a registration credential");
    };
    writeln!(file, "{}", token.credential)?;
    file.sync_all()?;
    println!("registration credential written to {}", path.display());
    Ok(())
}

/// 完成凭据文件写入；任何写入或响应类型错误都会删除预留文件，允许调用者安全重试。
fn persist_credential(
    path: &Path,
    mut file: std::fs::File,
    response: &ControlResponse,
) -> anyhow::Result<()> {
    let result = write_credential(path, &mut file, response);
    drop(file);
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
}

/// 以稳定 JSON 或紧凑的人类可读文本输出成功响应。
///
/// 未指定 `--credential-file` 时，创建 Token 的文本输出会包含一次完整凭据；调用方应
/// 避免将这一条输出写入共享日志。
fn print_response(response: &ControlResponse, output: OutputFormat) -> anyhow::Result<()> {
    if matches!(output, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(response)?);
        return Ok(());
    }
    match response {
        ControlResponse::Status(value) => println!(
            "version={} uptime_ms={} database={} sessions={}/{} agents(active/revoked)={}/{} tokens(active/used/revoked/expired)={}/{}/{}/{} keyring_revision={} shutting_down={}",
            value.version,
            value.uptime_ms,
            value.database_backend,
            value.active_sessions,
            value.max_agent_sessions,
            value.active_agents,
            value.revoked_agents,
            value.active_tokens,
            value.used_tokens,
            value.revoked_tokens,
            value.expired_tokens,
            value.keyring_revision,
            value.shutting_down
        ),
        ControlResponse::EffectiveConfig(value) => println!(
            "listen={}:{} database={} database_url={} sessions={} registrations={} grpc_message_bytes={}",
            value.listen_address,
            value.listen_port,
            value.database_backend,
            value.database_url,
            value.max_agent_sessions,
            value.max_registration_sessions,
            value.max_grpc_message_bytes
        ),
        ControlResponse::RegistrationTokenCreated(value) => println!(
            "token_id={} credential={} expires_at={}",
            value.token_id,
            value.credential,
            value
                .expires_at_unix_micros
                .map_or_else(|| "never".to_owned(), |value| value.to_string())
        ),
        ControlResponse::RegistrationTokens(values) => {
            for value in values {
                print_token(value);
            }
        }
        ControlResponse::RegistrationToken(Some(value))
        | ControlResponse::RegistrationTokenRevoked(value) => print_token(value),
        ControlResponse::RegistrationToken(None) => println!("registration Token not found"),
        ControlResponse::Agents(values) => {
            for value in values {
                print_agent(value);
            }
        }
        ControlResponse::Agent(Some(value)) | ControlResponse::AgentUpdated(value) => {
            print_agent(value)
        }
        ControlResponse::Agent(None) => println!("Agent not found"),
        ControlResponse::AgentRevoked {
            agent,
            disconnected_sessions,
        } => {
            print_agent(agent);
            println!("disconnected_sessions={disconnected_sessions}");
        }
        ControlResponse::AgentJobCatalog(Some(value)) => println!(
            "agent_id={} catalog_revision={} jobs={}",
            value.agent_id,
            value.catalog_revision,
            value.definitions.len()
        ),
        ControlResponse::AgentJobCatalog(None) => println!("Agent Job catalog not found"),
        ControlResponse::AgentPluginRuntimeUpdated { agent_id, revision } => {
            println!("agent_id={agent_id} plugin_runtime_revision={revision}")
        }
        ControlResponse::AgentPluginRuntime(Some(value)) => println!(
            "agent_id={} plugin_runtime_revision={} plugins={}",
            value.agent_id,
            value.revision,
            value.runtimes.len()
        ),
        ControlResponse::AgentPluginRuntime(None) => println!("Agent plugin runtime not found"),
        ControlResponse::TaskReports(values) => {
            for value in values {
                println!(
                    "report_id={} agent_id={} result_kind={} received_at={}",
                    value.report_id,
                    value.agent_id,
                    value.result_kind,
                    value.received_at_unix_micros
                );
            }
        }
        ControlResponse::JobEvents(values) => {
            for value in values {
                println!(
                    "event_id={} agent_id={} kind={} sequence={} emitted_at={}",
                    value.event_id,
                    value.agent_id,
                    value.kind,
                    value.sequence,
                    value.emitted_at_unix_micros
                );
            }
        }
        ControlResponse::Sessions(values) => {
            for value in values {
                print_session(value);
            }
        }
        ControlResponse::Session(Some(value)) => print_session(value),
        ControlResponse::Session(None) => println!("Session not found"),
        ControlResponse::SessionDisconnected { session_id } => {
            println!("disconnected session_id={session_id}")
        }
        ControlResponse::KeyringStatus(value) => println!(
            "revision={} current={} next={} previous={} rotation={}",
            value.revision,
            value.current_key_id,
            value.next_key_id.as_deref().unwrap_or("none"),
            value.previous_key_id.as_deref().unwrap_or("none"),
            value.rotation_id.as_deref().unwrap_or("none")
        ),
        ControlResponse::ShutdownAccepted => println!("Server shutdown accepted"),
        ControlResponse::Error { .. } => unreachable!("errors are handled before printing"),
    }
    Ok(())
}

/// 输出不含 PSK 的单条 Token 元数据。
fn print_token(value: &crate::management::RegistrationTokenView) {
    println!(
        "token_id={} status={} agent_name={} expires_at={}",
        value.token_id,
        value.status,
        value.agent_name.as_deref().unwrap_or("-"),
        value
            .expires_at_unix_micros
            .map_or_else(|| "never".to_owned(), |value| value.to_string())
    );
}

/// 输出 Agent 的稳定 ID、展示名称、授权和在线状态。
fn print_agent(value: &crate::management::AgentView) {
    println!(
        "agent_id={} name={} status={} online={}",
        value.agent_id, value.name, value.status, value.online
    );
}

/// 输出一个当前进程 Session 的生命周期信息。
fn print_session(value: &crate::management::SessionView) {
    println!(
        "session_id={} agent_id={} mode={} state={} connected_at={} last_activity_at={}",
        value.session_id,
        value.agent_id.as_deref().unwrap_or("-"),
        value.authentication_mode.as_deref().unwrap_or("-"),
        value.state,
        value.connected_at_unix_micros,
        value.last_activity_at_unix_micros
    );
}

#[cfg(test)]
mod tests {
    use super::{confirm_dangerous, persist_credential, reserve_credential};
    use crate::management::ControlResponse;

    #[test]
    fn yes_skips_interactive_confirmation() {
        assert!(confirm_dangerous(true, "test operation").unwrap());
    }

    #[test]
    fn failed_credential_write_removes_the_reserved_file() {
        let path = std::env::temp_dir().join(format!(
            "smalux-invalid-credential-response-{}",
            uuid::Uuid::new_v4()
        ));
        let file = reserve_credential(&path).expect("temporary credential file should be reserved");

        let error = persist_credential(&path, file, &ControlResponse::ShutdownAccepted)
            .expect_err("an unrelated response must not produce a credential file");

        assert!(error.to_string().contains("did not return"));
        assert!(!path.exists());
    }
}
