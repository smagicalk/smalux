//! 最小 Echo Plus，用于验证 Worker 协议、参数、取消和结果返回。

use async_trait::async_trait;
use prost::Message;
use smalux_plus_core::{PlusTask, PlusTaskContext, PlusTaskError, PlusTaskOutput};
use std::time::Duration;

pub const PLUGIN_ID: &str = "smalux.plus.echo";
pub const TASK_KIND: &str = "smalux.plus.echo.v1";

include!(concat!(env!("OUT_DIR"), "/smalux.plus.echo.v1.rs"));

/// 与 Worker、`plugin.json` 一同发布的完整参数 Schema Bundle。
pub const SCHEMA_BUNDLE: &[u8] = include_bytes!("../schema.pb");

#[cfg(test)]
const GENERATED_SCHEMA_BUNDLE: &[u8] = include_bytes!(env!("SMALUX_PLUS_ECHO_SCHEMA"));

pub struct EchoTask;

#[async_trait]
impl PlusTask for EchoTask {
    fn kind(&self) -> &'static str {
        TASK_KIND
    }

    async fn execute(
        &self,
        context: PlusTaskContext,
        config: &[u8],
    ) -> Result<PlusTaskOutput, PlusTaskError> {
        let config = EchoTaskConfig::decode(config)
            .map_err(|error| PlusTaskError::InvalidConfig(error.to_string()))?;
        if config.fail {
            return Err(PlusTaskError::Failed("requested Echo failure".to_owned()));
        }
        let mode = EchoMode::try_from(config.mode)
            .map_err(|_| PlusTaskError::InvalidConfig("unknown Echo mode".to_owned()))?;
        let target = if config.target.is_empty() {
            "default".to_owned()
        } else {
            config.target.clone()
        };
        let state_dir = std::path::Path::new(&context.agent.plugin_data_dir);
        tokio::fs::create_dir_all(state_dir)
            .await
            .map_err(|error| {
                PlusTaskError::Failed(format!("create Echo data directory: {error}"))
            })?;
        let state_file = state_dir.join("execution-count.txt");
        let previous = tokio::fs::read_to_string(&state_file)
            .await
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let execution_count = previous.saturating_add(1);
        tokio::fs::write(&state_file, execution_count.to_string())
            .await
            .map_err(|error| PlusTaskError::Failed(format!("persist Echo state: {error}")))?;
        if config.delay_millis > 0 {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(config.delay_millis)) => {}
                () = context.cancellation.cancelled() => {
                    return Err(PlusTaskError::Cancelled("cancelled by Agent".to_owned()));
                }
            }
        }
        let completed_at_unix_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| PlusTaskError::Failed(error.to_string()))?
            .as_millis() as u64;
        let payload = EchoTaskResult {
            message: config.message.clone(),
            completed_at_unix_millis,
            mode: mode as i32,
            target: target.clone(),
        }
        .encode_to_vec();
        Ok(PlusTaskOutput {
            summary: format!(
                "{} (mode={mode:?}, target={target}, count={execution_count})",
                config.message
            ),
            metrics: vec![
                ("echo.completed".to_owned(), 1.0),
                ("echo.execution_count".to_owned(), execution_count as f64),
            ],
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EchoMode, EchoTask, EchoTaskConfig, EchoTaskResult};
    use prost::Message;
    use smalux_plus_core::{PlusTask, PlusTaskContext};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn echo_task_round_trips_its_protobuf_parameter() {
        let data_dir =
            std::env::temp_dir().join(format!("smalux-echo-test-{}", uuid::Uuid::new_v4()));
        let config = EchoTaskConfig {
            message: "hello".to_owned(),
            delay_millis: 0,
            fail: false,
            mode: EchoMode::Normal as i32,
            target: "default".to_owned(),
        };
        let output = EchoTask
            .execute(
                PlusTaskContext {
                    request_id: "request-1".to_owned(),
                    run_id: vec![1; 16],
                    deadline: None,
                    cancellation: CancellationToken::new(),
                    agent: smalux_plus_core::AgentContext {
                        plugin_data_dir: data_dir.to_string_lossy().into_owned(),
                        ..Default::default()
                    },
                },
                &config.encode_to_vec(),
            )
            .await
            .unwrap();
        let result = EchoTaskResult::decode(output.payload.as_slice()).unwrap();
        assert_eq!(result.message, "hello");
        assert_eq!(result.mode, EchoMode::Normal as i32);
        assert_eq!(result.target, "default");
        assert!(output.summary.contains("count=1"));
        let second = EchoTask
            .execute(
                PlusTaskContext {
                    request_id: "request-2".to_owned(),
                    run_id: vec![2; 16],
                    deadline: None,
                    cancellation: CancellationToken::new(),
                    agent: smalux_plus_core::AgentContext {
                        plugin_data_dir: data_dir.to_string_lossy().into_owned(),
                        ..Default::default()
                    },
                },
                &config.encode_to_vec(),
            )
            .await
            .unwrap();
        assert!(second.summary.contains("count=2"));
        std::fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn packaged_schema_matches_echo_identity_and_task() {
        assert_eq!(super::SCHEMA_BUNDLE, super::GENERATED_SCHEMA_BUNDLE);
        let schema =
            smalux_plus_core::PluginSchemaBundle::decode_checked(super::SCHEMA_BUNDLE).unwrap();
        assert_eq!(schema.plugin_id, super::PLUGIN_ID);
        assert_eq!(schema.plugin_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(schema.tasks[0].task_kind, super::TASK_KIND);
    }
}
