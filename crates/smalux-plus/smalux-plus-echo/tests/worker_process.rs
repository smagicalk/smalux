use prost::Message;
use smalux_plus_core::{
    AgentContext,
    framing::{read_frame, write_frame},
    protocol::{
        self, AgentContextMessage, ExecuteTask, Hello, InitializeWorker, Shutdown, WorkerFrame,
        WorkerRequest, WorkerResponse, worker_frame, worker_request, worker_response,
    },
};
use smalux_plus_echo::{EchoMode, EchoTaskConfig, EchoTaskResult};
use tokio::{io::AsyncWriteExt, process::Command};

fn request(body: worker_request::Body) -> WorkerFrame {
    WorkerFrame {
        body: Some(worker_frame::Body::Request(WorkerRequest {
            body: Some(body),
        })),
    }
}

fn context(path: &std::path::Path) -> AgentContextMessage {
    let value = AgentContext {
        agent_version: "test-agent".to_owned(),
        operating_system: "test-os".to_owned(),
        architecture: "test-arch".to_owned(),
        agent_id: Some("agent-test".to_owned()),
        data_dir: path.display().to_string(),
        config_dir: path.join("config").display().to_string(),
        plugin_dir: path.join("plugins").display().to_string(),
        plugin_data_dir: path.join("plugin-data").display().to_string(),
    };
    AgentContextMessage {
        agent_version: value.agent_version,
        operating_system: value.operating_system,
        architecture: value.architecture,
        agent_id: value.agent_id,
        data_dir: value.data_dir,
        config_dir: value.config_dir,
        plugin_dir: value.plugin_dir,
        plugin_data_dir: value.plugin_data_dir,
    }
}

#[tokio::test]
async fn real_echo_worker_executes_with_agent_context_and_shutdown() {
    let binary = env!("CARGO_BIN_EXE_smalux-plus-echo-worker");
    let data_dir =
        std::env::temp_dir().join(format!("smalux-echo-worker-{}", uuid::Uuid::new_v4()));
    let mut child = Command::new(binary)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().unwrap());

    write_frame(
        &mut stdin,
        &request(worker_request::Body::Hello(Hello {
            protocol_version: protocol::WORKER_PROTOCOL_VERSION,
            expected_plugin_id: smalux_plus_echo::PLUGIN_ID.to_owned(),
        })),
    )
    .await
    .unwrap();
    let _ = read_frame(&mut stdout).await.unwrap().unwrap();
    write_frame(
        &mut stdin,
        &request(worker_request::Body::Initialize(InitializeWorker {
            protocol_version: protocol::WORKER_PROTOCOL_VERSION,
            plugin_id: smalux_plus_echo::PLUGIN_ID.to_owned(),
            config_revision: 7,
            runtime_config: Vec::new(),
            max_concurrency: 1,
            agent_context: Some(context(&data_dir)),
        })),
    )
    .await
    .unwrap();
    let _ = read_frame(&mut stdout).await.unwrap().unwrap();

    let config = EchoTaskConfig {
        message: "worker".to_owned(),
        delay_millis: 0,
        fail: false,
        mode: EchoMode::Diagnostic as i32,
        target: "test-target".to_owned(),
    };
    write_frame(
        &mut stdin,
        &request(worker_request::Body::Execute(ExecuteTask {
            request_id: "request-1".to_owned(),
            run_id: vec![1; 16],
            task_kind: smalux_plus_echo::TASK_KIND.to_owned(),
            schema_version: 1,
            config: config.encode_to_vec(),
            deadline_unix_millis: 0,
        })),
    )
    .await
    .unwrap();
    let _ = read_frame(&mut stdout).await.unwrap().unwrap();
    let result = read_frame(&mut stdout).await.unwrap().unwrap();
    let Some(worker_frame::Body::Response(WorkerResponse {
        body: Some(worker_response::Body::Result(result)),
    })) = result.body
    else {
        panic!("expected task result")
    };
    assert_eq!(result.status, protocol::TaskStatus::Succeeded as i32);
    let output = EchoTaskResult::decode(result.payload.as_slice()).unwrap();
    assert_eq!(output.target, "test-target");
    assert_eq!(output.mode, EchoMode::Diagnostic as i32);
    assert!(data_dir.join("plugin-data/execution-count.txt").is_file());

    write_frame(
        &mut stdin,
        &request(worker_request::Body::Shutdown(Shutdown {
            reason: "test".to_owned(),
        })),
    )
    .await
    .unwrap();
    let _ = read_frame(&mut stdout).await.unwrap().unwrap();
    stdin.shutdown().await.unwrap();
    assert!(child.wait().await.unwrap().success());
    std::fs::remove_dir_all(data_dir).unwrap();
}
