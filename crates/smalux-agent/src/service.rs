//! Agent 服务编排模块。
//!
//! 这里保留运行入口和模块组织；具体生命周期、控制消息、采集和上报逻辑拆到子模块。

mod bootstrap;
mod collector;
mod export;
mod message;
mod options;
mod public_ip;
mod remote;
mod reporter;

use crate::collect::LocalCollector;
use crate::config::ConfigManager;
use bootstrap::{bootstrap_once, public_ip_required_for_first_report, retry_identity_until_ready};
use collector::{collector_command_channel, collector_loop};
use export::export_supervisor;
use message::inbound::{ControlDispatcher, ControlDispatcherParts, inbound_command_loop};
use message::outbound::{OutboundSequence, outbound_channel};
use message::telemetry_update_channel;
use public_ip::public_ip_refresh_loop;
use remote::probe::RemoteProbeManager;
use remote::task::RemoteTaskManager;
use reporter::{ReporterLoopParts, reporter_command_channel, reporter_loop};
use tokio::sync::watch;

pub(crate) use message::inbound;
pub(crate) use message::inbound::{
    InboundCommand, InboundCommandEnvelope, InboundCommandSender, inbound_command_channel,
};
pub(crate) use message::listener as control;
pub(crate) use message::outbound;
pub(crate) use options::{RemoteMetricPermission, ServiceOptions};
pub(crate) use remote::probe;
pub(crate) use remote::probe::{RemoteProbeRunRequest, display_task_id as display_probe_task_id};
pub(crate) use remote::shell;
pub(crate) use remote::shell::RemoteShellOpenRequest;
pub(crate) use remote::task;
pub(crate) use remote::task::RemoteTaskRunRequest;
pub(crate) use smalux_protocol::RemoteProbeType;

/// 启动 agent 服务。
///
/// 这里是 agent 的运行时装配入口：先创建所有有界队列和远程能力管理器，再启动
/// 控制、导出、采集、公网 IP 刷新和上报任务。各任务之间不直接互相调用，统一通过
/// reporter 私有 latest 缓存、入站命令队列和出站事件队列协作，方便后续替换 transport 或新增功能。
pub(crate) async fn run(
    config_manager: ConfigManager,
    options: ServiceOptions,
) -> anyhow::Result<()> {
    options.validate()?;

    let config = config_manager.current();
    let mut config_rx = config_manager.subscribe();

    tracing::info!(
        agent_id = %config.agent_id,
        agent_version = options.agent_version,
        base_url = %config.export.base_url,
        core_interval_ms = config.core.interval.as_millis(),
        disk_interval_ms = config.disk.interval.as_millis(),
        network_interval_ms = config.network.interval.as_millis(),
        report_interval_ms = config.report.interval.as_millis(),
        basic_info_refresh_interval_ms = config.outbound.basic_info.refresh_interval.as_millis(),
        remote_shell_enabled = options.remote_shell.enabled,
        remote_shell_max_sessions = config.remote_shell.max_sessions,
        remote_task_enabled = options.remote_task.enabled,
        remote_task_max_concurrent = config.remote_task.max_concurrent,
        remote_probe_enabled = config.remote_probe.enabled,
        remote_probe_timeout_ms = config.remote_probe.timeout.as_millis(),
        "agent service starting"
    );

    // 出站事件队列承载 snapshot/delta/heartbeat、ack/error 和远程任务结果。
    // 队列有界，避免 server 断开或发送变慢时无限吃内存。
    let (outbound_tx, outbound_rx) = outbound_channel();
    let outbound_sequence = OutboundSequence::default();
    // reporter command 用于 snapshot_request 这类“控制 reporter 行为”的入站命令。
    let (reporter_command_tx, reporter_command_rx) = reporter_command_channel();
    // collector command 用于一次性进程/socket 采集，避免入站控制线程直接做重采样。
    let (collector_command_tx, collector_command_rx) = collector_command_channel();
    // telemetry update 队列承载采集结果；reporter 是 latest 缓存的唯一拥有者。
    let (telemetry_tx, telemetry_rx) = telemetry_update_channel();
    let remote_shell_manager = shell::RemoteShellManager::new(options.remote_shell.clone());
    let remote_task_manager = RemoteTaskManager::new(
        options.remote_task.clone(),
        config_manager.clone(),
        outbound_tx.clone(),
        outbound_sequence.clone(),
    );
    let remote_probe_manager = RemoteProbeManager::new(
        config_manager.clone(),
        outbound_tx.clone(),
        outbound_sequence.clone(),
    );
    let (inbound_command_tx, inbound_command_rx) = inbound_command_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let control_dispatcher = ControlDispatcher::new(ControlDispatcherParts {
        config_manager: config_manager.clone(),
        remote_shell: remote_shell_manager.clone(),
        remote_task: remote_task_manager,
        remote_probe: remote_probe_manager,
        collector_commands: collector_command_tx.clone(),
        reporter_commands: reporter_command_tx,
        outbound_tx: outbound_tx.clone(),
        sequence: outbound_sequence.clone(),
        diagnostics: options.diagnostics,
    });
    // 入站命令循环是所有协议 listener 的统一落点，Komari 和 Smalux 自有协议都会走这里。
    let control_task = tokio::spawn(inbound_command_loop(
        inbound_command_rx,
        control_dispatcher,
        shutdown_rx.clone(),
    ));
    let export_manager = config_manager.clone();
    let export_task = tokio::spawn(async move {
        export_supervisor(export_manager, outbound_rx, inbound_command_tx.clone()).await
    });
    let mut collector = LocalCollector::new();
    // bootstrap 先采一次必要指标，保证第一份 report 不会因为未采样分组缺失而空转。
    let mut state = bootstrap_once(&mut collector, &config_rx).await;
    let retry_config = config_rx.borrow().clone();
    let retry_required = public_ip_required_for_first_report(&retry_config);
    if retry_required && !state.latest_telemetry.public_ip_ready() {
        retry_identity_until_ready(&mut collector, &mut state.latest_telemetry, &mut config_rx)
            .await;
    }
    state.first_report_ready = state.latest_telemetry.first_report_ready();

    tracing::info!("agent service bootstrap completed");

    let latest_telemetry = state.latest_telemetry;
    // 后续采集循环和公网 IP 刷新只提交 update；reporter 持有 latest 缓存并组装业务 frame。
    let collector_task = tokio::spawn(collector_loop(
        collector,
        telemetry_tx.clone(),
        config_manager.subscribe(),
        collector_command_rx,
        shutdown_rx.clone(),
    ));
    let public_ip_task = tokio::spawn(public_ip_refresh_loop(
        LocalCollector::new(),
        telemetry_tx,
        config_manager.subscribe(),
        shutdown_rx.clone(),
    ));
    let reporter_task = tokio::spawn(reporter_loop(ReporterLoopParts {
        state: latest_telemetry,
        agent_version: options.agent_version,
        config_rx: config_manager.subscribe(),
        shutdown: shutdown_rx,
        outbound_tx,
        sequence: outbound_sequence,
        telemetry_updates: telemetry_rx,
        reporter_commands: reporter_command_rx,
    }));

    let service_result: anyhow::Result<()> = tokio::select! {
        result = export_task => result
            .map_err(|err| anyhow::anyhow!("export supervisor task failed: {err}"))?,
        result = collector_task => {
            result.map_err(|err| anyhow::anyhow!("collector task failed: {err}"))?;
            tracing::warn!("collector loop stopped");
            Ok(())
        }
        result = public_ip_task => {
            result.map_err(|err| anyhow::anyhow!("public IP refresh task failed: {err}"))?;
            tracing::warn!("Public IP refresh loop stopped");
            Ok(())
        }
        result = reporter_task => {
            result.map_err(|err| anyhow::anyhow!("reporter task failed: {err}"))?;
            tracing::warn!("reporter loop stopped");
            Ok(())
        }
        result = control_task => {
            result.map_err(|err| anyhow::anyhow!("inbound command task failed: {err}"))?;
            tracing::warn!("inbound command loop stopped");
            Ok(())
        }
    };

    shutdown_tx.send_replace(true);
    service_result
}

#[cfg(test)]
mod tests {
    //! 服务编排辅助逻辑测试。

    use super::export::export_supervisor;
    use super::inbound::{
        ControlDispatcher, ControlDispatcherParts, InboundCommandReceiver, inbound_command_channel,
    };
    use super::message::{TelemetryUpdateSender, telemetry_update_channel};
    use super::outbound::{
        OutboundEvent, OutboundSequence, RemoteProbeResultEnvelope, RemoteTaskResultEnvelope,
        outbound_channel,
    };
    use super::reporter::{ReporterLoopParts, reporter_command_channel, reporter_loop};
    use super::{RemoteMetricPermission, ServiceOptions};
    use crate::collect::{CoreSample, DiskSample, NetworkSample, ProcessSample, SocketSample};
    use crate::config::model::{AgentConfigPatch, ExportAuthMode, ExportFormat, GroupConfigPatch};
    use crate::export::wire;
    use crate::service::collector::{CollectorCommand, collector_command_channel};
    use crate::service::control::ServiceControlListener;
    use crate::service::reporter::reporter_tick_once;
    use crate::telemetry::TelemetryUpdate;
    use futures_util::{SinkExt, StreamExt};
    use smalux_core::model::info::{
        CoreInfo, DiskInfo, IdentityInfo, MemoryInfo, MetricLevel, NetworkInfo, ProcessInfo,
        SocketAccuracy, SocketInfo, SocketSource, SystemInfo,
    };
    use smalux_protocol::{
        ClientPayload, ServerFrame, SnapshotRequest, decode_client_frame, encode_server_frame,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, watch};
    use tokio::task::JoinHandle;
    use tokio_tungstenite::tungstenite::protocol::Message;

    /// 测试用接收超时时间。
    const TEST_RECV_TIMEOUT: Duration = Duration::from_secs(5);

    /// service 级 agent 测试句柄。
    struct ServiceHarness {
        /// 运行时关闭通知。
        shutdown_tx: watch::Sender<bool>,
        /// 导出监管任务。
        export_task: JoinHandle<anyhow::Result<()>>,
        /// 上报任务。
        reporter_task: JoinHandle<()>,
        /// mock WebSocket server 任务。
        server_task: JoinHandle<()>,
        /// telemetry 更新发送端。
        telemetry_tx: TelemetryUpdateSender,
    }

    impl ServiceHarness {
        /// 关闭运行级测试任务。
        async fn shutdown(self) {
            self.shutdown_tx.send_replace(true);

            let _ = tokio::time::timeout(Duration::from_secs(1), self.reporter_task).await;
            let _ = tokio::time::timeout(Duration::from_secs(1), self.export_task).await;
            self.server_task.abort();
            let _ = self.server_task.await;
        }
    }

    /// 启动一个收集 agent 文本消息的 WebSocket mock server。
    async fn spawn_collecting_ws_server() -> (String, mpsc::Receiver<String>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (received_tx, received_rx) = mpsc::channel(16);

        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Text(text) => {
                        received_tx.send(text.to_string()).await.unwrap();
                    }
                    Message::Binary(bytes) => {
                        let packet = wire::decode_wire_packet(&bytes).unwrap();
                        let text = String::from_utf8(packet.payload).unwrap();
                        received_tx.send(text).await.unwrap();
                    }
                    Message::Ping(payload) => {
                        let _ = websocket.send(Message::Pong(payload)).await;
                    }
                    Message::Close(frame) => {
                        let _ = websocket.send(Message::Close(frame)).await;
                        break;
                    }
                    _ => {}
                }
            }
        });

        (url, received_rx, task)
    }

    /// 启动首个连接读一条后关闭、第二个连接持续收集的 WebSocket mock server。
    async fn spawn_reconnecting_ws_server() -> (String, mpsc::Receiver<String>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (received_tx, received_rx) = mpsc::channel(16);

        let task = tokio::spawn(async move {
            let (first_stream, _) = listener.accept().await.unwrap();
            let mut first = tokio_tungstenite::accept_async(first_stream).await.unwrap();
            if let Some(message) = first.next().await {
                forward_ws_message(message.unwrap(), &received_tx).await;
            }
            let _ = first.close(None).await;

            let (second_stream, _) = listener.accept().await.unwrap();
            let mut second = tokio_tungstenite::accept_async(second_stream)
                .await
                .unwrap();
            while let Some(message) = second.next().await {
                forward_ws_message(message.unwrap(), &received_tx).await;
            }
        });

        (url, received_rx, task)
    }

    /// 把 WebSocket 消息转换为测试文本。
    async fn forward_ws_message(message: Message, received_tx: &mpsc::Sender<String>) {
        match message {
            Message::Text(text) => {
                received_tx.send(text.to_string()).await.unwrap();
            }
            Message::Binary(bytes) => {
                let packet = wire::decode_wire_packet(&bytes).unwrap();
                let text = String::from_utf8(packet.payload).unwrap();
                received_tx.send(text).await.unwrap();
            }
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => {}
            _ => {}
        }
    }

    /// mock HTTP server 捕获到的请求。
    #[derive(Debug)]
    struct HttpCapture {
        /// HTTP 请求方法。
        method: String,
        /// 请求 path 和 query。
        path_and_query: String,
        /// JSON 请求体。
        body: serde_json::Value,
    }

    /// 启动 Komari WebSocket + HTTP basic info mock server。
    async fn spawn_komari_ws_mock_server() -> (
        String,
        mpsc::Receiver<String>,
        mpsc::Receiver<HttpCapture>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (ws_tx, ws_rx) = mpsc::channel(16);
        let (http_tx, http_rx) = mpsc::channel(16);

        let task = tokio::spawn(async move {
            let (http_stream, _) = listener.accept().await.unwrap();
            let capture = read_http_json_request(http_stream).await.unwrap();
            http_tx.send(capture).await.unwrap();

            let (ws_stream, _) = listener.accept().await.unwrap();
            let ws_task = tokio::spawn(handle_komari_ws_connection(ws_stream, ws_tx));

            let _ = ws_task.await;
        });

        (url, ws_rx, http_rx, task)
    }

    /// 启动 Komari mock server，HTTP basic info 固定返回 500。
    async fn spawn_komari_ws_mock_server_with_http_failure() -> (
        String,
        mpsc::Receiver<String>,
        mpsc::Receiver<HttpCapture>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (ws_tx, ws_rx) = mpsc::channel(16);
        let (http_tx, http_rx) = mpsc::channel(16);

        let task = tokio::spawn(async move {
            let (http_stream, _) = listener.accept().await.unwrap();
            let capture = read_http_json_request_with_response(
                http_stream,
                b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 4\r\nconnection: close\r\n\r\nFAIL",
            )
            .await
            .unwrap();
            http_tx.send(capture).await.unwrap();

            let (ws_stream, _) = listener.accept().await.unwrap();
            let ws_task = tokio::spawn(handle_komari_ws_connection(ws_stream, ws_tx));

            let _ = ws_task.await;
        });

        (url, ws_rx, http_rx, task)
    }

    /// 启动只接收 Komari task result HTTP 请求的 mock server。
    async fn spawn_komari_task_result_mock_server()
    -> (String, mpsc::Receiver<HttpCapture>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (http_tx, http_rx) = mpsc::channel(16);

        let task = tokio::spawn(async move {
            let (http_stream, _) = listener.accept().await.unwrap();
            let capture = read_http_json_request(http_stream).await.unwrap();
            http_tx.send(capture).await.unwrap();
        });

        (url, http_rx, task)
    }

    /// 处理 Komari WebSocket report 连接。
    async fn handle_komari_ws_connection(stream: TcpStream, received_tx: mpsc::Sender<String>) {
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

        while let Some(message) = websocket.next().await {
            match message.unwrap() {
                Message::Text(text) => {
                    received_tx.send(text.to_string()).await.unwrap();
                }
                Message::Ping(payload) => {
                    let _ = websocket.send(Message::Pong(payload)).await;
                }
                Message::Close(frame) => {
                    let _ = websocket.send(Message::Close(frame)).await;
                    break;
                }
                _ => {}
            }
        }
    }

    /// 读取一个 HTTP JSON 请求并返回 200。
    async fn read_http_json_request(stream: TcpStream) -> anyhow::Result<HttpCapture> {
        read_http_json_request_with_response(
            stream,
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nOK",
        )
        .await
    }

    /// 读取一个 HTTP JSON 请求并返回指定响应。
    async fn read_http_json_request_with_response(
        mut stream: TcpStream,
        response: &'static [u8],
    ) -> anyhow::Result<HttpCapture> {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                anyhow::bail!("http client closed before request headers completed");
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(index) = find_header_end(&buffer) {
                break index;
            }
        };

        let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
        let content_length = parse_content_length(&headers)?;
        let body_start = header_end + 4;
        while buffer.len() < body_start + content_length {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                anyhow::bail!("http client closed before body completed");
            }
            buffer.extend_from_slice(&chunk[..read]);
        }

        let request_line = headers
            .lines()
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid http request line"))?;
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid http request method"))?
            .to_string();
        let path_and_query = request_parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid http request path"))?
            .to_string();
        let body = serde_json::from_slice(&buffer[body_start..body_start + content_length])?;

        stream.write_all(response).await?;

        Ok(HttpCapture {
            method,
            path_and_query,
            body,
        })
    }

    /// 查找 HTTP header 结束位置。
    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    /// 解析 HTTP Content-Length。
    fn parse_content_length(headers: &str) -> anyhow::Result<usize> {
        headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>())
            })
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("missing http content-length"))
    }

    /// 构造已满足第一包上报条件的测试缓存。
    fn ready_latest_telemetry() -> crate::telemetry::LatestTelemetry {
        let mut latest_telemetry = crate::telemetry::LatestTelemetry::default();
        latest_telemetry.set_identity(IdentityInfo {
            agent_id: "agent-service".to_string(),
            hostname: "host-service".to_string(),
            ..IdentityInfo::default()
        });
        latest_telemetry.set_system(SystemInfo {
            hostname: "host-service".to_string(),
            ..SystemInfo::default()
        });
        latest_telemetry.set_core(CoreSample {
            sampled_at: 1,
            value: CoreInfo::default(),
        });
        latest_telemetry.set_disk(DiskSample {
            sampled_at: 1,
            value: DiskInfo::default(),
        });
        latest_telemetry.set_network(NetworkSample {
            sampled_at: 1,
            value: NetworkInfo::default(),
        });
        latest_telemetry.set_processes(ProcessSample {
            sampled_at: 1,
            value: ProcessInfo::ready(42),
        });
        latest_telemetry.set_sockets(SocketSample {
            sampled_at: 1,
            value: SocketInfo::ready(
                10,
                3,
                SocketSource::SocketTable,
                SocketAccuracy::SocketTable,
            ),
        });
        latest_telemetry
    }

    /// 构造默认关闭的远程 shell manager。
    fn disabled_remote_shell_manager() -> super::shell::RemoteShellManager {
        super::shell::RemoteShellManager::new(super::shell::RemoteShellOptions::default())
    }

    /// 构造默认关闭的远程任务管理器。
    fn disabled_remote_task_manager(
        manager: crate::config::ConfigManager,
        outbound_tx: super::outbound::OutboundSender,
        sequence: OutboundSequence,
    ) -> super::task::RemoteTaskManager {
        super::task::RemoteTaskManager::new(
            super::task::RemoteTaskOptions::default(),
            manager,
            outbound_tx,
            sequence,
        )
    }

    /// 构造远程探测管理器。
    fn make_remote_probe_manager(
        manager: crate::config::ConfigManager,
        outbound_tx: super::outbound::OutboundSender,
        sequence: OutboundSequence,
    ) -> super::probe::RemoteProbeManager {
        super::probe::RemoteProbeManager::new(manager, outbound_tx, sequence)
    }

    /// 控制消息测试夹具。
    struct ControlHarness {
        /// 协议 listener。
        listener: ServiceControlListener,
        /// 内部命令调度器。
        dispatcher: ControlDispatcher,
        /// 入站命令接收端。
        command_rx: InboundCommandReceiver,
        /// 保持测试出站队列接收端存活，避免 rejected 结果投递失败。
        _outbound_rx: mpsc::Receiver<OutboundEvent>,
        /// reporter 控制命令接收端。
        reporter_command_rx: super::reporter::ReporterCommandReceiver,
    }

    impl ControlHarness {
        /// 解析、投递并执行一条 server 控制消息。
        async fn handle_message(&mut self, message: &str) -> anyhow::Result<()> {
            self.listener.handle_message(message).await?;
            let command = self
                .command_rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("inbound command channel closed"))?;
            self.dispatcher.dispatch(command)
        }
    }

    /// 构造测试用控制消息链路。
    fn service_control_harness(manager: crate::config::ConfigManager) -> ControlHarness {
        let (collector_commands, _collector_command_rx) = collector_command_channel();
        let (inbound_commands, command_rx) = inbound_command_channel();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (reporter_commands, reporter_command_rx) = reporter_command_channel();
        let remote_task_manager =
            disabled_remote_task_manager(manager.clone(), outbound_tx.clone(), sequence.clone());
        let remote_probe_manager =
            make_remote_probe_manager(manager.clone(), outbound_tx.clone(), sequence.clone());
        let dispatcher = ControlDispatcher::new(ControlDispatcherParts {
            config_manager: manager,
            remote_shell: disabled_remote_shell_manager(),
            remote_task: remote_task_manager,
            remote_probe: remote_probe_manager,
            collector_commands,
            reporter_commands,
            outbound_tx,
            sequence,
            diagnostics: ServiceOptions::default().diagnostics,
        });

        ControlHarness {
            listener: ServiceControlListener::new(inbound_commands),
            dispatcher,
            command_rx,
            _outbound_rx: outbound_rx,
            reporter_command_rx,
        }
    }

    /// 构造可接收采集命令的测试控制消息链路。
    fn service_control_harness_with_commands(
        manager: crate::config::ConfigManager,
        diagnostics: super::options::DiagnosticOptions,
    ) -> (ControlHarness, mpsc::Receiver<CollectorCommand>) {
        let (collector_commands, collector_command_rx) = collector_command_channel();
        let (inbound_commands, command_rx) = inbound_command_channel();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let sequence = OutboundSequence::default();
        let (reporter_commands, reporter_command_rx) = reporter_command_channel();
        let remote_task_manager =
            disabled_remote_task_manager(manager.clone(), outbound_tx.clone(), sequence.clone());
        let remote_probe_manager =
            make_remote_probe_manager(manager.clone(), outbound_tx.clone(), sequence.clone());
        let dispatcher = ControlDispatcher::new(ControlDispatcherParts {
            config_manager: manager,
            remote_shell: disabled_remote_shell_manager(),
            remote_task: remote_task_manager,
            remote_probe: remote_probe_manager,
            collector_commands,
            reporter_commands,
            outbound_tx,
            sequence,
            diagnostics,
        });

        let harness = ControlHarness {
            listener: ServiceControlListener::new(inbound_commands),
            dispatcher,
            command_rx,
            _outbound_rx: outbound_rx,
            reporter_command_rx,
        };

        (harness, collector_command_rx)
    }

    /// 启动 reporter + export supervisor，用真实 WebSocket 发送消息到 mock server。
    async fn spawn_report_export_service(
        base_url: String,
        server_task: JoinHandle<()>,
        latest_telemetry: crate::telemetry::LatestTelemetry,
        configure: impl FnOnce(&mut crate::config::AgentConfig),
    ) -> ServiceHarness {
        let mut config = crate::config::AgentConfig {
            agent_id: "agent-service".to_string(),
            export: crate::config::model::ExportConfig {
                base_url,
                heartbeat: Duration::from_secs(30),
                ..crate::config::model::ExportConfig::default()
            },
            report: crate::config::model::ReportConfig {
                interval: Duration::from_millis(100),
                ..crate::config::model::ReportConfig::default()
            },
            ..crate::config::AgentConfig::default()
        };
        configure(&mut config);

        let manager = crate::config::ConfigManager::new(config).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let (inbound_commands, _inbound_command_rx) = inbound_command_channel();
        let (telemetry_tx, telemetry_rx) = telemetry_update_channel();
        let (_reporter_command_tx, reporter_command_rx) = reporter_command_channel();
        let export_task = tokio::spawn(export_supervisor(
            manager.clone(),
            outbound_rx,
            inbound_commands,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let reporter_task = tokio::spawn(reporter_loop(ReporterLoopParts {
            state: latest_telemetry,
            agent_version: "0.1.0-service-test",
            config_rx: manager.subscribe(),
            shutdown: shutdown_rx,
            outbound_tx,
            sequence: OutboundSequence::default(),
            telemetry_updates: telemetry_rx,
            reporter_commands: reporter_command_rx,
        }));

        ServiceHarness {
            shutdown_tx,
            export_task,
            reporter_task,
            server_task,
            telemetry_tx,
        }
    }

    /// 接收并解码一条 agent 发出的 smalux_json frame。
    async fn recv_client_frame(
        receiver: &mut mpsc::Receiver<String>,
    ) -> smalux_protocol::ClientFrame {
        let text = tokio::time::timeout(TEST_RECV_TIMEOUT, receiver.recv())
            .await
            .expect("timed out waiting for agent message")
            .expect("agent message channel closed");

        decode_client_frame(&text).unwrap()
    }

    /// 在固定时间内接收指定类型的 agent frame。
    async fn recv_client_frame_matching(
        receiver: &mut mpsc::Receiver<String>,
        description: &str,
        mut accept: impl FnMut(&ClientPayload) -> bool,
    ) -> smalux_protocol::ClientFrame {
        let deadline = tokio::time::Instant::now() + TEST_RECV_TIMEOUT;

        loop {
            let now = tokio::time::Instant::now();
            let remaining = deadline
                .checked_duration_since(now)
                .unwrap_or_else(|| panic!("timed out waiting for {description}"));
            let text = tokio::time::timeout(remaining, receiver.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {description}"))
                .expect("agent message channel closed");
            let frame = decode_client_frame(&text).unwrap();
            if accept(&frame.payload) {
                return frame;
            }
        }
    }

    /// 接收并解析一条 Komari WebSocket JSON。
    async fn recv_komari_ws_json(receiver: &mut mpsc::Receiver<String>) -> serde_json::Value {
        let text = tokio::time::timeout(TEST_RECV_TIMEOUT, receiver.recv())
            .await
            .expect("timed out waiting for komari websocket report")
            .expect("komari websocket channel closed");

        serde_json::from_str(&text).unwrap()
    }

    /// 接收一个 mock HTTP 请求。
    async fn recv_http_capture(receiver: &mut mpsc::Receiver<HttpCapture>) -> HttpCapture {
        tokio::time::timeout(TEST_RECV_TIMEOUT, receiver.recv())
            .await
            .expect("timed out waiting for komari http request")
            .expect("komari http channel closed")
    }

    /// 验证默认运行级链路会通过 WebSocket 持续发送完整 snapshot。
    #[tokio::test]
    async fn service_export_sends_snapshot_by_default() {
        let (url, mut received_rx, server_task) = spawn_collecting_ws_server().await;
        let latest_telemetry = ready_latest_telemetry();
        let harness =
            spawn_report_export_service(url, server_task, latest_telemetry, |_config| {}).await;

        let first = recv_client_frame(&mut received_rx).await;
        let second = recv_client_frame(&mut received_rx).await;
        harness.shutdown().await;

        assert_eq!(first.agent_id, "agent-service");
        assert_eq!(first.sequence, 1);
        assert!(matches!(first.payload, ClientPayload::Snapshot { .. }));
        assert!(second.sequence > first.sequence);
        assert!(matches!(second.payload, ClientPayload::Snapshot { .. }));
    }

    /// 验证实时 WebSocket 发送失败后会重连并继续发送最新上报。
    #[tokio::test]
    async fn service_export_reconnects_after_realtime_transport_failure() {
        let (url, mut received_rx, server_task) = spawn_reconnecting_ws_server().await;
        let latest_telemetry = ready_latest_telemetry();
        let harness = spawn_report_export_service(url, server_task, latest_telemetry, |config| {
            config.export.reconnect_interval = Duration::from_millis(100);
        })
        .await;

        let first = recv_client_frame(&mut received_rx).await;
        let second = recv_client_frame(&mut received_rx).await;
        harness.shutdown().await;

        assert!(matches!(first.payload, ClientPayload::Snapshot { .. }));
        assert!(matches!(second.payload, ClientPayload::Snapshot { .. }));
        assert!(second.sequence > first.sequence);
    }

    /// 验证 export supervisor 会发送远程任务结果。
    #[tokio::test]
    async fn service_export_sends_remote_task_result() {
        let (url, mut received_rx, server_task) = spawn_collecting_ws_server().await;
        let config = crate::config::AgentConfig {
            agent_id: "agent-service".to_string(),
            export: crate::config::model::ExportConfig {
                base_url: url,
                ..crate::config::model::ExportConfig::default()
            },
            ..crate::config::AgentConfig::default()
        };
        let manager = crate::config::ConfigManager::new(config).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let (inbound_commands, _inbound_command_rx) = inbound_command_channel();
        let export_task = tokio::spawn(export_supervisor(
            manager.clone(),
            outbound_rx,
            inbound_commands,
        ));

        outbound_tx
            .send(OutboundEvent::RemoteTaskResult(RemoteTaskResultEnvelope {
                agent_id: "agent-service".to_string(),
                sequence: 1,
                created_at: 100,
                result: smalux_protocol::RemoteTaskResult {
                    task_id: "task-1".to_string(),
                    status: smalux_protocol::RemoteTaskStatus::Success,
                    exit_code: Some(0),
                    stdout: "ok".to_string(),
                    stderr: String::new(),
                    started_at: 99,
                    finished_at: 100,
                    duration_ms: 1000,
                    timed_out: false,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    error: None,
                },
            }))
            .await
            .unwrap();

        let frame = recv_client_frame(&mut received_rx).await;
        drop(outbound_tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), export_task).await;
        server_task.abort();
        let _ = server_task.await;

        match frame.payload {
            ClientPayload::RemoteTaskResult { result } => {
                assert_eq!(result.task_id, "task-1");
                assert_eq!(result.status, smalux_protocol::RemoteTaskStatus::Success);
            }
            _ => panic!("expected remote task result frame"),
        }
    }

    /// 验证 Komari 模式会把 remote task result 发送到 HTTP task/result。
    #[tokio::test]
    async fn service_export_sends_komari_remote_task_result() {
        let (url, mut http_rx, server_task) = spawn_komari_task_result_mock_server().await;
        let config = crate::config::AgentConfig {
            agent_id: "agent-service".to_string(),
            export: crate::config::model::ExportConfig {
                format: ExportFormat::Komari,
                auth_mode: ExportAuthMode::Query,
                token: Some("secret-token".to_string()),
                base_url: url,
                ..crate::config::model::ExportConfig::default()
            },
            ..crate::config::AgentConfig::default()
        };
        let manager = crate::config::ConfigManager::new(config).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let (inbound_commands, _inbound_command_rx) = inbound_command_channel();
        let export_task = tokio::spawn(export_supervisor(
            manager.clone(),
            outbound_rx,
            inbound_commands,
        ));

        outbound_tx
            .send(OutboundEvent::RemoteTaskResult(RemoteTaskResultEnvelope {
                agent_id: "agent-service".to_string(),
                sequence: 1,
                created_at: 100,
                result: smalux_protocol::RemoteTaskResult {
                    task_id: "task-1".to_string(),
                    status: smalux_protocol::RemoteTaskStatus::Success,
                    exit_code: Some(0),
                    stdout: "ok".to_string(),
                    stderr: String::new(),
                    started_at: 99,
                    finished_at: 100,
                    duration_ms: 1000,
                    timed_out: false,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    error: None,
                },
            }))
            .await
            .unwrap();

        let capture = recv_http_capture(&mut http_rx).await;
        drop(outbound_tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), export_task).await;
        server_task.abort();
        let _ = server_task.await;

        assert_eq!(capture.method, "POST");
        assert_eq!(
            capture.path_and_query,
            "/api/clients/task/result?token=secret-token"
        );
        assert_eq!(capture.body["task_id"], "task-1");
        assert_eq!(capture.body["result"], "ok");
        assert_eq!(capture.body["exit_code"], 0);
    }

    /// 验证 Komari 模式会把 remote probe result 发送为 WebSocket ping_result。
    #[tokio::test]
    async fn service_export_sends_komari_remote_probe_result() {
        let (base_url, mut ws_rx, server_task) = spawn_collecting_ws_server().await;
        let config = crate::config::AgentConfig {
            agent_id: "agent-service".to_string(),
            export: crate::config::model::ExportConfig {
                format: ExportFormat::Komari,
                auth_mode: ExportAuthMode::Query,
                token: Some("secret-token".to_string()),
                base_url,
                ..crate::config::model::ExportConfig::default()
            },
            ..crate::config::AgentConfig::default()
        };
        let manager = crate::config::ConfigManager::new(config).unwrap();
        let (outbound_tx, outbound_rx) = outbound_channel();
        let (inbound_commands, _inbound_command_rx) = inbound_command_channel();
        let export_task = tokio::spawn(export_supervisor(
            manager.clone(),
            outbound_rx,
            inbound_commands,
        ));

        outbound_tx
            .send(OutboundEvent::RemoteProbeResult(
                RemoteProbeResultEnvelope {
                    agent_id: "agent-service".to_string(),
                    sequence: 1,
                    created_at: 100,
                    result: smalux_protocol::RemoteProbeResult {
                        task_id: serde_json::Value::from(123),
                        probe_type: smalux_protocol::RemoteProbeType::Tcp,
                        target: "example.com:443".to_string(),
                        value: 13,
                        started_at: 99,
                        finished_at: 100,
                        duration_ms: 13,
                        error: None,
                    },
                },
            ))
            .await
            .unwrap();

        let message = recv_komari_ws_json(&mut ws_rx).await;
        drop(outbound_tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), export_task).await;
        server_task.abort();
        let _ = server_task.await;

        assert_eq!(message["type"], "ping_result");
        assert_eq!(message["task_id"], 123);
        assert_eq!(message["ping_type"], "tcp");
        assert_eq!(message["value"], 13);
    }

    /// 验证运行级链路在开启 delta 和业务心跳后会发送对应 frame。
    #[tokio::test]
    async fn service_export_sends_delta_and_business_heartbeat() {
        let (url, mut received_rx, server_task) = spawn_collecting_ws_server().await;
        let latest_telemetry = ready_latest_telemetry();
        let harness = spawn_report_export_service(url, server_task, latest_telemetry, |config| {
            config.report.delta_enabled = true;
            config.report.heartbeat_enabled = true;
            config.report.heartbeat_interval = Duration::from_secs(1);
            config.report.snapshot_interval = Duration::from_secs(60);
        })
        .await;

        let snapshot = recv_client_frame(&mut received_rx).await;
        harness
            .telemetry_tx
            .send(TelemetryUpdate::Core(CoreSample {
                sampled_at: 2,
                value: CoreInfo {
                    memory: MemoryInfo {
                        memory_usage: 99,
                        ..MemoryInfo::default()
                    },
                    ..CoreInfo::default()
                },
            }))
            .await
            .unwrap();
        let delta = recv_client_frame_matching(&mut received_rx, "delta frame", |payload| {
            matches!(payload, ClientPayload::Delta { .. })
        })
        .await;
        let heartbeat =
            recv_client_frame_matching(&mut received_rx, "heartbeat frame", |payload| {
                matches!(payload, ClientPayload::Heartbeat { .. })
            })
            .await;
        harness.shutdown().await;

        assert!(matches!(snapshot.payload, ClientPayload::Snapshot { .. }));
        match delta.payload {
            ClientPayload::Delta { delta } => {
                assert_eq!(delta.base_sequence, 1);
                assert!(delta.core.is_some());
                assert!(delta.identity.is_none());
            }
            _ => panic!("expected delta frame"),
        }
        match heartbeat.payload {
            ClientPayload::Heartbeat { heartbeat } => {
                assert_eq!(heartbeat.last_report_sequence, Some(1));
            }
            _ => panic!("expected heartbeat frame"),
        }
    }

    /// 验证 Komari WebSocket 模式会发送 WS report 和 HTTP basic info。
    #[tokio::test]
    async fn service_export_sends_komari_websocket_report_and_basic_info() {
        let (url, mut ws_rx, mut http_rx, server_task) = spawn_komari_ws_mock_server().await;
        let latest_telemetry = ready_latest_telemetry();
        let harness = spawn_report_export_service(url, server_task, latest_telemetry, |config| {
            config.export.format = ExportFormat::Komari;
            config.export.auth_mode = ExportAuthMode::Query;
            config.export.token = Some("secret-token".to_string());
        })
        .await;

        let report = recv_komari_ws_json(&mut ws_rx).await;
        let basic_info = recv_http_capture(&mut http_rx).await;
        harness.shutdown().await;

        assert!(report.get("type").is_none());
        assert!(report["cpu"]["usage"].is_number());
        assert!(report["network"]["totalUp"].is_number());
        assert_eq!(basic_info.method, "POST");
        assert_eq!(
            basic_info.path_and_query,
            "/api/clients/uploadBasicInfo?token=secret-token"
        );
        assert_eq!(basic_info.body["version"], "0.1.0-service-test");
        assert!(basic_info.body.get("mem_total").is_some());
    }

    /// 验证 Komari basic info HTTP 失败时实时 WebSocket 上报仍然继续。
    #[tokio::test]
    async fn service_export_continues_after_basic_info_transport_failure() {
        let (url, mut ws_rx, mut http_rx, server_task) =
            spawn_komari_ws_mock_server_with_http_failure().await;
        let latest_telemetry = ready_latest_telemetry();
        let harness = spawn_report_export_service(url, server_task, latest_telemetry, |config| {
            config.export.format = ExportFormat::Komari;
            config.export.auth_mode = ExportAuthMode::Query;
            config.export.token = Some("secret-token".to_string());
        })
        .await;

        let report = recv_komari_ws_json(&mut ws_rx).await;
        let basic_info = recv_http_capture(&mut http_rx).await;
        harness.shutdown().await;

        assert!(report["cpu"]["usage"].is_number());
        assert_eq!(basic_info.method, "POST");
    }

    /// 验证 reporter tick 会在缺少第一包必需数据时报错。
    #[tokio::test]
    async fn reporter_tick_requires_ready_latest_telemetry() {
        let latest_telemetry = crate::telemetry::LatestTelemetry::default();

        assert!(reporter_tick_once(&latest_telemetry, "0.1.0").is_err());
    }

    /// 验证 server 下发配置 patch 后会更新动态配置。
    #[tokio::test]
    async fn service_control_listener_applies_config_patch() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager.clone());

        harness
            .handle_message(
                r#"{
                    "type": "config_patch",
                    "patch": { "core": { "interval": "2s" } }
                }"#,
            )
            .await
            .unwrap();

        assert_eq!(manager.current().core.interval, Duration::from_secs(2));
    }

    /// 验证 framed snapshot_request 会进入 reporter 命令队列并回 ack。
    #[tokio::test]
    async fn service_control_listener_accepts_snapshot_request() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager);
        let message = encode_server_frame(&ServerFrame::snapshot_request(
            77,
            100,
            SnapshotRequest {
                reason: Some("delta_base_missing".to_string()),
            },
        ))
        .unwrap();

        harness.handle_message(&message).await.unwrap();

        let event = harness._outbound_rx.recv().await.unwrap();
        let OutboundEvent::ControlAck(ack) = event else {
            panic!("expected control ack");
        };
        assert_eq!(ack.ack.sequence, 77);
        let command = harness.reporter_command_rx.try_recv().unwrap();
        assert!(matches!(
            command,
            super::reporter::ReporterCommand::ForceSnapshot { .. }
        ));
    }

    /// 验证不能执行的 framed 控制命令会回 error。
    #[tokio::test]
    async fn service_control_listener_errors_snapshot_request_when_report_disabled() {
        let mut config = crate::config::AgentConfig::default();
        config.report.enabled = false;
        let manager = crate::config::ConfigManager::new(config).unwrap();
        let mut harness = service_control_harness(manager);
        let message = encode_server_frame(&ServerFrame::snapshot_request(
            78,
            100,
            SnapshotRequest {
                reason: Some("manual".to_string()),
            },
        ))
        .unwrap();

        let error = harness.handle_message(&message).await.unwrap_err();

        assert!(error.to_string().contains("reporting is disabled"));
        let event = harness._outbound_rx.recv().await.unwrap();
        let OutboundEvent::ControlError(error) = event else {
            panic!("expected control error");
        };
        assert_eq!(error.error.sequence, Some(78));
        assert_eq!(error.error.code, "snapshot_request_failed");
    }

    /// 验证一次性进程采集请求会投递到采集循环。
    #[tokio::test]
    async fn service_control_listener_sends_process_collection_command() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let diagnostics = super::options::DiagnosticOptions {
            process_permission: RemoteMetricPermission::Details,
            ..super::options::DiagnosticOptions::default()
        };
        let (mut harness, mut command_rx) =
            service_control_harness_with_commands(manager, diagnostics);

        harness
            .handle_message(
                r#"{
                    "type": "collect_processes_once",
                    "level": "details",
                    "limit": 5
                }"#,
            )
            .await
            .unwrap();

        assert_eq!(
            command_rx.try_recv().unwrap(),
            CollectorCommand::SampleProcessesOnce {
                level: MetricLevel::Details,
                limit: 5
            }
        );
    }

    /// 验证一次性 Socket 采集请求会投递到采集循环。
    #[tokio::test]
    async fn service_control_listener_sends_socket_collection_command() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let diagnostics = super::options::DiagnosticOptions {
            socket_permission: RemoteMetricPermission::Details,
            ..super::options::DiagnosticOptions::default()
        };
        let (mut harness, mut command_rx) =
            service_control_harness_with_commands(manager, diagnostics);

        harness
            .handle_message(
                r#"{
                    "type": "collect_sockets_once",
                    "level": "details",
                    "limit": 7
                }"#,
            )
            .await
            .unwrap();

        assert_eq!(
            command_rx.try_recv().unwrap(),
            CollectorCommand::SampleSocketsOnce {
                level: MetricLevel::Details,
                limit: 7
            }
        );
    }

    /// 验证 none 权限会拒绝 server 触发 count 采集。
    #[tokio::test]
    async fn service_control_listener_rejects_count_when_permission_none() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let diagnostics = super::options::DiagnosticOptions {
            process_permission: RemoteMetricPermission::None,
            ..super::options::DiagnosticOptions::default()
        };
        let (mut harness, mut command_rx) =
            service_control_harness_with_commands(manager, diagnostics);

        let error = harness
            .handle_message(
                r#"{
                    "type": "collect_processes_once",
                    "level": "count",
                    "limit": 5
                }"#,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("allowed level is none"));
        assert!(command_rx.try_recv().is_err());
    }

    /// 验证 light 权限允许 server 触发 light 采集。
    #[tokio::test]
    async fn service_control_listener_allows_light_when_permission_light() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let diagnostics = super::options::DiagnosticOptions {
            socket_permission: RemoteMetricPermission::Light,
            ..super::options::DiagnosticOptions::default()
        };
        let (mut harness, mut command_rx) =
            service_control_harness_with_commands(manager, diagnostics);

        harness
            .handle_message(
                r#"{
                    "type": "collect_sockets_once",
                    "level": "light",
                    "limit": 7
                }"#,
            )
            .await
            .unwrap();

        assert_eq!(
            command_rx.try_recv().unwrap(),
            CollectorCommand::SampleSocketsOnce {
                level: MetricLevel::Light,
                limit: 7
            }
        );
    }

    /// 验证未通过启动参数授权时，server 不能打开超过 count 的远程采集。
    #[tokio::test]
    async fn service_control_listener_rejects_remote_level_above_permission() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let (mut harness, _command_rx) = service_control_harness_with_commands(
            manager.clone(),
            ServiceOptions::default().diagnostics,
        );

        let error = harness
            .handle_message(
                r#"{
                    "type": "config_patch",
                    "patch": { "processes": { "level": "light" } }
                }"#,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("process light"));
        assert_eq!(manager.current().processes.level, MetricLevel::Count);
    }

    /// 验证一次性诊断同样受启动参数授权保护。
    #[tokio::test]
    async fn service_control_listener_rejects_one_shot_level_above_permission() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let (mut harness, mut command_rx) =
            service_control_harness_with_commands(manager, ServiceOptions::default().diagnostics);

        let error = harness
            .handle_message(
                r#"{
                    "type": "collect_sockets_once",
                    "level": "details",
                    "limit": 7
                }"#,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("socket details"));
        assert!(command_rx.try_recv().is_err());
    }

    /// 验证未知 server 消息不会被静默吞掉。
    #[tokio::test]
    async fn service_control_listener_rejects_unknown_message_type() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager);

        let error = harness
            .handle_message(r#"{ "type": "unknown", "patch": {} }"#)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("unknown variant"));
    }

    /// 验证远程 shell 默认关闭时控制消息会被拒绝。
    #[tokio::test]
    async fn service_control_listener_rejects_remote_shell_when_disabled() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager);

        let error = harness
            .handle_message(
                r#"{
                    "type": "remote_shell_open",
                    "session_id": "shell-1",
                    "stream_url": "ws://127.0.0.1:1/shell"
                }"#,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("remote shell is disabled"));
    }

    /// 验证远程任务默认关闭时不会执行，只回传 rejected 结果。
    #[tokio::test]
    async fn service_control_listener_rejects_remote_task_when_disabled() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager);

        harness
            .handle_message(
                r#"{
                    "type": "remote_task_run",
                    "task_id": "task-disabled",
                    "program": "noop",
                    "args": [],
                    "timeout": "1s"
                }"#,
            )
            .await
            .unwrap();

        let event = harness._outbound_rx.recv().await.unwrap();
        let OutboundEvent::RemoteTaskResult(result) = event else {
            panic!("expected remote task result");
        };

        assert_eq!(
            result.result.status,
            smalux_protocol::RemoteTaskStatus::Rejected
        );
    }

    /// 验证远程探测默认关闭时不会发包，只回传 value=-1。
    #[tokio::test]
    async fn service_control_listener_rejects_remote_probe_when_disabled() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager);

        harness
            .handle_message(
                r#"{
                    "type": "remote_probe_run",
                    "task_id": 123,
                    "probe_type": "tcp",
                    "target": "127.0.0.1:1"
                }"#,
            )
            .await
            .unwrap();

        let event = harness._outbound_rx.recv().await.unwrap();
        let OutboundEvent::RemoteProbeResult(result) = event else {
            panic!("expected remote probe result");
        };

        assert_eq!(result.result.task_id, serde_json::Value::from(123));
        assert_eq!(result.result.value, -1);
        assert!(result.result.error.as_deref().unwrap().contains("disabled"));
    }

    /// 验证过小采样间隔会被动态配置校验拒绝。
    #[tokio::test]
    async fn service_control_listener_rejects_invalid_patch() {
        let manager =
            crate::config::ConfigManager::new(crate::config::AgentConfig::default()).unwrap();
        let mut harness = service_control_harness(manager.clone());

        let error = harness
            .handle_message(
                &serde_json::to_string(&ServerControlMessageForTest {
                    message_type: "config_patch",
                    patch: AgentConfigPatch {
                        core: Some(GroupConfigPatch {
                            interval: Some(Duration::from_millis(1)),
                            ..GroupConfigPatch::default()
                        }),
                        ..AgentConfigPatch::default()
                    },
                })
                .unwrap(),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("must be at least"));
        assert_eq!(
            manager.current().core.interval,
            crate::config::AgentConfig::default().core.interval
        );
    }

    /// 测试专用 server 消息结构，避免手写复杂 JSON。
    #[derive(serde::Serialize)]
    struct ServerControlMessageForTest {
        /// 消息类型字段。
        #[serde(rename = "type")]
        message_type: &'static str,
        /// 配置 patch。
        patch: AgentConfigPatch,
    }
}
