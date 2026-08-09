use smalux_protocol::{
    agent::v1::{HealthRequest, HealthResponse, ProtocolFrame},
    tonic_transport::AgentTransportRpcClient,
};

pub(crate) mod service;

#[allow(dead_code)]
struct SmaluxClient {
    pub(crate) client: Option<AgentTransportRpcClient>,
}

#[allow(dead_code)]
impl SmaluxClient {
    pub(crate) fn new() -> Self {
        tracing::debug!("creating disconnected agent transport client");
        Self { client: None }
    }

    /// 建立路径感知 gRPC Client；调用方传入源站地址与可选统一 gRPC 前缀。
    pub(crate) async fn connect(
        &mut self,
        endpoint: &str,
        grpc_prefix: Option<&str>,
    ) -> anyhow::Result<()> {
        // Endpoint 与路径标签由协议 Client 统一脱敏后记录，避免包装层重复输出原始配置。
        tracing::info!("agent transport connecting");
        match AgentTransportRpcClient::connect(endpoint, grpc_prefix).await {
            Ok(client) => {
                self.client = Some(client);
                tracing::info!("agent transport connected");
                Ok(())
            }
            Err(error) => {
                tracing::warn!(error = %error, "agent transport connection failed");
                Err(error.into())
            }
        }
    }

    /// 调用未认证健康检查，并在 Agent 层记录 RPC 成功或失败。
    pub(crate) async fn health_check(&mut self) -> anyhow::Result<HealthResponse> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("agent transport is not connected"))?;
        tracing::debug!("agent health check sending");
        match client.health_check(HealthRequest::default()).await {
            Ok(response) => {
                let response = response.into_inner();
                tracing::debug!(code = response.code, "agent health check completed");
                Ok(response)
            }
            Err(error) => {
                tracing::warn!(error = %error, "agent health check failed");
                Err(error.into())
            }
        }
    }

    /// 打开 Agent 双向流；具体握手帧由调用方提供。
    pub(crate) async fn open_session<S>(
        &mut self,
        request: S,
    ) -> anyhow::Result<tonic::Streaming<ProtocolFrame>>
    where
        S: tonic::IntoStreamingRequest<Message = ProtocolFrame>,
    {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("agent transport is not connected"))?;
        tracing::debug!("agent session opening");
        match client.open_session(request).await {
            Ok(response) => {
                tracing::info!("agent session opened");
                Ok(response.into_inner())
            }
            Err(error) => {
                tracing::warn!(error = %error, "agent session open failed");
                Err(error.into())
            }
        }
    }

    /// 主动丢弃底层 gRPC Client，记录连接关闭原因由调用方决定。
    pub(crate) fn disconnect(&mut self) {
        if self.client.take().is_some() {
            tracing::info!("agent transport disconnected");
        } else {
            tracing::debug!("agent transport disconnect requested while already disconnected");
        }
    }
}

#[allow(dead_code)]
impl Drop for SmaluxClient {
    fn drop(&mut self) {
        if self.client.is_some() {
            tracing::debug!("agent transport client dropped with an open channel");
        }
    }
}

#[cfg(test)]
mod tests {
    use smalux_core::config::default::{DEFAULT_ADDRESS, DEFAULT_AGENT_PRIFIX, DEFAULT_PORT};
    use smalux_protocol::agent::v1::{ProtocolErrorCode, ProtocolFrame, protocol_frame};
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

    use super::SmaluxClient;

    const SERVER_ENDPOINT_ENV: &str = "SMALUX_SERVER_ENDPOINT";

    /// 连接外部已启动的正式 Server；不在测试进程中创建临时 Server。
    fn server_endpoint() -> String {
        std::env::var(SERVER_ENDPOINT_ENV)
            .unwrap_or_else(|_| format!("http://{DEFAULT_ADDRESS}:{DEFAULT_PORT}"))
    }

    #[tokio::test]
    #[ignore = "requires a manually started smalux-server"]
    async fn client_reports_unencrypted_frame_error() -> anyhow::Result<()> {
        let mut client = SmaluxClient::new();
        client
            .connect(&server_endpoint(), Some(DEFAULT_AGENT_PRIFIX))
            .await?;
        let health = client.health_check().await?;
        assert_eq!(health.code, 200);

        let (sender, receiver) = mpsc::channel(1);
        let mut inbound = client.open_session(ReceiverStream::new(receiver)).await?;
        // 当前 OpenSession 的第一帧必须是 NoiseHandshake；旧版裸 Ciphertext 已被拒绝。
        sender
            .send(ProtocolFrame {
                body: Some(protocol_frame::Body::Ciphertext(Vec::new())),
            })
            .await?;

        let frame = timeout(Duration::from_secs(1), inbound.message())
            .await??
            .ok_or_else(|| anyhow::anyhow!("server closed before returning ProtocolError"))?;
        let Some(protocol_frame::Body::ProtocolError(error)) = frame.body else {
            anyhow::bail!("server returned a non-ProtocolError frame");
        };
        assert_eq!(
            ProtocolErrorCode::try_from(error.code)?,
            ProtocolErrorCode::InvalidFrame
        );

        drop(inbound);
        drop(sender);
        client.disconnect();
        Ok(())
    }
}
