//! 导出层通用消息和 transport trait。

use std::future::Future;
use std::pin::Pin;

/// endpoint URL 的目标传输类型。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportEndpointScheme {
    /// HTTP/HTTPS endpoint。
    Http,
    /// WebSocket endpoint。
    WebSocket,
}

/// 当前可用的导出 transport 协议。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportProtocol {
    /// WebSocket 导出协议。
    WebSocket,
    /// HTTP 导出协议。
    Http,
}

impl ExportProtocol {
    /// 协议名称，用于结构化日志。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::Http => "http",
        }
    }
}

/// 已编码完成、可以交给 transport 发送的导出消息。
///
/// `Binary` 的 body 是业务 payload，不一定是最终 WebSocket frame。Smalux 自有协议会在
/// WebSocket transport 中再封成 `WirePacket`，并按 `wire_mode` 决定是否加密。
pub(crate) enum EncodedExportMessage {
    /// 文本消息。
    Text(String),
    /// 二进制业务数据，由具体 transport 决定是否封包或加密。
    Binary {
        /// 业务序号，用于 wire packet 和排查日志。
        sequence: u64,
        /// 业务 payload。
        body: Vec<u8>,
    },
}

/// 从 transport 收到的导出消息。
pub(crate) enum ExportInboundMessage {
    /// 文本消息。
    Text(String),
    /// 二进制消息。
    Binary(Vec<u8>),
}

/// 把入站消息统一转成 UTF-8 文本。
pub(crate) fn inbound_message_into_string(msg: ExportInboundMessage) -> anyhow::Result<String> {
    match msg {
        ExportInboundMessage::Text(text) => Ok(text),
        ExportInboundMessage::Binary(bytes) => Ok(String::from_utf8(bytes)?),
    }
}

/// 解析并校验 export base URL。
///
/// base URL 是用户唯一需要配置的 server 根地址，不能包含 path/query/fragment。
pub(crate) fn parse_export_base_url(base_url: &str) -> anyhow::Result<reqwest::Url> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        anyhow::bail!("export.base_url cannot be empty");
    }

    let url = reqwest::Url::parse(trimmed)
        .map_err(|err| anyhow::anyhow!("export.base_url must be a valid URL: {err}"))?;

    match url.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!("export.base_url must use http or https scheme, got {other}"),
    }
    if url.host_str().is_none() {
        anyhow::bail!("export.base_url must include a host");
    }
    if !matches!(url.path(), "" | "/") {
        anyhow::bail!("export.base_url must not include a path");
    }
    if url.query().is_some() {
        anyhow::bail!("export.base_url must not include query parameters");
    }
    if url.fragment().is_some() {
        anyhow::bail!("export.base_url must not include a fragment");
    }

    Ok(url)
}

/// 根据 base URL 和协议内固定 path 构造真实 endpoint。
pub(crate) fn build_export_endpoint(
    base_url: &str,
    scheme: ExportEndpointScheme,
    path: &str,
) -> anyhow::Result<String> {
    let mut url = parse_export_base_url(base_url)?;
    let next_scheme = match (url.scheme(), scheme) {
        ("http", ExportEndpointScheme::Http) => "http",
        ("https", ExportEndpointScheme::Http) => "https",
        ("http", ExportEndpointScheme::WebSocket) => "ws",
        ("https", ExportEndpointScheme::WebSocket) => "wss",
        _ => unreachable!("parse_export_base_url only allows http/https"),
    };
    url.set_scheme(next_scheme)
        .map_err(|_| anyhow::anyhow!("failed to set export endpoint scheme"))?;
    url.set_path(normalize_endpoint_path(path)?);
    Ok(url.to_string())
}

/// 校验并规范化协议内部固定 path。
fn normalize_endpoint_path(path: &str) -> anyhow::Result<&str> {
    if !path.starts_with('/') {
        anyhow::bail!("export endpoint path must start with /");
    }
    if path.trim() != path || path.trim().is_empty() {
        anyhow::bail!("export endpoint path cannot be empty or padded");
    }
    Ok(path)
}

/// 可动态分发的消息监听器。
///
/// trait 方法返回 boxed future，避免 `async fn` 直接出现在 trait object 中导致无法 dyn 兼容。
pub(crate) trait ExportMessageListener: Send + Sync + 'static {
    /// 收到 transport 消息后的回调。
    fn on_message(
        &self,
        msg: ExportInboundMessage,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}

/// 导出端统一发送接口。
///
/// 后续无论是 WebSocket、HTTP 还是本地文件，都可以按这个接口接入 pipeline。
pub(crate) trait ExportTransport {
    /// 建立连接并启动后台收发任务。
    async fn connect(&mut self) -> anyhow::Result<()>;

    /// 发送已经序列化好的文本消息。
    async fn send_text_message(&mut self, msg: &str) -> anyhow::Result<()>;

    /// 发送已经编码好的消息。
    async fn send_encoded_export_message(
        &mut self,
        msg: EncodedExportMessage,
    ) -> anyhow::Result<()>;

    /// 设置服务端消息监听器。
    async fn set_listener(
        &mut self,
        listener: Box<dyn ExportMessageListener>,
    ) -> anyhow::Result<()>;

    /// 主动关闭连接。
    async fn close(&mut self) -> anyhow::Result<()>;
}
