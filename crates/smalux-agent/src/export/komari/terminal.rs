//! Komari terminal 消息解析。
//!
//! Komari 的远程终端会先通过 report WebSocket 下发 `message=terminal` 和
//! `request_id`，然后 agent 再打开独立 terminal WebSocket 并复用 remote shell manager。

use serde::Deserialize;

/// Komari terminal 请求。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct TerminalRequest {
    /// Komari terminal 会话 ID。
    pub(super) request_id: String,
}

/// Komari server 文本消息。
#[derive(Debug, Deserialize)]
struct KomariServerMessage {
    /// 消息类型。
    message: String,
    /// terminal 请求 ID。
    request_id: Option<String>,
}

/// 解析 Komari terminal 请求。
pub(super) fn parse_terminal_request(input: &str) -> anyhow::Result<TerminalRequest> {
    let message: KomariServerMessage = serde_json::from_str(input)?;
    if message.message != "terminal" {
        anyhow::bail!("not a komari terminal message");
    }

    let request_id = message
        .request_id
        .filter(|request_id| !request_id.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("komari terminal request_id is required"))?;

    Ok(TerminalRequest { request_id })
}

#[cfg(test)]
mod tests {
    //! Komari terminal 消息解析测试。

    use super::*;

    /// 验证 terminal 消息可以解析 request_id。
    #[test]
    fn parse_terminal_request_reads_request_id() {
        let request =
            parse_terminal_request(r#"{ "message": "terminal", "request_id": "term-1" }"#).unwrap();

        assert_eq!(request.request_id, "term-1");
    }

    /// 验证非 terminal 消息会被忽略。
    #[test]
    fn parse_terminal_request_rejects_non_terminal_message() {
        let error = parse_terminal_request(r#"{ "message": "ping" }"#).unwrap_err();

        assert!(error.to_string().contains("not a komari terminal message"));
    }
}
