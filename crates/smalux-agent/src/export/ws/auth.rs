//! WebSocket 握手认证模型。

use std::fmt::Debug;

/// WebSocket 握手认证方式。
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum WebSocketAuth {
    /// 不携带认证信息，适合本地测试或明确无认证的第三方服务。
    None,
    /// 通过 query 参数携带 token，主要用于兼容不能设置 header 的服务。
    QueryToken {
        /// token 参数名，例如 token 或 access_token。
        param: String,
        /// token 明文；仅用于发起连接，禁止写入日志。
        token: String,
    },
    /// 通过 Authorization: Bearer 头携带 token，是 smalux 默认推荐方式。
    BearerToken {
        /// token 明文；仅用于发起连接，禁止写入日志。
        token: String,
    },
}

impl WebSocketAuth {
    /// 返回认证方式名称，用于结构化日志。
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::QueryToken { .. } => "query_token",
            Self::BearerToken { .. } => "bearer_token",
        }
    }

    /// 判断当前认证方式是否配置了 token。
    pub(crate) fn token_set(&self) -> bool {
        match self {
            Self::None => false,
            Self::QueryToken { token, .. } | Self::BearerToken { token } => !token.is_empty(),
        }
    }
}

impl Debug for WebSocketAuth {
    /// Debug 只输出认证类型和 token 是否存在，不输出 token 原文。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("WebSocketAuth");
        debug.field("kind", &self.kind());
        debug.field("token_set", &self.token_set());
        if let Self::QueryToken { param, .. } = self {
            debug.field("param", param);
        }
        debug.finish()
    }
}
