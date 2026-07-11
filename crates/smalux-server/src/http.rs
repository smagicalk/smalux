//! HTTP 服务模块入口。
//!
//! 这里分成两类入口：
//! - `agent`：agent 主连接和后续 agent transport 相关 HTTP/WS 入口
//! - `web`：前端 Web 面相关入口，内部再分 API、realtime 和静态前端
//!
//! 这样后续即使同时扩 agent 协议和管理后台，也不会把两类流量混在一个模块里。

pub mod agent;
pub mod middleware;
pub mod router;
pub mod web;
