//! HTTP 服务模块入口，负责组织 REST API、agent 接入、前端实时通道、静态前端资源和中间件。

pub mod agent;
pub mod frontend;
pub mod middleware;
pub mod realtime;
pub mod rest;
pub mod router;
