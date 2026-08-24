//! Agent 连接流程采用的默认参数。
//!
//! 这里只保存跨配置构造路径共享的默认值。环境变量解析和参数校验仍由具体配置
//! 类型负责，避免本模块逐渐演变成包含业务逻辑的全局配置入口。

use std::time::Duration;

/// 首次注册或重连握手允许等待的默认时间。
pub(crate) const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// 网络连接失败后第一次重试前的默认等待时间。
pub(crate) const DEFAULT_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
/// 连续连接失败时指数退避允许达到的默认上限。
pub(crate) const DEFAULT_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
