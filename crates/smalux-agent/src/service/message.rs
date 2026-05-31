//! Service 内部消息层。
//!
//! 协议 listener、入站命令和出站事件都集中在这里。外部 transport 只需要把
//! server 消息转换成入站命令，业务执行结果统一进入出站事件队列。

pub(crate) mod inbound;
pub(crate) mod listener;
pub(crate) mod outbound;
