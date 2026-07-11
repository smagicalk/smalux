//! agent 输入处理入口，负责聚合 frame 解码和 report 分发。
//!
//! 这里使用 `smalux-protocol` 的协议定义，但自身不是协议 crate；
//! 它属于 server 侧的输入处理与业务分发层。

pub mod frame;
pub mod report;
