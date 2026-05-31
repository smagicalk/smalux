//! 远程能力模块。
//!
//! 这里集中放置由 server 触发的 agent 远程能力：交互式 shell、非交互任务和网络探测。
//! CLI-only 的能力开关仍由 `ServiceOptions` 持有，运行期限制来自动态配置。

pub(crate) mod probe;
pub(crate) mod shell;
pub(crate) mod task;
