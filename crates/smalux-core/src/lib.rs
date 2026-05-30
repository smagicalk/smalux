//! smalux 共享核心库。
//!
//! 这里放 agent 和 server 都会使用的模型、单位换算、日志初始化和转换工具。

/// 内部模型和外部协议之间的转换逻辑。
pub mod convert;
/// 流量/容量单位换算工具。
pub mod flow;
/// 统一 tracing 初始化。
pub mod log;
/// 内部领域模型。
pub mod model;
/// 通用工具模块。
pub mod utils;

/// 模板函数，后续核心库接入真实能力后可以删除。
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    //! core crate 模板测试。

    use super::*;

    /// 验证模板加法函数。
    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
