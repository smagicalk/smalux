//! 通用工具模块。
//!
//! 当前为空，后续仅放多个 crate 共享且不属于具体领域的工具函数。

/// 通用校验工具。
pub mod validate {
    use std::time::Duration;

    /// 校验字符串去除首尾空白后非空。
    pub fn ensure_non_empty(name: &str, value: &str) -> anyhow::Result<()> {
        if value.trim().is_empty() {
            anyhow::bail!("{name} cannot be empty");
        }
        Ok(())
    }

    /// 校验时间间隔不低于指定最小值。
    pub fn ensure_interval_at_least(
        name: &str,
        value: Duration,
        min: Duration,
    ) -> anyhow::Result<()> {
        if value < min {
            anyhow::bail!("{name} must be at least {:?}", min);
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        //! 通用校验工具测试。

        use super::*;

        /// 验证空白字符串会被拒绝。
        #[test]
        fn ensure_non_empty_rejects_blank_string() {
            let error = ensure_non_empty("field", "  ").unwrap_err();

            assert!(error.to_string().contains("field cannot be empty"));
        }

        /// 验证非空字符串可以通过校验。
        #[test]
        fn ensure_non_empty_accepts_non_blank_string() {
            ensure_non_empty("field", "value").unwrap();
        }

        /// 验证低于最小值的时间间隔会被拒绝。
        #[test]
        fn ensure_interval_at_least_rejects_too_small_value() {
            let error = ensure_interval_at_least(
                "interval",
                Duration::from_millis(1),
                Duration::from_millis(100),
            )
            .unwrap_err();

            assert!(error.to_string().contains("interval must be at least"));
        }

        /// 验证满足最小值的时间间隔可以通过校验。
        #[test]
        fn ensure_interval_at_least_accepts_minimum_value() {
            ensure_interval_at_least(
                "interval",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .unwrap();
        }
    }
}
