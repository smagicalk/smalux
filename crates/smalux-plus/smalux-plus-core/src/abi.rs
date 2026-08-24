//! 动态插件 ABI 的占位定义。
//!
//! 当前只定义 ABI 版本和函数指针形状，尚未加载真实动态库。正式实现时必须保持：
//! 1. 所有跨库结构使用 `#[repr(C)]`；
//! 2. 输入输出使用指针加长度表示的字节，不跨边界传递 Rust 容器；
//! 3. 内存由创建它的一侧释放；
//! 4. 插件不得让 Rust panic 穿过 C ABI 边界。

/// 第一版 Plus ABI 的版本号。
pub const PLUS_ABI_VERSION_V1: u32 = 1;

/// 插件入口符号名。
pub const PLUS_ENTRY_SYMBOL_V1: &[u8] = b"smalux_plus_entry_v1\0";

/// 跨动态库传递的只读字节片段。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ByteSlice {
    pub ptr: *const u8,
    pub len: usize,
}

/// 插件 ABI 返回的函数表占位结构。
///
/// 只有在后续实现加载器时，才会为这些函数填入真实地址；现在不提供执行能力。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SmaluxPlusApiV1 {
    pub abi_version: u32,
    pub initialize: Option<unsafe extern "C" fn() -> i32>,
    pub execute: Option<unsafe extern "C" fn(ByteSlice) -> i32>,
    pub shutdown: Option<unsafe extern "C" fn()>,
}

// 原始指针仅作为 ABI 描述，不代表该类型可在线程间安全传递。
const _: () = {
    assert!(std::mem::size_of::<ByteSlice>() > 0);
};
