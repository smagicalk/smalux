use serde::{Deserialize, Serialize};

#[derive(Debug,Default,Clone, PartialEq,Serialize,Deserialize)]
pub struct MemoryInfo{
    pub memory_total:u64,
    pub memory_usage:u64,
    pub memory_available:u64,
    pub memory_free:u64,
    pub swap_total:u64,
    pub swap_usage:u64,
    pub swap_free:u64,
}

