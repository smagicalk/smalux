
use serde::{Deserialize, Serialize};


#[derive(Debug,Default,Serialize,Deserialize,Clone)]
pub struct Cpu{
    pub name: String,
    //品牌
    pub brand:String,
    //供应商id
    pub vendor_id:String,
    //使用百分比
    pub usage:f32,
    //频率
    pub frequency:u64
}

#[derive(Debug,Default,Serialize,Deserialize,Clone)]
pub struct CpuInfo{
    pub cpu_num : usize,
    pub cpu_usage:f32,
    pub cpus: Vec<Cpu>
}
