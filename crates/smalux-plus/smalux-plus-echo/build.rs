use std::{env, fs, path::PathBuf};

use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct SchemaBundle {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    plugin_id: String,
    #[prost(string, tag = "3")]
    plugin_version: String,
    #[prost(bytes = "bytes", tag = "4")]
    descriptor_set: Vec<u8>,
    #[prost(message, repeated, tag = "5")]
    tasks: Vec<TaskSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct TaskSchema {
    #[prost(string, tag = "1")]
    task_kind: String,
    #[prost(uint32, tag = "2")]
    schema_version: u32,
    #[prost(string, tag = "3")]
    config_message: String,
    #[prost(string, tag = "4")]
    result_message: String,
    #[prost(string, tag = "5")]
    display_name: String,
    #[prost(string, tag = "6")]
    description: String,
    #[prost(message, repeated, tag = "7")]
    fields: Vec<FieldSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct FieldSchema {
    #[prost(string, tag = "1")]
    path: String,
    #[prost(string, tag = "2")]
    label: String,
    #[prost(string, tag = "3")]
    description: String,
    #[prost(int32, tag = "4")]
    control: i32,
    #[prost(bool, tag = "5")]
    required: bool,
    #[prost(string, optional, tag = "6")]
    default_value: Option<String>,
    #[prost(string, optional, tag = "7")]
    minimum: Option<String>,
    #[prost(string, optional, tag = "8")]
    maximum: Option<String>,
    #[prost(uint32, optional, tag = "9")]
    min_length: Option<u32>,
    #[prost(uint32, optional, tag = "10")]
    max_length: Option<u32>,
    #[prost(string, optional, tag = "11")]
    format: Option<String>,
    #[prost(message, repeated, tag = "12")]
    options: Vec<FieldOption>,
    #[prost(uint32, tag = "13")]
    order: u32,
    #[prost(string, tag = "14")]
    unit: String,
}

#[derive(Clone, PartialEq, Message)]
struct FieldOption {
    #[prost(string, tag = "1")]
    value: String,
    #[prost(string, tag = "2")]
    label: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is missing")?);
    let descriptor_path = out_dir.join("echo_descriptor.bin");
    let schema_path = out_dir.join("schema.pb");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.file_descriptor_set_path(&descriptor_path);
    config.compile_protos(&["proto/echo.proto"], &["proto"])?;

    let bundle = SchemaBundle {
        format_version: 1,
        plugin_id: "smalux.plus.echo".to_owned(),
        plugin_version: env::var("CARGO_PKG_VERSION")?,
        descriptor_set: fs::read(descriptor_path)?,
        tasks: vec![TaskSchema {
            task_kind: "smalux.plus.echo.v1".to_owned(),
            schema_version: 1,
            config_message: "smalux.plus.echo.v1.EchoTaskConfig".to_owned(),
            result_message: "smalux.plus.echo.v1.EchoTaskResult".to_owned(),
            display_name: "Echo".to_owned(),
            description: "回显文本，用于验证 Plus 参数和结果链路。".to_owned(),
            fields: fields(),
        }],
    };
    fs::write(&schema_path, bundle.encode_to_vec())?;
    println!(
        "cargo:rustc-env=SMALUX_PLUS_ECHO_SCHEMA={}",
        schema_path.display()
    );
    println!("cargo:rerun-if-changed=proto/echo.proto");
    Ok(())
}

fn fields() -> Vec<FieldSchema> {
    let mut delay = field("delay_millis", "延迟", 3, 2, "ms");
    delay.default_value = Some("0".to_owned());
    delay.minimum = Some("0".to_owned());
    delay.maximum = Some("60000".to_owned());
    let mut fail = field("fail", "模拟失败", 6, 3, "");
    fail.default_value = Some("false".to_owned());
    vec![
        delay,
        fail,
        FieldSchema {
            path: "message".to_owned(),
            label: "消息".to_owned(),
            description: "Worker 成功时原样返回的文本。".to_owned(),
            control: 1,
            required: true,
            default_value: None,
            minimum: None,
            maximum: None,
            min_length: Some(1),
            max_length: Some(1024),
            format: None,
            options: Vec::new(),
            order: 1,
            unit: String::new(),
        },
        FieldSchema {
            path: "mode".to_owned(),
            label: "模式".to_owned(),
            description: "固定枚举下拉示例。".to_owned(),
            control: 4,
            required: false,
            default_value: Some("ECHO_MODE_NORMAL".to_owned()),
            minimum: None,
            maximum: None,
            min_length: None,
            max_length: None,
            format: None,
            options: vec![
                FieldOption {
                    value: "ECHO_MODE_DIAGNOSTIC".to_owned(),
                    label: "诊断".to_owned(),
                },
                FieldOption {
                    value: "ECHO_MODE_NORMAL".to_owned(),
                    label: "正常".to_owned(),
                },
            ],
            order: 4,
            unit: String::new(),
        },
        FieldSchema {
            path: "target".to_owned(),
            label: "目标".to_owned(),
            description: "可选择预设值，也可自行填写。".to_owned(),
            control: 5,
            required: false,
            default_value: Some("default".to_owned()),
            minimum: None,
            maximum: None,
            min_length: Some(1),
            max_length: Some(128),
            format: None,
            options: vec![FieldOption {
                value: "default".to_owned(),
                label: "默认".to_owned(),
            }],
            order: 5,
            unit: String::new(),
        },
    ]
}

fn field(path: &str, label: &str, control: i32, order: u32, unit: &str) -> FieldSchema {
    FieldSchema {
        path: path.to_owned(),
        label: label.to_owned(),
        description: String::new(),
        control,
        required: false,
        default_value: None,
        minimum: None,
        maximum: None,
        min_length: None,
        max_length: None,
        format: None,
        options: Vec::new(),
        order,
        unit: unit.to_owned(),
    }
}
