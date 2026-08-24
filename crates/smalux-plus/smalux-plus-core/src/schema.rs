//! Plus 任务参数的动态 Schema 契约。
//!
//! `PluginSchemaBundle` 是插件包中的 `schema.pb` 内容，也是 Agent 与 Server 之间交换的
//! 唯一参数描述。Protobuf 描述符负责真实消息类型；字段提示只描述管理端如何生成表单，
//! 不允许包含脚本、HTML 或任意可执行内容。

use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet, field_descriptor_proto,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Schema Bundle 的当前格式版本。
pub const SCHEMA_FORMAT_VERSION: u32 = 1;
/// 单个 Bundle 的最大字节数，避免恶意插件占满会话和数据库。
pub const MAX_SCHEMA_BUNDLE_BYTES: usize = 1024 * 1024;
const MAX_TASKS: usize = 256;
const MAX_FIELDS_PER_TASK: usize = 512;
const MAX_DESCRIPTOR_MESSAGES: usize = 4096;

/// 公开给管理页面的字段控件类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum FieldControl {
    Auto = 0,
    Text = 1,
    Textarea = 2,
    Number = 3,
    Select = 4,
    Combobox = 5,
    Toggle = 6,
    Password = 7,
}

/// 一个插件版本的完整参数 Schema。
#[derive(Clone, PartialEq, Message)]
pub struct PluginSchemaBundle {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(string, tag = "2")]
    pub plugin_id: String,
    #[prost(string, tag = "3")]
    pub plugin_version: String,
    #[prost(bytes = "bytes", tag = "4")]
    pub descriptor_set: Vec<u8>,
    #[prost(message, repeated, tag = "5")]
    pub tasks: Vec<PluginTaskSchema>,
}

/// 一个可执行 Task 的配置和结果消息入口。
#[derive(Clone, PartialEq, Message)]
pub struct PluginTaskSchema {
    #[prost(string, tag = "1")]
    pub task_kind: String,
    #[prost(uint32, tag = "2")]
    pub schema_version: u32,
    #[prost(string, tag = "3")]
    pub config_message: String,
    #[prost(string, tag = "4")]
    pub result_message: String,
    #[prost(string, tag = "5")]
    pub display_name: String,
    #[prost(string, tag = "6")]
    pub description: String,
    #[prost(message, repeated, tag = "7")]
    pub fields: Vec<PluginFieldSchema>,
}

/// 一个配置字段的声明式表单提示和基础验证规则。
#[derive(Clone, PartialEq, Message)]
pub struct PluginFieldSchema {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub label: String,
    #[prost(string, tag = "3")]
    pub description: String,
    #[prost(enumeration = "FieldControl", tag = "4")]
    pub control: i32,
    #[prost(bool, tag = "5")]
    pub required: bool,
    #[prost(string, optional, tag = "6")]
    pub default_value: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub minimum: Option<String>,
    #[prost(string, optional, tag = "8")]
    pub maximum: Option<String>,
    #[prost(uint32, optional, tag = "9")]
    pub min_length: Option<u32>,
    #[prost(uint32, optional, tag = "10")]
    pub max_length: Option<u32>,
    #[prost(string, optional, tag = "11")]
    pub format: Option<String>,
    #[prost(message, repeated, tag = "12")]
    pub options: Vec<PluginFieldOption>,
    #[prost(uint32, tag = "13")]
    pub order: u32,
    #[prost(string, tag = "14")]
    pub unit: String,
}

/// SELECT/COMBOBOX 的静态选项。
#[derive(Clone, PartialEq, Message)]
pub struct PluginFieldOption {
    #[prost(string, tag = "1")]
    pub value: String,
    #[prost(string, tag = "2")]
    pub label: String,
}

/// 内容寻址的 Schema 标识。
pub type SchemaHash = [u8; 32];

/// Schema 校验错误。
#[derive(Debug, thiserror::Error)]
pub enum PluginSchemaError {
    #[error("schema bundle exceeds {MAX_SCHEMA_BUNDLE_BYTES} bytes")]
    TooLarge,
    #[error("failed to decode schema bundle: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("schema bundle is invalid: {0}")]
    Invalid(String),
}

impl PluginSchemaBundle {
    /// 编码规范化 Bundle，并拒绝明显不安全或不一致的描述。
    pub fn encode_checked(&self) -> Result<Vec<u8>, PluginSchemaError> {
        let normalized = self.normalized()?;
        let mut bytes = Vec::with_capacity(normalized.encoded_len());
        normalized
            .encode(&mut bytes)
            .map_err(|error| PluginSchemaError::Invalid(error.to_string()))?;
        if bytes.len() > MAX_SCHEMA_BUNDLE_BYTES {
            return Err(PluginSchemaError::TooLarge);
        }
        Ok(bytes)
    }

    /// 解码并校验插件包中的 `schema.pb`。
    pub fn decode_checked(bytes: &[u8]) -> Result<Self, PluginSchemaError> {
        if bytes.len() > MAX_SCHEMA_BUNDLE_BYTES {
            return Err(PluginSchemaError::TooLarge);
        }
        let bundle = Self::decode(bytes)?;
        bundle.validate()?;
        Ok(bundle)
    }

    /// 计算规范化编码的 SHA-256。协议只发送该摘要，不发送十六进制文本。
    pub fn hash(&self) -> Result<SchemaHash, PluginSchemaError> {
        let bytes = self.encode_checked()?;
        Ok(Sha256::digest(bytes).into())
    }

    /// 验证身份、任务排序、字段选项以及 Protobuf 描述符集合。
    pub fn validate(&self) -> Result<(), PluginSchemaError> {
        if self.format_version != SCHEMA_FORMAT_VERSION {
            return Err(PluginSchemaError::Invalid(
                "unsupported schema format version".to_owned(),
            ));
        }
        if self.plugin_id.is_empty()
            || self.plugin_id.len() > 256
            || self.plugin_version.is_empty()
            || self.plugin_version.len() > 64
        {
            return Err(PluginSchemaError::Invalid(
                "plugin identity and version are required".to_owned(),
            ));
        }
        if self.descriptor_set.is_empty() {
            return Err(PluginSchemaError::Invalid(
                "descriptor_set must not be empty".to_owned(),
            ));
        }
        let descriptors =
            FileDescriptorSet::decode(self.descriptor_set.as_slice()).map_err(|error| {
                PluginSchemaError::Invalid(format!("invalid descriptor set: {error}"))
            })?;
        if descriptors.file.is_empty() || descriptors.file.len() > 256 {
            return Err(PluginSchemaError::Invalid(
                "descriptor set must contain 1..=256 files".to_owned(),
            ));
        }
        let messages = descriptor_messages(&descriptors);
        if messages.len() > MAX_DESCRIPTOR_MESSAGES {
            return Err(PluginSchemaError::Invalid(
                "descriptor set contains too many messages".to_owned(),
            ));
        }
        if self.tasks.is_empty()
            || self.tasks.len() > MAX_TASKS
            || self
                .tasks
                .windows(2)
                .any(|pair| pair[0].task_kind >= pair[1].task_kind)
        {
            return Err(PluginSchemaError::Invalid(
                "tasks must be sorted, unique and non-empty".to_owned(),
            ));
        }
        for task in &self.tasks {
            if task.task_kind.is_empty()
                || task.task_kind.len() > 256
                || task.schema_version == 0
                || task.config_message.is_empty()
                || task.config_message.len() > 512
                || task.result_message.len() > 512
                || task.display_name.len() > 256
                || task.description.len() > 4096
            {
                return Err(PluginSchemaError::Invalid(
                    "task identity is incomplete".to_owned(),
                ));
            }
            let Some(config) = messages.get(task.config_message.trim_start_matches('.')) else {
                return Err(PluginSchemaError::Invalid(format!(
                    "task {} references an unknown config message",
                    task.task_kind
                )));
            };
            if !task.result_message.is_empty()
                && !messages.contains_key(task.result_message.trim_start_matches('.'))
            {
                return Err(PluginSchemaError::Invalid(format!(
                    "task {} references an unknown result message",
                    task.task_kind
                )));
            }
            if task.fields.len() > MAX_FIELDS_PER_TASK
                || task
                    .fields
                    .windows(2)
                    .any(|pair| pair[0].path >= pair[1].path)
            {
                return Err(PluginSchemaError::Invalid(
                    "fields must be sorted and unique".to_owned(),
                ));
            }
            for field in &task.fields {
                if field.path.is_empty()
                    || field.path.len() > 256
                    || field.label.is_empty()
                    || field.label.len() > 256
                    || field.description.len() > 4096
                    || field.unit.len() > 64
                {
                    return Err(PluginSchemaError::Invalid(
                        "field path and label are required".to_owned(),
                    ));
                }
                let Some(descriptor) = config
                    .field
                    .iter()
                    .find(|descriptor| descriptor.name.as_deref() == Some(field.path.as_str()))
                else {
                    return Err(PluginSchemaError::Invalid(format!(
                        "field {} is missing from {}",
                        field.path, task.config_message
                    )));
                };
                let minimum = field
                    .minimum
                    .as_deref()
                    .map(|value| parse_number("minimum", &field.path, value))
                    .transpose()?;
                let maximum = field
                    .maximum
                    .as_deref()
                    .map(|value| parse_number("maximum", &field.path, value))
                    .transpose()?;
                if let (Some(minimum), Some(maximum)) = (minimum, maximum)
                    && minimum > maximum
                {
                    return Err(PluginSchemaError::Invalid(format!(
                        "field {} has inverted range",
                        field.path
                    )));
                }
                if let (Some(min), Some(max)) = (field.min_length, field.max_length)
                    && min > max
                {
                    return Err(PluginSchemaError::Invalid(format!(
                        "field {} has inverted length range",
                        field.path
                    )));
                }
                let control = FieldControl::try_from(field.control).map_err(|_| {
                    PluginSchemaError::Invalid(format!(
                        "field {} has an unknown control",
                        field.path
                    ))
                })?;
                validate_control_type(&field.path, control, descriptor)?;
                validate_default_value(field, control, descriptor, minimum, maximum)?;
                if !field.options.is_empty()
                    && !matches!(control, FieldControl::Select | FieldControl::Combobox)
                {
                    return Err(PluginSchemaError::Invalid(format!(
                        "field {} provides options for a non-choice control",
                        field.path
                    )));
                }
                if let Some(format) = &field.format
                    && !matches!(format.as_str(), "hostname" | "ip" | "url" | "port")
                {
                    return Err(PluginSchemaError::Invalid(format!(
                        "field {} has an unsupported format",
                        field.path
                    )));
                }
                if field
                    .options
                    .windows(2)
                    .any(|pair| pair[0].value >= pair[1].value)
                    || field
                        .options
                        .iter()
                        .any(|option| option.value.is_empty() || option.label.is_empty())
                {
                    return Err(PluginSchemaError::Invalid(format!(
                        "field {} has invalid options",
                        field.path
                    )));
                }
            }
        }
        Ok(())
    }

    /// 清除不影响 wire 类型的源码位置信息，并按文件名稳定排序。
    fn normalized(&self) -> Result<Self, PluginSchemaError> {
        self.validate()?;
        let mut normalized = self.clone();
        let mut descriptors = FileDescriptorSet::decode(normalized.descriptor_set.as_slice())
            .map_err(|error| {
                PluginSchemaError::Invalid(format!("invalid descriptor set: {error}"))
            })?;
        for file in &mut descriptors.file {
            file.source_code_info = None;
        }
        descriptors
            .file
            .sort_by(|left, right| left.name.cmp(&right.name));
        normalized.descriptor_set = descriptors.encode_to_vec();
        Ok(normalized)
    }
}

fn validate_control_type(
    path: &str,
    control: FieldControl,
    descriptor: &FieldDescriptorProto,
) -> Result<(), PluginSchemaError> {
    use field_descriptor_proto::Type;

    let field_type = Type::try_from(descriptor.r#type.unwrap_or_default()).map_err(|_| {
        PluginSchemaError::Invalid(format!("field {path} has an unknown Protobuf type"))
    })?;
    let valid = match control {
        FieldControl::Auto => true,
        FieldControl::Number => matches!(
            field_type,
            Type::Double
                | Type::Float
                | Type::Int64
                | Type::Uint64
                | Type::Int32
                | Type::Fixed64
                | Type::Fixed32
                | Type::Uint32
                | Type::Sfixed32
                | Type::Sfixed64
                | Type::Sint32
                | Type::Sint64
        ),
        FieldControl::Toggle => field_type == Type::Bool,
        FieldControl::Select => matches!(field_type, Type::Enum | Type::String),
        FieldControl::Text
        | FieldControl::Textarea
        | FieldControl::Combobox
        | FieldControl::Password => matches!(field_type, Type::String | Type::Bytes),
    };
    if !valid {
        return Err(PluginSchemaError::Invalid(format!(
            "field {path} control is incompatible with its Protobuf type"
        )));
    }
    Ok(())
}

fn validate_default_value(
    field: &PluginFieldSchema,
    control: FieldControl,
    descriptor: &FieldDescriptorProto,
    minimum: Option<f64>,
    maximum: Option<f64>,
) -> Result<(), PluginSchemaError> {
    use field_descriptor_proto::Type;

    let Some(default) = field.default_value.as_deref() else {
        return Ok(());
    };
    let field_type = Type::try_from(descriptor.r#type.unwrap_or_default()).map_err(|_| {
        PluginSchemaError::Invalid(format!("field {} has an unknown Protobuf type", field.path))
    })?;
    let numeric = control == FieldControl::Number
        || control == FieldControl::Auto
            && matches!(
                field_type,
                Type::Double
                    | Type::Float
                    | Type::Int64
                    | Type::Uint64
                    | Type::Int32
                    | Type::Fixed64
                    | Type::Fixed32
                    | Type::Uint32
                    | Type::Sfixed32
                    | Type::Sfixed64
                    | Type::Sint32
                    | Type::Sint64
            );
    if numeric {
        let default = parse_number("default", &field.path, default)?;
        if minimum.is_some_and(|minimum| default < minimum) {
            return Err(PluginSchemaError::Invalid(format!(
                "field {} default is below minimum",
                field.path
            )));
        }
        if maximum.is_some_and(|maximum| default > maximum) {
            return Err(PluginSchemaError::Invalid(format!(
                "field {} default is above maximum",
                field.path
            )));
        }
    }
    if control == FieldControl::Toggle || control == FieldControl::Auto && field_type == Type::Bool
    {
        default.parse::<bool>().map_err(|_| {
            PluginSchemaError::Invalid(format!(
                "field {} has an invalid boolean default",
                field.path
            ))
        })?;
    }
    if matches!(
        control,
        FieldControl::Text
            | FieldControl::Textarea
            | FieldControl::Combobox
            | FieldControl::Password
    ) || control == FieldControl::Auto && matches!(field_type, Type::String | Type::Bytes)
    {
        let length = default.chars().count();
        if field
            .min_length
            .is_some_and(|minimum| length < minimum as usize)
        {
            return Err(PluginSchemaError::Invalid(format!(
                "field {} default is shorter than min_length",
                field.path
            )));
        }
        if field
            .max_length
            .is_some_and(|maximum| length > maximum as usize)
        {
            return Err(PluginSchemaError::Invalid(format!(
                "field {} default is longer than max_length",
                field.path
            )));
        }
    }
    if control == FieldControl::Select
        && !field.options.is_empty()
        && !field.options.iter().any(|option| option.value == default)
    {
        return Err(PluginSchemaError::Invalid(format!(
            "field {} default is not one of its options",
            field.path
        )));
    }
    Ok(())
}

fn descriptor_messages(descriptors: &FileDescriptorSet) -> HashMap<String, &DescriptorProto> {
    let mut messages = HashMap::new();
    for file in &descriptors.file {
        let package = file.package.as_deref().unwrap_or_default();
        for message in &file.message_type {
            collect_message(package, message, &mut messages);
        }
    }
    messages
}

fn collect_message<'a>(
    parent: &str,
    message: &'a DescriptorProto,
    messages: &mut HashMap<String, &'a DescriptorProto>,
) {
    let Some(name) = message.name.as_deref() else {
        return;
    };
    let full_name = if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}.{name}")
    };
    messages.insert(full_name.clone(), message);
    for nested in &message.nested_type {
        collect_message(&full_name, nested, messages);
    }
}

fn parse_number(name: &str, path: &str, value: &str) -> Result<f64, PluginSchemaError> {
    let number = value.parse::<f64>().map_err(|_| {
        PluginSchemaError::Invalid(format!("field {path} has an invalid numeric {name}"))
    })?;
    if !number.is_finite() {
        return Err(PluginSchemaError::Invalid(format!(
            "field {path} has a non-finite numeric {name}"
        )));
    }
    Ok(number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn bundle() -> PluginSchemaBundle {
        let mut descriptor_set = FileDescriptorSet { file: Vec::new() };
        descriptor_set.file.push(prost_types::FileDescriptorProto {
            name: Some("echo.proto".to_owned()),
            package: Some("smalux.plus.echo.v1".to_owned()),
            message_type: vec![prost_types::DescriptorProto {
                name: Some("EchoTaskConfig".to_owned()),
                field: vec![prost_types::FieldDescriptorProto {
                    name: Some("delay_millis".to_owned()),
                    r#type: Some(field_descriptor_proto::Type::Uint64 as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        PluginSchemaBundle {
            format_version: SCHEMA_FORMAT_VERSION,
            plugin_id: "smalux.plus.echo".to_owned(),
            plugin_version: "0.1.0".to_owned(),
            descriptor_set: descriptor_set.encode_to_vec(),
            tasks: vec![PluginTaskSchema {
                task_kind: "smalux.plus.echo.v1".to_owned(),
                schema_version: 1,
                config_message: "smalux.plus.echo.v1.EchoTaskConfig".to_owned(),
                ..Default::default()
            }],
        }
    }

    fn bundle_with_field(
        field_type: field_descriptor_proto::Type,
        control: FieldControl,
    ) -> PluginSchemaBundle {
        let mut schema = bundle();
        let mut descriptors = FileDescriptorSet::decode(schema.descriptor_set.as_slice()).unwrap();
        descriptors.file[0].message_type[0].field[0].r#type = Some(field_type as i32);
        schema.descriptor_set = descriptors.encode_to_vec();
        schema.tasks[0].fields.push(PluginFieldSchema {
            path: "delay_millis".to_owned(),
            label: "Value".to_owned(),
            control: control as i32,
            ..Default::default()
        });
        schema
    }

    #[test]
    fn checked_bundle_round_trips_and_hashes() {
        let original = bundle();
        let bytes = original.encode_checked().unwrap();
        let decoded = PluginSchemaBundle::decode_checked(&bytes).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(original.hash().unwrap(), decoded.hash().unwrap());
    }

    #[test]
    fn rejects_unsorted_tasks_and_options() {
        let mut invalid = bundle();
        invalid.tasks[0].fields.push(PluginFieldSchema {
            path: "z".to_owned(),
            label: "Z".to_owned(),
            ..Default::default()
        });
        invalid.tasks[0].fields.push(PluginFieldSchema {
            path: "a".to_owned(),
            label: "A".to_owned(),
            ..Default::default()
        });
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn numeric_ranges_use_numeric_instead_of_lexical_order() {
        let mut valid = bundle();
        valid.tasks[0].fields.push(PluginFieldSchema {
            path: "delay_millis".to_owned(),
            label: "Delay".to_owned(),
            control: FieldControl::Number as i32,
            minimum: Some("2".to_owned()),
            maximum: Some("10".to_owned()),
            ..Default::default()
        });
        assert!(valid.validate().is_ok());

        valid.tasks[0].fields[0].minimum = Some("not-a-number".to_owned());
        assert!(valid.validate().is_err());
    }

    #[test]
    fn default_value_must_satisfy_the_declared_field_rules() {
        let mut schema =
            bundle_with_field(field_descriptor_proto::Type::Uint64, FieldControl::Number);
        schema.tasks[0].fields[0].minimum = Some("10".to_owned());
        schema.tasks[0].fields[0].maximum = Some("20".to_owned());
        schema.tasks[0].fields[0].default_value = Some("9".to_owned());

        assert!(matches!(
            schema.validate(),
            Err(PluginSchemaError::Invalid(message))
                if message.contains("default is below minimum")
        ));

        schema.tasks[0].fields[0].default_value = Some("15".to_owned());
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn text_boolean_and_select_defaults_are_validated() {
        let mut text = bundle_with_field(field_descriptor_proto::Type::String, FieldControl::Text);
        text.tasks[0].fields[0].min_length = Some(2);
        text.tasks[0].fields[0].default_value = Some("a".to_owned());
        assert!(text.validate().is_err());

        let mut toggle =
            bundle_with_field(field_descriptor_proto::Type::Bool, FieldControl::Toggle);
        toggle.tasks[0].fields[0].default_value = Some("yes".to_owned());
        assert!(toggle.validate().is_err());

        let mut select =
            bundle_with_field(field_descriptor_proto::Type::String, FieldControl::Select);
        select.tasks[0].fields[0].options = vec![PluginFieldOption {
            value: "known".to_owned(),
            label: "Known".to_owned(),
        }];
        select.tasks[0].fields[0].default_value = Some("unknown".to_owned());
        assert!(select.validate().is_err());
    }

    #[test]
    fn hash_ignores_descriptor_source_locations() {
        let original = bundle();
        let mut with_locations = original.clone();
        let mut descriptors =
            FileDescriptorSet::decode(with_locations.descriptor_set.as_slice()).unwrap();
        descriptors.file[0].source_code_info = Some(prost_types::SourceCodeInfo {
            location: Vec::new(),
        });
        with_locations.descriptor_set = descriptors.encode_to_vec();
        assert_eq!(original.hash().unwrap(), with_locations.hash().unwrap());
    }
}
