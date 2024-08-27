
use arrow::datatypes::{DataType, Field};
use serde_json::Value;

pub fn infer_metadata_schema(metadata: &Value) -> Vec<Field> {
    let mut fields = vec![];

    if let Some(obj) = metadata.as_object() {
        for (key, value) in obj {
            let field_type = match value {
                Value::String(_) => DataType::Utf8,
                Value::Number(_) => DataType::Float64, // Handle all numbers as floats for simplicity
                Value::Bool(_) => DataType::Boolean,
                _ => DataType::Utf8, // Fallback for other types like arrays or objects
            };
            fields.push(Field::new(key, field_type, true));
        }
    }

    fields
}
