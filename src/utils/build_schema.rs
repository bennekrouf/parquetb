
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;

use crate::utils::infer_metadata_schema::infer_metadata_schema;

pub fn build_schema(log_entry: &Value) -> Schema {
    let mut fields = vec![
        Field::new("item_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("qty", DataType::Float64, false),
    ];

    // Infer metadata fields dynamically
    if let Some(metadata) = log_entry.get("metadata") {
        let inferred_fields = infer_metadata_schema(metadata);
        fields.extend(inferred_fields);
    }

    Schema::new(fields)
}
