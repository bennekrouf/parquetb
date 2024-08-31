
use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampNanosecondBuilder, BooleanBuilder};
use arrow::datatypes::{DataType, Schema};
use serde_json::Value;

use crate::utils::truncate_to_minute::truncate_to_minute;

use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::error::Error;

// Convert log entry to Arrow arrays based on the schema
pub fn log_entry_to_arrays(log_entry: &Value, schema: &Schema) -> Result<Vec<ArrayRef>, Box<dyn Error>> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Iterate through the schema fields and match with the log_entry data
    for field in schema.fields() {
        let array_ref: ArrayRef = match field.name().as_str() {
            "datetime" | "minute" => {
                let datetime_str = log_entry["datetime"].as_str().unwrap_or_default();
                let datetime = datetime_str.parse::<DateTime<Utc>>()?;
                let value = if field.name() == "minute" {
                    truncate_to_minute(&datetime).timestamp_nanos_opt().unwrap_or_default()
                } else {
                    datetime.timestamp_nanos_opt().unwrap_or_default()
                };
                let mut builder = TimestampNanosecondBuilder::new();
                builder.append_value(value);
                Arc::new(builder.finish()) as ArrayRef
            }
            "tenant_name" | "item_id" | "status" => {
                let mut builder = StringBuilder::new();
                builder.append_value(log_entry[field.name()].as_str().unwrap_or_default());
                Arc::new(builder.finish()) as ArrayRef
            }
            "qty" => {
                let mut builder = Float64Builder::new();
                builder.append_value(log_entry["qty"].as_f64().unwrap_or(0.0));
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => {
                // Handle dynamically inferred metadata fields
                build_optional_metadata_array(&log_entry["metadata"], field)?
            }
        };
        arrays.push(array_ref);
    }

    Ok(arrays)
}

fn build_optional_metadata_array(metadata: &Value, field: &arrow::datatypes::Field) -> Result<ArrayRef, Box<dyn Error>> {
    let array_ref: ArrayRef = match field.data_type() {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            builder.append_value(metadata.get(field.name()).and_then(Value::as_str).unwrap_or_default());
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            builder.append_value(metadata.get(field.name()).and_then(Value::as_f64).unwrap_or(0.0));
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            builder.append_value(metadata.get(field.name()).and_then(Value::as_bool).unwrap_or(false));
            Arc::new(builder.finish()) as ArrayRef
        }
        _ => return Err(format!("Unsupported data type for field: {}", field.name()).into()),
    };
    Ok(array_ref)
}

