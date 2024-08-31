
use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampNanosecondBuilder, BooleanBuilder};
use arrow::datatypes::{DataType, Schema};
use serde_json::Value;

use crate::utils::truncate_to_minute::truncate_to_minute;

use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::error::Error;
use tracing::{info, error};  // Add tracing macros

// Convert log entry to Arrow arrays based on the schema
pub fn log_entry_to_arrays(log_entry: &Value, schema: &Schema) -> Result<Vec<ArrayRef>, Box<dyn Error>> {
    info!("Converting log entry to arrays based on the schema.");
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Iterate through the schema fields and match with the log_entry data
    for field in schema.fields() {
        info!("Processing field: {}", field.name());

        let array_ref: ArrayRef = match field.name().as_str() {
            "datetime" | "minute" => {
                let datetime_str = log_entry["datetime"].as_str().unwrap_or_default();
                info!("Parsing datetime: {}", datetime_str);

                let datetime = match datetime_str.parse::<DateTime<Utc>>() {
                    Ok(dt) => dt,
                    Err(e) => {
                        error!("Failed to parse datetime: {}", e);
                        return Err(Box::new(e));
                    }
                };

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
                let value = log_entry[field.name()].as_str().unwrap_or_default();
                info!("Field {} value: {}", field.name(), value);

                let mut builder = StringBuilder::new();
                builder.append_value(value);
                Arc::new(builder.finish()) as ArrayRef
            }
            "qty" => {
                let value = log_entry["qty"].as_f64().unwrap_or(0.0);
                info!("Field qty value: {}", value);

                let mut builder = Float64Builder::new();
                builder.append_value(value);
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => {
                // Handle dynamically inferred metadata fields
                info!("Processing metadata field: {}", field.name());
                match build_optional_metadata_array(&log_entry["metadata"], field) {
                    Ok(array) => array,
                    Err(e) => {
                        error!("Failed to build metadata array for field {}: {}", field.name(), e);
                        return Err(e);
                    }
                }
            }
        };

        arrays.push(array_ref);
    }

    info!("Successfully converted log entry to arrays.");
    Ok(arrays)
}


fn build_optional_metadata_array(metadata: &Value, field: &arrow::datatypes::Field) -> Result<ArrayRef, Box<dyn Error>> {
    info!("Building optional metadata array for field: {}", field.name());

    let array_ref: ArrayRef = match field.data_type() {
        DataType::Utf8 => {
            let value = metadata.get(field.name()).and_then(Value::as_str).unwrap_or_default();
            info!("Field {} value: {}", field.name(), value);

            let mut builder = StringBuilder::new();
            builder.append_value(value);
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Float64 => {
            let value = metadata.get(field.name()).and_then(Value::as_f64).unwrap_or(0.0);
            info!("Field {} value: {}", field.name(), value);

            let mut builder = Float64Builder::new();
            builder.append_value(value);
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Boolean => {
            let value = metadata.get(field.name()).and_then(Value::as_bool).unwrap_or(false);
            info!("Field {} value: {}", field.name(), value);

            let mut builder = BooleanBuilder::new();
            builder.append_value(value);
            Arc::new(builder.finish()) as ArrayRef
        }
        _ => {
            error!("Unsupported data type for field: {}", field.name());
            return Err(format!("Unsupported data type for field: {}", field.name()).into());
        }
    };

    info!("Successfully built metadata array for field: {}", field.name());
    Ok(array_ref)
}
