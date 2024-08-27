
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
        match field.name().as_str() {
            "datetime" => {
                let datetime_str = log_entry["datetime"].as_str().unwrap();
                let datetime = datetime_str.parse::<DateTime<Utc>>()?;
                let mut builder = TimestampNanosecondBuilder::new();
                builder.append_value(datetime.timestamp_nanos_opt().unwrap());
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            "minute" => {
                let datetime_str = log_entry["datetime"].as_str().unwrap();
                let datetime = datetime_str.parse::<DateTime<Utc>>()?;
                let truncated_minute = truncate_to_minute(&datetime);
                let mut builder = TimestampNanosecondBuilder::new();
                builder.append_value(truncated_minute.timestamp_nanos_opt().unwrap());
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            "tenant_name" => {
                let mut builder = StringBuilder::new();
                builder.append_value(log_entry["tenant_name"].as_str().unwrap());
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            "item_id" => {
                let mut builder = StringBuilder::new();
                builder.append_value(log_entry["item_id"].as_str().unwrap());
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            "status" => {
                let mut builder = StringBuilder::new();
                builder.append_value(log_entry["status"].as_str().unwrap());
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            "qty" => {
                let mut builder = Float64Builder::new();
                builder.append_value(log_entry["qty"].as_f64().unwrap());
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
            _ => {
                // Handle dynamically inferred metadata fields
                if let Some(metadata_value) = log_entry["metadata"].get(field.name()) {
                    let array_ref: ArrayRef = match field.data_type() {
                        DataType::Utf8 => {
                            let mut builder = StringBuilder::new();
                            builder.append_value(metadata_value.as_str().unwrap());
                            Arc::new(builder.finish()) as ArrayRef
                        }
                        DataType::Float64 => {
                            let mut builder = Float64Builder::new();
                            builder.append_value(metadata_value.as_f64().unwrap());
                            Arc::new(builder.finish()) as ArrayRef
                        }
                        DataType::Boolean => {
                            let mut builder = BooleanBuilder::new();
                            builder.append_value(metadata_value.as_bool().unwrap());
                            Arc::new(builder.finish()) as ArrayRef
                        }
                        _ => {
                            return Err(format!("Unsupported data type for field: {}", field.name()).into());
                        }
                    };
                    arrays.push(array_ref);
                } else {
                    return Err(format!("Missing metadata field in log entry: {}", field.name()).into());
                }
            }
        }
    }

    Ok(arrays)
}

