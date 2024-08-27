use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::Schema;
use serde_json::Value;

use crate::utils::truncate_to_minute::truncate_to_minute;

use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::error::Error;

// Convert log entry to Arrow arrays based on the schema
pub fn log_entry_to_arrays(log_entry: &Value, schema: &Schema) -> Result<Vec<ArrayRef>, Box<dyn Error>> {
    // Create Arrow builders for each field
    let mut datetime_builder = TimestampNanosecondBuilder::new();
    let mut minute_builder = TimestampNanosecondBuilder::new();
    let mut tenant_name_builder = StringBuilder::new();
    let mut item_id_builder = StringBuilder::new();
    let mut status_builder = StringBuilder::new();
    let mut qty_builder = Float64Builder::new();

    // Parse the datetime and truncate to minute precision
    let datetime_str = log_entry["datetime"].as_str().unwrap();
    let datetime = datetime_str.parse::<DateTime<Utc>>()?;
    let truncated_minute = truncate_to_minute(&datetime);

    // Append values to builders
    datetime_builder.append_value(datetime.timestamp_nanos());
    minute_builder.append_value(truncated_minute.timestamp_nanos());
    tenant_name_builder.append_value(log_entry["tenant_name"].as_str().unwrap());
    item_id_builder.append_value(log_entry["item_id"].as_str().unwrap());
    status_builder.append_value(log_entry["status"].as_str().unwrap());
    qty_builder.append_value(log_entry["qty"].as_f64().unwrap());

    Ok(vec![
        Arc::new(datetime_builder.finish()) as ArrayRef,
        Arc::new(minute_builder.finish()) as ArrayRef,
        Arc::new(tenant_name_builder.finish()) as ArrayRef,
        Arc::new(item_id_builder.finish()) as ArrayRef,
        Arc::new(status_builder.finish()) as ArrayRef,
        Arc::new(qty_builder.finish()) as ArrayRef,
    ])
}
