
// use chrono::{DateTime, Utc};
use serde_json::Value;
use std::error::Error;
use std::sync::Arc;

mod utils;

// use crate::utils::truncate_to_minute::truncate_to_minute;
use crate::utils::build_schema::build_schema;
use crate::utils::log_entry_to_arrays::log_entry_to_arrays;
use crate::utils::write_parquet_file::write_parquet_file;

fn main() -> Result<(), Box<dyn Error>> {
    // Example log entry
    let log = r#"{
        "datetime": "2024-08-26T10:15:42Z",
        "tenant_name": "TenantA",
        "item_id": "Item123",
        "status": "SUCCESS",
        "qty": 100.5,
        "metadata": {
        }
    }"#;

    // Parse the log entry into a serde_json::Value
    let log_entry: Value = serde_json::from_str(log)?;

    // Build the schema
    let schema = build_schema(&log_entry);

    // Convert log entry to Arrow arrays
    let arrays = log_entry_to_arrays(&log_entry, &schema)?;

    // Write the Arrow arrays to a Parquet file
    write_parquet_file("logs.parquet", Arc::new(schema), arrays)?;

    println!("Parquet file created successfully.");

    Ok(())
}
