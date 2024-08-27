
// use chrono::{DateTime, Utc};
use serde_json::Value;
use std::error::Error;
use std::sync::Arc;

mod utils;

use chrono::{DateTime, Utc}; // Ensure chrono is imported for datetime parsing
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
        "toto": 2
        }
    }"#;

    // Parse the log entry into a serde_json::Value
    let log_entry: Value = serde_json::from_str(log)?;

    // Extract tenant name
    let tenant_name = log_entry["tenant_name"].as_str().unwrap();

    // Extract and format datetime
    let datetime_str = log_entry["datetime"].as_str().unwrap();
    let datetime = datetime_str.parse::<DateTime<Utc>>()?;
    let formatted_datetime = datetime.format("%Y%m%d_%H%M").to_string();

    // Generate the file name
    let file_name = format!("{}_{}.parquet", tenant_name, formatted_datetime);

    // Build the schema
    let schema = build_schema(&log_entry);

    // Convert log entry to Arrow arrays
    let arrays = log_entry_to_arrays(&log_entry, &schema)?;

    // Write the Arrow arrays to a Parquet file with the generated file name
    write_parquet_file(&file_name, Arc::new(schema), arrays)?;

    println!("Parquet file '{}' created successfully.", file_name);

    Ok(())
}

