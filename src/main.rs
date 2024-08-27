
use chrono::{DateTime, Utc};
use serde_json::Value;

mod utils;

use crate::utils::truncate_to_minute::truncate_to_minute;
use crate::utils::build_schema::build_schema;

fn main() {
    // Example log entry with fixed and dynamic metadata fields
    let log = r#"{
        "datetime": "2024-08-26T10:15:42Z",
        "tenant_name": "TenantA",
        "item_id": "Item123",
        "status": "SUCCESS",
        "qty": 100.5,
        "metadata": {
            "metadata_1": "Some text",
            "metadata_2": 42,
            "metadata_3": true
        }
    }"#;

    // Parse the log entry into a serde_json::Value
    let log_entry: Value = serde_json::from_str(log).unwrap();

    // Parse the datetime and truncate to the minute level
    let datetime: DateTime<Utc> = log_entry["datetime"].as_str().unwrap().parse().unwrap();
    let minute = truncate_to_minute(&datetime);

    // Build the schema, combining fixed and dynamically inferred fields
    let schema = build_schema(&log_entry);

    println!("Generated Schema: {:?}", schema);
    println!("Original Datetime: {}", datetime);
    println!("Truncated to Minute: {}", minute);
}

