
use tonic::{Request, Response, Status, Streaming};
use tonic::async_trait;
use futures::StreamExt;
use std::sync::Arc;

pub mod parquetb {
    tonic::include_proto!("parquetb");
}

use parquetb::parquetb_service_server::ParquetbService;
use parquetb::{LogEntry, UploadResponse};

use crate::utils::{build_schema::build_schema, log_entry_to_arrays::log_entry_to_arrays, write_parquet_file::write_parquet_file};
use crate::client::send_log::send_log;
use serde_json::json;
// use arrow::datatypes::Schema;
use std::error::Error;
use tracing::{info, error};

#[derive(Debug, Default)]
pub struct MyParquetbService;

#[async_trait]
impl ParquetbService for MyParquetbService {
    async fn stream_logs(
        &self,
        request: Request<Streaming<LogEntry>>,
    ) -> Result<Response<UploadResponse>, Status> {
        let mut stream = request.into_inner();
        let mut log_entries = vec![];

        // Process the incoming stream of log entries
        while let Some(log_entry) = stream.next().await {
            match log_entry {
                Ok(entry) => {
                    // Convert LogEntry to serde_json::Value for processing
                    let log_value = json!({
                        // "datetime": entry.datetime,
                        "tenant_name": entry.tenant_name,
                        "item_id": entry.item_id,
                        "status": entry.status,
                        "qty": entry.qty,
                        "metadata": entry.metadata,
                    });

                    log_entries.push(log_value);
                }
                Err(_) => return Err(Status::internal("Error reading stream")),
            }
        }

        if log_entries.is_empty() {
            return Err(Status::invalid_argument("No log entries provided"));
        }

        // Process the log entries and generate Parquet file
        let file_name = match process_logs(&log_entries).await {
            Ok(file_name) => file_name,
            Err(e) => return Err(Status::internal(format!("Error processing logs: {}", e))),
        };

        // Call send_log to upload the Parquet file
        if let Err(e) = send_log(&file_name, &log_entries[0]["tenant_name"].as_str().unwrap(), &file_name).await {
            return Err(Status::internal(format!("Error sending parquetb file: {}", e)));
        }

        // Return a successful response
        let reply = UploadResponse {
            message: "Parquet file created and uploaded successfully!".to_string(),
        };
        Ok(Response::new(reply))
    }
}

async fn process_logs(log_entries: &[serde_json::Value]) -> Result<String, Box<dyn Error>> {
    info!("Starting log processing.");

    // Use the first log entry to build the schema and get tenant info
    let first_log = &log_entries[0];
    let tenant_name = match first_log["tenant_name"].as_str() {
        Some(tenant) => tenant,
        None => {
            error!("Tenant name missing in the first log entry.");
            return Err("Missing tenant name".into());
        }
    };
    info!("Tenant name: {}", tenant_name);

    // Generate the current UTC datetime instead of using the user-provided datetime
    let datetime = chrono::Utc::now();
    let formatted_datetime = datetime.format("%Y%m%d_%H%M").to_string();
    info!("Generated datetime: {}", formatted_datetime);

    let file_name = format!("{}_{}.parquet", tenant_name, formatted_datetime);
    info!("Generated file name: {}", file_name);

    // Build the schema based on the first log entry
    let schema = build_schema(first_log);
    info!("Schema built successfully.");

    // Convert each log entry to Arrow arrays
    let mut arrays = vec![];
    for log_entry in log_entries {
        match log_entry_to_arrays(log_entry, &schema) {
            Ok(array) => {
                info!("Converted log entry to arrays.");
                arrays.extend(array);
            }
            Err(e) => {
                error!("Failed to convert log entry to arrays: {}", e);
                return Err(e.into());
            }
        }
    }

    // Write to Parquet file
    match write_parquet_file(&file_name, Arc::new(schema), arrays) {
        Ok(_) => info!("Parquet file written successfully."),
        Err(e) => {
            error!("Failed to write Parquet file: {}", e);
            return Err(e.into());
        }
    }

    info!("Log processing completed successfully.");
    Ok(file_name)
}
