
use tonic::{Request, Response, Status, Streaming};
use tonic::async_trait;
use futures::StreamExt;
use std::sync::Arc;

pub mod log {
    tonic::include_proto!("log");
}

use log::log_service_server::LogService;
use log::{LogEntry, UploadResponse};

use crate::utils::{build_schema::build_schema, log_entry_to_arrays::log_entry_to_arrays, write_parquet_file::write_parquet_file};
use crate::client::send_log::send_log;
use serde_json::json;
// use arrow::datatypes::Schema;
use std::error::Error;

#[derive(Debug, Default)]
pub struct MyLogService;

#[async_trait]
impl LogService for MyLogService {
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
                        "datetime": entry.datetime,
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
            return Err(Status::internal(format!("Error sending log file: {}", e)));
        }

        // Return a successful response
        let reply = UploadResponse {
            message: "Parquet file created and uploaded successfully!".to_string(),
        };
        Ok(Response::new(reply))
    }
}

async fn process_logs(log_entries: &[serde_json::Value]) -> Result<String, Box<dyn Error>> {
    // Use the first log entry to build the schema and get tenant info
    let first_log = &log_entries[0];
    let tenant_name = first_log["tenant_name"].as_str().unwrap();
    let datetime_str = first_log["datetime"].as_str().unwrap();
    let datetime = datetime_str.parse::<chrono::DateTime<chrono::Utc>>()?;
    let formatted_datetime = datetime.format("%Y%m%d_%H%M").to_string();

    let file_name = format!("{}_{}.parquet", tenant_name, formatted_datetime);

    // Build the schema based on the first log entry
    let schema = build_schema(first_log);

    // Convert each log entry to Arrow arrays
    let mut arrays = vec![];
    for log_entry in log_entries {
        let array = log_entry_to_arrays(log_entry, &schema)?;
        arrays.extend(array);
    }

    // Write to Parquet file
    write_parquet_file(&file_name, Arc::new(schema), arrays)?;

    Ok(file_name)
}

