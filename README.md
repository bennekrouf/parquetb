
# Parquet builder gRPC Service

## Overview

This Rust-based gRPC service allows clients to stream log entries and automatically convert them into Parquet files. The service processes each log entry, infers its schema, and writes the log data into a Parquet file. The file is named based on the tenant and timestamp from the log entries.

### Key Features

- **Stream Processing:** Logs are received in a streaming manner.
- **Schema Inference:** The schema of the logs is dynamically inferred based on the fields in the log entries.
- **Parquet File Generation:** The processed logs are written to a Parquet file on the server.
- **gRPC Interface:** The service exposes a gRPC interface for seamless log streaming.

## Setup

### Prerequisites

- Rust and Cargo installed.
- `grpcurl` installed for testing the service.
  
### Running the Service

1. Clone the repository and navigate to the project directory:

    ```bash
    git clone https://your-repo-url.git
    cd your-repo
    ```

2. Run the service using Cargo:

    ```bash
    cargo run
    ```

   The service will start and listen on `0.0.0.0:50051` by default.

## Testing with `grpcurl`

You can test the service by streaming log entries using `grpcurl`. Here's how to do it.

### Example Log Entries

Prepare a `logs.json` file containing multiple log entries in JSON format. Each log entry should have fields like `datetime`, `tenant_name`, `item_id`, `status`, `qty`, and `metadata`. For example:

```json
{
  "datetime": "2024-08-26T10:15:42Z",
  "tenant_name": "TenantA",
  "item_id": "Item123",
  "status": "SUCCESS",
  "qty": 100.5,
  "metadata": {
    "key1": "value1"
  }
}
{
  "datetime": "2024-08-26T11:15:42Z",
  "tenant_name": "TenantA",
  "item_id": "Item456",
  "status": "FAILURE",
  "qty": 200.0,
  "metadata": {
    "key2": "value2"
  }
}
