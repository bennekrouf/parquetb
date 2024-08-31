mod log_service;
mod utils;
mod client;

use tonic::transport::Server;
use std::env;
use tonic_reflection::server::Builder;

use crate::log_service::log::log_service_server::LogServiceServer;
use crate::log_service::MyLogService;
use dotenvy::from_path;
use std::path::Path;
use tracing_subscriber;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing or logging
    tracing_subscriber::fmt::init();
    // Load the environment variables from a custom file
    let custom_env_path = Path::new("proto-definitions/.service");
    from_path(custom_env_path).expect("Failed to load environment variables from custom path");

    // Retrieve the necessary values from environment variables
    let ip = env::var("LOG_DOMAIN").expect("Missing 'domain' environment variable");
    let port = env::var("LOG_PORT").expect("Missing 'port' environment variable");
    let addr = format!("{}:{}", ip, port).parse().unwrap();

    let log_service = MyLogService::default();

    println!("LogService server listening on {}", addr);

    let descriptor_set = include_bytes!(concat!(env!("OUT_DIR"), "/log_descriptor.bin"));
    let reflection_service = Builder::configure()
        .register_encoded_file_descriptor_set(descriptor_set)
        .build_v1()?;

    // Build and start the gRPC server
    Server::builder()
        .add_service(LogServiceServer::new(log_service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

