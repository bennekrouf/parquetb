
mod parquetb_service;
mod utils;
mod client;

use tonic::transport::Server;
use std::env;
use tonic_reflection::server::Builder;
use crate::parquetb_service::parquetb::parquetb_service_server::ParquetbServiceServer;
use crate::parquetb_service::MyParquetbService;
use dotenvy::from_path;
use std::path::Path;
use messengerc::{connect_to_messenger_service, MessagingService};
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the environment variables from a custom file
    let custom_env_path = Path::new("proto-definitions/.service");
    from_path(custom_env_path).expect("Failed to load environment variables from custom path");

    // Retrieve the necessary values from environment variables
    let ip = env::var("LOG_DOMAIN").expect("Missing 'domain' environment variable");
    let port = env::var("LOG_PORT").expect("Missing 'port' environment variable");
    let addr = format!("{}:{}", ip, port).parse().unwrap();

    // Create and initialize the gRPC client for the messaging service
    let messenger_client = connect_to_messenger_service().await
        .ok_or("Failed to connect to messenger service")?;

    let messaging_service = MessagingService::new(
        Arc::new(Mutex::new(messenger_client)),
        "parquetb".to_string(),
    );

    // Publish a message through the messaging service
    let message = format!("ParquetbService server listening on {}", addr);
    if let Err(e) = messaging_service.publish_message(message.clone(), None).await {
        eprintln!("Failed to publish message: {:?}", e);
    }

    let parquetb_service = MyParquetbService::default();

    println!("{}", &message);

    let descriptor_set = include_bytes!(concat!(env!("OUT_DIR"), "/parquetb_descriptor.bin"));
    let reflection_service = Builder::configure()
        .register_encoded_file_descriptor_set(descriptor_set)
        .build_v1()?;

    // Build and start the gRPC server
    Server::builder()
        .add_service(ParquetbServiceServer::new(parquetb_service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

