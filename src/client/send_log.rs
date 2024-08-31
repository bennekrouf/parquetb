pub mod minioc {
    tonic::include_proto!("minioc");
}
use tonic::{Request, metadata::MetadataValue};
use std::str::FromStr;
use tracing::{info, error};
use tonic::transport::Endpoint;
// use tonic::Request;
// use tonic::Streaming;
// use tonic::transport::Channel;
use minioc::minioc_service_client::MiniocServiceClient;
use minioc::{FileChunk, UploadRequest};
use tokio::sync::mpsc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use dotenvy::from_path;
use std::env;
use std::path::Path;
use std::error::Error;
use futures_util::Stream; // Add this import
// use futures_util::stream::StreamExt; // Add this import
use http::Uri;
// Implement a custom stream wrapper
struct FileChunkStream {
    receiver: mpsc::Receiver<FileChunk>,
}

impl Stream for FileChunkStream {
    type Item = FileChunk; // Change this line

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.receiver.poll_recv(cx) {
            std::task::Poll::Ready(Some(chunk)) => std::task::Poll::Ready(Some(chunk)),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub async fn send_log(file_path: &str, tenant: &str, filename: &str) -> Result<(), Box<dyn Error>> {
    info!("Starting send_log with file_path: {}, tenant: {}, filename: {}", file_path, tenant, filename);

    // Load environment variables
    let custom_env_path = Path::new("proto-definitions/.service");
    from_path(custom_env_path).expect("Failed to load environment variables from custom path");
    info!("Loaded environment variables from {:?}", custom_env_path);

    // Retrieve connection parameters
    let ip = env::var("MINIOC_DOMAIN").expect("Missing 'MINIOC_DOMAIN' environment variable");
    let port = env::var("MINIOC_PORT").expect("Missing 'MINIOC_PORT' environment variable");
    let addr: Uri = format!("http://{}:{}", ip, port).parse()?;
    info!("Parsed address: {}", addr);

    // Create the gRPC client
    let endpoint = Endpoint::from_shared(addr.to_string())?
        .connect_timeout(std::time::Duration::from_secs(5))
        .tcp_nodelay(true)
        .timeout(std::time::Duration::from_secs(30))
        // .disable_timeout()
        // .http2_keep_alive_interval(Some(std::time::Duration::from_secs(30)))
        // .http2_keep_alive_timeout(Some(std::time::Duration::from_secs(30)))
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)));
        // .tls(false);

    let mut client = MiniocServiceClient::connect(endpoint).await.map_err(|e| {
        error!("Failed to connect to server {}:{} : {}", &ip, &port, e);
        e
    })?;
    info!("Successfully connected to server at {}", addr);

    // Open the file
    let mut file = File::open(file_path).await.map_err(|e| {
        error!("Failed to open file {}: {}", file_path, e);
        e
    })?;
    info!("Successfully opened file: {}", file_path);

    // Create a channel to stream the file chunks
    let (tx, rx) = mpsc::channel(4);
    info!("Created a channel for streaming file chunks");

    // Spawn a task to read the file in chunks and send to the channel
    tokio::spawn(async move {
        let mut buffer = [0u8; 1024];
        loop {
            match file.read(&mut buffer).await {
                Ok(n) if n == 0 => {
                    info!("Reached end of file");
                    break;
                }
                Ok(n) => {
                    let chunk = FileChunk { data: buffer[..n].to_vec() };
                    if tx.send(chunk).await.is_err() {
                        error!("Receiver dropped, stopping file read");
                        break;
                    }
                }
                Err(e) => {
                    error!("Error reading file: {}", e);
                    break;
                }
            }
        }
    });

    // Create the stream wrapper
    let chunk_stream = FileChunkStream { receiver: rx };

    // Create the request with metadata
    // let request = Request::new(chunk_stream);
    // let metadata = UploadRequest {
    //     tenant: tenant.to_string(),
    //     filename: filename.to_string(),
    // };

    // Create the stream with the metadata
    let mut request = Request::new(chunk_stream);
    // Attach metadata headers
    request.metadata_mut().insert("tenant", MetadataValue::from_str(tenant)?);
    request.metadata_mut().insert("filename", MetadataValue::from_str(filename)?);

    info!("Created gRPC request with metadata: tenant = {}, filename = {}", tenant, filename);

    // Call the stream_upload gRPC method
    let response = client.stream_upload(request).await.map_err(|e| {
        error!("Failed to send stream_upload request: {}", e);
        e
    })?;

    // Extract the response
    let response = response.into_inner();
    info!("Upload response: {}", response.message);

    Ok(())
}
