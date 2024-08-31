
use std::env;
use std::path::PathBuf;

fn main() {
    // Get the OUT_DIR environment variable at runtime
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Construct the path to the descriptor set file
    let descriptor_path = out_dir.join("parquetb_descriptor.bin");

    // Configure and compile the proto files (parquetb.proto and minioc.proto)
    tonic_build::configure()
        .file_descriptor_set_path(descriptor_path)
        .compile(
            &[
                "proto-definitions/parquetb.proto",  // Path to your parquetb.proto
                "proto-definitions/minioc.proto" // Path to your minioc.proto
            ], 
            &["proto"]
        )
        .unwrap_or_else(|e| panic!("Failed to compile proto files: {}", e));
}

