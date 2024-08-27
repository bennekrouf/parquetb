
use std::fs::File;
use std::error::Error;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

// Write Arrow arrays to a Parquet file
pub fn write_parquet_file(file_path: &str, schema: Arc<Schema>, arrays: Vec<ArrayRef>) -> Result<(), Box<dyn Error>> {
    // Check if the number of arrays matches the number of fields in the schema
    if arrays.len() != schema.fields().len() {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput,
            format!("Number of columns({}) must match number of fields({}) in schema", arrays.len(), schema.fields().len()))));
    }

    let file = File::create(file_path)?;
    let props = WriterProperties::builder().build();

    // Create a new RecordBatch
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    // Create the ArrowWriter, which takes care of writing Arrow data to Parquet
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    // Write the batch to the Parquet file
    writer.write(&batch)?;

    // Close the writer to finish writing
    writer.close()?;

    Ok(())
}

