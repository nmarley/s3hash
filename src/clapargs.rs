use clap::Parser;

/// s3checksummer - calculate the sha256 hashes of s3 objects
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The s3 bucket from which to fetch objects
    #[arg(long = "bucket", required = true)]
    pub s3_bucket: String,
    /// The optional prefix to append to s3 bucket from which to fetch objects
    #[arg(long = "prefix")]
    pub s3_prefix: Option<String>,
    /// The input file from which to read s3 keys
    #[arg(long = "keys_file")]
    pub keys_file: String,
    /// The starting byte position in the keys file to read (useful for
    /// restarts)
    #[arg(long = "start_position", default_value = "0")]
    pub start_position: u64,
    /// The number of lines to read from the keys file in each batch
    #[arg(long = "batch_size", default_value = "200")]
    pub batch_size: usize,
}
