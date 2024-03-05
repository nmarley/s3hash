use clap::Parser;

// command-line arguments are defined here

/// s3hash - calculate the sha256 hashes of s3 objects
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The s3 bucket from which to fetch objects
    #[arg(long = "bucket")]
    pub s3_bucket: String,
    /// The optional prefix to append to s3 bucket from which to fetch objects
    #[arg(long = "prefix")]
    pub s3_prefix: Option<String>,
    /// The input file from which to read s3 keys
    #[arg(long)]
    pub keys_file: String,
    /// The number of threads to use (defaults to NUM_CPUS * 8)
    #[arg(long)]
    pub num_threads: Option<usize>,
}
