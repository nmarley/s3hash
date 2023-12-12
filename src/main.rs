use clap::Parser;
use simplelog::*;
use std::fs;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::signal::unix::{signal, SignalKind};

// I know I'm calling these "DEFAULT" but I'm also not implementing options b/c
// I don't want to deal w/the extra logic and ensuring the names don't conflict.
const DEFAULT_OUTFILE: &str = "s3hashes.csv";
const DEFAULT_LOGFILE: &str = "s3checksummer.log";

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

mod batch_file_lines_reader;
mod clapargs;
mod parallel_hasher;
mod s3agent;

use batch_file_lines_reader::BatchFileLinesReader;
use parallel_hasher::par_sha256;
use s3agent::S3Agent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = clapargs::Args::parse();
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Debug,
            Config::default(),
            fs::File::create(DEFAULT_LOGFILE)?,
        ),
    ])
    .unwrap();

    let mut outfile_handle = fs::File::create(DEFAULT_OUTFILE)?;
    let bin_name = env!("CARGO_PKG_NAME");
    info!(
        "{} started, pid: {}, outfile: {}",
        bin_name,
        std::process::id(),
        DEFAULT_OUTFILE,
    );

    // Create an object to read a batch of lines from a file at a time.
    let batch_reader = Arc::new(Mutex::new(BatchFileLinesReader::new(
        args.keys_file,
        args.start_position,
    )?));

    // Signal handling in a non-blocking manner
    let br_signal = Arc::clone(&batch_reader);
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to handle SIGINT");
        loop {
            sigint.recv().await.expect("Failed to receive SIGINT");
            let br = br_signal.lock().unwrap();
            warn!("Received SIGINT. Current position: {}", br.get_position());
        }
    });

    let s3agent = S3Agent::new(&args.s3_bucket, args.s3_prefix.as_deref()).await;
    let br_main = Arc::clone(&batch_reader);
    let mut count_hashes_written: u64 = 0;
    loop {
        // main processing here
        let batch_result = {
            let mut br = br_main.lock().unwrap();
            br.get_batch(args.batch_size)
        };

        let batch = match batch_result {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(e) => return Err(e.into()),
        };

        // download objects from s3
        let s3_data = s3agent.batch_get_objects(&batch).await?;
        let keys_hashes = par_sha256(&s3_data);
        for (key, hash) in keys_hashes {
            // here is where we append to the output file
            match writeln!(&mut outfile_handle, "{},\\\\x{}", key, hex::encode(hash)) {
                Ok(_) => {
                    count_hashes_written += 1;
                }
                Err(e) => {
                    debug!("error writing sha256 hash to outfile: {}", e);
                }
            };
        }
    }

    info!(
        "{} finished, wrote {} hashes to {}",
        bin_name, count_hashes_written, DEFAULT_OUTFILE,
    );

    Ok(())
}
