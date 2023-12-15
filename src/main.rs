use clap::Parser;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use simplelog::*;
use std::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

mod clapargs;
mod input_line_streamer;
mod s3agent;

use input_line_streamer::InputLineStreamer;
use s3agent::S3Agent;

// "DEFAULT" is a misnomer, these are the only filenames that will be used
const DEFAULT_OUTFILE: &str = "s3hashes.csv";
const DEFAULT_LOGFILE: &str = "s3checksummer.log";

// lazy static is used to only retrieve the number of cores once
lazy_static! {
    static ref NUM_CORES: usize = num_cpus::get();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line args
    let args = clapargs::Args::parse();
    // Set up logging
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

    let bin_name = env!("CARGO_PKG_NAME");
    info!(
        "{} started, pid: {}, outfile: {}, num_cores: {}",
        bin_name,
        std::process::id(),
        DEFAULT_OUTFILE,
        *NUM_CORES,
    );

    // Create an object to stream lines from a file.
    let lines_reader = InputLineStreamer::new(args.keys_file);

    // Create an object to interact with s3
    let s3agent = S3Agent::new(&args.s3_bucket, args.s3_prefix.as_deref()).await;

    // Track number of hashes written to output file (see also note below)
    let mut count_hashes_written: u64 = 0;
    let num_workers = match args.num_threads {
        Some(num) => num,
        // Use twice the number of cores as the default channel buffer size
        None => *NUM_CORES * 2,
    };

    // Create an mpmc channel for streaming lines from the input file into
    // worker tasks. Bounded in order to handle backpressure
    let (tx, rx) = async_channel::bounded(num_workers);

    // Spawn a task to stream lines from the input file into a channel for
    // worker tasks to read from
    tokio::spawn(async move {
        if let Ok(lines_stream) = lines_reader.stream_lines().await {
            lines_stream
                .for_each(|line| {
                    let tx = tx.clone();
                    async move {
                        if let Ok(line) = line {
                            // Send each line to the channel
                            if tx.send(line).await.is_err() {
                                error!("error sending line to channel");
                            }
                        }
                    }
                })
                .await;
            // debug!("CLOSING INPUT CHANNEL");
            tx.close();
        }
    });

    // Create a channel for sending results from the workers to the output task
    let (writer_tx, mut writer_rx) = mpsc::channel::<(String, Vec<u8>)>(num_workers);

    // Open the output file (the output task will take ownership of this)
    let mut outfh = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(DEFAULT_OUTFILE)
        .await?;

    // Create a oneshot channel for sending the count of written hashes from the
    // output task back to the main task once it's finished
    let (count_tx, count_rx) = oneshot::channel::<u64>();

    // Spawn a task to read results from the workers and write them to the
    // output file
    let output_handle = tokio::spawn(async move {
        while let Some((key, hash)) = writer_rx.recv().await {
            // Write key + hash to a CSV output file in the format that psql can
            // read and insert into a table
            let str_out = format!("{},\\\\x{}\n", key, hex::encode(hash));
            match outfh.write_all(str_out.as_bytes()).await {
                Ok(_) => {
                    count_hashes_written += 1;
                }
                Err(e) => {
                    debug!("error writing sha256 hash to outfile: {}", e);
                }
            }
        }
        // Send the count of hashes written to the output file back to the main
        // task
        let _ = count_tx.send(count_hashes_written);
    });

    // Create a vec to hold the handles for the worker + output tasks
    let mut handles = Vec::with_capacity(num_workers + 1);
    handles.push(output_handle);

    // Spawn num_workers tasks to fetch objects from s3, hash them and write
    // results to the output channel
    for _ in 0..num_workers {
        let rx = rx.clone();
        let writer_tx = writer_tx.clone();
        let s3agent = s3agent.clone();
        let handle = tokio::spawn(async move {
            while let Ok(line) = rx.recv().await {
                let key = line.trim();
                // Fetch the object bytes from s3
                match s3agent.get_object(key).await {
                    Ok(bytes) => {
                        let hash = sha256(&bytes);
                        // Send the results to the output channel for appending
                        // to the outfile
                        if writer_tx.send((key.to_owned(), hash)).await.is_err() {
                            error!("error sending result back to channel");
                        }
                    }
                    Err(e) => {
                        error!("error getting object from s3: {}", e);
                    }
                };
            }
            // debug!("DROPPING WRITER CHANNEL TX");
            drop(writer_tx);
        });
        // Add to the vec of handles so we can wait for all the workers to
        // finish before exiting the main task
        handles.push(handle);
    }
    // Drop the writer channel tx in the main task, only the workers will use
    // this.
    drop(writer_tx);

    // Wait for all the worker and output tasks to finish
    futures::future::join_all(handles).await;

    // Attempt to fetch the count of entries written to the output file
    let hashes_written = match count_rx.await {
        Ok(count) => count.to_string(),
        Err(_e) => "some".to_string(),
    };

    info!(
        "{} finished, wrote {} hashes to {}",
        bin_name, hashes_written, DEFAULT_OUTFILE,
    );

    Ok(())
}

pub(crate) fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}
