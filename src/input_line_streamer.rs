use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;

/// InputLineStreamer opens a file and asynchronously streams lines from it
#[derive(Clone, Debug)]
pub struct InputLineStreamer {
    filename: PathBuf,
}

impl InputLineStreamer {
    /// Initialize a new InputLineStreamer
    pub fn new<P: AsRef<Path>>(filename: P) -> Self {
        Self {
            filename: filename.as_ref().to_path_buf(),
        }
    }

    /// Returns a stream of lines from the file
    pub async fn stream_lines(&self) -> tokio::io::Result<LinesStream<BufReader<File>>> {
        let file = File::open(&self.filename).await?;
        let reader = BufReader::new(file);
        let lines_stream = LinesStream::new(reader.lines());
        Ok(lines_stream)
    }
}
