use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Debug)]
pub struct BatchFileLinesReader {
    reader: BufReader<File>,
    position: u64,
}

impl BatchFileLinesReader {
    pub fn new<P: AsRef<Path>>(filepath: P, start_pos: u64) -> io::Result<Self> {
        let f = File::open(filepath.as_ref())?;
        let reader = BufReader::new(f);
        Ok(Self {
            reader,
            position: start_pos,
        })
    }

    pub fn get_batch(&mut self, batch_size: usize) -> io::Result<Option<Vec<String>>> {
        // Seek to the current position
        self.reader.seek(SeekFrom::Start(self.position))?;

        let mut batch = Vec::with_capacity(batch_size);
        let mut len = 0;

        for line in self.reader.by_ref().lines().take(batch_size) {
            match line {
                Ok(line) => {
                    // add one for newline
                    len += line.len() as u64 + 1;
                    batch.push(line);
                }
                Err(e) => return Err(e),
            }
        }

        self.position += len;

        if batch.is_empty() {
            return Ok(None);
        }

        Ok(Some(batch))
    }

    /// Get the position of the underlying cursor for restart logic, so as to
    /// prevent re-processing data.
    pub fn get_position(&self) -> u64 {
        self.position
    }
}
