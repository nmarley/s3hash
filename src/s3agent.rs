use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Error};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Semaphore};

#[derive(Clone, Debug)]
pub struct S3Agent {
    bucket: String,
    prefix: Option<String>,
    client: Client,
}

lazy_static! {
    static ref NUM_CORES: usize = num_cpus::get_physical();
}

// see:
// https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html

impl S3Agent {
    pub async fn new(bucket: &str, prefix: Option<&str>) -> Self {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&config);

        Self {
            bucket: bucket.to_owned(),
            prefix: prefix.map(|e| e.to_owned()),
            client,
        }
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, Error> {
        let key = &(match self.prefix {
            Some(ref prefix) => Path::new(&prefix)
                .join(key)
                .into_os_string()
                .into_string()
                .unwrap(),
            None => key.to_owned(),
        });

        debug!("downloading {} from s3", key);

        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let mut data = Vec::new();
        let mut stream = ByteStream::into_async_read(object.body);
        // Read the stream into the vec
        stream.read_to_end(&mut data).await.unwrap();

        Ok(data)
    }

    #[allow(dead_code)]
    /// Example method to list all buckets, needs s3 iam permission
    pub async fn list_buckets(&self) -> Result<Vec<String>, aws_sdk_s3::Error> {
        let resp = self.client.list_buckets().send().await?;
        let buckets: Vec<String> = resp
            .buckets()
            .iter()
            .map(|e| e.name().unwrap().to_string())
            .collect();
        Ok(buckets)
    }

    pub async fn batch_get_objects(
        &self,
        keys: &Vec<String>,
    ) -> tokio::io::Result<HashMap<String, Vec<u8>>> {
        let mut results = HashMap::new();

        let semaphore = Arc::new(Semaphore::new(*NUM_CORES));
        let (tx, mut rx) = mpsc::channel(*NUM_CORES);

        for k in keys {
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            let tx = tx.clone();
            let key = k.to_owned();

            let agent_clone = self.clone();
            tokio::spawn(async move {
                // TODO?: handle NoSuchKey?
                match agent_clone.get_object(&key).await {
                    Ok(bytes) => {
                        // Send the result back
                        if let Err(e) = tx.send((key, bytes)).await {
                            debug!("Got channel send error: {}", e);
                        }
                    }
                    Err(e) => {
                        debug!("Got error getting object {} from s3: {}", key, e);
                    }
                };
                // Release the permit
                drop(permit);
            });
        }

        // All tasks are spawned, so we can drop the original sender
        drop(tx);

        while let Some((key, bytes)) = rx.recv().await {
            debug!("Got bytes for {}", key);
            results.insert(key, bytes);
        }

        tokio::io::Result::Ok(results)
    }
}
