use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Error};
use std::path::Path;
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug)]
pub struct S3Agent {
    bucket: String,
    prefix: Option<String>,
    client: Client,
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

        // debug!("downloading {} from s3", key);

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

    // note: This was added as a sanity check, ensure we can see the bucket
    // before trying to download a shit-ton of files... or handle 'NoSuchBucket'
    // error and abort if we get one upon trying to get_object

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
}
