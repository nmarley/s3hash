# s3hash

> Calculate the sha256 hash of s3 objects

The S3 hash util is designed to compute SHA256 hashes for objects stored in Amazon S3. It is written in Rust and uses the Tokio async runtime to stream the input as well as s3 objects, compute the sha256 checksums and write them to a file. S3 objects are only kept in memory and are not stored on the filesystem. This tool is useful for calculating the sha256 hash of large volumes of data stored in an S3 bucket.

## LICENSE

Dual-licensed as MIT or Apache 2
