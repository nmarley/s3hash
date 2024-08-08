# s3hash

> Calculate the sha256 hash of s3 objects

The S3 hash util is an asynchronous utility written in Rust and designed to compute SHA256 hashes for objects stored in Amazon S3. It leverages Tokio for efficient asynchronous I/O operations. This utility concurrently fetches s3 objects, computes their sha256 hashes, and writes the keys + hashes to a CSV output file. The S3 objects are only kept in memory and are not stored on the filesystem. This tool is useful for calculating the sha256 hash of large volumes of data stored in an S3 bucket.

### Build:

```bash
cargo build --release
```

### Usage:

```bash
./target/release/s3hash --bucket <bucket_name> --keys-file <file.txt>
```

### Input

The input is a file containing the s3 object keys, one per line, e.g.:

```text
key1
key2
```

### Output

The output is (currently a hard-coded) file named `s3hashes.csv`, that contains the s3 object key and the sha256 hash in CSV format, e.g.:

```text
key1,hash1
key2,hash2
```

## LICENSE

Dual-licensed as MIT or Apache 2
