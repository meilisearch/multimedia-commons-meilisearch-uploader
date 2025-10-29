# Multimedia Commons Meilisearch Uploader

A high-performance Rust application that streams images from an S3 bucket, filters out monocolor images, and uploads them to Meilisearch in batches with full parallelization and constant memory usage.

## Features

- **Streaming Architecture**: Memory-efficient streaming of S3 objects without loading all keys into memory
- **S3 Integration**: Recursively scans S3 buckets using rusty-s3 with pagination
- **Image Processing**: Supports JPEG and PNG images with intelligent monocolor filtering
- **Parallel Processing**: Fully parallelized downloads and uploads using tokio
- **Batch Upload**: Intelligent batching with size limits (100MB default)
- **Retry Logic**: Built-in retry mechanism with exponential backoff
- **Base64 Encoding**: Converts images to base64 for storage
- **Configurable**: Command-line options for all parameters
- **Dry Run Mode**: Test without actually uploading to Meilisearch
- **Progress Tracking**: Real-time statistics and progress monitoring

## Installation

Make sure you have Rust installed, then build the project:

```bash
cargo build --release
```

The binary will be available at `target/release/multimedia-commons-meilisearch-uploader`.

## Usage

### Basic Usage

```bash
./target/release/multimedia-commons-meilisearch-uploader
```

This will use the default configuration:
- S3 Bucket: `multimedia-commons`
- S3 Region: `us-west-2`
- S3 Prefix: `data/images/`
- Meilisearch URL: `https://ms-66464012cf08-103.fra.meilisearch.io`

### Custom Configuration

```bash
./target/release/multimedia-commons-meilisearch-uploader \
    --bucket my-bucket \
    --region us-east-1 \
    --prefix images/ \
    --meilisearch-url https://my-meilisearch.com \
    --meilisearch-key your-api-key \
    --max-downloads 100 \
    --max-uploads 20 \
    --batch-size 50
```

### Dry Run

Test the configuration without uploading to Meilisearch:

```bash
./target/release/multimedia-commons-meilisearch-uploader --dry-run
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | `multimedia-commons` | S3 bucket name |
| `--region` | `us-west-2` | S3 region |
| `--prefix` | `data/images/` | S3 prefix path |
| `--meilisearch-url` | `https://ms-66464012cf08-103.fra.meilisearch.io` | Meilisearch URL |
| `--meilisearch-key` | (default provided) | Meilisearch API key |
| `--max-downloads` | `50` | Maximum concurrent downloads |
| `--max-uploads` | `10` | Maximum concurrent uploads |
| `--batch-size` | `100` | Number of documents per batch |
| `--max-batch-bytes` | `104857600` | Maximum batch size in bytes (100MB) |
| `--dry-run` | `false` | Don't upload to Meilisearch |

## Output Format

Each image is converted to a JSON document with the following structure:

```json
{
  "id": "filename_without_extension",
  "base64": "base64_encoded_image_data",
  "url": "https://bucket.s3-region.amazonaws.com/path/to/image.jpg"
}
```

## AWS Credentials

The application uses AWS credentials from the environment. You have several options:

### Option 1: Environment Variables
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

### Option 2: AWS Credentials File
Create `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
```

### Option 3: IAM Roles (for EC2 instances)
If running on EC2, the application will automatically use IAM roles.

### Option 4: Anonymous Access
For public buckets, the application will attempt anonymous access if no credentials are found.

**Note**: The multimedia-commons bucket is publicly accessible, so you can run the application without AWS credentials for read-only access.

## Performance

The application is designed for high performance and memory efficiency with:

- **Streaming S3 Processing**: Uses Rust Streams to process S3 objects without loading all keys into memory
- **Constant Memory Usage**: Processes images in streaming batches, maintaining constant memory footprint regardless of dataset size
- **Parallel S3 object listing**: Uses continuation tokens for paginated S3 API calls
- **Concurrent image downloads**: Semaphore-based rate limiting for optimal throughput
- **Efficient monocolor detection**: Grid-based pixel sampling with compression artifact tolerance
- **Intelligent batch processing**: Minimizes Meilisearch API calls while respecting size limits
- **Built-in retry logic**: Exponential backoff for transient failures
- **Real-time progress tracking**: Detailed statistics and progress monitoring

### Performance Tuning

- Adjust `--max-downloads` based on your network bandwidth and S3 rate limits (default: 50)
- Adjust `--max-uploads` based on your Meilisearch instance capacity (default: 10)
- Use `--batch-size` to control processing batch size - larger batches improve parallelization (default: 100)
- The streaming architecture maintains constant memory usage regardless of dataset size
- Monitor the progress output to gauge optimal concurrency settings for your environment

## Image Processing

- **Supported Formats**: JPEG, PNG (detected by file extension)
- **Advanced Monocolor Detection**: Grid-based pixel sampling with tolerance for compression artifacts (<1% variation threshold)
- **Base64 Encoding**: All valid images are encoded to base64 for Meilisearch storage
- **Streaming Processing**: Images are processed as they are discovered, not batched in memory
- **Error Handling**: Failed downloads/processing are logged and counted but don't stop the process

## Dependencies

- `anyhow` - Error handling
- `base64` - Base64 encoding
- `clap` - Command line parsing
- `futures` - Async utilities
- `image` - Image processing
- `reqwest` - HTTP client
- `rusty-s3` - S3 client
- `serde` - Serialization
- `tokio` - Async runtime
- `url` - URL parsing

## Error Handling

The application includes comprehensive error handling:

- Automatic retries for transient failures
- Graceful handling of invalid images
- Logging of errors without stopping the entire process
- Final summary of errors encountered

## Troubleshooting

### Common Issues

1. **Certificate Errors**: Make sure your system time is correct and you have updated CA certificates
2. **Access Denied**: Verify your AWS credentials have S3 read permissions
3. **Out of Memory**: Reduce `--batch-size` or `--max-downloads` if processing large images
4. **Meilisearch Errors**: Check that your Meilisearch URL and API key are correct

### Testing

Use the `--dry-run` flag to test your configuration without uploading to Meilisearch:

```bash
./target/release/multimedia-commons-meilisearch-uploader --dry-run --max-downloads 5
```

## License

This project is licensed under the MIT License.