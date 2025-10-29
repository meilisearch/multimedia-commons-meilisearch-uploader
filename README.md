# Multimedia Commons Meilisearch Uploader

A high-performance Rust application with a concurrent pipeline architecture that streams images from an S3 bucket, processes them in parallel, and uploads them to Meilisearch with optimal throughput and constant memory usage.

## Features

- **Concurrent Pipeline Architecture**: Three-stage pipeline with S3 listing, image processing, and uploading running concurrently
- **True Streaming**: Processing starts immediately as images are discovered, no waiting for full S3 scan
- **Memory Efficient**: Constant memory usage regardless of dataset size using channels and bounded queues
- **S3 Integration**: Recursively scans S3 buckets using rusty-s3 with pagination
- **Parallel Image Processing**: Configurable concurrent image downloads and processing
- **Intelligent Filtering**: Advanced monocolor detection with compression artifact tolerance
- **Intelligent Batching**: Dynamic batch uploading with adaptive sizing - never skips large images
- **Built-in Resilience**: Retry logic with exponential backoff for transient failures
- **Base64 Encoding**: Converts images to base64 for Meilisearch storage
- **Highly Configurable**: Command-line options for all performance parameters
- **Dry Run Mode**: Test configuration without uploading to Meilisearch
- **Real-time Monitoring**: Live progress tracking and detailed statistics

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

The application uses a **concurrent pipeline architecture** for maximum performance:

### Pipeline Architecture
```
S3 Lister → [Channel] → Image Processor → [Channel] → Batch Uploader
    ↓                        ↓                          ↓
Discovers images         Downloads &                Uploads to
continuously            processes images            Meilisearch
                        in parallel                 in batches
```

- **Concurrent Execution**: All three stages run simultaneously for maximum throughput
- **No Blocking**: Image processing starts immediately as images are discovered
- **Constant Memory**: Bounded channels prevent memory buildup regardless of dataset size
- **Configurable Parallelism**: Control concurrent downloads and uploads independently
- **Efficient Resource Usage**: CPU, network, and memory optimally utilized

### Performance Features
- **S3 Streaming**: Continuous S3 object discovery with pagination
- **Parallel Processing**: Up to N concurrent image downloads/processing (configurable)
- **Adaptive Batching**: Intelligent batch sizing that handles large images by sending them separately
- **Advanced Filtering**: Grid-based monocolor detection with compression tolerance
- **Retry Logic**: Exponential backoff for transient failures
- **Real-time Stats**: Live progress monitoring across all pipeline stages

### Performance Tuning

**Throughput Optimization:**
- `--max-downloads`: Controls concurrent image processing (default: 50)
  - Higher values = more parallel processing but more memory/CPU usage
  - Tune based on your system resources and S3 rate limits
- `--max-uploads`: Controls Meilisearch upload concurrency (default: 10)
  - Tune based on your Meilisearch instance capacity
- `--batch-size`: Target documents per upload batch (default: 100)
  - Larger batches = fewer API calls but more memory per batch
  - Large images are automatically sent in separate batches to ensure no data loss

**Memory Management:**
- Channel buffer sizes are automatically tuned for optimal memory usage
- The pipeline maintains constant memory regardless of dataset size
- Processing memory scales with `--max-downloads` setting only

**Monitoring:**
- Watch the live output to see pipeline balance
- Optimal setup: S3 discovery keeps ahead of processing, processing keeps ahead of uploads

## Image Processing

- **Supported Formats**: JPEG, PNG (detected by file extension)
- **Advanced Monocolor Detection**: Grid-based pixel sampling with tolerance for compression artifacts (<1% variation threshold)
- **Base64 Encoding**: All valid images are encoded to base64 for Meilisearch storage
- **Pipeline Processing**: Images flow through the pipeline as discovered - no batching in memory
- **Concurrent Downloads**: Multiple images processed simultaneously with semaphore-based rate limiting
- **Adaptive Upload Strategy**: Large images that exceed batch size limits are sent in separate batches
- **Zero Data Loss**: No images are skipped due to size - all valid images are uploaded
- **Graceful Error Handling**: Failed downloads/processing are logged and counted but don't stop the pipeline

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
3. **Out of Memory**: Reduce `--max-downloads` if processing very large images (the pipeline itself uses constant memory)
4. **Slow Processing**: Increase `--max-downloads` for more parallel processing, but watch system resources
5. **Large Image Handling**: Very large images are automatically sent in separate batches with logging
6. **Meilisearch Errors**: Check that your Meilisearch URL and API key are correct
7. **Pipeline Stalls**: If one stage becomes a bottleneck, tune the related concurrency parameters

### Testing

Use the `--dry-run` flag to test the pipeline without uploading to Meilisearch:

```bash
# Test with lower concurrency to see pipeline stages clearly
./target/release/multimedia-commons-meilisearch-uploader --dry-run --max-downloads 5 --batch-size 10

# Test with higher concurrency for performance evaluation
./target/release/multimedia-commons-meilisearch-uploader --dry-run --max-downloads 20 --batch-size 50

# Test batch handling with smaller limits to see adaptive batching
./target/release/multimedia-commons-meilisearch-uploader --dry-run --max-batch-bytes 100000 --batch-size 3
```

## License

This project is licensed under the MIT License.