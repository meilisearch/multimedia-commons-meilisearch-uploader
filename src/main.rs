use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use image::{ImageBuffer, Rgb};

use rusty_s3::{Bucket, Credentials, S3Action};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore},
    time::sleep,
};
use url::Url;

const MAX_RETRIES: usize = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// S3 bucket name
    #[arg(long, default_value = "multimedia-commons")]
    bucket: String,

    /// S3 region
    #[arg(long, default_value = "us-west-2")]
    region: String,

    /// S3 prefix path
    #[arg(long, default_value = "data/images/")]
    prefix: String,

    /// Meilisearch URL
    #[arg(long, default_value = "https://ms-66464012cf08-103.fra.meilisearch.io")]
    meilisearch_url: String,

    /// Meilisearch API key
    #[arg(
        long,
        default_value = "0d330f6cbb050b59c30234491ee469a43b2e9c48bfcbea4776e2c7d38792d96a"
    )]
    meilisearch_key: String,

    /// Maximum concurrent downloads
    #[arg(long, default_value_t = 50)]
    max_downloads: usize,

    /// Maximum concurrent uploads
    #[arg(long, default_value_t = 10)]
    max_uploads: usize,

    /// Batch size for uploads
    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    /// Maximum batch size in bytes (100MB default)
    #[arg(long, default_value_t = 100 * 1024 * 1024)]
    max_batch_bytes: usize,

    /// Dry run mode (don't upload to Meilisearch)
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ImageDocument {
    id: String,
    base64: String,
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct S3ListResponse {
    #[serde(default)]
    contents: Vec<S3Object>,
    #[serde(default)]
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct S3Object {
    key: String,
    size: u64,
}

#[derive(Debug, Default)]
struct Statistics {
    total_found: u64,
    processed: u64,
    monocolor_filtered: u64,
    decode_errors: u64,
    download_errors: u64,
    uploaded: u64,
}

struct AppContext {
    s3_bucket: Bucket,
    http_client: reqwest::Client,
    download_semaphore: Arc<Semaphore>,
    upload_semaphore: Arc<Semaphore>,
    stats: Arc<Mutex<Statistics>>,
    args: Args,
}

impl AppContext {
    fn new(args: Args) -> Result<Self> {
        let bucket = Bucket::new(
            Url::parse(&format!("https://s3.{}.amazonaws.com", args.region))?,
            rusty_s3::UrlStyle::VirtualHost,
            args.bucket.clone(),
            args.region.clone(),
        )?;

        let http_client = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(60))
            .build()?;

        Ok(Self {
            s3_bucket: bucket,
            http_client,
            download_semaphore: Arc::new(Semaphore::new(args.max_downloads)),
            upload_semaphore: Arc::new(Semaphore::new(args.max_uploads)),
            stats: Arc::new(Mutex::new(Statistics::default())),
            args,
        })
    }
}

fn is_image_file(key: &str) -> bool {
    let path = Path::new(key);
    if let Some(extension) = path.extension() {
        let ext = extension.to_string_lossy().to_lowercase();
        matches!(ext.as_str(), "jpg" | "jpeg" | "png")
    } else {
        false
    }
}

fn is_monocolor_image(img: &ImageBuffer<Rgb<u8>, Vec<u8>>) -> bool {
    if img.is_empty() {
        return true;
    }

    let width = img.width();
    let height = img.height();

    if width == 0 || height == 0 {
        return true;
    }

    let first_pixel = img.get_pixel(0, 0);
    let first_color = first_pixel.0;

    // Sample pixels across the image for performance
    // Use a grid sampling approach with at least 100 samples for better accuracy
    let samples_per_dim = std::cmp::max(10, std::cmp::min(width, height) / 50);
    let x_step = std::cmp::max(1, width / samples_per_dim);
    let y_step = std::cmp::max(1, height / samples_per_dim);

    let mut different_pixels = 0;
    let mut total_samples = 0;

    for y in (0..height).step_by(y_step as usize) {
        for x in (0..width).step_by(x_step as usize) {
            let pixel = img.get_pixel(x, y);
            total_samples += 1;

            // Allow for small variations due to compression artifacts
            let r_diff = (pixel.0[0] as i16 - first_color[0] as i16).abs();
            let g_diff = (pixel.0[1] as i16 - first_color[1] as i16).abs();
            let b_diff = (pixel.0[2] as i16 - first_color[2] as i16).abs();

            if r_diff > 5 || g_diff > 5 || b_diff > 5 {
                different_pixels += 1;
            }
        }
    }

    // Consider image monocolor if less than 1% of samples show significant variation
    let variation_ratio = different_pixels as f64 / total_samples as f64;
    variation_ratio < 0.01
}

fn extract_id_from_key(key: &str) -> String {
    Path::new(key)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string()
}

fn build_public_url(key: &str, bucket: &str, region: &str) -> String {
    format!("https://{}.s3-{}.amazonaws.com/{}", bucket, region, key)
}

async fn list_s3_objects(
    ctx: &AppContext,
    prefix: &str,
    continuation_token: Option<String>,
) -> Result<S3ListResponse> {
    let credentials = Credentials::from_env();

    let mut list_action = ctx.s3_bucket.list_objects_v2(credentials.as_ref());
    list_action.with_prefix(prefix);
    list_action.with_max_keys(1000);

    if let Some(ref token) = continuation_token {
        list_action.with_continuation_token(token);
    }

    let url = list_action.sign(Duration::from_secs(300));

    let response = ctx
        .http_client
        .get(url)
        .send()
        .await
        .context("Failed to list S3 objects")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "S3 list request failed with status {}: {}",
            status,
            text
        ));
    }

    let response_text = response
        .text()
        .await
        .context("Failed to read S3 response")?;

    let list_response: S3ListResponse = quick_xml::de::from_str(&response_text)
        .map_err(|e| anyhow::anyhow!("Failed to parse S3 XML response: {}", e))?;

    Ok(list_response)
}

async fn retry_with_backoff<T, F, Fut>(operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = Some(e);
                if attempt < MAX_RETRIES - 1 {
                    let delay = RETRY_DELAY * (2_u32.pow(attempt as u32));
                    sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap())
}

async fn download_and_process_image(
    ctx: &AppContext,
    key: String,
) -> Result<Option<ImageDocument>> {
    let _permit = ctx.download_semaphore.acquire().await?;

    if ctx.args.max_downloads <= 20 {
        println!("Processing image: {}", key);
    }

    let download_operation = || async {
        // Use credentials from environment or anonymous access
        let credentials = Credentials::from_env();
        let get_action = ctx.s3_bucket.get_object(credentials.as_ref(), &key);
        let url = get_action.sign(Duration::from_secs(300));

        let response = ctx
            .http_client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Failed to download image: {}", key))?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("HTTP {}: {}", response.status(), key));
        }

        Ok(response
            .bytes()
            .await
            .with_context(|| format!("Failed to read image bytes: {}", key))?)
    };

    let image_bytes = match retry_with_backoff(download_operation).await {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Failed to download {} after retries: {}", key, e);
            {
                let mut stats = ctx.stats.lock().await;
                stats.download_errors += 1;
            }
            return Ok(None);
        }
    };

    // Load and decode the image
    let img = match image::load_from_memory(&image_bytes) {
        Ok(img) => img.to_rgb8(),
        Err(e) => {
            eprintln!("Failed to decode image {}: {}", key, e);
            {
                let mut stats = ctx.stats.lock().await;
                stats.decode_errors += 1;
            }
            return Ok(None);
        }
    };

    // Filter out monocolor images
    if is_monocolor_image(&img) {
        if ctx.args.max_downloads <= 20 {
            println!("Skipping monocolor image: {}", key);
        }
        {
            let mut stats = ctx.stats.lock().await;
            stats.monocolor_filtered += 1;
        }
        return Ok(None);
    }

    // Encode to base64
    let base64_data = general_purpose::STANDARD.encode(&image_bytes);

    let document = ImageDocument {
        id: extract_id_from_key(&key),
        base64: base64_data,
        url: build_public_url(&key, &ctx.args.bucket, &ctx.args.region),
    };

    if ctx.args.max_downloads <= 20 {
        println!("Successfully processed: {}", key);
    }

    {
        let mut stats = ctx.stats.lock().await;
        stats.processed += 1;
        if stats.processed % 100 == 0 {
            println!("Progress: {} images processed", stats.processed);
        }
    }

    Ok(Some(document))
}

async fn upload_batch_to_meilisearch(
    ctx: &AppContext,
    documents: Vec<ImageDocument>,
) -> Result<()> {
    let _permit = ctx.upload_semaphore.acquire().await?;

    println!(
        "Uploading batch of {} documents to Meilisearch",
        documents.len()
    );

    if ctx.args.dry_run {
        println!("Dry run mode: skipping actual upload");
        return Ok(());
    }

    let url = format!("{}/indexes/images/documents", ctx.args.meilisearch_url);

    let upload_operation = || async {
        let response = ctx
            .http_client
            .post(&url)
            .header(
                "Authorization",
                &format!("Bearer {}", ctx.args.meilisearch_key),
            )
            .header("Content-Type", "application/json")
            .json(&documents)
            .send()
            .await
            .context("Failed to upload batch to Meilisearch")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Meilisearch upload failed with status {}: {}",
                status,
                text
            ));
        }

        Ok(())
    };

    retry_with_backoff(upload_operation).await?;

    {
        let mut stats = ctx.stats.lock().await;
        stats.uploaded += documents.len() as u64;
    }

    println!(
        "Successfully uploaded batch of {} documents",
        documents.len()
    );
    Ok(())
}

fn estimate_document_size(doc: &ImageDocument) -> usize {
    // Rough estimate of JSON serialized size
    doc.id.len() + doc.base64.len() + doc.url.len() + 100 // extra for JSON overhead
}

async fn process_batch(ctx: Arc<AppContext>, keys: Vec<String>) -> Result<()> {
    println!("Processing batch of {} images", keys.len());

    let documents: Vec<ImageDocument> = stream::iter(keys)
        .map(|key| {
            let ctx = ctx.clone();
            async move { download_and_process_image(&ctx, key).await }
        })
        .buffer_unordered(ctx.args.max_downloads)
        .filter_map(|result| async move {
            match result {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => None,
                Err(e) => {
                    eprintln!("Error processing image: {}", e);
                    None
                }
            }
        })
        .collect()
        .await;

    if documents.is_empty() {
        println!("No valid documents in batch, skipping upload");
        return Ok(());
    }

    // Split into smaller batches if needed to respect size limits
    let mut current_batch = Vec::new();
    let mut current_size = 0;

    for doc in documents {
        let doc_size = estimate_document_size(&doc);

        if current_batch.len() >= ctx.args.batch_size
            || (current_size + doc_size) > ctx.args.max_batch_bytes && !current_batch.is_empty()
        {
            // Upload current batch
            if let Err(e) = upload_batch_to_meilisearch(&ctx, current_batch.clone()).await {
                eprintln!("Failed to upload batch: {}", e);
            }
            current_batch.clear();
            current_size = 0;
        }

        current_batch.push(doc);
        current_size += doc_size;
    }

    // Upload remaining documents
    if !current_batch.is_empty() {
        if let Err(e) = upload_batch_to_meilisearch(&ctx, current_batch).await {
            eprintln!("Failed to upload final batch: {}", e);
        }
    }

    Ok(())
}

fn image_keys_stream(ctx: Arc<AppContext>) -> impl Stream<Item = Result<String>> {
    stream::unfold(
        (ctx, None::<String>),
        |(ctx, continuation_token)| async move {
            match list_s3_objects(&ctx, &ctx.args.prefix, continuation_token).await {
                Ok(response) => {
                    let image_keys: Vec<Result<String>> = response
                        .contents
                        .into_iter()
                        .filter_map(|obj| {
                            if is_image_file(&obj.key) {
                                Some(Ok(obj.key))
                            } else {
                                None
                            }
                        })
                        .collect();

                    let next_token = if response.is_truncated {
                        response.next_continuation_token
                    } else {
                        None
                    };

                    if image_keys.is_empty() && next_token.is_some() {
                        // Continue to next page if no images found but more pages exist
                        Some((stream::iter(vec![]), (ctx, next_token)))
                    } else {
                        let keys_stream = stream::iter(image_keys);
                        if next_token.is_some() {
                            Some((keys_stream, (ctx, next_token)))
                        } else {
                            Some((keys_stream, (ctx, None)))
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error listing S3 objects: {}", e);
                    Some((stream::iter(vec![Err(e)]), (ctx, None)))
                }
            }
        },
    )
    .flatten()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Starting multimedia commons S3 to Meilisearch uploader");
    println!("Configuration:");
    println!("  S3 Bucket: {}", args.bucket);
    println!("  S3 Region: {}", args.region);
    println!("  S3 Prefix: {}", args.prefix);
    println!("  Meilisearch URL: {}", args.meilisearch_url);
    println!("  Max Downloads: {}", args.max_downloads);
    println!("  Max Uploads: {}", args.max_uploads);
    println!("  Batch Size: {}", args.batch_size);
    println!("  Dry Run: {}", args.dry_run);

    let ctx = Arc::new(AppContext::new(args)?);

    println!("Starting to stream image files from S3...");

    // Create the image keys stream
    let image_stream = image_keys_stream(ctx.clone());

    // Process images in streaming batches
    let mut batch_buffer = Vec::with_capacity(ctx.args.batch_size * 2);
    let mut total_found = 0u64;
    let mut batch_count = 0;
    let mut errors = 0;

    let mut stream = Box::pin(image_stream);

    while let Some(key_result) = stream.try_next().await? {
        batch_buffer.push(key_result);
        total_found += 1;

        // Update stats periodically
        if total_found % 1000 == 0 {
            let mut stats = ctx.stats.lock().await;
            stats.total_found = total_found;
            println!("Found {} image files so far...", total_found);
        }

        // Process batch when buffer is full
        if batch_buffer.len() >= ctx.args.batch_size * 2 {
            batch_count += 1;
            println!(
                "Processing batch {} ({} images)",
                batch_count,
                batch_buffer.len()
            );

            let keys_to_process = std::mem::take(&mut batch_buffer);
            let ctx_clone = ctx.clone();

            match tokio::spawn(async move { process_batch(ctx_clone, keys_to_process).await }).await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    eprintln!("Batch {} processing error: {}", batch_count, e);
                    errors += 1;
                }
                Err(e) => {
                    eprintln!("Batch {} task join error: {}", batch_count, e);
                    errors += 1;
                }
            }
        }
    }

    // Process remaining images in buffer
    if !batch_buffer.is_empty() {
        batch_count += 1;
        println!(
            "Processing final batch {} ({} images)",
            batch_count,
            batch_buffer.len()
        );

        let ctx_clone = ctx.clone();
        match tokio::spawn(async move { process_batch(ctx_clone, batch_buffer).await }).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("Final batch processing error: {}", e);
                errors += 1;
            }
            Err(e) => {
                eprintln!("Final batch task join error: {}", e);
                errors += 1;
            }
        }
    }

    // Update final stats
    {
        let mut stats = ctx.stats.lock().await;
        stats.total_found = total_found;
    }

    if total_found == 0 {
        println!("No image files found in S3 bucket");
        return Ok(());
    }

    println!(
        "Completed processing {} total images in {} batches",
        total_found, batch_count
    );

    // Print final statistics
    let stats = ctx.stats.lock().await;
    println!("\n=== Final Statistics ===");
    println!("Total images found: {}", stats.total_found);
    println!("Successfully processed: {}", stats.processed);
    println!("Monocolor filtered: {}", stats.monocolor_filtered);
    println!("Decode errors: {}", stats.decode_errors);
    println!("Download errors: {}", stats.download_errors);
    println!("Successfully uploaded: {}", stats.uploaded);

    if errors > 0 {
        println!("\nUpload process completed with {} batch errors", errors);
    } else {
        println!("\nUpload process completed successfully");
    }
    Ok(())
}
