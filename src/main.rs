use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;

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
const MAX_IMAGE_SIZE_BYTES: usize = 50 * 1024 * 1024; // 50MB per image
const MAX_BASE64_SIZE: usize = 100 * 1024 * 1024; // 100MB base64 limit

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
    #[arg(long)]
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

    // Log large images but don't skip them - let the batch uploader handle sizing
    if image_bytes.len() > MAX_IMAGE_SIZE_BYTES {
        println!(
            "Warning: Large image {} ({} bytes) - will be sent in separate batch",
            key,
            image_bytes.len()
        );
    }

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

    // Encode to base64 and validate
    let base64_data = general_purpose::STANDARD.encode(&image_bytes);

    // Log very large base64 but don't skip - let batch uploader handle it
    if base64_data.len() > MAX_BASE64_SIZE {
        println!(
            "Warning: Very large base64 for {} ({} bytes) - will be sent in separate batch",
            key,
            base64_data.len()
        );
    }

    // Validate base64 encoding is valid UTF-8 and contains only valid base64 characters
    if !base64_data.is_ascii()
        || !base64_data
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
    {
        eprintln!(
            "Warning: Base64 encoding produced invalid characters for {}",
            key
        );
        return Ok(None);
    }

    // Create and validate document
    let id = extract_id_from_key(&key);
    let url = build_public_url(&key, &ctx.args.bucket, &ctx.args.region);

    // Validate document fields don't contain problematic characters
    if id.chars().any(|c| c.is_control()) || url.chars().any(|c| c.is_control()) {
        eprintln!(
            "Warning: Document contains control characters, skipping: {}",
            key
        );
        return Ok(None);
    }

    let document = ImageDocument {
        id,
        base64: base64_data,
        url,
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

    // For single large documents, validate they can be serialized
    if documents.len() == 1 {
        let estimated_size = estimate_document_size(&documents[0]);
        if estimated_size > ctx.args.max_batch_bytes * 2 {
            // If document is more than 2x the batch limit, it might be too large for Meilisearch
            println!(
                "Warning: Document {} is extremely large ({} bytes estimated), attempting upload anyway",
                documents[0].id, estimated_size
            );
        }
    }

    // Validate and serialize JSON first to catch issues early
    let json_payload = match serde_json::to_string(&documents) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Failed to serialize documents to JSON: {}", e);
            eprintln!("Problematic batch had {} documents", documents.len());
            // Log first document for debugging
            if let Some(first_doc) = documents.first() {
                eprintln!("First document ID: {}", first_doc.id);
                eprintln!("First document base64 length: {}", first_doc.base64.len());
                eprintln!("First document URL: {}", first_doc.url);
            }
            return Err(anyhow::anyhow!("JSON serialization failed: {}", e));
        }
    };

    // Log payload size for monitoring
    let payload_size = json_payload.len();
    if documents.len() == 1 && payload_size > ctx.args.max_batch_bytes {
        println!(
            "Uploading oversized single document: {} bytes ({}x larger than batch limit)",
            payload_size,
            payload_size / ctx.args.max_batch_bytes
        );
    } else if payload_size > ctx.args.max_batch_bytes {
        println!(
            "Warning: Batch payload size {} exceeds configured max {} bytes",
            payload_size, ctx.args.max_batch_bytes
        );
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
            .body(json_payload.clone())
            .send()
            .await
            .context("Failed to upload batch to Meilisearch")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            eprintln!(
                "Meilisearch upload failed. Payload size: {} bytes",
                payload_size
            );
            eprintln!(
                "First 1000 chars of payload: {}",
                if json_payload.len() > 1000 {
                    &json_payload[..1000]
                } else {
                    &json_payload
                }
            );
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
    // More accurate estimate of JSON serialized size
    // JSON structure: {"id":"...","base64":"...","url":"..."}
    // Account for quotes, colons, commas, braces, and field names
    let field_names_overhead = "id".len() + "base64".len() + "url".len(); // 13 bytes
    let json_syntax_overhead = 2 + 6 + 4 + 2; // braces + quotes + colons + commas = 14 bytes
    let content_size = doc.id.len() + doc.base64.len() + doc.url.len();

    content_size + field_names_overhead + json_syntax_overhead + 50 // extra buffer for safety
}

async fn batch_uploader(
    ctx: Arc<AppContext>,
    mut document_receiver: tokio::sync::mpsc::Receiver<ImageDocument>,
) -> Result<()> {
    let mut current_batch = Vec::new();
    let mut current_size = 0;
    let mut batch_count = 0;

    while let Some(document) = document_receiver.recv().await {
        let doc_size = estimate_document_size(&document);

        // If this single document exceeds max batch size, send it alone
        if doc_size > ctx.args.max_batch_bytes {
            // Upload current batch first if it has documents
            if !current_batch.is_empty() {
                batch_count += 1;
                if let Err(e) =
                    upload_batch_to_meilisearch(&ctx, std::mem::take(&mut current_batch)).await
                {
                    eprintln!("Failed to upload batch {}: {}", batch_count, e);
                }
                current_size = 0;
            }

            // Send the large document alone
            batch_count += 1;
            println!(
                "Sending oversized document {} ({} bytes) in separate batch {}",
                document.id, doc_size, batch_count
            );
            if let Err(e) = upload_batch_to_meilisearch(&ctx, vec![document]).await {
                eprintln!(
                    "Failed to upload large document batch {}: {}",
                    batch_count, e
                );
            }
            continue;
        }

        // Check if adding this document would exceed limits
        if current_batch.len() >= ctx.args.batch_size
            || (current_size + doc_size) > ctx.args.max_batch_bytes && !current_batch.is_empty()
        {
            // Upload current batch
            batch_count += 1;
            if let Err(e) =
                upload_batch_to_meilisearch(&ctx, std::mem::take(&mut current_batch)).await
            {
                eprintln!("Failed to upload batch {}: {}", batch_count, e);
            }
            current_size = 0;
        }

        // Add document to current batch
        current_batch.push(document);
        current_size += doc_size;
    }

    // Upload remaining documents
    if !current_batch.is_empty() {
        batch_count += 1;
        if let Err(e) = upload_batch_to_meilisearch(&ctx, current_batch).await {
            eprintln!("Failed to upload final batch {}: {}", batch_count, e);
        }
    }

    println!("Uploaded {} batches total", batch_count);
    Ok(())
}

async fn image_processor(
    ctx: Arc<AppContext>,
    mut key_receiver: tokio::sync::mpsc::Receiver<String>,
    document_sender: tokio::sync::mpsc::Sender<ImageDocument>,
) -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();

    while let Some(key) = key_receiver.recv().await {
        let ctx = ctx.clone();
        let sender = document_sender.clone();

        // Limit concurrent processing
        while tasks.len() >= ctx.args.max_downloads {
            // Wait for at least one task to complete
            if let Some(result) = tasks.join_next().await {
                if let Err(e) = result {
                    eprintln!("Image processing task error: {}", e);
                }
            }
        }

        tasks.spawn(async move {
            match download_and_process_image(&ctx, key).await {
                Ok(Some(document)) => {
                    if sender.send(document).await.is_err() {
                        eprintln!("Failed to send document to uploader");
                    }
                }
                Ok(None) => {
                    // Image was filtered or failed processing
                }
                Err(e) => {
                    eprintln!("Error processing image: {}", e);
                }
            }
        });
    }

    // Wait for all remaining tasks to complete
    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Image processing task error: {}", e);
        }
    }

    Ok(())
}

async fn s3_lister(
    ctx: Arc<AppContext>,
    key_sender: tokio::sync::mpsc::Sender<String>,
) -> Result<()> {
    let mut continuation_token = None;
    let mut total_found = 0u64;

    loop {
        match list_s3_objects(&ctx, &ctx.args.prefix, continuation_token).await {
            Ok(response) => {
                let mut found_images = 0;

                for obj in response.contents {
                    if is_image_file(&obj.key) {
                        total_found += 1;
                        found_images += 1;

                        if key_sender.send(obj.key).await.is_err() {
                            println!("Key receiver closed, stopping S3 listing");
                            return Ok(());
                        }

                        // Update stats periodically
                        if total_found % 1000 == 0 {
                            let mut stats = ctx.stats.lock().await;
                            stats.total_found = total_found;
                            println!("Discovered {} image files so far...", total_found);
                        }
                    }
                }

                if found_images > 0 {
                    println!("Found {} images in current S3 page", found_images);
                }

                if !response.is_truncated {
                    break;
                }

                continuation_token = response.next_continuation_token;

                if continuation_token.is_none() {
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error listing S3 objects: {}", e);
                return Err(e);
            }
        }
    }

    // Update final stats
    {
        let mut stats = ctx.stats.lock().await;
        stats.total_found = total_found;
    }

    println!(
        "Finished listing S3 objects. Found {} total images",
        total_found
    );
    Ok(())
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

    println!("Starting concurrent S3 listing, processing, and upload pipeline...");

    // Create channels for the pipeline
    let (key_sender, key_receiver) = tokio::sync::mpsc::channel::<String>(1000);
    let (document_sender, document_receiver) = tokio::sync::mpsc::channel::<ImageDocument>(100);

    // Start the three concurrent tasks
    let s3_task = {
        let ctx = ctx.clone();
        tokio::spawn(async move { s3_lister(ctx, key_sender).await })
    };

    let processor_task = {
        let ctx = ctx.clone();
        tokio::spawn(async move { image_processor(ctx, key_receiver, document_sender).await })
    };

    let uploader_task = {
        let ctx = ctx.clone();
        tokio::spawn(async move { batch_uploader(ctx, document_receiver).await })
    };

    // Wait for all tasks to complete
    let (s3_result, processor_result, uploader_result) =
        tokio::try_join!(s3_task, processor_task, uploader_task)?;

    // Check for errors
    let mut errors = 0;
    if let Err(e) = s3_result {
        eprintln!("S3 listing error: {}", e);
        errors += 1;
    }
    if let Err(e) = processor_result {
        eprintln!("Image processing error: {}", e);
        errors += 1;
    }
    if let Err(e) = uploader_result {
        eprintln!("Upload error: {}", e);
        errors += 1;
    }

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
        println!("\nProcess completed with {} component errors", errors);
    } else {
        println!("\nProcess completed successfully");
    }

    if stats.total_found == 0 {
        println!("No image files found in S3 bucket");
    }

    Ok(())
}
