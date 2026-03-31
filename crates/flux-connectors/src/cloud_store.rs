// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cloud object store integration for file connectors.
//!
//! Detects cloud URL schemes (`s3://`, `gs://`, `az://`, `https://`) and builds
//! the appropriate [`ObjectStore`] with credentials resolved from environment
//! variables and per-connector `storage_options`.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use tracing::debug;
use url::Url;

/// The cloud storage backend for a URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudScheme {
    /// Amazon S3 or S3-compatible (MinIO, R2, DigitalOcean Spaces).
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
    /// HTTP/HTTPS (read-only).
    Https,
    /// Local filesystem (not a cloud URL).
    Local,
}

/// Parsed components of a cloud storage URL.
#[derive(Debug, Clone)]
pub struct CloudUrl {
    pub scheme: CloudScheme,
    /// Bucket (S3/GCS) or container (Azure) name.
    pub bucket: String,
    /// Object path within the bucket (no leading `/`).
    pub object_path: String,
    /// Base URL for registering with DataFusion (`s3://bucket`).
    pub base_url: Url,
}

/// Detect the cloud scheme from a path string.
pub fn detect_scheme(path: &str) -> CloudScheme {
    if path.starts_with("s3://") || path.starts_with("s3a://") {
        CloudScheme::S3
    } else if path.starts_with("gs://") {
        CloudScheme::Gcs
    } else if path.starts_with("az://") || path.starts_with("abfs://") {
        CloudScheme::Azure
    } else if path.starts_with("https://") || path.starts_with("http://") {
        CloudScheme::Https
    } else {
        CloudScheme::Local
    }
}

/// Returns `true` if the path is a cloud or HTTP URL (not a local path).
pub fn is_cloud_url(path: &str) -> bool {
    !matches!(detect_scheme(path), CloudScheme::Local)
}

/// Parse a cloud URL into its components.
pub fn parse_cloud_url(path: &str) -> Result<CloudUrl, Box<dyn std::error::Error + Send + Sync>> {
    let scheme = detect_scheme(path);
    if scheme == CloudScheme::Local {
        return Err("not a cloud URL".into());
    }

    let url = Url::parse(path).map_err(|e| format!("invalid cloud URL '{path}': {e}"))?;

    let bucket = url
        .host_str()
        .ok_or_else(|| format!("cloud URL '{path}' has no bucket/container"))?
        .to_string();

    // Strip leading `/` from URL path to get the object path.
    let object_path = url.path().trim_start_matches('/').to_string();

    // Base URL: scheme + bucket (no object path).
    let base_url = Url::parse(&format!("{}://{}", url.scheme(), bucket))
        .map_err(|e| format!("failed to build base URL: {e}"))?;

    Ok(CloudUrl {
        scheme,
        bucket,
        object_path,
        base_url,
    })
}

/// Build an [`ObjectStore`] for the given cloud URL and storage options.
///
/// Credentials are resolved in order:
/// 1. Explicit values in `storage_options`
/// 2. Standard environment variables (AWS_*, GOOGLE_*, AZURE_*)
/// 3. Anonymous/unsigned access (when `aws_skip_signature=true`, etc.)
pub fn build_object_store(
    cloud_url: &CloudUrl,
    storage_options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    match cloud_url.scheme {
        CloudScheme::S3 => build_s3_store(&cloud_url.bucket, storage_options),
        CloudScheme::Gcs => build_gcs_store(&cloud_url.bucket, storage_options),
        CloudScheme::Azure => build_azure_store(&cloud_url.bucket, storage_options),
        CloudScheme::Https => build_http_store(&cloud_url.base_url, storage_options),
        CloudScheme::Local => unreachable!("build_object_store called with local path"),
    }
}

/// Register a cloud object store on a DataFusion [`SessionContext`].
///
/// Does nothing for local paths. For cloud URLs, builds the appropriate store
/// and registers it so DataFusion can read/write through it.
pub fn register_cloud_store(
    ctx: &datafusion::prelude::SessionContext,
    path: &str,
    storage_options: &HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !is_cloud_url(path) {
        return Ok(());
    }
    let cloud_url = parse_cloud_url(path)?;
    let store = build_object_store(&cloud_url, storage_options)?;
    debug!(
        scheme = ?cloud_url.scheme,
        bucket = %cloud_url.bucket,
        "registering cloud object store"
    );
    ctx.register_object_store(&cloud_url.base_url, store);
    Ok(())
}

// ---------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------

fn build_s3_store(
    bucket: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

    // Explicit storage_options override environment variables.
    if let Some(v) = options.get("aws_access_key_id") {
        builder = builder.with_access_key_id(v);
    }
    if let Some(v) = options.get("aws_secret_access_key") {
        builder = builder.with_secret_access_key(v);
    }
    if let Some(v) = options.get("aws_session_token") {
        builder = builder.with_token(v);
    }
    if let Some(v) = options.get("aws_region") {
        builder = builder.with_region(v);
    }
    // Custom endpoint for S3-compatible services (MinIO, R2, etc.).
    if let Some(v) = options.get("aws_endpoint") {
        builder = builder.with_endpoint(v);
    }
    if options
        .get("aws_allow_http")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        builder = builder.with_allow_http(true);
    }
    // Anonymous access for public buckets.
    if options
        .get("aws_skip_signature")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        builder = builder.with_skip_signature(true);
    }

    Ok(Arc::new(builder.build()?))
}

// ---------------------------------------------------------------------------
// GCS
// ---------------------------------------------------------------------------

fn build_gcs_store(
    bucket: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);

    // Service account JSON key (inline or path).
    if let Some(v) = options.get("google_service_account_key") {
        builder = builder.with_service_account_key(v);
    }
    if let Some(v) = options.get("google_application_credentials") {
        builder = builder.with_application_credentials(v);
    }

    Ok(Arc::new(builder.build()?))
}

// ---------------------------------------------------------------------------
// Azure
// ---------------------------------------------------------------------------

fn build_azure_store(
    container: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container);

    if let Some(v) = options.get("azure_storage_account_name") {
        builder = builder.with_account(v);
    }
    if let Some(v) = options.get("azure_storage_account_key") {
        builder = builder.with_access_key(v);
    }

    Ok(Arc::new(builder.build()?))
}

// ---------------------------------------------------------------------------
// HTTP/HTTPS
// ---------------------------------------------------------------------------

fn build_http_store(
    base_url: &Url,
    _options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let store = HttpBuilder::new().with_url(base_url.as_str()).build()?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_s3_scheme() {
        assert_eq!(detect_scheme("s3://my-bucket/path"), CloudScheme::S3);
        assert_eq!(detect_scheme("s3a://my-bucket/path"), CloudScheme::S3);
    }

    #[test]
    fn detect_gcs_scheme() {
        assert_eq!(detect_scheme("gs://my-bucket/path"), CloudScheme::Gcs);
    }

    #[test]
    fn detect_azure_scheme() {
        assert_eq!(detect_scheme("az://container/path"), CloudScheme::Azure);
        assert_eq!(detect_scheme("abfs://container/path"), CloudScheme::Azure);
    }

    #[test]
    fn detect_https_scheme() {
        assert_eq!(
            detect_scheme("https://example.com/file.csv"),
            CloudScheme::Https
        );
        assert_eq!(
            detect_scheme("http://localhost:9000/bucket/file"),
            CloudScheme::Https
        );
    }

    #[test]
    fn detect_local_scheme() {
        assert_eq!(detect_scheme("/data/file.csv"), CloudScheme::Local);
        assert_eq!(detect_scheme("./relative/path.csv"), CloudScheme::Local);
        assert_eq!(detect_scheme("data/file.csv"), CloudScheme::Local);
    }

    #[test]
    fn is_cloud_url_works() {
        assert!(is_cloud_url("s3://bucket/key"));
        assert!(is_cloud_url("gs://bucket/key"));
        assert!(is_cloud_url("az://container/blob"));
        assert!(is_cloud_url("https://example.com/data"));
        assert!(!is_cloud_url("/local/path"));
        assert!(!is_cloud_url("relative/path"));
    }

    #[test]
    fn parse_s3_url() {
        let url = parse_cloud_url("s3://my-bucket/path/to/file.csv").unwrap();
        assert_eq!(url.scheme, CloudScheme::S3);
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.object_path, "path/to/file.csv");
        assert_eq!(url.base_url.as_str(), "s3://my-bucket");
    }

    #[test]
    fn parse_gcs_url() {
        let url = parse_cloud_url("gs://my-bucket/data/output.parquet").unwrap();
        assert_eq!(url.scheme, CloudScheme::Gcs);
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.object_path, "data/output.parquet");
    }

    #[test]
    fn parse_azure_url() {
        let url = parse_cloud_url("az://my-container/blob/path.csv").unwrap();
        assert_eq!(url.scheme, CloudScheme::Azure);
        assert_eq!(url.bucket, "my-container");
        assert_eq!(url.object_path, "blob/path.csv");
    }

    #[test]
    fn parse_local_url_is_error() {
        assert!(parse_cloud_url("/local/path").is_err());
    }

    #[test]
    fn parse_s3_url_with_glob() {
        let url = parse_cloud_url("s3://my-bucket/data/*.parquet").unwrap();
        assert_eq!(url.scheme, CloudScheme::S3);
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.object_path, "data/*.parquet");
        assert_eq!(url.base_url.as_str(), "s3://my-bucket");
    }

    #[test]
    fn parse_s3_url_with_partitioned_glob() {
        let url =
            parse_cloud_url("s3://my-bucket/data/year=2026/month=03/*.parquet").unwrap();
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.object_path, "data/year=2026/month=03/*.parquet");
        assert_eq!(url.base_url.as_str(), "s3://my-bucket");
    }

    #[test]
    fn parse_gs_url_with_glob() {
        let url = parse_cloud_url("gs://my-bucket/prefix/*.csv").unwrap();
        assert_eq!(url.scheme, CloudScheme::Gcs);
        assert_eq!(url.object_path, "prefix/*.csv");
    }
}
