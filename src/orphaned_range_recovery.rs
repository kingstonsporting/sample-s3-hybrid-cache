//! Orphaned Range Recovery Module
//!
//! Provides detection and recovery of orphaned range files in the shared cache system.
//! Orphaned ranges are range files that exist but have no corresponding metadata entries.

use crate::cache_types::{NewCacheMetadata, RangeSpec};
use crate::compression::CompressionAlgorithm;
use crate::hybrid_metadata_writer::HybridMetadataWriter;
use crate::journal_manager::range_file_version;
use crate::path_safety::is_safe_relative_file_path;
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Orphaned range recovery manager
pub struct OrphanedRangeRecovery {
    cache_dir: PathBuf,
    metadata_writer: Arc<Mutex<HybridMetadataWriter>>,
    metrics: Arc<RecoveryMetrics>,
    max_metadata_file_bytes: u64,
}

/// Metrics for orphaned range recovery operations
#[derive(Debug, Default)]
pub struct RecoveryMetrics {
    pub orphans_detected: std::sync::atomic::AtomicU64,
    pub orphans_recovered: std::sync::atomic::AtomicU64,
    pub orphans_cleaned: std::sync::atomic::AtomicU64,
    pub bytes_recovered: std::sync::atomic::AtomicU64,
    pub bytes_cleaned: std::sync::atomic::AtomicU64,
    pub recovery_failures: std::sync::atomic::AtomicU64,
}

/// Represents an orphaned range file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrphanedRange {
    pub cache_key: String,
    pub range_file_path: PathBuf,
    pub range_spec: Option<RangeSpec>,
    pub validation_status: ValidationStatus,
    pub access_frequency: u64,
    pub last_accessed: Option<SystemTime>,
    pub file_size: u64,
}

/// Validation status of an orphaned range
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationStatus {
    Valid,
    Invalid,
    Corrupted,
    Unknown,
}

/// Result of recovery operation
#[derive(Debug, Clone)]
pub enum RecoveryResult {
    Recovered(RangeSpec),
    Cleaned(u64), // bytes freed
    Failed(String),
}

impl OrphanedRangeRecovery {
    /// Create a new orphaned range recovery manager
    pub fn new(cache_dir: PathBuf, metadata_writer: Arc<Mutex<HybridMetadataWriter>>) -> Self {
        Self {
            cache_dir,
            metadata_writer,
            metrics: Arc::new(RecoveryMetrics::default()),
            max_metadata_file_bytes: crate::config::CacheConfig::default().max_metadata_file_bytes,
        }
    }

    pub fn with_max_metadata_file_bytes(mut self, limit: u64) -> Self {
        self.max_metadata_file_bytes = limit;
        self
    }

    /// Get recovery metrics
    pub fn get_metrics(&self) -> Arc<RecoveryMetrics> {
        self.metrics.clone()
    }

    /// Scan for orphaned ranges for a specific cache key
    pub async fn scan_for_orphans(&self, cache_key: &str) -> Result<Vec<OrphanedRange>> {
        debug!("Scanning for orphaned ranges for cache key: {}", cache_key);

        // Parse cache key to get bucket and object
        let (bucket, object_key) = self.parse_cache_key(cache_key)?;

        // Get the sharded path for this cache key
        let shard_path = self.get_shard_path(&bucket, &object_key);

        // Get metadata file path
        let metadata_path = crate::disk_cache::get_sharded_path(
            &self.cache_dir.join("metadata"),
            cache_key,
            ".meta",
        )?;

        // Load existing metadata if it exists
        let existing_metadata = if metadata_path.exists() {
            match self.load_metadata(&metadata_path).await {
                Ok(metadata) => Some(metadata),
                Err(e) => {
                    warn!("Failed to load metadata for {}: {}", cache_key, e);
                    None
                }
            }
        } else {
            None
        };

        // Get range files directory
        let ranges_dir = self.cache_dir.join("ranges").join(&shard_path);

        if !ranges_dir.exists() {
            return Ok(Vec::new());
        }

        // Scan for range files
        let mut orphaned_ranges = Vec::new();
        let range_prefix = format!("{}_", self.sanitize_key(&object_key));
        let hashed_prefix = format!("{}_", blake3::hash(cache_key.as_bytes()).to_hex());

        for entry in std::fs::read_dir(&ranges_dir)? {
            let entry = entry?;
            let file_path = entry.path();

            if !file_path.is_file() {
                continue;
            }

            let file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Check if this is a range file for our object
            if !(file_name.starts_with(&range_prefix) || file_name.starts_with(&hashed_prefix))
                || !file_name.ends_with(".bin")
            {
                continue;
            }

            // Parse range from filename
            let range_spec = match self.parse_range_file(&file_path, cache_key) {
                Ok(spec) => spec,
                Err(e) => {
                    warn!("Failed to parse range from filename {}: {}", file_name, e);
                    continue;
                }
            };

            // Check if this range is referenced in metadata
            let is_orphaned = match &existing_metadata {
                Some(metadata) => !metadata
                    .ranges
                    .iter()
                    .any(|r| r.file_path == range_spec.file_path),
                None => true, // No metadata means all ranges are orphaned
            };

            if is_orphaned {
                let file_size = entry.metadata()?.len();
                let access_frequency = self.estimate_access_frequency(&file_path).await;
                let last_accessed = self.get_last_accessed(&file_path).await;

                let validation_status = self
                    .validate_orphan_version(&file_path, &range_spec, existing_metadata.as_ref())
                    .await;

                let orphaned_range = OrphanedRange {
                    cache_key: cache_key.to_string(),
                    range_file_path: file_path,
                    range_spec: Some(range_spec),
                    validation_status,
                    access_frequency,
                    last_accessed,
                    file_size,
                };

                orphaned_ranges.push(orphaned_range);
                self.metrics
                    .orphans_detected
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Sort by priority (access frequency descending, then by file size descending)
        orphaned_ranges.sort_by(|a, b| {
            b.access_frequency
                .cmp(&a.access_frequency)
                .then_with(|| b.file_size.cmp(&a.file_size))
        });

        info!(
            "Found {} orphaned ranges for cache key: {}",
            orphaned_ranges.len(),
            cache_key
        );
        Ok(orphaned_ranges)
    }

    /// Recover an orphaned range by adding it to metadata
    pub async fn recover_orphan(&self, orphan: &OrphanedRange) -> Result<RecoveryResult> {
        debug!(
            "Attempting to recover orphaned range: {:?}",
            orphan.range_file_path
        );

        match orphan.validation_status {
            ValidationStatus::Valid => {
                // Attempt to add the range to metadata
                match self.add_range_to_metadata(orphan).await {
                    Ok(range_spec) => {
                        info!(
                            "Successfully recovered orphaned range: {:?}",
                            orphan.range_file_path
                        );
                        self.metrics
                            .orphans_recovered
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.metrics
                            .bytes_recovered
                            .fetch_add(orphan.file_size, std::sync::atomic::Ordering::Relaxed);
                        Ok(RecoveryResult::Recovered(range_spec))
                    }
                    Err(e) => {
                        error!(
                            "Failed to recover orphaned range {:?}: {}",
                            orphan.range_file_path, e
                        );
                        self.metrics
                            .recovery_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(RecoveryResult::Failed(e.to_string()))
                    }
                }
            }
            ValidationStatus::Invalid | ValidationStatus::Corrupted => {
                // Clean up invalid/corrupted orphaned range
                match self.cleanup_invalid_orphan(orphan).await {
                    Ok(bytes_freed) => Ok(RecoveryResult::Cleaned(bytes_freed)),
                    Err(e) => Ok(RecoveryResult::Failed(e.to_string())),
                }
            }
            ValidationStatus::Unknown => {
                warn!(
                    "Cannot recover orphaned range with unknown validation status: {:?}",
                    orphan.range_file_path
                );
                Ok(RecoveryResult::Failed(
                    "Unknown validation status".to_string(),
                ))
            }
        }
    }

    /// Clean up an invalid or corrupted orphaned range
    pub async fn cleanup_invalid_orphan(&self, orphan: &OrphanedRange) -> Result<u64> {
        debug!(
            "Cleaning up invalid orphaned range: {:?}",
            orphan.range_file_path
        );

        let relative_path = self.relative_range_path(&orphan.range_file_path)?;
        let writer = self.metadata_writer.lock().await.clone();
        let _lock = writer.acquire_metadata_lock(&orphan.cache_key).await?;
        if writer
            .has_pending_range_file(&orphan.cache_key, &relative_path)
            .await?
        {
            // Consolidation owns journaled bytes and their size debit.
            return Ok(0);
        }
        let metadata = self.load_metadata_for_cache_key(&orphan.cache_key).await;
        if metadata.as_ref().is_some_and(|metadata| {
            metadata
                .ranges
                .iter()
                .any(|range| range.file_path == relative_path)
        }) {
            return Ok(0);
        }
        if let Some(range) = &orphan.range_spec {
            if range_file_version(&relative_path).is_some()
                && matches!(
                    self.validate_orphan_version(&orphan.range_file_path, range, metadata.as_ref())
                        .await,
                    ValidationStatus::Valid | ValidationStatus::Unknown
                )
            {
                // Identity changed since scanning, or publication is still pending.
                // The next scan can recover this version instead of deleting it.
                return Ok(0);
            }
        }
        let file_size = orphan.file_size;

        match std::fs::remove_file(&orphan.range_file_path) {
            Ok(()) => {
                info!(
                    "Successfully cleaned up orphaned range: {:?}",
                    orphan.range_file_path
                );
                self.metrics
                    .orphans_cleaned
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .bytes_cleaned
                    .fetch_add(file_size, std::sync::atomic::Ordering::Relaxed);
                Ok(file_size)
            }
            Err(e) => {
                error!(
                    "Failed to clean up orphaned range {:?}: {}",
                    orphan.range_file_path, e
                );
                self.metrics
                    .recovery_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(ProxyError::CacheError(format!(
                    "Failed to clean up orphaned range: {}",
                    e
                )))
            }
        }
    }
    /// Scan a single shard for orphaned ranges (scalable approach for large caches)
    ///
    /// Instead of scanning all files at once, this scans one shard (XX/YYY directory)
    /// at a time. Call this repeatedly with different shards to cover the entire cache.
    ///
    /// # Arguments
    /// * `bucket` - Bucket name to scan within
    /// * `shard_prefix` - Two-character hex prefix (e.g., "a3") for the shard
    /// * `timeout` - Maximum time to spend scanning this shard
    /// * `max_orphans` - Maximum orphans to return (for bounded processing)
    ///
    /// # Returns
    /// HashMap of cache_key -> Vec<OrphanedRange> for orphans found in this shard
    pub async fn scan_shard_for_orphans(
        &self,
        bucket: &str,
        shard_prefix: &str,
        timeout: Duration,
        max_orphans: usize,
    ) -> Result<HashMap<String, Vec<OrphanedRange>>> {
        let start_time = std::time::Instant::now();
        let mut all_orphans = HashMap::new();
        let mut total_orphans_found = 0;

        // Construct shard directory path: ranges/bucket/XX/
        let shard_dir = self
            .cache_dir
            .join("ranges")
            .join(bucket)
            .join(shard_prefix);

        if !shard_dir.exists() {
            debug!("Shard directory does not exist: {:?}", shard_dir);
            return Ok(all_orphans);
        }

        debug!("Scanning shard for orphans: {:?}", shard_dir);

        // Iterate through sub-shards (YYY directories)
        let sub_shard_entries = match std::fs::read_dir(&shard_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read shard directory {:?}: {}", shard_dir, e);
                return Ok(all_orphans);
            }
        };

        for sub_shard_entry in sub_shard_entries {
            // Check timeout
            if start_time.elapsed() > timeout {
                info!(
                    "Shard scan timeout reached after {:?}, processed {} orphans",
                    start_time.elapsed(),
                    total_orphans_found
                );
                break;
            }

            // Check max orphans limit
            if total_orphans_found >= max_orphans {
                info!(
                    "Max orphans limit ({}) reached, stopping shard scan",
                    max_orphans
                );
                break;
            }

            let sub_shard_entry = match sub_shard_entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let sub_shard_path = sub_shard_entry.path();
            if !sub_shard_path.is_dir() {
                continue;
            }

            // Scan .bin files in this sub-shard
            let bin_files = match std::fs::read_dir(&sub_shard_path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            // Hash-shortened names cannot recover their key from the filename.
            // Metadata in this shard supplies the original key when available.
            let metadata_dir = self.cache_dir.join("metadata").join(
                sub_shard_path
                    .strip_prefix(self.cache_dir.join("ranges"))
                    .map_err(|_| ProxyError::CacheError("Invalid shard path".to_string()))?,
            );
            let mut hashed_keys = HashMap::new();
            if let Ok(entries) = std::fs::read_dir(&metadata_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path
                        .extension()
                        .is_some_and(|extension| extension == "meta")
                    {
                        if let Ok(metadata) = self.load_metadata(&path).await {
                            if let Ok((_, object_key)) = self.parse_cache_key(&metadata.cache_key) {
                                hashed_keys.insert(
                                    blake3::hash(object_key.as_bytes()).to_hex().to_string(),
                                    metadata.cache_key.clone(),
                                );
                                hashed_keys.insert(
                                    blake3::hash(metadata.cache_key.as_bytes())
                                        .to_hex()
                                        .to_string(),
                                    metadata.cache_key,
                                );
                            }
                        }
                    }
                }
            }

            // Group files by cache key to batch metadata lookups
            let mut files_by_cache_key: HashMap<String, Vec<std::path::PathBuf>> = HashMap::new();

            for file_entry in bin_files {
                let file_entry = match file_entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let file_path = file_entry.path();
                if !file_path.is_file() {
                    continue;
                }

                let file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                    Some(name) if name.ends_with(".bin") => name,
                    _ => continue,
                };

                // Extract cache key from filename
                let object_name = match self.extract_object_key_from_filename(file_name) {
                    Ok(name) => name,
                    Err(_) => continue,
                };
                let cache_key = match hashed_keys.get(&object_name) {
                    Some(key) => key.clone(),
                    None => match self.extract_cache_key_from_path(&file_path) {
                        Ok(key) => key,
                        Err(_) => continue,
                    },
                };

                files_by_cache_key
                    .entry(cache_key)
                    .or_default()
                    .push(file_path);
            }

            // Process each cache key
            for (cache_key, bin_files) in files_by_cache_key {
                if total_orphans_found >= max_orphans {
                    break;
                }

                // Load metadata once per cache key
                let metadata = self.load_metadata_for_cache_key(&cache_key).await;

                for bin_file_path in bin_files {
                    if total_orphans_found >= max_orphans {
                        break;
                    }

                    let range_spec = match self.parse_range_file(&bin_file_path, &cache_key) {
                        Ok(spec) => spec,
                        Err(_) => continue,
                    };

                    // Check if orphaned
                    let is_orphaned = match &metadata {
                        Some(meta) => !meta
                            .ranges
                            .iter()
                            .any(|r| r.file_path == range_spec.file_path),
                        None => true,
                    };

                    if is_orphaned {
                        let file_size = std::fs::metadata(&bin_file_path)
                            .map(|m| m.len())
                            .unwrap_or(0);

                        let validation_status = self
                            .validate_orphan_version(&bin_file_path, &range_spec, metadata.as_ref())
                            .await;
                        let access_frequency = self.estimate_access_frequency(&bin_file_path).await;
                        let last_accessed = self.get_last_accessed(&bin_file_path).await;

                        let orphan = OrphanedRange {
                            cache_key: cache_key.clone(),
                            range_file_path: bin_file_path,
                            range_spec: Some(range_spec),
                            validation_status,
                            access_frequency,
                            last_accessed,
                            file_size,
                        };

                        all_orphans
                            .entry(cache_key.clone())
                            .or_insert_with(Vec::new)
                            .push(orphan);

                        total_orphans_found += 1;
                        self.metrics
                            .orphans_detected
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        if total_orphans_found > 0 {
            info!(
                "Shard scan complete: bucket={}, shard={}, orphans_found={}, duration={:?}",
                bucket,
                shard_prefix,
                total_orphans_found,
                start_time.elapsed()
            );
        }

        Ok(all_orphans)
    }

    /// Get list of all shards (XX prefixes) for a bucket
    ///
    /// Returns a list of two-character hex prefixes that exist in the ranges directory.
    /// Use this to iterate through shards for incremental scanning.
    pub fn list_shards(&self, bucket: &str) -> Result<Vec<String>> {
        let bucket_dir = self.cache_dir.join("ranges").join(bucket);

        if !bucket_dir.exists() {
            return Ok(Vec::new());
        }

        let mut shards = Vec::new();

        for entry in std::fs::read_dir(&bucket_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Validate it's a 2-character hex prefix
                    if name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                        shards.push(name.to_string());
                    }
                }
            }
        }

        // Sort for deterministic iteration
        shards.sort();

        Ok(shards)
    }

    /// Get list of all buckets in the cache
    pub fn list_buckets(&self) -> Result<Vec<String>> {
        let ranges_dir = self.cache_dir.join("ranges");

        if !ranges_dir.exists() {
            return Ok(Vec::new());
        }

        let mut buckets = Vec::new();

        for entry in std::fs::read_dir(&ranges_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    buckets.push(name.to_string());
                }
            }
        }

        buckets.sort();
        Ok(buckets)
    }

    /// Load metadata for a cache key (helper for shard scanning)
    async fn load_metadata_for_cache_key(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        let metadata_path = crate::disk_cache::get_sharded_path(
            &self.cache_dir.join("metadata"),
            cache_key,
            ".meta",
        )
        .ok()?;

        if !metadata_path.exists() {
            return None;
        }

        match self.load_metadata(&metadata_path).await {
            Ok(metadata) => Some(metadata),
            Err(e) => {
                debug!("Failed to load metadata for {}: {}", cache_key, e);
                None
            }
        }
    }
    // Private helper methods

    /// Parse cache key into bucket and object key
    fn parse_cache_key(&self, cache_key: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = cache_key.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(ProxyError::CacheError(format!(
                "Invalid cache key format: {}",
                cache_key
            )));
        }
        Ok((parts[0].to_string(), parts[1].to_string()))
    }

    /// Get sharded path for bucket and object key
    fn get_shard_path(&self, bucket: &str, object_key: &str) -> String {
        let hash = blake3::hash(object_key.as_bytes());
        let hash_hex = hash.to_hex();

        format!("{}/{}/{}", bucket, &hash_hex[0..2], &hash_hex[2..5])
    }

    /// Sanitize object key for filename use
    fn sanitize_key(&self, key: &str) -> String {
        crate::disk_cache::sanitize_object_key_for_filename(key, "")
    }

    /// Load metadata from file
    async fn load_metadata(&self, metadata_path: &Path) -> Result<NewCacheMetadata> {
        let path = metadata_path.to_path_buf();
        let limit = self.max_metadata_file_bytes;
        let outcome = tokio::task::spawn_blocking(move || {
            crate::disk_cache::read_and_parse_metadata_blocking(&path, limit)
        })
        .await
        .map_err(|error| ProxyError::CacheError(format!("Metadata read failed: {}", error)))?;
        match outcome {
            crate::disk_cache::MetadataReadOutcome::Parsed(metadata) => Ok(*metadata),
            _ => Err(ProxyError::CacheError(
                "Cannot establish object identity from unreadable metadata".to_string(),
            )),
        }
    }

    /// Parse range specification from filename
    fn parse_range_from_filename(&self, filename: &str, object_key: &str) -> Result<RangeSpec> {
        let sanitized_key = self.sanitize_key(object_key);
        let prefix = format!("{}_", sanitized_key);
        let filename_stem = Self::range_filename_stem(filename)?;

        if !filename_stem.starts_with(&prefix) {
            return Err(ProxyError::CacheError(format!(
                "Invalid range filename format: {}",
                filename
            )));
        }

        let range_part = &filename_stem[prefix.len()..];
        let parts: Vec<&str> = range_part.split('-').collect();

        if parts.len() != 2 {
            return Err(ProxyError::CacheError(format!(
                "Invalid range format in filename: {}",
                filename
            )));
        }

        let start: u64 = parts[0]
            .parse()
            .map_err(|_| ProxyError::CacheError(format!("Invalid start range: {}", parts[0])))?;
        let end: u64 = parts[1]
            .parse()
            .map_err(|_| ProxyError::CacheError(format!("Invalid end range: {}", parts[1])))?;

        if start > end {
            return Err(ProxyError::CacheError(format!(
                "Invalid range: start {} > end {}",
                start, end
            )));
        }

        // Create a basic RangeSpec - we'll fill in more details during validation.
        //
        // `staged: None` is the correct answer here rather than an omission: this range
        // is reconstructed from a `.bin` filename found on disk, so its tier is
        // genuinely unknown. `None` routes it to the object-flag fallback, which is the
        // same answer this sweep produced before per-range membership existed.
        // Spec: write-cache-accounting-and-eviction. Requirements: 12.1
        let now = SystemTime::now();

        Ok(RangeSpec {
            start,
            end,
            file_path: filename.to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4, // All data uses frame format
            compressed_size: 0, // Will be determined during validation
            uncompressed_size: end - start + 1,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
        })
    }

    fn range_filename_stem(filename: &str) -> Result<&str> {
        let stem = filename
            .strip_suffix(".bin")
            .ok_or_else(|| ProxyError::CacheError("Invalid range filename".to_string()))?;
        Ok(match range_file_version(filename) {
            Some(version) => &stem[..stem.len() - version.len() - 1],
            None => stem,
        })
    }

    fn relative_range_path(&self, file_path: &Path) -> Result<String> {
        let relative = file_path
            .strip_prefix(self.cache_dir.join("ranges"))
            .map_err(|_| ProxyError::CacheError("Invalid range file path".to_string()))?
            .to_str()
            .ok_or_else(|| ProxyError::CacheError("Invalid range filename".to_string()))?;
        if !is_safe_relative_file_path(relative) {
            return Err(ProxyError::CacheError("Unsafe range file path".to_string()));
        }
        Ok(relative.to_string())
    }

    fn parse_range_file(&self, file_path: &Path, cache_key: &str) -> Result<RangeSpec> {
        let filename = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ProxyError::CacheError("Invalid range filename".to_string()))?;
        let (_, object_key) = self.parse_cache_key(cache_key)?;
        let hashed_key = blake3::hash(cache_key.as_bytes()).to_hex().to_string();
        let object_name =
            if Self::range_filename_stem(filename)?.starts_with(&format!("{}_", hashed_key)) {
                hashed_key.as_str()
            } else {
                object_key.as_str()
            };
        let mut range = self.parse_range_from_filename(filename, object_name)?;
        range.file_path = self.relative_range_path(file_path)?;
        Ok(range)
    }

    async fn validate_orphan_version(
        &self,
        file_path: &Path,
        range_spec: &RangeSpec,
        metadata: Option<&NewCacheMetadata>,
    ) -> ValidationStatus {
        // Bytes without a version in their name can never be served again.
        let Some(version) = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(range_file_version)
        else {
            return ValidationStatus::Invalid;
        };
        let Some(object) = metadata
            .map(|metadata| &metadata.object_metadata)
            .filter(|object| !object.etag.is_empty())
        else {
            return ValidationStatus::Unknown;
        };
        if !version.eq_ignore_ascii_case(&crate::disk_cache::object_version_id(&object.etag)) {
            return ValidationStatus::Invalid;
        }
        self.validate_range_file(file_path, range_spec).await
    }

    /// Validate a range file
    async fn validate_range_file(
        &self,
        file_path: &Path,
        range_spec: &RangeSpec,
    ) -> ValidationStatus {
        // Check if file exists and is readable
        if !file_path.exists() {
            return ValidationStatus::Invalid;
        }

        // Check file size
        let file_size = match std::fs::metadata(file_path) {
            Ok(metadata) => metadata.len(),
            Err(_) => return ValidationStatus::Invalid,
        };

        if file_size == 0 {
            return ValidationStatus::Invalid;
        }

        // Basic validation: file size should be reasonable for the range
        let expected_min_size = (range_spec.end - range_spec.start + 1) / 10; // Allow 10:1 compression
        let expected_max_size = (range_spec.end - range_spec.start + 1) * 2; // Allow some overhead

        if file_size < expected_min_size || file_size > expected_max_size {
            warn!(
                "Range file size {} is outside expected range [{}, {}] for range {}-{}",
                file_size, expected_min_size, expected_max_size, range_spec.start, range_spec.end
            );
            return ValidationStatus::Invalid;
        }

        // Try to read first few bytes to check if file is corrupted
        match std::fs::File::open(file_path) {
            Ok(mut file) => {
                use std::io::Read;
                let mut buffer = [0u8; 1024];
                match file.read(&mut buffer) {
                    Ok(bytes_read) if bytes_read > 0 => ValidationStatus::Valid,
                    Ok(_) => ValidationStatus::Corrupted, // Empty file
                    Err(_) => ValidationStatus::Corrupted,
                }
            }
            Err(_) => ValidationStatus::Invalid,
        }
    }

    /// Estimate access frequency based on file metadata
    async fn estimate_access_frequency(&self, file_path: &Path) -> u64 {
        // Simple heuristic: use file access time if available
        if let Ok(metadata) = std::fs::metadata(file_path) {
            if let Ok(accessed) = metadata.accessed() {
                if let Ok(duration) = SystemTime::now().duration_since(accessed) {
                    // More recent access = higher frequency score
                    let days_since_access = duration.as_secs() / 86400;
                    return (100_u64).saturating_sub(days_since_access);
                }
            }
        }

        // Default frequency
        1
    }

    /// Get last accessed time for a file
    async fn get_last_accessed(&self, file_path: &Path) -> Option<SystemTime> {
        std::fs::metadata(file_path)
            .ok()
            .and_then(|metadata| metadata.accessed().ok())
    }

    /// Add orphaned range to metadata
    async fn add_range_to_metadata(&self, orphan: &OrphanedRange) -> Result<RangeSpec> {
        let range_spec = orphan
            .range_spec
            .as_ref()
            .ok_or_else(|| ProxyError::CacheError("No range spec for orphan".to_string()))?;

        // Update range spec with actual file information
        let file_size = orphan.file_size;
        let mut updated_range_spec = range_spec.clone();
        updated_range_spec.compressed_size = file_size;
        updated_range_spec.file_path = self.relative_range_path(&orphan.range_file_path)?;

        // Determine compression algorithm from file content
        updated_range_spec.compression_algorithm =
            self.detect_compression(&orphan.range_file_path).await;

        // Only a matching filename digest proves these bytes share the current
        // metadata's identity. Legacy bytes continue to carry no identity.
        let object_metadata = self
            .load_metadata_for_cache_key(&orphan.cache_key)
            .await
            .filter(|metadata| {
                !metadata.object_metadata.etag.is_empty()
                    && range_file_version(&updated_range_spec.file_path).is_some_and(|version| {
                        version.eq_ignore_ascii_case(
                            blake3::hash(metadata.object_metadata.etag.as_bytes())
                                .to_hex()
                                .as_str(),
                        )
                    })
            })
            .map(|metadata| metadata.object_metadata);
        let mut writer = self.metadata_writer.lock().await.clone();
        match writer
            .write_range_metadata(
                &orphan.cache_key,
                updated_range_spec.clone(),
                crate::hybrid_metadata_writer::WriteMode::Hybrid,
                object_metadata,
                std::time::Duration::ZERO, // Force revalidation since original TTL is unknown
            )
            .await
        {
            Ok(()) => Ok(updated_range_spec),
            Err(e) => Err(e),
        }
    }

    /// Detect compression algorithm from file content
    async fn detect_compression(&self, file_path: &Path) -> CompressionAlgorithm {
        // Read first few bytes to detect compression
        if let Ok(mut file) = std::fs::File::open(file_path) {
            use std::io::Read;
            let mut buffer = [0u8; 16];
            if let Ok(bytes_read) = file.read(&mut buffer) {
                if bytes_read >= 4 {
                    // Check for LZ4 magic number
                    if &buffer[0..4] == b"\x04\"M\x18" {
                        return CompressionAlgorithm::Lz4;
                    }
                }
            }
        }

        CompressionAlgorithm::Lz4
    }

    /// Extract cache key from file path
    fn extract_cache_key_from_path(&self, file_path: &Path) -> Result<String> {
        // Path structure: cache_dir/ranges/bucket/XX/YYY/object_start-end.bin
        let ranges_dir = self.cache_dir.join("ranges");
        let relative_path = file_path
            .strip_prefix(&ranges_dir)
            .map_err(|_| ProxyError::CacheError("Invalid range file path".to_string()))?;

        let components: Vec<&std::ffi::OsStr> = relative_path.iter().collect();
        if components.len() < 4 {
            return Err(ProxyError::CacheError(
                "Invalid range file path structure".to_string(),
            ));
        }

        // Extract bucket from first component
        let bucket = components[0]
            .to_str()
            .ok_or_else(|| ProxyError::CacheError("Invalid bucket name".to_string()))?;

        // Extract object key from filename
        let filename = components[components.len() - 1]
            .to_str()
            .ok_or_else(|| ProxyError::CacheError("Invalid filename".to_string()))?;

        // Parse object key from filename (remove range suffix and .bin extension)
        let object_key = self.extract_object_key_from_filename(filename)?;

        Ok(format!("{}/{}", bucket, object_key))
    }

    /// Extract object key from range filename
    fn extract_object_key_from_filename(&self, filename: &str) -> Result<String> {
        if !filename.ends_with(".bin") {
            return Err(ProxyError::CacheError("Invalid range filename".to_string()));
        }

        let name_without_ext = Self::range_filename_stem(filename)?;

        // Find the last underscore followed by range pattern (digits-digits)
        if let Some(last_underscore) = name_without_ext.rfind('_') {
            let range_part = &name_without_ext[last_underscore + 1..];

            // Check if this looks like a range (digits-digits)
            if range_part.contains('-')
                && range_part.chars().all(|c| c.is_ascii_digit() || c == '-')
            {
                let object_key_sanitized = &name_without_ext[..last_underscore];

                // Unsanitize the object key
                let object_key = percent_encoding::percent_decode_str(object_key_sanitized)
                    .decode_utf8()
                    .map_err(|_| ProxyError::CacheError("Invalid encoded object key".to_string()))?
                    .into_owned();

                return Ok(object_key);
            }
        }

        Err(ProxyError::CacheError(format!(
            "Cannot extract object key from filename: {}",
            filename
        )))
    }
}

impl RecoveryMetrics {
    /// Get total orphans detected
    pub fn get_orphans_detected(&self) -> u64 {
        self.orphans_detected
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total orphans recovered
    pub fn get_orphans_recovered(&self) -> u64 {
        self.orphans_recovered
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total orphans cleaned
    pub fn get_orphans_cleaned(&self) -> u64 {
        self.orphans_cleaned
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    fn create_test_recovery(temp_dir: &TempDir) -> OrphanedRangeRecovery {
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            std::time::Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024 * 1024, 100));
        let metadata_writer = Arc::new(Mutex::new(HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        )));

        OrphanedRangeRecovery::new(temp_dir.path().to_path_buf(), metadata_writer)
    }

    fn write_range_fixture(
        recovery: &OrphanedRangeRecovery,
        cache_key: &str,
        etag: Option<&str>,
        fill: u8,
    ) -> (PathBuf, RangeSpec) {
        let disk = crate::disk_cache::DiskCacheManager::new(
            recovery.cache_dir.clone(),
            false,
            1024,
            false,
            1024,
        );
        let path = match etag {
            Some(etag) => disk.get_versioned_range_file_path(cache_key, 0, 511, etag),
            None => disk.get_new_range_file_path(cache_key, 0, 511),
        };
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let encoded = crate::compression::CompressionHandler::new(1024, false)
            .compress_with_metadata(&[fill; 512], cache_key, false);
        std::fs::write(&path, &encoded.data).unwrap();
        let mut range = recovery.parse_range_file(&path, cache_key).unwrap();
        range.compressed_size = encoded.data.len() as u64;
        (path, range)
    }

    async fn seed_metadata(
        recovery: &OrphanedRangeRecovery,
        cache_key: &str,
        etag: &str,
        ranges: Vec<RangeSpec>,
    ) {
        let path = crate::disk_cache::get_sharded_path(
            &recovery.cache_dir.join("metadata"),
            cache_key,
            ".meta",
        )
        .unwrap();
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                etag: etag.to_string(),
                content_length: 4096,
                response_headers: HashMap::from([("etag".to_string(), etag.to_string())]),
                ..Default::default()
            },
            ranges,
            ..Default::default()
        };
        tokio::fs::write(path, serde_json::to_vec(&metadata).unwrap())
            .await
            .unwrap();
    }

    #[test]
    fn parses_versioned_range_offsets_and_object_key() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let filename = format!(
            "path%2Fto%2Fobject.txt_1024-2047.{}.bin",
            blake3::hash(b"version-a").to_hex(),
        );
        let range = recovery
            .parse_range_from_filename(&filename, "path/to/object.txt")
            .unwrap();
        assert_eq!(
            (range.start, range.end, range.uncompressed_size),
            (1024, 2047, 1024)
        );
        assert_eq!(
            recovery
                .extract_object_key_from_filename(&filename)
                .unwrap(),
            "path/to/object.txt"
        );
        assert!(recovery
            .parse_range_from_filename("object_0-511.invalid.bin", "object")
            .is_err());
    }

    #[tokio::test]
    async fn scanners_distinguish_versions_at_the_same_offsets_and_cleanup_only_old_file() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let key = "bucket/path/form.parquet";
        let (old_path, _) = write_range_fixture(&recovery, key, Some("version-a"), b'A');
        let (current_path, current_range) =
            write_range_fixture(&recovery, key, Some("version-b"), b'B');
        seed_metadata(&recovery, key, "version-b", vec![current_range.clone()]).await;

        let orphans = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].range_file_path, old_path);
        assert_eq!(orphans[0].validation_status, ValidationStatus::Invalid);
        let shard = recovery.get_shard_path("bucket", "path/form.parquet");
        let prefix = shard.split('/').nth(1).unwrap();
        let by_key = recovery
            .scan_shard_for_orphans("bucket", prefix, Duration::from_secs(10), 10)
            .await
            .unwrap();
        assert_eq!(by_key[key].len(), 1);
        assert_eq!(by_key[key][0].range_file_path, old_path);
        assert_eq!(by_key[key][0].validation_status, ValidationStatus::Invalid);

        assert!(
            matches!(recovery.recover_orphan(&orphans[0]).await.unwrap(), RecoveryResult::Cleaned(bytes) if bytes > 0)
        );
        assert!(!old_path.exists());
        assert!(current_path.exists());
        assert_eq!(
            recovery
                .load_metadata_for_cache_key(key)
                .await
                .unwrap()
                .ranges,
            vec![current_range]
        );
    }

    #[tokio::test]
    async fn matching_version_orphan_recovers_full_relative_path_and_original_identity() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let key = "bucket/path/form.parquet";
        let (_, expected_range) = write_range_fixture(&recovery, key, Some("version-b"), b'B');
        seed_metadata(&recovery, key, "version-b", Vec::new()).await;
        let orphans = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].validation_status, ValidationStatus::Valid);
        let recovered = match recovery.recover_orphan(&orphans[0]).await.unwrap() {
            RecoveryResult::Recovered(range) => range,
            other => panic!("matching version was not recovered: {:?}", other),
        };
        assert_eq!(recovered.file_path, expected_range.file_path);
        let metadata = recovery.load_metadata_for_cache_key(key).await.unwrap();
        assert_eq!(metadata.object_metadata.etag, "version-b");
        assert_eq!(metadata.ranges, vec![recovered.clone()]);
        let disk = crate::disk_cache::DiskCacheManager::new(
            temp.path().to_path_buf(),
            false,
            1024,
            false,
            1024,
        );
        assert_eq!(
            disk.load_range_data(&recovered).await.unwrap(),
            vec![b'B'; 512]
        );
    }

    #[tokio::test]
    async fn legacy_orphan_cannot_borrow_known_identity_and_unknown_version_stays_unknown() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let key = "bucket/legacy.parquet";
        let (legacy_path, _) = write_range_fixture(&recovery, key, None, b'A');
        let legacy_size = std::fs::metadata(&legacy_path).unwrap().len();
        seed_metadata(&recovery, key, "version-b", Vec::new()).await;
        let orphans = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(orphans.len(), 1);
        // Unversioned bytes can never be served again, so they are cleaned rather than kept.
        assert_eq!(orphans[0].validation_status, ValidationStatus::Invalid);
        assert!(matches!(
            recovery.recover_orphan(&orphans[0]).await.unwrap(),
            RecoveryResult::Cleaned(bytes) if bytes == legacy_size
        ));
        assert!(!legacy_path.exists());
        assert!(recovery
            .load_metadata_for_cache_key(key)
            .await
            .unwrap()
            .ranges
            .is_empty());

        let unknown_key = "bucket/unknown.parquet";
        write_range_fixture(&recovery, unknown_key, Some("version-a"), b'A');
        let unknown = recovery.scan_for_orphans(unknown_key).await.unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].validation_status, ValidationStatus::Unknown);
        assert!(matches!(
            recovery.recover_orphan(&unknown[0]).await.unwrap(),
            RecoveryResult::Failed(_)
        ));
        assert!(recovery
            .load_metadata_for_cache_key(unknown_key)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn hashed_versioned_names_resolve_to_metadata_key_in_both_scanners() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let object_key = "a".repeat(190);
        let key = format!("bucket/{}", object_key);
        let (path, _) = write_range_fixture(&recovery, &key, Some("version-b"), b'B');
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(blake3::hash(key.as_bytes()).to_hex().as_str()));
        seed_metadata(&recovery, &key, "version-b", Vec::new()).await;
        let direct = recovery.scan_for_orphans(&key).await.unwrap();
        assert_eq!(direct.len(), 1);
        assert_eq!(direct[0].validation_status, ValidationStatus::Valid);
        let shard = recovery.get_shard_path("bucket", &object_key);
        let prefix = shard.split('/').nth(1).unwrap();
        let by_key = recovery
            .scan_shard_for_orphans("bucket", prefix, Duration::from_secs(10), 10)
            .await
            .unwrap();
        assert_eq!(by_key[&key].len(), 1);
        assert_eq!(by_key[&key][0].cache_key, key);
        assert_eq!(by_key[&key][0].range_file_path, path);
        assert_eq!(by_key[&key][0].validation_status, ValidationStatus::Valid);
    }

    #[tokio::test]
    async fn cleanup_rechecks_identity_and_recovery_rejects_a_changed_version() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let key = "bucket/path/form.parquet";
        let (path, _) = write_range_fixture(&recovery, key, Some("version-a"), b'A');
        seed_metadata(&recovery, key, "version-b", Vec::new()).await;
        let old_scan = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(old_scan[0].validation_status, ValidationStatus::Invalid);
        seed_metadata(&recovery, key, "version-a", Vec::new()).await;
        assert_eq!(
            recovery.cleanup_invalid_orphan(&old_scan[0]).await.unwrap(),
            0
        );
        assert!(path.exists());

        let valid_scan = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(valid_scan[0].validation_status, ValidationStatus::Valid);
        seed_metadata(&recovery, key, "version-b", Vec::new()).await;
        assert!(matches!(
            recovery.recover_orphan(&valid_scan[0]).await.unwrap(),
            RecoveryResult::Failed(_)
        ));
        assert!(recovery
            .load_metadata_for_cache_key(key)
            .await
            .unwrap()
            .ranges
            .is_empty());
    }

    #[tokio::test]
    async fn cleanup_leaves_another_instances_journaled_old_version_and_size_credit_intact() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let key = "bucket/path/form.parquet";
        let (old_path, old_range) = write_range_fixture(&recovery, key, Some("version-a"), b'A');
        seed_metadata(&recovery, key, "version-b", Vec::new()).await;
        let accumulator = crate::journal_consolidator::SizeAccumulator::new(
            temp.path(),
            "remote-writer".to_string(),
        );
        assert!(accumulator.add_range_file(key, &old_range));
        let journal = JournalManager::new(temp.path().to_path_buf(), "remote-writer".to_string());
        journal
            .append_range_entry(
                key,
                crate::journal_manager::JournalEntry {
                    timestamp: SystemTime::now(),
                    instance_id: "remote-writer".to_string(),
                    cache_key: key.to_string(),
                    range_spec: old_range.clone(),
                    operation: crate::journal_manager::JournalOperation::Add,
                    range_file_path: old_range.file_path.clone(),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: Some(0),
                    access_increment: None,
                    object_metadata: Some(crate::cache_types::ObjectMetadata {
                        etag: "version-a".to_string(),
                        ..Default::default()
                    }),
                },
            )
            .await
            .unwrap();

        let orphans = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].validation_status, ValidationStatus::Invalid);
        assert_eq!(
            recovery.cleanup_invalid_orphan(&orphans[0]).await.unwrap(),
            0
        );
        assert!(
            old_path.exists(),
            "the consolidator must retain ownership of the deletion and debit"
        );
        assert_eq!(
            accumulator.current_delta(),
            old_range.compressed_size as i64
        );
        assert_eq!(
            journal
                .get_all_entries_for_cache_key(key)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn oversized_metadata_cannot_supply_an_orphans_identity() {
        let temp = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp);
        let key = "bucket/form.parquet";
        write_range_fixture(&recovery, key, Some("version-b"), b'B');
        seed_metadata(&recovery, key, "version-b", Vec::new()).await;
        let metadata_path =
            crate::disk_cache::get_sharded_path(&temp.path().join("metadata"), key, ".meta")
                .unwrap();
        let size = std::fs::metadata(&metadata_path).unwrap().len();
        assert_eq!(
            recovery
                .load_metadata(&metadata_path)
                .await
                .unwrap()
                .object_metadata
                .etag,
            "version-b"
        );
        let recovery = recovery.with_max_metadata_file_bytes(size - 1);
        assert!(recovery.load_metadata(&metadata_path).await.is_err());
        let orphans = recovery.scan_for_orphans(key).await.unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].validation_status, ValidationStatus::Unknown);
        assert!(matches!(
            recovery.recover_orphan(&orphans[0]).await.unwrap(),
            RecoveryResult::Failed(_)
        ));
    }

    #[test]
    fn test_parse_cache_key() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let (bucket, object) = recovery
            .parse_cache_key("test-bucket/path/to/object.txt")
            .unwrap();
        assert_eq!(bucket, "test-bucket");
        assert_eq!(object, "path/to/object.txt");

        // Test invalid cache key
        assert!(recovery.parse_cache_key("invalid-key").is_err());
    }

    #[test]
    fn test_sanitize_key() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let sanitized = recovery.sanitize_key("path/to/file:with*special?chars");
        assert_eq!(sanitized, "path%2Fto%2Ffile%3Awith%2Aspecial%3Fchars");
    }

    #[test]
    fn test_parse_range_from_filename() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let range_spec = recovery
            .parse_range_from_filename("test-object_0-8388607.bin", "test-object")
            .unwrap();

        assert_eq!(range_spec.start, 0);
        assert_eq!(range_spec.end, 8388607);
        assert_eq!(range_spec.uncompressed_size, 8388608);
    }

    #[test]
    fn test_extract_object_key_from_filename() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let object_key = recovery
            .extract_object_key_from_filename("path%2Fto%2Fobject.txt_0-8388607.bin")
            .unwrap();

        assert_eq!(object_key, "path/to/object.txt");
    }

    #[test]
    fn test_validation_status() {
        assert_eq!(ValidationStatus::Valid, ValidationStatus::Valid);
        assert_ne!(ValidationStatus::Valid, ValidationStatus::Invalid);
    }

    #[test]
    fn test_recovery_metrics() {
        let metrics = RecoveryMetrics::default();

        assert_eq!(metrics.get_orphans_detected(), 0);
        assert_eq!(metrics.get_orphans_recovered(), 0);
        assert_eq!(metrics.get_orphans_cleaned(), 0);

        metrics
            .orphans_detected
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        metrics
            .orphans_recovered
            .fetch_add(3, std::sync::atomic::Ordering::Relaxed);
        metrics
            .orphans_cleaned
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);

        assert_eq!(metrics.get_orphans_detected(), 5);
        assert_eq!(metrics.get_orphans_recovered(), 3);
        assert_eq!(metrics.get_orphans_cleaned(), 2);
    }
}
