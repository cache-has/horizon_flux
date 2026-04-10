// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Output cache for materialized node data.
//!
//! During pipeline execution, each node's output `RecordBatch`es are written to
//! Arrow IPC files on disk. Preview reads from these cached files instead of
//! re-executing the pipeline.
//!
//! Layout:
//! ```text
//! {base_dir}/cache/{pipeline_name}/
//!   manifest.json          — per-node fingerprints and metadata
//!   {node_id}.arrow        — Arrow IPC file with cached output
//! ```
//!
//! Fingerprint-based invalidation: each node's fingerprint is computed from its
//! config, code, and upstream edges. When a pipeline is saved and a node's
//! fingerprint has changed, that node plus all transitive downstream nodes are
//! invalidated (cache files deleted).

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use flux_engine::Pipeline;
use flux_engine::node::{NodeId, NodeKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// Global default when neither node nor pipeline specifies a cache row limit.
pub const DEFAULT_CACHE_ROW_LIMIT: usize = 10_000;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Manifest types
// ---------------------------------------------------------------------------

/// Per-pipeline manifest tracking cached nodes and their fingerprints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheManifest {
    pub nodes: HashMap<String, CacheNodeEntry>,
}

/// Metadata about a single cached node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheNodeEntry {
    /// Deterministic fingerprint of the node's config, code, and upstream edges.
    pub fingerprint: String,
    /// Unix timestamp (seconds) when this cache entry was written.
    pub cached_at: u64,
    /// Number of rows stored in the cache file.
    pub rows: u64,
    /// Size of the cache file in bytes.
    pub size_bytes: u64,
}

// ---------------------------------------------------------------------------
// OutputCache
// ---------------------------------------------------------------------------

/// On-disk cache for materialized node outputs.
///
/// Thread-safe: all operations are stateless file I/O. Concurrent writes to the
/// same node may race, but the last writer wins and the file remains valid.
#[derive(Debug, Clone)]
pub struct OutputCache {
    /// Root directory for all caches (e.g. `~/.horizon-flux/cache`).
    cache_dir: PathBuf,
}

impl OutputCache {
    /// Create an `OutputCache` rooted at `base_dir/cache`.
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            cache_dir: base_dir.as_ref().join("cache"),
        }
    }

    /// Directory for a specific pipeline's cache files.
    fn pipeline_dir(&self, pipeline_name: &str) -> PathBuf {
        self.cache_dir.join(sanitize_name(pipeline_name))
    }

    /// Path to a node's Arrow IPC cache file.
    fn node_path(&self, pipeline_name: &str, node_id: &str) -> PathBuf {
        self.pipeline_dir(pipeline_name)
            .join(format!("{}.arrow", sanitize_name(node_id)))
    }

    /// Path to a pipeline's manifest file.
    fn manifest_path(&self, pipeline_name: &str) -> PathBuf {
        self.pipeline_dir(pipeline_name).join("manifest.json")
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /// Write a node's output batches to the cache, capping at `row_limit` rows.
    pub fn write_node(
        &self,
        pipeline_name: &str,
        node_id: &str,
        batches: &[RecordBatch],
        row_limit: usize,
    ) -> Result<CacheNodeEntry, CacheError> {
        let dir = self.pipeline_dir(pipeline_name);
        std::fs::create_dir_all(&dir)?;

        let capped = cap_batches(batches, row_limit);
        let path = self.node_path(pipeline_name, node_id);
        write_ipc(&path, &capped)?;

        let rows: u64 = capped.iter().map(|b| b.num_rows() as u64).sum();
        let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let entry = CacheNodeEntry {
            fingerprint: String::new(), // caller sets this via update_manifest
            cached_at: now_unix_secs(),
            rows,
            size_bytes,
        };

        debug!(
            pipeline = pipeline_name,
            node = node_id,
            rows,
            size_bytes,
            "cached node output"
        );
        Ok(entry)
    }

    /// Cache all node outputs from a pipeline execution result.
    ///
    /// Writes IPC files for every node that produced output and updates the
    /// manifest with current fingerprints. Returns the number of nodes cached.
    pub fn cache_pipeline_outputs(
        &self,
        pipeline: &Pipeline,
        node_outputs: &HashMap<NodeId, Vec<RecordBatch>>,
    ) -> Result<usize, CacheError> {
        let mut manifest = self.read_manifest(&pipeline.name).unwrap_or_default();
        let mut count = 0;

        for (node_id, batches) in node_outputs {
            let node = match pipeline.node(node_id) {
                Some(n) => n,
                None => continue,
            };

            // Skip sinks — they don't produce previewable output.
            if node.kind.is_sink() {
                continue;
            }

            let row_limit = pipeline.effective_cache_row_limit(node);
            let mut entry = self.write_node(&pipeline.name, &node_id.0, batches, row_limit)?;
            entry.fingerprint = compute_node_fingerprint(pipeline, node);
            manifest.nodes.insert(node_id.0.clone(), entry);
            count += 1;
        }

        self.write_manifest(&pipeline.name, &manifest)?;
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /// Read a node's cached output batches. Returns `None` if no cache exists.
    pub fn read_node(
        &self,
        pipeline_name: &str,
        node_id: &str,
    ) -> Result<Option<Vec<RecordBatch>>, CacheError> {
        let path = self.node_path(pipeline_name, node_id);
        if !path.exists() {
            return Ok(None);
        }
        let batches = read_ipc(&path)?;
        Ok(Some(batches))
    }

    /// Check whether a cached output exists for a node.
    pub fn exists(&self, pipeline_name: &str, node_id: &str) -> bool {
        self.node_path(pipeline_name, node_id).exists()
    }

    // -----------------------------------------------------------------------
    // Manifest
    // -----------------------------------------------------------------------

    /// Read the cache manifest for a pipeline. Returns `None` if not found.
    pub fn read_manifest(&self, pipeline_name: &str) -> Option<CacheManifest> {
        let path = self.manifest_path(pipeline_name);
        let data = std::fs::read_to_string(&path).ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Write the cache manifest for a pipeline.
    pub fn write_manifest(
        &self,
        pipeline_name: &str,
        manifest: &CacheManifest,
    ) -> Result<(), CacheError> {
        let dir = self.pipeline_dir(pipeline_name);
        std::fs::create_dir_all(&dir)?;
        let path = self.manifest_path(pipeline_name);
        let data = serde_json::to_string_pretty(manifest)?;
        std::fs::write(&path, data)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------------

    /// Invalidate cache for nodes whose fingerprints have changed, plus all
    /// their transitive downstream dependents.
    ///
    /// Call this when a pipeline is saved/updated. Returns the IDs of
    /// invalidated nodes.
    pub fn invalidate_changed(&self, pipeline: &Pipeline) -> Result<Vec<NodeId>, CacheError> {
        let manifest = match self.read_manifest(&pipeline.name) {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        // Find nodes whose fingerprints changed or that are new.
        let mut changed: Vec<NodeId> = Vec::new();
        for node in &pipeline.nodes {
            let current_fp = compute_node_fingerprint(pipeline, node);
            let cached_fp = manifest
                .nodes
                .get(&node.id.0)
                .map(|e| e.fingerprint.as_str());

            if cached_fp != Some(&current_fp) {
                changed.push(node.id.clone());
            }
        }

        // Collect transitive downstream of each changed node.
        let mut to_invalidate = std::collections::HashSet::new();
        for id in &changed {
            to_invalidate.insert(id.clone());
            for downstream in pipeline.all_downstream_of(id) {
                to_invalidate.insert(downstream);
            }
        }

        // Also remove entries for nodes that no longer exist in the pipeline.
        let current_ids: std::collections::HashSet<&str> =
            pipeline.nodes.iter().map(|n| n.id.0.as_str()).collect();
        let removed: Vec<String> = manifest
            .nodes
            .keys()
            .filter(|k| !current_ids.contains(k.as_str()))
            .cloned()
            .collect();
        for id in &removed {
            to_invalidate.insert(NodeId::new(id.clone()));
        }

        // Delete cache files and update manifest.
        let mut manifest = manifest;
        for id in &to_invalidate {
            let path = self.node_path(&pipeline.name, &id.0);
            if path.exists() {
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!(node = %id, error = %e, "failed to remove cached node output");
                }
            }
            manifest.nodes.remove(&id.0);
        }
        self.write_manifest(&pipeline.name, &manifest)?;

        let invalidated: Vec<NodeId> = to_invalidate.into_iter().collect();
        if !invalidated.is_empty() {
            debug!(
                pipeline = %pipeline.name,
                count = invalidated.len(),
                "invalidated cached node outputs"
            );
        }
        Ok(invalidated)
    }

    /// Delete a single node's cache file and remove it from the manifest.
    pub fn delete_node(&self, pipeline_name: &str, node_id: &str) -> Result<(), CacheError> {
        let path = self.node_path(pipeline_name, node_id);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        if let Some(mut manifest) = self.read_manifest(pipeline_name) {
            manifest.nodes.remove(node_id);
            self.write_manifest(pipeline_name, &manifest)?;
        }
        Ok(())
    }

    /// Delete all cached data for a pipeline.
    pub fn delete_pipeline(&self, pipeline_name: &str) -> Result<(), CacheError> {
        let dir = self.pipeline_dir(pipeline_name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
            debug!(pipeline = pipeline_name, "deleted pipeline cache");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fingerprinting
// ---------------------------------------------------------------------------

/// Compute a deterministic fingerprint for a node based on its config, code
/// content, and upstream edges. Used to detect when a node has changed and
/// its cache should be invalidated.
pub fn compute_node_fingerprint(pipeline: &Pipeline, node: &flux_engine::node::Node) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    match &node.kind {
        NodeKind::Source(cfg) => {
            "source".hash(&mut hasher);
            cfg.connector.hash(&mut hasher);
            cfg.config.to_string().hash(&mut hasher);
        }
        NodeKind::Transform(cfg) => {
            "transform".hash(&mut hasher);
            format!("{:?}", cfg.mode).hash(&mut hasher);
            // Resolve code from file if needed, fall back to inline.
            let code = pipeline
                .resolve_code(cfg)
                .unwrap_or_else(|_| cfg.code.clone());
            code.hash(&mut hasher);
        }
        NodeKind::Sink(cfg) => {
            "sink".hash(&mut hasher);
            cfg.connector.hash(&mut hasher);
            cfg.config.to_string().hash(&mut hasher);
        }
        NodeKind::Test(cfg) => {
            "test".hash(&mut hasher);
            // Hash the serialized assertions so cache invalidates on config change.
            serde_json::to_string(cfg)
                .unwrap_or_default()
                .hash(&mut hasher);
        }
        NodeKind::Snippet(_) => {
            unreachable!("snippets must be expanded before fingerprinting")
        }
    }

    // Include sorted upstream edges so edge changes invalidate the cache.
    let mut upstream: Vec<&str> = pipeline
        .upstream_of(&node.id)
        .iter()
        .map(|id| id.0.as_str())
        .collect();
    upstream.sort();
    upstream.hash(&mut hasher);

    format!("{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// Arrow IPC helpers
// ---------------------------------------------------------------------------

/// Write `RecordBatch`es to an Arrow IPC file.
fn write_ipc(path: &Path, batches: &[RecordBatch]) -> Result<(), CacheError> {
    if batches.is_empty() {
        // Write an empty file so `exists()` still returns true and we can
        // distinguish "cached with 0 rows" from "never cached".
        std::fs::write(path, b"")?;
        return Ok(());
    }

    let schema = batches[0].schema();
    let file = std::fs::File::create(path)?;
    let mut writer = FileWriter::try_new(file, &schema)?;

    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

/// Read `RecordBatch`es from an Arrow IPC file.
fn read_ipc(path: &Path) -> Result<Vec<RecordBatch>, CacheError> {
    let metadata = std::fs::metadata(path)?;
    if metadata.len() == 0 {
        return Ok(Vec::new());
    }

    let file = std::fs::File::open(path)?;
    let reader = FileReader::try_new(file, None)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }
    Ok(batches)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Cap batches to at most `limit` rows total.
fn cap_batches(batches: &[RecordBatch], limit: usize) -> Vec<RecordBatch> {
    let mut remaining = limit;
    let mut result = Vec::new();

    for batch in batches {
        if remaining == 0 {
            break;
        }
        let rows = batch.num_rows();
        if rows <= remaining {
            remaining -= rows;
            result.push(batch.clone());
        } else {
            result.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }

    result
}

/// Sanitize a name for use as a filesystem path component.
/// Replaces path separators and other problematic characters with underscores.
fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | '\0' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}

/// Current time as Unix seconds.
fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use flux_engine::edge::Edge;
    use flux_engine::node::*;
    use std::sync::Arc;

    fn test_batches(rows: usize) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let array = Int32Array::from((0..rows as i32).collect::<Vec<_>>());
        vec![RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()]
    }

    fn test_pipeline() -> Pipeline {
        Pipeline {
            name: "test_pipe".into(),
            version: 1,
            default_environment: "dev".into(),
            variables: Default::default(),
            environment_overrides: Default::default(),
            sample_config: None,
            cache_row_limit: None,
            code_dir: None,
            udfs_dir: None,
            snippets_dir: None,
            snippet: None,
            params: Default::default(),
            outputs: Vec::new(),
            nodes: vec![
                Node {
                    id: NodeId::new("src"),
                    name: "src".into(),
                    kind: NodeKind::Source(SourceConfig {
                        connector: "csv".into(),
                        config: serde_json::json!({"path": "data.csv"}),
                        cache_row_limit: None,
                    }),
                    position: Position::default(),
                    pinned_position: false,
                    snippet_parent: None,
                    snippet_name: None,
                },
                Node {
                    id: NodeId::new("xform"),
                    name: "xform".into(),
                    kind: NodeKind::Transform(TransformConfig {
                        mode: TransformMode::Sql,
                        code: "SELECT * FROM src".into(),
                        code_path: None,
                        materialized: false,
                        cache_row_limit: None,
                        lineage_annotations: None,
                    }),
                    position: Position::default(),
                    pinned_position: false,
                    snippet_parent: None,
                    snippet_name: None,
                },
            ],
            edges: vec![Edge::new("src", "xform")],
        }
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());
        let batches = test_batches(50);

        cache
            .write_node("pipe", "node_a", &batches, 10_000)
            .unwrap();

        let loaded = cache.read_node("pipe", "node_a").unwrap().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].num_rows(), 50);
    }

    #[test]
    fn write_caps_rows() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());
        let batches = test_batches(500);

        let entry = cache.write_node("pipe", "big", &batches, 100).unwrap();
        assert_eq!(entry.rows, 100);

        let loaded = cache.read_node("pipe", "big").unwrap().unwrap();
        let total: usize = loaded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn read_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());
        assert!(cache.read_node("pipe", "nope").unwrap().is_none());
    }

    #[test]
    fn delete_node_removes_file_and_manifest_entry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());
        let batches = test_batches(10);

        cache.write_node("pipe", "a", &batches, 10_000).unwrap();
        let manifest = CacheManifest {
            nodes: [(
                "a".into(),
                CacheNodeEntry {
                    fingerprint: "fp".into(),
                    cached_at: 0,
                    rows: 10,
                    size_bytes: 0,
                },
            )]
            .into_iter()
            .collect(),
        };
        cache.write_manifest("pipe", &manifest).unwrap();

        cache.delete_node("pipe", "a").unwrap();
        assert!(!cache.exists("pipe", "a"));
        let m = cache.read_manifest("pipe").unwrap();
        assert!(!m.nodes.contains_key("a"));
    }

    #[test]
    fn delete_pipeline_removes_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());
        cache
            .write_node("pipe", "a", &test_batches(5), 10_000)
            .unwrap();
        assert!(cache.exists("pipe", "a"));

        cache.delete_pipeline("pipe").unwrap();
        assert!(!cache.exists("pipe", "a"));
    }

    #[test]
    fn invalidate_changed_detects_code_change() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());

        let pipeline = test_pipeline();

        // Simulate a prior cache with the current fingerprints.
        let mut manifest = CacheManifest::default();
        for node in &pipeline.nodes {
            let fp = compute_node_fingerprint(&pipeline, node);
            cache
                .write_node(&pipeline.name, &node.id.0, &test_batches(5), 10_000)
                .unwrap();
            manifest.nodes.insert(
                node.id.0.clone(),
                CacheNodeEntry {
                    fingerprint: fp,
                    cached_at: 0,
                    rows: 5,
                    size_bytes: 0,
                },
            );
        }
        cache.write_manifest(&pipeline.name, &manifest).unwrap();

        // No changes → nothing invalidated.
        let invalidated = cache.invalidate_changed(&pipeline).unwrap();
        assert!(invalidated.is_empty());

        // Change the transform code → xform should be invalidated.
        let mut changed = pipeline.clone();
        if let NodeKind::Transform(ref mut cfg) = changed.nodes[1].kind {
            cfg.code = "SELECT x + 1 FROM src".into();
        }
        let invalidated = cache.invalidate_changed(&changed).unwrap();
        assert!(invalidated.iter().any(|id| id.0 == "xform"));
        assert!(!cache.exists(&pipeline.name, "xform"));
        // Source should still be cached.
        assert!(cache.exists(&pipeline.name, "src"));
    }

    #[test]
    fn cache_pipeline_outputs_writes_all_non_sink_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = OutputCache::new(dir.path());
        let pipeline = test_pipeline();

        let mut outputs: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();
        outputs.insert(NodeId::new("src"), test_batches(100));
        outputs.insert(NodeId::new("xform"), test_batches(50));

        let count = cache.cache_pipeline_outputs(&pipeline, &outputs).unwrap();
        assert_eq!(count, 2);
        assert!(cache.exists(&pipeline.name, "src"));
        assert!(cache.exists(&pipeline.name, "xform"));

        let manifest = cache.read_manifest(&pipeline.name).unwrap();
        assert_eq!(manifest.nodes.len(), 2);
        assert!(!manifest.nodes["src"].fingerprint.is_empty());
    }
}
