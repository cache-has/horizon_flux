// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resource catalog: a searchable, browseable view of the tables, files, and
//! other resources that armillary pipelines produce and consume.
//!
//! The catalog is a **projection over existing metadata** — lineage bindings,
//! schemas from runs, and user-authored annotation YAML files — rather than a
//! new primary data model.

use crate::lineage::{BindingDirection, LineageGraph, ResourceFingerprint};
use crate::pipeline_store::PipelineId;
use crate::sla::SlaConfig;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Annotation types (parsed from YAML metadata files)
// ---------------------------------------------------------------------------

/// A user-authored annotation file for a single resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceAnnotation {
    pub resource: AnnotationResource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<AnnotationOwner>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, ColumnAnnotation>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, serde_yaml::Value>,
    /// Freshness SLA configuration (planning doc 37, sub-feature 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<SlaConfig>,
}

/// The `resource:` block in an annotation file. Only `fingerprint` is required.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnotationResource {
    pub fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
}

/// Owner metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnotationOwner {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
}

/// Per-column annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAnnotation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_values: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Auto-derived facts
// ---------------------------------------------------------------------------

/// Facts about a resource that armillary derives automatically from pipeline
/// definitions and execution history.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AutoDerivedFacts {
    /// Which type of resource this is (e.g. "postgres", "csv", "parquet", "s3").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    /// Pipelines that write to this resource.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub producers: Vec<PipelineBinding>,
    /// Pipelines that read from this resource.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumers: Vec<PipelineBinding>,
    /// Schema columns detected from the most recent run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_columns: Vec<SchemaColumn>,
    /// Timestamp of the most recent successful producing run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<String>,
    /// Row count from the most recent run stats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    /// Size in bytes from the most recent run stats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// A pipeline + node that touches a resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineBinding {
    pub pipeline_id: PipelineId,
    pub node_id: String,
}

/// A single column in the auto-detected schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// CatalogEntry — the merged view
// ---------------------------------------------------------------------------

/// A single resource in the catalog, combining auto-derived facts with
/// user-authored annotations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub fingerprint: ResourceFingerprint,
    /// Human-readable name (from annotation, or derived from fingerprint).
    pub name: String,
    /// Description (from annotation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Owner (from annotation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<AnnotationOwner>,
    /// Tags (from annotation).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Auto-derived facts.
    pub derived: AutoDerivedFacts,
    /// Merged column info: auto-detected schema + annotation descriptions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<MergedColumn>,
    /// Custom metadata from annotation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, serde_yaml::Value>,
    /// Environment (from annotation, if present).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    /// Path to the annotation YAML file, if one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotation_file: Option<PathBuf>,
}

/// A column combining auto-detected schema info with annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergedColumn {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_values: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Annotation parsing
// ---------------------------------------------------------------------------

/// Errors that can occur during catalog operations.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("failed to read annotation file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse annotation file {path}: {source}")]
    ParseYaml {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("annotation file {path} is missing required field: resource.fingerprint")]
    MissingFingerprint { path: PathBuf },
}

/// Parse a single annotation YAML file from a string.
pub fn parse_annotation(yaml: &str) -> Result<ResourceAnnotation, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
}

/// Parse an annotation file from disk.
pub fn parse_annotation_file(path: &Path) -> Result<ResourceAnnotation, CatalogError> {
    let contents = std::fs::read_to_string(path).map_err(|e| CatalogError::ReadFile {
        path: path.to_path_buf(),
        source: e,
    })?;
    let ann: ResourceAnnotation =
        serde_yaml::from_str(&contents).map_err(|e| CatalogError::ParseYaml {
            path: path.to_path_buf(),
            source: e,
        })?;
    if ann.resource.fingerprint.is_empty() {
        return Err(CatalogError::MissingFingerprint {
            path: path.to_path_buf(),
        });
    }
    Ok(ann)
}

/// Scan a `metadata/` directory and return all parsed annotations keyed by
/// fingerprint, plus any parse errors encountered.
pub fn load_annotations(
    metadata_dir: &Path,
) -> (
    HashMap<ResourceFingerprint, AnnotationFile>,
    Vec<CatalogError>,
) {
    let mut annotations = HashMap::new();
    let mut errors = Vec::new();

    if !metadata_dir.is_dir() {
        return (annotations, errors);
    }

    fn walk(dir: &Path, results: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, results);
            } else if path.extension().is_some_and(|e| e == "yaml" || e == "yml") {
                results.push(path);
            }
        }
    }

    let mut yaml_files = Vec::new();
    walk(metadata_dir, &mut yaml_files);

    for path in yaml_files {
        match parse_annotation_file(&path) {
            Ok(ann) => {
                let fp = ResourceFingerprint::new(&ann.resource.fingerprint);
                annotations.insert(
                    fp,
                    AnnotationFile {
                        annotation: ann,
                        path,
                    },
                );
            }
            Err(e) => errors.push(e),
        }
    }

    (annotations, errors)
}

/// An annotation paired with its source file path.
#[derive(Debug, Clone)]
pub struct AnnotationFile {
    pub annotation: ResourceAnnotation,
    pub path: PathBuf,
}

// ---------------------------------------------------------------------------
// Filename normalization
// ---------------------------------------------------------------------------

/// Convert a resource fingerprint to a normalized filename for the metadata
/// directory. Path separators in the fingerprint become `__` (double underscore),
/// and the scheme becomes the subdirectory.
///
/// # Examples
/// - `postgres://host:5432/db/public.orders` → `postgres/host__5432__db__public.orders.yaml`
/// - `file:///data/orders.csv` → `files/data__orders.csv.yaml`
/// - `s3://bucket/path/data.parquet` → `s3/bucket__path__data.parquet.yaml`
pub fn fingerprint_to_filename(fingerprint: &ResourceFingerprint) -> PathBuf {
    let s = &fingerprint.0;

    let (scheme, rest) = if let Some(idx) = s.find("://") {
        (&s[..idx], &s[idx + 3..])
    } else {
        ("unknown", s.as_str())
    };

    // Map schemes to directory names.
    let dir = match scheme {
        "file" => "files",
        "postgres" | "postgresql" => "postgres",
        other => other,
    };

    // Strip leading slashes (file:///path → path).
    let rest = rest.trim_start_matches('/');

    // Replace path separators and colons with double underscore.
    let normalized = rest.replace(['/', ':'], "__");

    PathBuf::from(dir).join(format!("{normalized}.yaml"))
}

// ---------------------------------------------------------------------------
// Resource discovery
// ---------------------------------------------------------------------------

/// Discover all unique resource fingerprints from a lineage graph's bindings.
/// Returns a map from fingerprint to its bindings grouped by direction.
pub fn discover_resources(
    graph: &LineageGraph,
) -> HashMap<ResourceFingerprint, DiscoveredResource> {
    let mut resources: HashMap<ResourceFingerprint, DiscoveredResource> = HashMap::new();

    for binding in &graph.bindings {
        let entry = resources
            .entry(binding.fingerprint.clone())
            .or_insert_with(|| DiscoveredResource {
                fingerprint: binding.fingerprint.clone(),
                resource_type: infer_resource_type(&binding.fingerprint),
                producers: Vec::new(),
                consumers: Vec::new(),
            });

        let pb = PipelineBinding {
            pipeline_id: binding.pipeline_id.clone(),
            node_id: binding.node_id.clone(),
        };

        match binding.direction {
            BindingDirection::Source => entry.consumers.push(pb),
            BindingDirection::Sink => entry.producers.push(pb),
        }
    }

    resources
}

/// A resource discovered from pipeline bindings, before annotation merge.
#[derive(Debug, Clone)]
pub struct DiscoveredResource {
    pub fingerprint: ResourceFingerprint,
    pub resource_type: Option<String>,
    pub producers: Vec<PipelineBinding>,
    pub consumers: Vec<PipelineBinding>,
}

/// Infer the resource type from its fingerprint scheme.
fn infer_resource_type(fp: &ResourceFingerprint) -> Option<String> {
    let s = &fp.0;
    if s.starts_with("postgres://") || s.starts_with("postgresql://") {
        Some("postgres".to_string())
    } else if s.starts_with("file://") {
        // Try to infer from extension.
        if s.ends_with(".csv") {
            Some("csv".to_string())
        } else if s.ends_with(".parquet") {
            Some("parquet".to_string())
        } else {
            Some("file".to_string())
        }
    } else if s.starts_with("s3://") {
        Some("s3".to_string())
    } else if s.starts_with("gs://") {
        Some("gcs".to_string())
    } else if s.starts_with("az://") {
        Some("azure".to_string())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Merge logic
// ---------------------------------------------------------------------------

/// Merge auto-derived facts from discovery with user-authored annotations to
/// produce catalog entries.
pub fn merge_catalog(
    discovered: &HashMap<ResourceFingerprint, DiscoveredResource>,
    annotations: &HashMap<ResourceFingerprint, AnnotationFile>,
) -> Vec<CatalogEntry> {
    let mut entries = Vec::new();

    // All discovered resources get an entry, with or without annotation.
    for (fp, resource) in discovered {
        let ann = annotations.get(fp);
        entries.push(build_entry(resource, ann));
    }

    // Sort by fingerprint for deterministic output.
    entries.sort_by(|a, b| a.fingerprint.0.cmp(&b.fingerprint.0));
    entries
}

fn build_entry(resource: &DiscoveredResource, ann: Option<&AnnotationFile>) -> CatalogEntry {
    let default_name = default_name_from_fingerprint(&resource.fingerprint);

    let (name, description, owner, tags, col_annotations, custom, environment, annotation_file) =
        if let Some(af) = ann {
            let a = &af.annotation;
            (
                a.name.clone().unwrap_or(default_name),
                a.description.clone(),
                a.owner.clone(),
                a.tags.clone(),
                a.columns.clone(),
                a.custom.clone(),
                a.resource.environment.clone(),
                Some(af.path.clone()),
            )
        } else {
            (
                default_name,
                None,
                None,
                Vec::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
                None,
            )
        };

    // Merge columns: start with auto-derived schema, overlay annotations.
    let columns = merge_columns(&[], &col_annotations);

    let derived = AutoDerivedFacts {
        resource_type: resource.resource_type.clone(),
        producers: resource.producers.clone(),
        consumers: resource.consumers.clone(),
        // Schema, last_updated, row_count, size_bytes are populated later
        // from run history — not available at discovery time.
        ..Default::default()
    };

    CatalogEntry {
        fingerprint: resource.fingerprint.clone(),
        name,
        description,
        owner,
        tags,
        derived,
        columns,
        custom,
        environment,
        annotation_file,
    }
}

/// Derive a human-readable name from a fingerprint.
/// Takes the last meaningful segment (table name, filename, etc.).
fn default_name_from_fingerprint(fp: &ResourceFingerprint) -> String {
    let s = &fp.0;

    // Strip scheme.
    let rest = if let Some(idx) = s.find("://") {
        &s[idx + 3..]
    } else {
        s.as_str()
    };

    // Take the last path segment.
    rest.rsplit('/')
        .find(|seg| !seg.is_empty())
        .unwrap_or(rest)
        .to_string()
}

/// Merge auto-detected schema columns with annotation column descriptions.
/// Annotations for columns not in the schema are appended at the end.
pub fn merge_columns(
    schema: &[SchemaColumn],
    annotations: &BTreeMap<String, ColumnAnnotation>,
) -> Vec<MergedColumn> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();

    // First, all schema columns (preserving order).
    for col in schema {
        seen.insert(col.name.clone());
        let ann = annotations.get(&col.name);
        result.push(MergedColumn {
            name: col.name.clone(),
            data_type: Some(col.data_type.clone()),
            nullable: Some(col.nullable),
            description: ann.and_then(|a| a.description.clone()),
            accepted_values: ann.and_then(|a| a.accepted_values.clone()),
        });
    }

    // Then, annotation-only columns (not in schema).
    for (name, ann) in annotations {
        if !seen.contains(name) {
            result.push(MergedColumn {
                name: name.clone(),
                data_type: None,
                nullable: None,
                description: ann.description.clone(),
                accepted_values: ann.accepted_values.clone(),
            });
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Validation: dangling and duplicate detection
// ---------------------------------------------------------------------------

/// A warning produced during catalog validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogWarning {
    /// An annotation file references a resource that no pipeline uses.
    DanglingAnnotation {
        fingerprint: ResourceFingerprint,
        path: PathBuf,
    },
    /// Multiple annotation files claim the same resource fingerprint.
    DuplicateAnnotation {
        fingerprint: ResourceFingerprint,
        paths: Vec<PathBuf>,
    },
}

/// Detect dangling annotations: annotation files whose fingerprint doesn't
/// match any discovered resource.
pub fn detect_dangling(
    discovered: &HashMap<ResourceFingerprint, DiscoveredResource>,
    annotations: &HashMap<ResourceFingerprint, AnnotationFile>,
) -> Vec<CatalogWarning> {
    annotations
        .iter()
        .filter(|(fp, _)| !discovered.contains_key(fp))
        .map(|(fp, af)| CatalogWarning::DanglingAnnotation {
            fingerprint: fp.clone(),
            path: af.path.clone(),
        })
        .collect()
}

/// Scan for duplicate annotations during loading. This is called during the
/// load phase, before deduplication into the HashMap.
pub fn detect_duplicates_from_files(
    metadata_dir: &Path,
) -> (
    HashMap<ResourceFingerprint, AnnotationFile>,
    Vec<CatalogWarning>,
    Vec<CatalogError>,
) {
    let mut by_fingerprint: HashMap<ResourceFingerprint, Vec<PathBuf>> = HashMap::new();
    let mut annotations = HashMap::new();
    let mut errors = Vec::new();

    if !metadata_dir.is_dir() {
        return (annotations, Vec::new(), errors);
    }

    fn walk(dir: &Path, results: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, results);
            } else if path.extension().is_some_and(|e| e == "yaml" || e == "yml") {
                results.push(path);
            }
        }
    }

    let mut yaml_files = Vec::new();
    walk(metadata_dir, &mut yaml_files);

    for path in yaml_files {
        match parse_annotation_file(&path) {
            Ok(ann) => {
                let fp = ResourceFingerprint::new(&ann.resource.fingerprint);
                by_fingerprint
                    .entry(fp.clone())
                    .or_default()
                    .push(path.clone());
                // Last one wins in the map (consistent with load_annotations).
                annotations.insert(
                    fp,
                    AnnotationFile {
                        annotation: ann,
                        path,
                    },
                );
            }
            Err(e) => errors.push(e),
        }
    }

    let warnings = by_fingerprint
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .map(|(fp, paths)| CatalogWarning::DuplicateAnnotation {
            fingerprint: fp,
            paths,
        })
        .collect();

    (annotations, warnings, errors)
}

// ---------------------------------------------------------------------------
// Search index
// ---------------------------------------------------------------------------

/// A simple inverted index for searching catalog entries by name, description,
/// tags, and column names.
#[derive(Debug, Default)]
pub struct SearchIndex {
    /// Maps lowercase tokens → set of fingerprints that contain that token.
    index: HashMap<String, HashSet<ResourceFingerprint>>,
}

impl SearchIndex {
    /// Build a search index from catalog entries.
    pub fn build(entries: &[CatalogEntry]) -> Self {
        let mut si = Self::default();
        for entry in entries {
            let fp = &entry.fingerprint;

            // Index the name.
            si.index_text(fp, &entry.name);

            // Index description.
            if let Some(desc) = &entry.description {
                si.index_text(fp, desc);
            }

            // Index tags.
            for tag in &entry.tags {
                si.index_token(fp, tag);
            }

            // Index column names and descriptions.
            for col in &entry.columns {
                si.index_token(fp, &col.name);
                if let Some(desc) = &col.description {
                    si.index_text(fp, desc);
                }
            }

            // Index the fingerprint itself.
            si.index_text(fp, &fp.0);

            // Index owner team.
            if let Some(owner) = &entry.owner {
                if let Some(team) = &owner.team {
                    si.index_token(fp, team);
                }
            }

            // Index resource type.
            if let Some(rt) = &entry.derived.resource_type {
                si.index_token(fp, rt);
            }
        }
        si
    }

    /// Search the index, returning fingerprints that match ALL query tokens.
    pub fn search(&self, query: &str) -> Vec<ResourceFingerprint> {
        let tokens = tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }

        let mut result: Option<HashSet<ResourceFingerprint>> = None;
        for token in &tokens {
            let matches = self.matching_fingerprints(token);
            result = Some(match result {
                None => matches,
                Some(acc) => acc.intersection(&matches).cloned().collect(),
            });
        }

        let mut out: Vec<_> = result.unwrap_or_default().into_iter().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn index_text(&mut self, fp: &ResourceFingerprint, text: &str) {
        for token in tokenize(text) {
            self.index_token(fp, &token);
        }
    }

    fn index_token(&mut self, fp: &ResourceFingerprint, token: &str) {
        let lower = token.to_lowercase();
        self.index.entry(lower).or_default().insert(fp.clone());
    }

    /// Get all fingerprints that have an indexed token containing the query token
    /// as a substring (prefix matching for usability).
    fn matching_fingerprints(&self, query_token: &str) -> HashSet<ResourceFingerprint> {
        let mut result = HashSet::new();
        let lower = query_token.to_lowercase();
        for (indexed_token, fps) in &self.index {
            if indexed_token.contains(&lower) {
                result.extend(fps.iter().cloned());
            }
        }
        result
    }
}

/// Tokenize text for indexing/searching: split on whitespace and common
/// delimiters, lowercase, discard empties.
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| c.is_whitespace() || "/:@._-".contains(c))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

// ---------------------------------------------------------------------------
// Catalog — the top-level orchestrator
// ---------------------------------------------------------------------------

/// The assembled resource catalog.
#[derive(Debug)]
pub struct Catalog {
    pub entries: Vec<CatalogEntry>,
    pub warnings: Vec<CatalogWarning>,
    pub errors: Vec<CatalogError>,
    search_index: SearchIndex,
}

impl Catalog {
    /// Build a catalog from a lineage graph and a metadata directory.
    pub fn build(graph: &LineageGraph, metadata_dir: &Path) -> Self {
        let discovered = discover_resources(graph);
        let (annotations, dup_warnings, errors) = detect_duplicates_from_files(metadata_dir);
        let dangling = detect_dangling(&discovered, &annotations);
        let entries = merge_catalog(&discovered, &annotations);
        let search_index = SearchIndex::build(&entries);

        let mut warnings = dup_warnings;
        warnings.extend(dangling);

        Self {
            entries,
            warnings,
            errors,
            search_index,
        }
    }

    /// Build from pre-loaded components (useful when annotations are loaded
    /// separately or in tests).
    pub fn from_parts(
        discovered: &HashMap<ResourceFingerprint, DiscoveredResource>,
        annotations: &HashMap<ResourceFingerprint, AnnotationFile>,
    ) -> Self {
        let dangling = detect_dangling(discovered, annotations);
        let entries = merge_catalog(discovered, annotations);
        let search_index = SearchIndex::build(&entries);

        Self {
            entries,
            warnings: dangling,
            errors: Vec::new(),
            search_index,
        }
    }

    /// Get a catalog entry by fingerprint.
    pub fn get(&self, fingerprint: &ResourceFingerprint) -> Option<&CatalogEntry> {
        self.entries.iter().find(|e| &e.fingerprint == fingerprint)
    }

    /// Search the catalog by query string.
    pub fn search(&self, query: &str) -> Vec<&CatalogEntry> {
        let fps = self.search_index.search(query);
        fps.iter().filter_map(|fp| self.get(fp)).collect()
    }

    /// Filter entries by tag.
    pub fn filter_by_tag(&self, tag: &str) -> Vec<&CatalogEntry> {
        self.entries
            .iter()
            .filter(|e| e.tags.iter().any(|t| t == tag))
            .collect()
    }

    /// Filter entries by owner team.
    pub fn filter_by_owner(&self, team: &str) -> Vec<&CatalogEntry> {
        self.entries
            .iter()
            .filter(|e| {
                e.owner
                    .as_ref()
                    .and_then(|o| o.team.as_ref())
                    .is_some_and(|t| t == team)
            })
            .collect()
    }

    /// Get all unique tags across all entries.
    pub fn all_tags(&self) -> Vec<String> {
        let mut tags: Vec<_> = self
            .entries
            .iter()
            .flat_map(|e| e.tags.iter().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        tags.sort();
        tags
    }

    /// Get all unique owner teams across all entries.
    pub fn all_owners(&self) -> Vec<String> {
        let mut owners: Vec<_> = self
            .entries
            .iter()
            .filter_map(|e| e.owner.as_ref()?.team.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        owners.sort();
        owners
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Scaffold: generate annotation YAML for a resource
// ---------------------------------------------------------------------------

/// Generate a scaffold annotation YAML string for a discovered resource.
pub fn scaffold_annotation(resource: &DiscoveredResource) -> String {
    let name = default_name_from_fingerprint(&resource.fingerprint);
    let mut yaml = format!(
        "resource:\n  fingerprint: \"{}\"\n\nname: {}\ndescription: \"\"\n",
        resource.fingerprint.0, name,
    );
    yaml.push_str("owner:\n  team: \"\"\n  contact: \"\"\ntags: []\n");
    yaml.push_str("columns: {}\ncustom: {}\n");
    yaml
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::ResourceBinding;

    fn make_graph(bindings: Vec<ResourceBinding>) -> LineageGraph {
        LineageGraph {
            edges: Vec::new(),
            bindings,
        }
    }

    fn make_binding(
        _pipeline: &str,
        node: &str,
        dir: BindingDirection,
        fp: &str,
    ) -> ResourceBinding {
        ResourceBinding {
            pipeline_id: PipelineId(uuid::Uuid::nil()),
            node_id: node.to_string(),
            direction: dir,
            fingerprint: ResourceFingerprint::new(fp),
        }
    }

    // -- Annotation parsing --

    #[test]
    fn parse_full_annotation() {
        let yaml = r#"
resource:
  fingerprint: "postgres://host:5432/db/public.orders"
  environment: prod
name: Orders
description: "All customer orders"
owner:
  team: commerce
  contact: team@example.com
tags:
  - commerce
  - pii
columns:
  order_id:
    description: Primary key
  amount:
    description: USD cents
    accepted_values: null
custom:
  retention: 7y
"#;
        let ann = parse_annotation(yaml).unwrap();
        assert_eq!(
            ann.resource.fingerprint,
            "postgres://host:5432/db/public.orders"
        );
        assert_eq!(ann.name.as_deref(), Some("Orders"));
        assert_eq!(ann.tags, vec!["commerce", "pii"]);
        assert_eq!(ann.columns.len(), 2);
        assert_eq!(
            ann.columns["order_id"].description.as_deref(),
            Some("Primary key")
        );
    }

    #[test]
    fn parse_minimal_annotation() {
        let yaml = "resource:\n  fingerprint: \"s3://bucket/path\"\n";
        let ann = parse_annotation(yaml).unwrap();
        assert_eq!(ann.resource.fingerprint, "s3://bucket/path");
        assert!(ann.name.is_none());
        assert!(ann.tags.is_empty());
        assert!(ann.columns.is_empty());
    }

    // -- Filename normalization --

    #[test]
    fn filename_postgres() {
        let fp = ResourceFingerprint::new("postgres://host:5432/db/public.orders");
        let path = fingerprint_to_filename(&fp);
        assert_eq!(
            path,
            PathBuf::from("postgres/host__5432__db__public.orders.yaml")
        );
    }

    #[test]
    fn filename_file() {
        let fp = ResourceFingerprint::new("file:///data/curated/orders.csv");
        let path = fingerprint_to_filename(&fp);
        assert_eq!(path, PathBuf::from("files/data__curated__orders.csv.yaml"));
    }

    #[test]
    fn filename_s3() {
        let fp = ResourceFingerprint::new("s3://my-bucket/exports/parquet");
        let path = fingerprint_to_filename(&fp);
        assert_eq!(path, PathBuf::from("s3/my-bucket__exports__parquet.yaml"));
    }

    // -- Resource discovery --

    #[test]
    fn discover_groups_by_direction() {
        let graph = make_graph(vec![
            make_binding(
                "p1",
                "sink1",
                BindingDirection::Sink,
                "postgres://h:5432/db/public.t",
            ),
            make_binding(
                "p2",
                "src1",
                BindingDirection::Source,
                "postgres://h:5432/db/public.t",
            ),
        ]);
        let resources = discover_resources(&graph);
        assert_eq!(resources.len(), 1);
        let r = &resources[&ResourceFingerprint::new("postgres://h:5432/db/public.t")];
        assert_eq!(r.producers.len(), 1);
        assert_eq!(r.consumers.len(), 1);
        assert_eq!(r.resource_type.as_deref(), Some("postgres"));
    }

    // -- Merge logic --

    #[test]
    fn merge_with_annotation() {
        let fp = ResourceFingerprint::new("postgres://h:5432/db/public.orders");
        let mut discovered = HashMap::new();
        discovered.insert(
            fp.clone(),
            DiscoveredResource {
                fingerprint: fp.clone(),
                resource_type: Some("postgres".to_string()),
                producers: vec![PipelineBinding {
                    pipeline_id: PipelineId(uuid::Uuid::nil()),
                    node_id: "sink1".to_string(),
                }],
                consumers: Vec::new(),
            },
        );

        let mut annotations = HashMap::new();
        annotations.insert(
            fp.clone(),
            AnnotationFile {
                annotation: ResourceAnnotation {
                    resource: AnnotationResource {
                        fingerprint: fp.0.clone(),
                        environment: Some("prod".to_string()),
                    },
                    name: Some("Orders".to_string()),
                    description: Some("All orders".to_string()),
                    owner: Some(AnnotationOwner {
                        team: Some("commerce".to_string()),
                        contact: None,
                    }),
                    tags: vec!["pii".to_string()],
                    columns: BTreeMap::new(),
                    custom: BTreeMap::new(),
                    sla: None,
                },
                path: PathBuf::from("metadata/postgres/orders.yaml"),
            },
        );

        let entries = merge_catalog(&discovered, &annotations);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "Orders");
        assert_eq!(entries[0].description.as_deref(), Some("All orders"));
        assert_eq!(entries[0].tags, vec!["pii"]);
        assert_eq!(entries[0].environment.as_deref(), Some("prod"));
    }

    #[test]
    fn merge_without_annotation_uses_default_name() {
        let fp = ResourceFingerprint::new("file:///data/orders.csv");
        let mut discovered = HashMap::new();
        discovered.insert(
            fp.clone(),
            DiscoveredResource {
                fingerprint: fp.clone(),
                resource_type: Some("csv".to_string()),
                producers: Vec::new(),
                consumers: Vec::new(),
            },
        );

        let entries = merge_catalog(&discovered, &HashMap::new());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "orders.csv");
        assert!(entries[0].description.is_none());
        assert!(entries[0].tags.is_empty());
    }

    // -- Column merge --

    #[test]
    fn merge_columns_combines_schema_and_annotations() {
        let schema = vec![
            SchemaColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            },
            SchemaColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
            },
        ];
        let mut annotations = BTreeMap::new();
        annotations.insert(
            "id".to_string(),
            ColumnAnnotation {
                description: Some("Primary key".to_string()),
                accepted_values: None,
            },
        );
        annotations.insert(
            "extra".to_string(),
            ColumnAnnotation {
                description: Some("Annotation-only column".to_string()),
                accepted_values: None,
            },
        );

        let merged = merge_columns(&schema, &annotations);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].name, "id");
        assert_eq!(merged[0].data_type.as_deref(), Some("Int64"));
        assert_eq!(merged[0].description.as_deref(), Some("Primary key"));
        assert_eq!(merged[1].name, "name");
        assert!(merged[1].description.is_none());
        assert_eq!(merged[2].name, "extra");
        assert!(merged[2].data_type.is_none());
        assert_eq!(
            merged[2].description.as_deref(),
            Some("Annotation-only column")
        );
    }

    // -- Dangling detection --

    #[test]
    fn detect_dangling_finds_orphaned_annotations() {
        let discovered = HashMap::new(); // no resources
        let fp = ResourceFingerprint::new("postgres://h:5432/db/public.gone");
        let mut annotations = HashMap::new();
        annotations.insert(
            fp.clone(),
            AnnotationFile {
                annotation: ResourceAnnotation {
                    resource: AnnotationResource {
                        fingerprint: fp.0.clone(),
                        environment: None,
                    },
                    name: None,
                    description: None,
                    owner: None,
                    tags: Vec::new(),
                    columns: BTreeMap::new(),
                    custom: BTreeMap::new(),
                    sla: None,
                },
                path: PathBuf::from("metadata/postgres/gone.yaml"),
            },
        );

        let warnings = detect_dangling(&discovered, &annotations);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(
            &warnings[0],
            CatalogWarning::DanglingAnnotation { .. }
        ));
    }

    // -- Search --

    #[test]
    fn search_finds_by_name() {
        let entries = vec![
            CatalogEntry {
                fingerprint: ResourceFingerprint::new("postgres://h/db/public.orders"),
                name: "Customer Orders".to_string(),
                description: None,
                owner: None,
                tags: Vec::new(),
                derived: AutoDerivedFacts::default(),
                columns: Vec::new(),
                custom: BTreeMap::new(),
                environment: None,
                annotation_file: None,
            },
            CatalogEntry {
                fingerprint: ResourceFingerprint::new("postgres://h/db/public.users"),
                name: "Users".to_string(),
                description: Some("All registered users".to_string()),
                owner: None,
                tags: vec!["pii".to_string()],
                derived: AutoDerivedFacts::default(),
                columns: Vec::new(),
                custom: BTreeMap::new(),
                environment: None,
                annotation_file: None,
            },
        ];

        let idx = SearchIndex::build(&entries);

        // Search by name.
        let results = idx.search("orders");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "postgres://h/db/public.orders");

        // Search by description.
        let results = idx.search("registered");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "postgres://h/db/public.users");

        // Search by tag.
        let results = idx.search("pii");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "postgres://h/db/public.users");
    }

    #[test]
    fn search_multi_token_intersects() {
        let entries = vec![CatalogEntry {
            fingerprint: ResourceFingerprint::new("fp1"),
            name: "Customer Orders".to_string(),
            description: None,
            owner: None,
            tags: vec!["commerce".to_string()],
            derived: AutoDerivedFacts::default(),
            columns: Vec::new(),
            custom: BTreeMap::new(),
            environment: None,
            annotation_file: None,
        }];

        let idx = SearchIndex::build(&entries);

        // Both tokens present → match.
        assert_eq!(idx.search("customer orders").len(), 1);
        // One token missing → no match.
        assert_eq!(idx.search("customer nonexistent").len(), 0);
    }

    // -- Scaffold --

    #[test]
    fn scaffold_produces_valid_yaml() {
        let resource = DiscoveredResource {
            fingerprint: ResourceFingerprint::new("postgres://h:5432/db/public.orders"),
            resource_type: Some("postgres".to_string()),
            producers: Vec::new(),
            consumers: Vec::new(),
        };
        let yaml = scaffold_annotation(&resource);
        assert!(yaml.contains("fingerprint: \"postgres://h:5432/db/public.orders\""));
        assert!(yaml.contains("name: public.orders"));
        // Should be parseable.
        let _ann: ResourceAnnotation = serde_yaml::from_str(&yaml).unwrap();
    }

    // -- Default name derivation --

    #[test]
    fn default_name_extracts_last_segment() {
        assert_eq!(
            default_name_from_fingerprint(&ResourceFingerprint::new(
                "postgres://h:5432/db/public.orders"
            )),
            "public.orders"
        );
        assert_eq!(
            default_name_from_fingerprint(&ResourceFingerprint::new(
                "file:///data/curated/orders.csv"
            )),
            "orders.csv"
        );
        assert_eq!(
            default_name_from_fingerprint(&ResourceFingerprint::new(
                "s3://bucket/exports/data.parquet"
            )),
            "data.parquet"
        );
    }

    // -- Annotation file I/O (using tempdir) --

    #[test]
    fn load_annotations_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let pg_dir = dir.path().join("postgres");
        std::fs::create_dir_all(&pg_dir).unwrap();

        let yaml = r#"
resource:
  fingerprint: "postgres://h:5432/db/public.orders"
name: Orders
tags: [commerce]
"#;
        std::fs::write(pg_dir.join("orders.yaml"), yaml).unwrap();

        let (annotations, errors) = load_annotations(dir.path());
        assert!(errors.is_empty());
        assert_eq!(annotations.len(), 1);
        let fp = ResourceFingerprint::new("postgres://h:5432/db/public.orders");
        assert!(annotations.contains_key(&fp));
        assert_eq!(annotations[&fp].annotation.name.as_deref(), Some("Orders"));
    }

    // -- Duplicate detection --

    #[test]
    fn detect_duplicates_from_files_catches_dupes() {
        let dir = tempfile::tempdir().unwrap();
        let sub1 = dir.path().join("a");
        let sub2 = dir.path().join("b");
        std::fs::create_dir_all(&sub1).unwrap();
        std::fs::create_dir_all(&sub2).unwrap();

        let yaml = "resource:\n  fingerprint: \"fp://same\"\nname: A\n";
        std::fs::write(sub1.join("a.yaml"), yaml).unwrap();
        std::fs::write(sub2.join("b.yaml"), yaml).unwrap();

        let (annotations, warnings, errors) = detect_duplicates_from_files(dir.path());
        assert!(errors.is_empty());
        assert_eq!(annotations.len(), 1); // deduped in map
        assert_eq!(warnings.len(), 1);
        assert!(
            matches!(&warnings[0], CatalogWarning::DuplicateAnnotation { paths, .. } if paths.len() == 2)
        );
    }

    // -- Full catalog build --

    #[test]
    fn catalog_build_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("postgres")).unwrap();
        let yaml = r#"
resource:
  fingerprint: "postgres://h:5432/db/public.orders"
name: Orders
tags: [commerce, pii]
owner:
  team: platform
"#;
        std::fs::write(dir.path().join("postgres/orders.yaml"), yaml).unwrap();

        let graph = make_graph(vec![
            make_binding(
                "p1",
                "sink1",
                BindingDirection::Sink,
                "postgres://h:5432/db/public.orders",
            ),
            make_binding(
                "p2",
                "src1",
                BindingDirection::Source,
                "postgres://h:5432/db/public.orders",
            ),
            make_binding(
                "p3",
                "src2",
                BindingDirection::Source,
                "file:///data/raw.csv",
            ),
        ]);

        let catalog = Catalog::build(&graph, dir.path());
        assert_eq!(catalog.len(), 2);
        assert!(catalog.warnings.is_empty());

        // Search.
        let results = catalog.search("orders");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Orders");

        // Filter by tag.
        assert_eq!(catalog.filter_by_tag("pii").len(), 1);
        assert_eq!(catalog.filter_by_tag("commerce").len(), 1);
        assert_eq!(catalog.filter_by_tag("nonexistent").len(), 0);

        // Filter by owner.
        assert_eq!(catalog.filter_by_owner("platform").len(), 1);

        // All tags / owners.
        assert_eq!(catalog.all_tags(), vec!["commerce", "pii"]);
        assert_eq!(catalog.all_owners(), vec!["platform"]);

        // Unannotated resource still present.
        let csv = catalog
            .get(&ResourceFingerprint::new("file:///data/raw.csv"))
            .unwrap();
        assert_eq!(csv.name, "raw.csv");
    }
}
