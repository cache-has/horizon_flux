// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared application state for all API handlers.

use flux_connectors::ConnectorRegistry;
use flux_datafusion::{
    BackfillStorage, ColumnLineageStorage, EnvironmentStorage, ExecutionEvent,
    IncrementalStateStorage, LineageStorage, OutputCache, RunStorage, SecretResolver,
    SessionFactory, SlaStorage,
};
use flux_engine::PipelineStorage;
use flux_plugin_host::PluginRegistry;
use flux_scheduler::{Scheduler, TriggerStorage};
use flux_secrets::SecretStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Default capacity for the execution event broadcast channel.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Capacity for the plugin lifecycle event broadcast channel. Plugin events
/// are infrequent (only fire on discovery / reload), so a small buffer is
/// plenty.
const PLUGIN_EVENT_CHANNEL_CAPACITY: usize = 16;

/// Plugin lifecycle events broadcast over the WebSocket so the frontend can
/// react to install/reload without polling.
///
/// Serialized with a `type` tag matching the snake_case variant name, e.g.
/// `{"type":"plugin_registry_reloaded","count":3,"ok_count":3,"invalid_count":0}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginEvent {
    /// The plugin registry has been (re)scanned and swapped in.
    PluginRegistryReloaded {
        /// Total number of plugins discovered.
        count: usize,
        /// Number with `status: ok`.
        ok_count: usize,
        /// Number with `status: invalid`.
        invalid_count: usize,
    },
}

/// Catalog lifecycle events broadcast over the WebSocket so the frontend can
/// react to metadata annotation changes without polling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogEvent {
    /// A resource's annotation metadata was created or updated.
    MetadataUpdated {
        /// The resource fingerprint whose metadata changed.
        fingerprint: String,
    },
}

/// Capacity for the catalog event broadcast channel.
const CATALOG_EVENT_CHANNEL_CAPACITY: usize = 32;

/// Capacity for the column lineage event broadcast channel.
const COLUMN_LINEAGE_EVENT_CHANNEL_CAPACITY: usize = 32;

/// Column lineage lifecycle events broadcast over the WebSocket so the
/// frontend can refresh lineage views without polling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ColumnLineageEvent {
    /// Column lineage edges were re-derived for a pipeline.
    ColumnLineageUpdated {
        /// The pipeline whose lineage was updated.
        pipeline_id: String,
        /// The environment in which lineage was derived.
        environment: String,
        /// Number of edges in the new lineage set.
        edge_count: usize,
    },
}

/// How long the secret store stays unlocked without activity (30 minutes).
const SECRET_AUTO_LOCK_SECS: u64 = 30 * 60;

/// Maximum unlock attempts per rate-limit window.
const SECRET_MAX_UNLOCK_ATTEMPTS: usize = 5;

/// Rate-limit window for unlock attempts (60 seconds).
const SECRET_RATE_LIMIT_WINDOW_SECS: u64 = 60;

/// Manages the secret store session: unlock state, auto-lock timeout, and rate
/// limiting for password attempts.
pub struct SecretSession {
    store: Option<SecretStore>,
    last_activity: Option<Instant>,
    unlock_attempts: VecDeque<Instant>,
    pub store_path: PathBuf,
    auto_lock_duration: Duration,
}

impl SecretSession {
    /// Create a new locked session pointing at the given store path.
    pub fn new(store_path: PathBuf) -> Self {
        Self {
            store: None,
            last_activity: None,
            unlock_attempts: VecDeque::new(),
            store_path,
            auto_lock_duration: Duration::from_secs(SECRET_AUTO_LOCK_SECS),
        }
    }

    /// Create a session that is already unlocked (e.g. from env-var password).
    pub fn new_unlocked(store: SecretStore, store_path: PathBuf) -> Self {
        Self {
            store: Some(store),
            last_activity: Some(Instant::now()),
            unlock_attempts: VecDeque::new(),
            store_path,
            auto_lock_duration: Duration::from_secs(SECRET_AUTO_LOCK_SECS),
        }
    }

    /// Whether the store database file exists and has been initialized.
    pub fn is_initialized(&self) -> bool {
        SecretStore::is_initialized(&self.store_path)
    }

    /// Whether the store is currently unlocked (and not auto-locked).
    pub fn is_unlocked(&self) -> bool {
        if self.store.is_none() {
            return false;
        }
        // Check auto-lock
        if let Some(last) = self.last_activity {
            if last.elapsed() >= self.auto_lock_duration {
                return false;
            }
        }
        true
    }

    /// Get a reference to the unlocked store, refreshing the activity timer.
    /// Returns `None` if locked or auto-lock has expired.
    pub fn get_store(&mut self) -> Option<&SecretStore> {
        if !self.is_unlocked() {
            // Auto-lock: clear the store if timeout expired
            if self.store.is_some() {
                self.store = None;
                self.last_activity = None;
            }
            return None;
        }
        self.last_activity = Some(Instant::now());
        self.store.as_ref()
    }

    /// Check rate limiting. Returns `true` if the attempt is allowed.
    pub fn check_rate_limit(&mut self) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(SECRET_RATE_LIMIT_WINDOW_SECS);
        // Prune old attempts
        while self
            .unlock_attempts
            .front()
            .is_some_and(|t| now.duration_since(*t) >= window)
        {
            self.unlock_attempts.pop_front();
        }
        self.unlock_attempts.len() < SECRET_MAX_UNLOCK_ATTEMPTS
    }

    /// Record an unlock attempt (call after rate-limit check).
    pub fn record_attempt(&mut self) {
        self.unlock_attempts.push_back(Instant::now());
    }

    /// Unlock the store with the given password.
    pub fn unlock(&mut self, password: &[u8]) -> Result<(), flux_secrets::SecretError> {
        let store = SecretStore::open(&self.store_path, password)?;
        self.store = Some(store);
        self.last_activity = Some(Instant::now());
        Ok(())
    }

    /// Initialize a new store with the given password.
    pub fn init(&mut self, password: &[u8]) -> Result<(), flux_secrets::SecretError> {
        let store = SecretStore::init(&self.store_path, password)?;
        self.store = Some(store);
        self.last_activity = Some(Instant::now());
        Ok(())
    }

    /// Lock the store (clear from memory).
    pub fn lock(&mut self) {
        self.store = None;
        self.last_activity = None;
    }
}

/// Metadata backend information captured at server startup for the system info
/// endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataInfo {
    /// `"sqlite"` or `"postgresql"`.
    pub backend: String,
    /// Data directory path (always present, even for PostgreSQL — used for secrets, cache, etc.).
    pub data_dir: PathBuf,
    /// Redacted connection string for PostgreSQL, or `None` for SQLite.
    pub connection_string: Option<String>,
    /// How the backend was configured: `"--metadata-url flag"`, `"HORIZON_FLUX_METADATA_URL env var"`,
    /// `"config.toml"`, or `"default"`.
    pub config_source: String,
}

/// Redact the password component of a PostgreSQL connection string.
///
/// Handles both URI-style (`postgresql://user:pass@host/db`) and key-value-style
/// (`host=... password=secret ...`) connection strings.
pub fn redact_connection_string(conn: &str) -> String {
    // URI style: postgresql://user:password@host...
    if let Some(scheme_end) = conn.find("://") {
        let prefix = &conn[..scheme_end + 3];
        let rest = &conn[scheme_end + 3..];
        // Find the @ that separates userinfo from host
        if let Some(at_pos) = rest.find('@') {
            let userinfo = &rest[..at_pos];
            let after_at = &rest[at_pos..];
            // Redact the password portion (after the colon in userinfo)
            if let Some(colon) = userinfo.find(':') {
                let user = &userinfo[..colon];
                return format!("{prefix}{user}:***{after_at}");
            }
        }
        return conn.to_string();
    }

    // Key-value style: host=localhost password=secret dbname=flux
    let mut result = String::with_capacity(conn.len());
    let mut remaining = conn;
    while let Some(pos) = remaining.find("password=") {
        result.push_str(&remaining[..pos]);
        result.push_str("password=***");
        let after_key = &remaining[pos + 9..];
        // Skip until next space or end
        match after_key.find(' ') {
            Some(space) => remaining = &after_key[space..],
            None => {
                remaining = "";
                break;
            }
        }
    }
    result.push_str(remaining);
    result
}

/// Shared state available to all request handlers via Axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    pub pipeline_store: Arc<dyn PipelineStorage>,
    pub run_store: Arc<dyn RunStorage>,
    /// Incremental sink materialization state (planning doc 27). Backed by
    /// the same database as `run_store` for both SQLite and PostgreSQL.
    pub incremental_state_store: Arc<dyn IncrementalStateStorage>,
    /// Cross-pipeline lineage storage (planning doc 31). Backed by the same
    /// database as `run_store` for both SQLite and PostgreSQL.
    pub lineage_store: Arc<dyn LineageStorage>,
    pub connector_registry: Arc<ConnectorRegistry>,
    pub environment_store: Arc<dyn EnvironmentStorage>,
    /// Secret store session with unlock/lock lifecycle and auto-lock timeout.
    pub secret_session: Arc<Mutex<SecretSession>>,
    /// Broadcast channel for real-time execution events (WebSocket consumers
    /// subscribe via `event_tx.subscribe()`).
    pub event_tx: broadcast::Sender<ExecutionEvent>,
    /// Broadcast channel for plugin lifecycle events (registry reloads,
    /// status transitions). WebSocket clients receive these alongside
    /// [`ExecutionEvent`]s on `/ws`.
    pub plugin_event_tx: broadcast::Sender<PluginEvent>,
    /// On-disk cache for materialized node outputs (preview reads from here).
    pub output_cache: Arc<OutputCache>,
    /// Shared DataFusion session factory with memory pool and spill-to-disk.
    pub session_factory: Option<Arc<SessionFactory>>,
    /// Metadata backend info for the system info endpoint.
    pub metadata_info: MetadataInfo,
    /// Discovered plugin registry. Wrapped in `RwLock` so the
    /// `POST /api/plugins/reload` endpoint can swap it without restarting
    /// the server. Handlers should clone the inner `Arc` for cheap reads.
    pub plugin_registry: Arc<RwLock<Arc<PluginRegistry>>>,
    /// Backfill metadata storage (planning doc 33).
    pub backfill_store: Arc<dyn BackfillStorage>,
    /// Trigger storage for the scheduler (planning doc 32).
    pub trigger_store: Arc<dyn TriggerStorage>,
    /// Scheduler instance for firing triggers (sensors, webhook, completion).
    /// `None` when the scheduler is not enabled (e.g. in test harnesses).
    pub scheduler: Option<Arc<Scheduler>>,
    /// Working directory used for plugin discovery (workspace-local
    /// `./plugins/` resolution). Captured at startup so reload remains
    /// consistent even if the process `cwd` changes later.
    pub plugin_cwd: PathBuf,
    /// Optional explicit override for the plugin scan roots. When `Some`,
    /// the plugin reload endpoint scans **only** these roots and skips the
    /// platform-derived locations entirely. Used by integration tests so
    /// the developer machine's installed plugins do not leak in. Production
    /// leaves this `None` to get the full default discovery behavior.
    pub plugin_scan_roots: Option<Vec<PathBuf>>,
    /// Path to the `metadata/` directory for resource catalog annotations
    /// (planning doc 34). `None` disables the catalog feature.
    pub metadata_dir: Option<PathBuf>,
    /// Broadcast channel for catalog events (metadata annotation changes).
    /// WebSocket clients receive these alongside execution and plugin events.
    pub catalog_event_tx: broadcast::Sender<CatalogEvent>,
    /// Column-level lineage storage (planning doc 35). `None` when the
    /// backend does not implement `ColumnLineageStorage` (e.g. PostgreSQL).
    pub column_lineage_store: Option<Arc<dyn ColumnLineageStorage>>,
    /// Broadcast channel for column lineage events. Fired when lineage
    /// edges are re-derived so the frontend can refresh lineage views.
    pub column_lineage_event_tx: broadcast::Sender<ColumnLineageEvent>,
    /// Optional OpenLineage client for emitting lineage events to external
    /// catalogs (planning doc 36, sub-feature 4).
    pub openlineage_client: Option<Arc<flux_observability::openlineage::OpenLineageClient>>,
    /// SLA evaluation storage (planning doc 37, sub-feature 3). `None` when
    /// the backend has not been configured.
    pub sla_store: Option<Arc<dyn SlaStorage>>,
}

impl AppState {
    /// Create a new broadcast sender for execution events.
    pub fn new_event_channel() -> broadcast::Sender<ExecutionEvent> {
        broadcast::channel(EVENT_CHANNEL_CAPACITY).0
    }

    /// Create a new broadcast sender for plugin lifecycle events.
    pub fn new_plugin_event_channel() -> broadcast::Sender<PluginEvent> {
        broadcast::channel(PLUGIN_EVENT_CHANNEL_CAPACITY).0
    }

    /// Create a new broadcast sender for catalog events.
    pub fn new_catalog_event_channel() -> broadcast::Sender<CatalogEvent> {
        broadcast::channel(CATALOG_EVENT_CHANNEL_CAPACITY).0
    }

    /// Create a new broadcast sender for column lineage events.
    pub fn new_column_lineage_event_channel() -> broadcast::Sender<ColumnLineageEvent> {
        broadcast::channel(COLUMN_LINEAGE_EVENT_CHANNEL_CAPACITY).0
    }

    /// Build a [`SecretResolver`] backed by the current secret session.
    ///
    /// Returns `None` if the store is not initialized. The resolver will
    /// return errors at resolution time if the store is locked when a
    /// pipeline with `{{ secret:... }}` references is executed.
    pub fn secret_resolver(&self) -> Option<Arc<dyn SecretResolver>> {
        let session = self.secret_session.lock().ok()?;
        if !session.is_initialized() {
            return None;
        }
        drop(session);
        Some(Arc::new(SessionSecretResolver {
            session: Arc::clone(&self.secret_session),
        }))
    }
}

/// [`SecretResolver`] implementation backed by the server's [`SecretSession`].
///
/// Each call to [`resolve_json`] acquires the session mutex, checks that the
/// store is unlocked, and delegates to [`flux_secrets::resolve_json_secrets`].
struct SessionSecretResolver {
    session: Arc<Mutex<SecretSession>>,
}

impl SecretResolver for SessionSecretResolver {
    fn resolve_json(
        &self,
        value: &Value,
        environment: Option<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut session = self
            .session
            .lock()
            .map_err(|e| format!("secret session mutex poisoned: {e}"))?;
        let store = session.get_store().ok_or(
            "secret store is locked — unlock it before running pipelines with secret references",
        )?;
        flux_secrets::resolve_json_secrets(value, store, environment)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    fn resolve_json_collecting(
        &self,
        value: &Value,
        environment: Option<&str>,
    ) -> Result<(Value, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = self
            .session
            .lock()
            .map_err(|e| format!("secret session mutex poisoned: {e}"))?;
        let store = session.get_store().ok_or(
            "secret store is locked — unlock it before running pipelines with secret references",
        )?;
        flux_secrets::resolve_json_secrets_collecting(value, store, environment)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_uri_with_password() {
        assert_eq!(
            redact_connection_string("postgresql://user:s3cret@localhost:5432/mydb"),
            "postgresql://user:***@localhost:5432/mydb"
        );
    }

    #[test]
    fn redact_uri_without_password() {
        let input = "postgresql://localhost:5432/mydb";
        assert_eq!(redact_connection_string(input), input);
    }

    #[test]
    fn redact_uri_user_only() {
        let input = "postgresql://user@localhost/mydb";
        assert_eq!(redact_connection_string(input), input);
    }

    #[test]
    fn redact_key_value_style() {
        assert_eq!(
            redact_connection_string("host=localhost password=s3cret dbname=mydb"),
            "host=localhost password=*** dbname=mydb"
        );
    }

    #[test]
    fn redact_key_value_password_at_end() {
        assert_eq!(
            redact_connection_string("host=localhost password=s3cret"),
            "host=localhost password=***"
        );
    }

    #[test]
    fn redact_no_password() {
        let input = "host=localhost dbname=mydb";
        assert_eq!(redact_connection_string(input), input);
    }
}
