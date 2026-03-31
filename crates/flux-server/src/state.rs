// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared application state for all API handlers.

use flux_connectors::ConnectorRegistry;
use flux_datafusion::{
    EnvironmentStorage, ExecutionEvent, OutputCache, RunStorage, SecretResolver, SessionFactory,
};
use flux_engine::PipelineStorage;
use flux_secrets::SecretStore;
use serde_json::Value;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Default capacity for the execution event broadcast channel.
const EVENT_CHANNEL_CAPACITY: usize = 256;

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

/// Shared state available to all request handlers via Axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    pub pipeline_store: Arc<dyn PipelineStorage>,
    pub run_store: Arc<dyn RunStorage>,
    pub connector_registry: Arc<ConnectorRegistry>,
    pub environment_store: Arc<dyn EnvironmentStorage>,
    /// Secret store session with unlock/lock lifecycle and auto-lock timeout.
    pub secret_session: Arc<Mutex<SecretSession>>,
    /// Broadcast channel for real-time execution events (WebSocket consumers
    /// subscribe via `event_tx.subscribe()`).
    pub event_tx: broadcast::Sender<ExecutionEvent>,
    /// On-disk cache for materialized node outputs (preview reads from here).
    pub output_cache: Arc<OutputCache>,
    /// Shared DataFusion session factory with memory pool and spill-to-disk.
    pub session_factory: Option<Arc<SessionFactory>>,
}

impl AppState {
    /// Create a new broadcast sender for execution events.
    pub fn new_event_channel() -> broadcast::Sender<ExecutionEvent> {
        broadcast::channel(EVENT_CHANNEL_CAPACITY).0
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
}
