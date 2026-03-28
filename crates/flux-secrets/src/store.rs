// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite-backed encrypted secret store.
//!
//! Secrets are encrypted at rest using AES-256-GCM with a master key derived
//! from a user password via Argon2id. The master key salt and a verification
//! token are stored in the database metadata table.

use std::path::{Path, PathBuf};

use chrono::Utc;
use rusqlite::{Connection, params};
use tracing::debug;

use crate::crypto;
use crate::error::SecretError;

/// Metadata about a stored secret (never includes the decrypted value).
#[derive(Debug, Clone)]
pub struct SecretMetadata {
    pub name: String,
    pub environment: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// An encrypted secret store backed by SQLite.
pub struct SecretStore {
    conn: Connection,
    key: [u8; 32],
}

impl SecretStore {
    /// Default store directory: `~/.horizon-flux/`.
    pub fn default_dir() -> Option<PathBuf> {
        dirs::home_dir().map(|h| h.join(".horizon-flux"))
    }

    /// Default store database path: `~/.horizon-flux/secrets.db`.
    pub fn default_path() -> Option<PathBuf> {
        Self::default_dir().map(|d| d.join("secrets.db"))
    }

    /// Initialize a new secret store at the given path.
    ///
    /// Creates the database, schema, and stores the Argon2 salt plus a
    /// verification token so that future opens can confirm the password.
    pub fn init(path: &Path, password: &[u8]) -> Result<Self, SecretError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(path)?;
        Self::create_schema(&conn)?;

        // Check if already initialized.
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM metadata", [], |r| r.get(0))?;
        if count > 0 {
            return Err(SecretError::AlreadyInitialized);
        }

        let salt = crypto::generate_salt();
        let key = crypto::derive_key(password, &salt)?;

        // Store salt and an encrypted verification token so we can confirm
        // the password on subsequent opens.
        let verification = crypto::encrypt(&key, b"horizon-flux-secrets")?;

        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('salt', ?1)",
            params![salt.to_vec()],
        )?;
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('verification', ?1)",
            params![verification],
        )?;

        debug!("Secret store initialized at {}", path.display());
        Ok(Self { conn, key })
    }

    /// Open an existing secret store, verifying the password.
    pub fn open(path: &Path, password: &[u8]) -> Result<Self, SecretError> {
        if !path.exists() {
            return Err(SecretError::NotInitialized);
        }

        let conn = Connection::open(path)?;

        // Read salt.
        let salt: Vec<u8> = conn
            .query_row("SELECT value FROM metadata WHERE key = 'salt'", [], |r| {
                r.get(0)
            })
            .map_err(|_| SecretError::NotInitialized)?;

        let key = crypto::derive_key(password, &salt)?;

        // Verify password by decrypting the verification token.
        let verification: Vec<u8> = conn.query_row(
            "SELECT value FROM metadata WHERE key = 'verification'",
            [],
            |r| r.get(0),
        )?;

        let plaintext = crypto::decrypt(&key, &verification)
            .map_err(|_| SecretError::Decryption("incorrect password".to_string()))?;

        if plaintext != b"horizon-flux-secrets" {
            return Err(SecretError::Decryption("incorrect password".to_string()));
        }

        debug!("Secret store opened at {}", path.display());
        Ok(Self { conn, key })
    }

    /// Open an existing store, or initialize a new one if it doesn't exist.
    pub fn open_or_init(path: &Path, password: &[u8]) -> Result<Self, SecretError> {
        if path.exists() {
            Self::open(path, password)
        } else {
            Self::init(path, password)
        }
    }

    /// Set (create or update) a secret.
    pub fn set(
        &self,
        name: &str,
        value: &[u8],
        environment: Option<&str>,
    ) -> Result<(), SecretError> {
        let encrypted = crypto::encrypt(&self.key, value)?;
        let now = Utc::now().to_rfc3339();
        let env = environment.unwrap_or("");

        self.conn.execute(
            "INSERT INTO secrets (name, environment, encrypted_value, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)
             ON CONFLICT(name, environment) DO UPDATE SET
                encrypted_value = excluded.encrypted_value,
                updated_at = excluded.updated_at",
            params![name, env, encrypted, now],
        )?;

        debug!(name, environment, "Secret stored");
        Ok(())
    }

    /// Get a decrypted secret value (internal use only — never expose via API).
    pub fn get(&self, name: &str, environment: Option<&str>) -> Result<Vec<u8>, SecretError> {
        let env = environment.unwrap_or("");

        let encrypted: Vec<u8> = self
            .conn
            .query_row(
                "SELECT encrypted_value FROM secrets WHERE name = ?1 AND environment = ?2",
                params![name, env],
                |r| r.get(0),
            )
            .map_err(|_| SecretError::NotFound(name.to_string()))?;

        crypto::decrypt(&self.key, &encrypted)
    }

    /// Resolve a secret with environment fallback.
    ///
    /// Tries environment-specific first, then falls back to the default
    /// (unscoped) secret.
    pub fn resolve(&self, name: &str, environment: Option<&str>) -> Result<Vec<u8>, SecretError> {
        if let Some(env) = environment {
            // Try environment-specific first.
            match self.get(name, Some(env)) {
                Ok(v) => return Ok(v),
                Err(SecretError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }

        // Fall back to default (unscoped).
        self.get(name, None)
    }

    /// List all secret metadata (names and environments, never values).
    pub fn list(&self) -> Result<Vec<SecretMetadata>, SecretError> {
        let mut stmt = self.conn.prepare(
            "SELECT name, environment, created_at, updated_at FROM secrets ORDER BY name, environment",
        )?;

        let rows = stmt.query_map([], |row| {
            let env: String = row.get(1)?;
            Ok(SecretMetadata {
                name: row.get(0)?,
                environment: if env.is_empty() { None } else { Some(env) },
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Delete a secret.
    pub fn delete(&self, name: &str, environment: Option<&str>) -> Result<(), SecretError> {
        let env = environment.unwrap_or("");
        let changes = self.conn.execute(
            "DELETE FROM secrets WHERE name = ?1 AND environment = ?2",
            params![name, env],
        )?;

        if changes == 0 {
            return Err(SecretError::NotFound(name.to_string()));
        }

        debug!(name, environment, "Secret deleted");
        Ok(())
    }

    /// Check whether the store has been initialized (has the verification
    /// token).
    pub fn is_initialized(path: &Path) -> bool {
        if !path.exists() {
            return false;
        }
        let Ok(conn) = Connection::open(path) else {
            return false;
        };
        conn.query_row("SELECT 1 FROM metadata WHERE key = 'salt'", [], |_| Ok(()))
            .is_ok()
    }

    fn create_schema(conn: &Connection) -> Result<(), SecretError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS secrets (
                name            TEXT NOT NULL,
                environment     TEXT NOT NULL DEFAULT '',
                encrypted_value BLOB NOT NULL,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                PRIMARY KEY (name, environment)
            );",
        )?;
        Ok(())
    }
}
