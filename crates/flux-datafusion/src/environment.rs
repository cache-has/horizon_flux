// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Environment data model and SQLite-backed persistence.
//!
//! Each environment has a name, an optional fallback environment, and tracks
//! which tables have overrides registered in it. The default environments are
//! `prod` (no fallback) and `dev` (falls back to `prod`).

use crate::error::EnvironmentError;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

/// An environment definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Environment {
    /// Unique name (e.g. "prod", "dev", "staging").
    pub name: String,
    /// Name of the fallback environment, or `None` for the root (prod).
    pub fallback: Option<String>,
}

/// A record of a table override registered in a specific environment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableOverride {
    pub environment: String,
    pub schema_name: String,
    pub table_name: String,
}

/// SQLite-backed storage for environment metadata and table override tracking.
pub struct EnvironmentStore {
    conn: Mutex<Connection>,
}

impl EnvironmentStore {
    /// Open (or create) an environment store at the given file path.
    pub fn open(path: &Path) -> Result<Self, EnvironmentError> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        store.ensure_defaults()?;
        Ok(store)
    }

    /// Open an in-memory environment store (useful for tests).
    pub fn open_in_memory() -> Result<Self, EnvironmentError> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        store.ensure_defaults()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS environments (
                name     TEXT PRIMARY KEY,
                fallback TEXT REFERENCES environments(name)
            );

            CREATE TABLE IF NOT EXISTS table_overrides (
                environment TEXT NOT NULL REFERENCES environments(name) ON DELETE CASCADE,
                schema_name TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                PRIMARY KEY (environment, schema_name, table_name)
            );",
        )?;
        Ok(())
    }

    /// Ensure default `prod` and `dev` environments exist.
    fn ensure_defaults(&self) -> Result<(), EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO environments (name, fallback) VALUES (?1, NULL)",
            params!["prod"],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO environments (name, fallback) VALUES (?1, ?2)",
            params!["dev", "prod"],
        )?;
        Ok(())
    }

    /// Create a new environment. Returns an error if it already exists or the
    /// fallback environment does not exist.
    pub fn create(
        &self,
        name: &str,
        fallback: Option<&str>,
    ) -> Result<Environment, EnvironmentError> {
        let conn = self.conn.lock().unwrap();

        if let Some(fb) = fallback {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM environments WHERE name = ?1)",
                params![fb],
                |row| row.get(0),
            )?;
            if !exists {
                return Err(EnvironmentError::FallbackNotFound(fb.to_string()));
            }
        }

        conn.execute(
            "INSERT INTO environments (name, fallback) VALUES (?1, ?2)",
            params![name, fallback],
        )
        .map_err(|e| match e {
            rusqlite::Error::SqliteFailure(err, _)
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                EnvironmentError::AlreadyExists(name.to_string())
            }
            other => EnvironmentError::Sqlite(other),
        })?;

        Ok(Environment {
            name: name.to_string(),
            fallback: fallback.map(String::from),
        })
    }

    /// Delete an environment. Cannot delete `prod`.
    pub fn delete(&self, name: &str) -> Result<(), EnvironmentError> {
        if name == "prod" {
            return Err(EnvironmentError::CannotDeleteProd);
        }

        let conn = self.conn.lock().unwrap();

        // Re-point any environments that fall back to this one to this one's fallback.
        let fallback_of_deleted: Option<String> = conn
            .query_row(
                "SELECT fallback FROM environments WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    EnvironmentError::NotFound(name.to_string())
                }
                other => EnvironmentError::Sqlite(other),
            })?;

        conn.execute(
            "UPDATE environments SET fallback = ?1 WHERE fallback = ?2",
            params![fallback_of_deleted, name],
        )?;

        conn.execute(
            "DELETE FROM table_overrides WHERE environment = ?1",
            params![name],
        )?;

        conn.execute("DELETE FROM environments WHERE name = ?1", params![name])?;

        Ok(())
    }

    /// Get a single environment by name.
    pub fn get(&self, name: &str) -> Result<Option<Environment>, EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT name, fallback FROM environments WHERE name = ?1")?;
        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => Ok(Some(Environment {
                name: row.get(0)?,
                fallback: row.get(1)?,
            })),
            None => Ok(None),
        }
    }

    /// List all environments.
    pub fn list(&self) -> Result<Vec<Environment>, EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT name, fallback FROM environments ORDER BY name")?;
        let mut rows = stmt.query([])?;
        let mut envs = Vec::new();
        while let Some(row) = rows.next()? {
            envs.push(Environment {
                name: row.get(0)?,
                fallback: row.get(1)?,
            });
        }
        Ok(envs)
    }

    /// Update the fallback chain for an environment.
    pub fn update_fallback(
        &self,
        name: &str,
        fallback: Option<&str>,
    ) -> Result<(), EnvironmentError> {
        if name == "prod" && fallback.is_some() {
            return Err(EnvironmentError::ProdCannotHaveFallback);
        }

        let conn = self.conn.lock().unwrap();

        if let Some(fb) = fallback {
            if fb == name {
                return Err(EnvironmentError::CyclicFallback);
            }
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM environments WHERE name = ?1)",
                params![fb],
                |row| row.get(0),
            )?;
            if !exists {
                return Err(EnvironmentError::FallbackNotFound(fb.to_string()));
            }
        }

        let rows = conn.execute(
            "UPDATE environments SET fallback = ?1 WHERE name = ?2",
            params![fallback, name],
        )?;
        if rows == 0 {
            return Err(EnvironmentError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Compute the full fallback chain starting from the given environment.
    /// Returns the chain as a list of environment names (including the start).
    pub fn fallback_chain(&self, start: &str) -> Result<Vec<String>, EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        let mut chain = Vec::new();
        let mut current = start.to_string();

        loop {
            if chain.contains(&current) {
                return Err(EnvironmentError::CyclicFallback);
            }
            chain.push(current.clone());

            let fallback: Option<String> = conn
                .query_row(
                    "SELECT fallback FROM environments WHERE name = ?1",
                    params![current],
                    |row| row.get(0),
                )
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => {
                        EnvironmentError::NotFound(current.clone())
                    }
                    other => EnvironmentError::Sqlite(other),
                })?;

            match fallback {
                Some(fb) => current = fb,
                None => break,
            }
        }

        Ok(chain)
    }

    /// Record that a table override exists in the given environment.
    pub fn register_table_override(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO table_overrides (environment, schema_name, table_name)
             VALUES (?1, ?2, ?3)",
            params![environment, schema_name, table_name],
        )?;
        Ok(())
    }

    /// Remove a table override from an environment.
    pub fn deregister_table_override(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool, EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM table_overrides
             WHERE environment = ?1 AND schema_name = ?2 AND table_name = ?3",
            params![environment, schema_name, table_name],
        )?;
        Ok(rows > 0)
    }

    /// List all table overrides in an environment.
    pub fn list_table_overrides(
        &self,
        environment: &str,
    ) -> Result<Vec<TableOverride>, EnvironmentError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT environment, schema_name, table_name
             FROM table_overrides WHERE environment = ?1
             ORDER BY schema_name, table_name",
        )?;
        let mut rows = stmt.query(params![environment])?;
        let mut overrides = Vec::new();
        while let Some(row) = rows.next()? {
            overrides.push(TableOverride {
                environment: row.get(0)?,
                schema_name: row.get(1)?,
                table_name: row.get(2)?,
            });
        }
        Ok(overrides)
    }
}
