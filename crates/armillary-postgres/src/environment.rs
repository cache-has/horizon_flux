// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL-backed storage for environment metadata and table overrides.

use crate::block_on;
use armillary_datafusion::environment::{Environment, TableOverride};
use armillary_datafusion::error::EnvironmentError;
use armillary_datafusion::storage::EnvironmentStorage;
use deadpool_postgres::Pool;

/// PostgreSQL-backed environment storage.
pub struct PostgresEnvironmentStore {
    pool: Pool,
}

impl PostgresEnvironmentStore {
    /// Create a new store backed by the given connection pool.
    ///
    /// Ensures the default `prod` and `dev` environments exist.
    pub fn new(pool: Pool) -> Result<Self, EnvironmentError> {
        let store = Self { pool };
        store.ensure_defaults()?;
        Ok(store)
    }

    fn ensure_defaults(&self) -> Result<(), EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            client
                .execute(
                    "INSERT INTO environments (name, fallback) VALUES ($1, NULL)
                     ON CONFLICT (name) DO NOTHING",
                    &[&"prod"],
                )
                .await
                .map_err(pg_err)?;
            client
                .execute(
                    "INSERT INTO environments (name, fallback) VALUES ($1, $2)
                     ON CONFLICT (name) DO NOTHING",
                    &[&"dev", &"prod"],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }
}

impl EnvironmentStorage for PostgresEnvironmentStore {
    fn create(&self, name: &str, fallback: Option<&str>) -> Result<Environment, EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            if let Some(fb) = fallback {
                let row = client
                    .query_opt("SELECT 1 FROM environments WHERE name = $1", &[&fb])
                    .await
                    .map_err(pg_err)?;
                if row.is_none() {
                    return Err(EnvironmentError::FallbackNotFound(fb.to_string()));
                }
            }

            let result = client
                .execute(
                    "INSERT INTO environments (name, fallback) VALUES ($1, $2)",
                    &[
                        &name,
                        &fallback as &(dyn tokio_postgres::types::ToSql + Sync),
                    ],
                )
                .await;

            match result {
                Ok(_) => Ok(Environment {
                    name: name.to_string(),
                    fallback: fallback.map(String::from),
                }),
                Err(e) => {
                    // Check for unique violation (SQLSTATE 23505).
                    if let Some(db_err) = e.as_db_error() {
                        if db_err.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                            return Err(EnvironmentError::AlreadyExists(name.to_string()));
                        }
                    }
                    Err(pg_err(e))
                }
            }
        })
    }

    fn delete(&self, name: &str) -> Result<(), EnvironmentError> {
        if name == "prod" {
            return Err(EnvironmentError::CannotDeleteProd);
        }

        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            // Get the fallback of the environment being deleted.
            let row = client
                .query_opt(
                    "SELECT fallback FROM environments WHERE name = $1",
                    &[&name],
                )
                .await
                .map_err(pg_err)?;

            let fallback_of_deleted: Option<String> = match row {
                Some(row) => row.get(0),
                None => return Err(EnvironmentError::NotFound(name.to_string())),
            };

            // Re-point children to the deleted environment's fallback.
            client
                .execute(
                    "UPDATE environments SET fallback = $1 WHERE fallback = $2",
                    &[
                        &fallback_of_deleted as &(dyn tokio_postgres::types::ToSql + Sync),
                        &name,
                    ],
                )
                .await
                .map_err(pg_err)?;

            // Delete table overrides (cascade should handle this, but be explicit).
            client
                .execute(
                    "DELETE FROM table_overrides WHERE environment = $1",
                    &[&name],
                )
                .await
                .map_err(pg_err)?;

            client
                .execute("DELETE FROM environments WHERE name = $1", &[&name])
                .await
                .map_err(pg_err)?;

            Ok(())
        })
    }

    fn get(&self, name: &str) -> Result<Option<Environment>, EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let row = client
                .query_opt(
                    "SELECT name, fallback FROM environments WHERE name = $1",
                    &[&name],
                )
                .await
                .map_err(pg_err)?;

            Ok(row.map(|r| Environment {
                name: r.get(0),
                fallback: r.get(1),
            }))
        })
    }

    fn list(&self) -> Result<Vec<Environment>, EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let rows = client
                .query("SELECT name, fallback FROM environments ORDER BY name", &[])
                .await
                .map_err(pg_err)?;

            Ok(rows
                .iter()
                .map(|r| Environment {
                    name: r.get(0),
                    fallback: r.get(1),
                })
                .collect())
        })
    }

    fn update_fallback(&self, name: &str, fallback: Option<&str>) -> Result<(), EnvironmentError> {
        if name == "prod" && fallback.is_some() {
            return Err(EnvironmentError::ProdCannotHaveFallback);
        }

        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            if let Some(fb) = fallback {
                if fb == name {
                    return Err(EnvironmentError::CyclicFallback);
                }
                let exists = client
                    .query_opt("SELECT 1 FROM environments WHERE name = $1", &[&fb])
                    .await
                    .map_err(pg_err)?;
                if exists.is_none() {
                    return Err(EnvironmentError::FallbackNotFound(fb.to_string()));
                }
            }

            let rows_affected = client
                .execute(
                    "UPDATE environments SET fallback = $1 WHERE name = $2",
                    &[
                        &fallback as &(dyn tokio_postgres::types::ToSql + Sync),
                        &name,
                    ],
                )
                .await
                .map_err(pg_err)?;
            if rows_affected == 0 {
                return Err(EnvironmentError::NotFound(name.to_string()));
            }
            Ok(())
        })
    }

    fn fallback_chain(&self, start: &str) -> Result<Vec<String>, EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let mut chain = Vec::new();
            let mut current = start.to_string();

            loop {
                if chain.contains(&current) {
                    return Err(EnvironmentError::CyclicFallback);
                }
                chain.push(current.clone());

                let row = client
                    .query_opt(
                        "SELECT fallback FROM environments WHERE name = $1",
                        &[&current],
                    )
                    .await
                    .map_err(pg_err)?;

                match row {
                    Some(row) => {
                        let fallback: Option<String> = row.get(0);
                        match fallback {
                            Some(fb) => current = fb,
                            None => break,
                        }
                    }
                    None => return Err(EnvironmentError::NotFound(current)),
                }
            }

            Ok(chain)
        })
    }

    fn register_table_override(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            client
                .execute(
                    "INSERT INTO table_overrides (environment, schema_name, table_name)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (environment, schema_name, table_name) DO NOTHING",
                    &[&environment, &schema_name, &table_name],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn deregister_table_override(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool, EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let rows_affected = client
                .execute(
                    "DELETE FROM table_overrides
                     WHERE environment = $1 AND schema_name = $2 AND table_name = $3",
                    &[&environment, &schema_name, &table_name],
                )
                .await
                .map_err(pg_err)?;
            Ok(rows_affected > 0)
        })
    }

    fn list_table_overrides(
        &self,
        environment: &str,
    ) -> Result<Vec<TableOverride>, EnvironmentError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let rows = client
                .query(
                    "SELECT environment, schema_name, table_name
                     FROM table_overrides WHERE environment = $1
                     ORDER BY schema_name, table_name",
                    &[&environment],
                )
                .await
                .map_err(pg_err)?;

            Ok(rows
                .iter()
                .map(|r| TableOverride {
                    environment: r.get(0),
                    schema_name: r.get(1),
                    table_name: r.get(2),
                })
                .collect())
        })
    }
}

fn pg_err(e: impl std::fmt::Display) -> EnvironmentError {
    EnvironmentError::Database(e.to_string())
}
