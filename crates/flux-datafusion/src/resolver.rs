// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! DataFusion catalog integration for environment-based table resolution.
//!
//! The [`EnvironmentResolver`] implements [`CatalogProviderList`] and provides
//! per-table fallback chain resolution across environments. When a query
//! references a table, the resolver walks the fallback chain (e.g. dev -> prod)
//! until the table is found.

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

/// Implements [`CatalogProviderList`] for environment-based resolution.
///
/// Presents a single default catalog (`datafusion`) that uses
/// [`FallbackSchemaProvider`] to resolve tables through the environment
/// fallback chain.
pub struct EnvironmentResolver {
    /// The active environment name (e.g. "dev").
    active_environment: String,
    /// Ordered fallback chain starting from the active environment (e.g. ["dev", "prod"]).
    fallback_chain: Vec<String>,
    /// Per-environment catalogs holding registered tables.
    catalogs: RwLock<HashMap<String, Arc<EnvironmentCatalog>>>,
}

impl fmt::Debug for EnvironmentResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentResolver")
            .field("active_environment", &self.active_environment)
            .field("fallback_chain", &self.fallback_chain)
            .finish()
    }
}

impl EnvironmentResolver {
    /// Create a new resolver for the given active environment and fallback chain.
    ///
    /// `fallback_chain` must start with the active environment and end at the
    /// root (e.g. `["dev", "prod"]`).
    pub fn new(active_environment: String, fallback_chain: Vec<String>) -> Self {
        let mut catalogs = HashMap::new();
        for env in &fallback_chain {
            catalogs.insert(env.clone(), Arc::new(EnvironmentCatalog::new(env.clone())));
        }
        Self {
            active_environment,
            fallback_chain,
            catalogs: RwLock::new(catalogs),
        }
    }

    /// Get the active environment name.
    pub fn active_environment(&self) -> &str {
        &self.active_environment
    }

    /// Get the fallback chain.
    pub fn fallback_chain(&self) -> &[String] {
        &self.fallback_chain
    }

    /// Get or create the catalog for a specific environment.
    pub fn environment_catalog(&self, env: &str) -> Option<Arc<EnvironmentCatalog>> {
        self.catalogs.read().unwrap().get(env).cloned()
    }

    /// Register a table in a specific environment's catalog.
    pub fn register_table(
        &self,
        environment: &str,
        schema_name: &str,
        table_name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        let catalogs = self.catalogs.read().unwrap();
        let catalog = catalogs.get(environment).ok_or_else(|| {
            DataFusionError::Plan(format!("environment `{environment}` not found in resolver"))
        })?;
        let schema = catalog.get_or_create_schema(schema_name);
        schema.register_table(table_name.to_string(), table)?;
        Ok(())
    }

    /// Build the merged catalog that performs fallback resolution.
    fn build_merged_catalog(&self) -> Arc<dyn CatalogProvider> {
        let catalogs = self.catalogs.read().unwrap();
        let chain: Vec<Arc<EnvironmentCatalog>> = self
            .fallback_chain
            .iter()
            .filter_map(|env| catalogs.get(env).cloned())
            .collect();
        Arc::new(MergedCatalog { chain })
    }
}

impl CatalogProviderList for EnvironmentResolver {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // If registering to an environment name we know, wrap it.
        // Otherwise store the raw catalog for pass-through.
        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(env_cat) = catalog.as_any().downcast_ref::<EnvironmentCatalog>() {
            catalogs
                .insert(name, Arc::new(env_cat.clone()))
                .map(|c| c as Arc<dyn CatalogProvider>)
        } else {
            // For non-environment catalogs, we don't store them in our map.
            None
        }
    }

    fn catalog_names(&self) -> Vec<String> {
        // Return "datafusion" as the default catalog name, plus all environment names.
        let mut names = vec!["datafusion".to_string()];
        let catalogs = self.catalogs.read().unwrap();
        for env in catalogs.keys() {
            if env != "datafusion" {
                names.push(env.clone());
            }
        }
        names
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == "datafusion" {
            // Default catalog uses fallback resolution.
            Some(self.build_merged_catalog())
        } else {
            // Named catalog returns that specific environment's catalog directly.
            self.catalogs
                .read()
                .unwrap()
                .get(name)
                .map(|c| c.clone() as Arc<dyn CatalogProvider>)
        }
    }
}

// ── Per-environment catalog ──────────────────────────────────────────────────

/// A catalog holding tables for a single environment.
#[derive(Clone)]
pub struct EnvironmentCatalog {
    name: String,
    schemas: Arc<RwLock<HashMap<String, Arc<EnvironmentSchema>>>>,
}

impl fmt::Debug for EnvironmentCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentCatalog")
            .field("name", &self.name)
            .finish()
    }
}

impl EnvironmentCatalog {
    pub fn new(name: String) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(
            "public".to_string(),
            Arc::new(EnvironmentSchema::new("public".to_string())),
        );
        Self {
            name,
            schemas: Arc::new(RwLock::new(schemas)),
        }
    }

    /// Get or create a schema in this catalog.
    pub fn get_or_create_schema(&self, name: &str) -> Arc<EnvironmentSchema> {
        let mut schemas = self.schemas.write().unwrap();
        schemas
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(EnvironmentSchema::new(name.to_string())))
            .clone()
    }
}

impl CatalogProvider for EnvironmentCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.read().unwrap().keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .read()
            .unwrap()
            .get(name)
            .map(|s| s.clone() as Arc<dyn SchemaProvider>)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        if let Some(env_schema) = schema.as_any().downcast_ref::<EnvironmentSchema>() {
            let mut schemas = self.schemas.write().unwrap();
            let prev = schemas
                .insert(name.to_string(), Arc::new(env_schema.clone()))
                .map(|s| s as Arc<dyn SchemaProvider>);
            Ok(prev)
        } else {
            Ok(None)
        }
    }
}

// ── Per-environment schema ───────────────────────────────────────────────────

/// A schema holding tables for a single environment. Tables are stored in
/// memory and looked up by name.
#[derive(Clone)]
pub struct EnvironmentSchema {
    name: String,
    tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
}

impl fmt::Debug for EnvironmentSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentSchema")
            .field("name", &self.name)
            .field("tables", &self.tables.read().unwrap().keys().collect::<Vec<_>>())
            .finish()
    }
}

impl EnvironmentSchema {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if this schema has a specific table (synchronous).
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.read().unwrap().contains_key(name)
    }

    /// Get a table synchronously (for use in fallback resolution).
    pub fn get_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.read().unwrap().get(name).cloned()
    }
}

#[async_trait::async_trait]
impl SchemaProvider for EnvironmentSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.get_table(name))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.has_table(name)
    }
}

// ── Merged catalog (fallback resolution) ─────────────────────────────────────

/// A catalog that merges multiple environment catalogs with fallback resolution.
/// When looking up a table, it walks the chain in order (active env first,
/// then fallbacks) and returns the first match.
struct MergedCatalog {
    chain: Vec<Arc<EnvironmentCatalog>>,
}

impl fmt::Debug for MergedCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergedCatalog")
            .field("chain_len", &self.chain.len())
            .finish()
    }
}

impl CatalogProvider for MergedCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for cat in &self.chain {
            for name in cat.schema_names() {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Collect all schemas with this name from the chain, then wrap in a
        // FallbackSchemaProvider.
        let schemas: Vec<Arc<EnvironmentSchema>> = self
            .chain
            .iter()
            .filter_map(|cat| {
                cat.schemas
                    .read()
                    .unwrap()
                    .get(name)
                    .cloned()
            })
            .collect();

        if schemas.is_empty() {
            None
        } else {
            Some(Arc::new(FallbackSchemaProvider {
                name: name.to_string(),
                chain: schemas,
            }))
        }
    }
}

// ── Fallback schema provider ─────────────────────────────────────────────────

/// A schema provider that resolves tables by walking a fallback chain.
/// Each table name is resolved independently: the first environment in the
/// chain that contains the table wins.
struct FallbackSchemaProvider {
    name: String,
    chain: Vec<Arc<EnvironmentSchema>>,
}

impl fmt::Debug for FallbackSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FallbackSchemaProvider")
            .field("name", &self.name)
            .field("chain_len", &self.chain.len())
            .finish()
    }
}

#[async_trait::async_trait]
impl SchemaProvider for FallbackSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for schema in &self.chain {
            for name in schema.table_names() {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        names
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        // Walk the chain; return the first hit.
        for schema in &self.chain {
            if let Some(table) = schema.get_table(name) {
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        // Register in the first (active) environment.
        if let Some(schema) = self.chain.first() {
            schema.register_table(name, table)
        } else {
            datafusion::common::exec_err!("no environment in fallback chain")
        }
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        // Deregister from the first (active) environment only.
        if let Some(schema) = self.chain.first() {
            schema.deregister_table(name)
        } else {
            datafusion::common::exec_err!("no environment in fallback chain")
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.chain.iter().any(|schema| schema.has_table(name))
    }
}
