// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite-backed trigger storage.

use crate::error::TriggerStoreError;
use crate::types::{
    RunPolicy, Trigger, TriggerHistoryEntry, TriggerId, TriggerKind, TriggerOutcome, TriggerState,
};
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;

/// Trait for trigger persistence, allowing alternative backends.
pub trait TriggerStorage: Send + Sync {
    fn create_trigger(&self, trigger: &Trigger) -> Result<(), TriggerStoreError>;
    fn get_trigger(&self, id: &TriggerId) -> Result<Trigger, TriggerStoreError>;
    fn list_triggers(
        &self,
        pipeline_id: Option<&str>,
        environment: Option<&str>,
    ) -> Result<Vec<Trigger>, TriggerStoreError>;
    fn update_trigger(&self, trigger: &Trigger) -> Result<(), TriggerStoreError>;
    fn delete_trigger(&self, id: &TriggerId) -> Result<(), TriggerStoreError>;
    fn set_enabled(&self, id: &TriggerId, enabled: bool) -> Result<(), TriggerStoreError>;

    // Trigger state
    fn get_state(&self, id: &TriggerId) -> Result<Option<TriggerState>, TriggerStoreError>;
    fn upsert_state(&self, state: &TriggerState) -> Result<(), TriggerStoreError>;

    // Trigger history
    fn record_history(&self, entry: &TriggerHistoryEntry) -> Result<(), TriggerStoreError>;
    fn get_history(
        &self,
        trigger_id: &TriggerId,
        limit: u32,
    ) -> Result<Vec<TriggerHistoryEntry>, TriggerStoreError>;

    /// Count pending/queued runs for a trigger in a given environment.
    fn count_pending_runs(&self, trigger_id: &TriggerId) -> Result<u32, TriggerStoreError>;

    /// Load all enabled triggers (for the scheduler tick loop).
    fn list_enabled_triggers(&self) -> Result<Vec<Trigger>, TriggerStoreError>;
}

/// SQLite-backed trigger store.
pub struct SqliteTriggerStore {
    conn: Mutex<Connection>,
}

impl SqliteTriggerStore {
    /// Open (or create) a trigger store at the given file path.
    pub fn open(path: &Path) -> Result<Self, TriggerStoreError> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Open an in-memory trigger store (useful for tests).
    pub fn open_in_memory() -> Result<Self, TriggerStoreError> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS triggers (
                id                      TEXT PRIMARY KEY,
                name                    TEXT NOT NULL,
                pipeline_id             TEXT NOT NULL,
                environment             TEXT NOT NULL,
                enabled                 INTEGER NOT NULL DEFAULT 1,
                kind                    TEXT NOT NULL,
                config_json             TEXT NOT NULL,
                run_policy              TEXT NOT NULL DEFAULT 'queue',
                variable_overrides_json TEXT,
                max_queue_depth         INTEGER NOT NULL DEFAULT 3,
                created_at              TEXT NOT NULL,
                updated_at              TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_triggers_pipeline
                ON triggers (pipeline_id);

            CREATE INDEX IF NOT EXISTS idx_triggers_enabled
                ON triggers (enabled);

            CREATE TABLE IF NOT EXISTS trigger_state (
                trigger_id          TEXT PRIMARY KEY REFERENCES triggers(id) ON DELETE CASCADE,
                last_evaluated_at   TEXT,
                last_fired_at       TEXT,
                next_fire_at        TEXT,
                sensor_state_json   TEXT,
                consecutive_errors  INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS trigger_history (
                id           TEXT PRIMARY KEY,
                trigger_id   TEXT NOT NULL REFERENCES triggers(id) ON DELETE CASCADE,
                fired_at     TEXT NOT NULL,
                outcome      TEXT NOT NULL,
                run_id       TEXT,
                details_json TEXT,
                error        TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_th_trigger
                ON trigger_history (trigger_id, fired_at DESC);",
        )?;
        Ok(())
    }
}

impl TriggerStorage for SqliteTriggerStore {
    fn create_trigger(&self, trigger: &Trigger) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let config_json = serde_json::to_string(&trigger.kind)?;
        let var_json = trigger
            .variable_overrides
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        conn.execute(
            "INSERT INTO triggers (id, name, pipeline_id, environment, enabled, kind, config_json,
             run_policy, variable_overrides_json, max_queue_depth, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                trigger.id.to_string(),
                trigger.name,
                trigger.pipeline_id,
                trigger.environment,
                trigger.enabled as i32,
                kind_tag(&trigger.kind),
                config_json,
                trigger.run_policy.to_string(),
                var_json,
                trigger.max_queue_depth,
                trigger.created_at,
                trigger.updated_at,
            ],
        )?;

        // Initialize trigger_state row.
        conn.execute(
            "INSERT OR IGNORE INTO trigger_state (trigger_id, consecutive_errors) VALUES (?1, 0)",
            params![trigger.id.to_string()],
        )?;

        Ok(())
    }

    fn get_trigger(&self, id: &TriggerId) -> Result<Trigger, TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, pipeline_id, environment, enabled, config_json,
                    run_policy, variable_overrides_json, max_queue_depth, created_at, updated_at
             FROM triggers WHERE id = ?1",
        )?;

        stmt.query_row(params![id.to_string()], |row| Ok(row_to_trigger(row)))?
    }

    fn list_triggers(
        &self,
        pipeline_id: Option<&str>,
        environment: Option<&str>,
    ) -> Result<Vec<Trigger>, TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT id, name, pipeline_id, environment, enabled, config_json,
                    run_policy, variable_overrides_json, max_queue_depth, created_at, updated_at
             FROM triggers WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(pid) = pipeline_id {
            sql.push_str(&format!(" AND pipeline_id = ?{}", param_values.len() + 1));
            param_values.push(Box::new(pid.to_string()));
        }
        if let Some(env) = environment {
            sql.push_str(&format!(" AND environment = ?{}", param_values.len() + 1));
            param_values.push(Box::new(env.to_string()));
        }
        sql.push_str(" ORDER BY created_at DESC");

        let mut stmt = conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| Ok(row_to_trigger(row)))?;

        let mut triggers = Vec::new();
        for r in rows {
            triggers.push(r??);
        }
        Ok(triggers)
    }

    fn update_trigger(&self, trigger: &Trigger) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let config_json = serde_json::to_string(&trigger.kind)?;
        let var_json = trigger
            .variable_overrides
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        let rows = conn.execute(
            "UPDATE triggers SET name = ?2, pipeline_id = ?3, environment = ?4,
             enabled = ?5, kind = ?6, config_json = ?7, run_policy = ?8,
             variable_overrides_json = ?9, max_queue_depth = ?10, updated_at = ?11
             WHERE id = ?1",
            params![
                trigger.id.to_string(),
                trigger.name,
                trigger.pipeline_id,
                trigger.environment,
                trigger.enabled as i32,
                kind_tag(&trigger.kind),
                config_json,
                trigger.run_policy.to_string(),
                var_json,
                trigger.max_queue_depth,
                trigger.updated_at,
            ],
        )?;

        if rows == 0 {
            return Err(TriggerStoreError::NotFound(trigger.id.to_string()));
        }
        Ok(())
    }

    fn delete_trigger(&self, id: &TriggerId) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM triggers WHERE id = ?1",
            params![id.to_string()],
        )?;
        if rows == 0 {
            return Err(TriggerStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    fn set_enabled(&self, id: &TriggerId, enabled: bool) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE triggers SET enabled = ?2 WHERE id = ?1",
            params![id.to_string(), enabled as i32],
        )?;
        if rows == 0 {
            return Err(TriggerStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    fn get_state(&self, id: &TriggerId) -> Result<Option<TriggerState>, TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trigger_id, last_evaluated_at, last_fired_at, next_fire_at,
                    sensor_state_json, consecutive_errors
             FROM trigger_state WHERE trigger_id = ?1",
        )?;

        let result = stmt.query_row(params![id.to_string()], |row| {
            let sensor_json: Option<String> = row.get(4)?;
            Ok(TriggerState {
                trigger_id: id.clone(),
                last_evaluated_at: row.get(1)?,
                last_fired_at: row.get(2)?,
                next_fire_at: row.get(3)?,
                sensor_state: sensor_json
                    .map(|s| serde_json::from_str(&s))
                    .transpose()
                    .unwrap_or(None),
                consecutive_errors: row.get::<_, i32>(5)? as u32,
            })
        });

        match result {
            Ok(state) => Ok(Some(state)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn upsert_state(&self, state: &TriggerState) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let sensor_json = state
            .sensor_state
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        conn.execute(
            "INSERT INTO trigger_state (trigger_id, last_evaluated_at, last_fired_at,
             next_fire_at, sensor_state_json, consecutive_errors)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(trigger_id) DO UPDATE SET
                last_evaluated_at = excluded.last_evaluated_at,
                last_fired_at = excluded.last_fired_at,
                next_fire_at = excluded.next_fire_at,
                sensor_state_json = excluded.sensor_state_json,
                consecutive_errors = excluded.consecutive_errors",
            params![
                state.trigger_id.to_string(),
                state.last_evaluated_at,
                state.last_fired_at,
                state.next_fire_at,
                sensor_json,
                state.consecutive_errors as i32,
            ],
        )?;
        Ok(())
    }

    fn record_history(&self, entry: &TriggerHistoryEntry) -> Result<(), TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let details_json = entry
            .details
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        conn.execute(
            "INSERT INTO trigger_history (id, trigger_id, fired_at, outcome, run_id,
             details_json, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                entry.id,
                entry.trigger_id.to_string(),
                entry.fired_at,
                entry.outcome.to_string(),
                entry.run_id,
                details_json,
                entry.error,
            ],
        )?;
        Ok(())
    }

    fn get_history(
        &self,
        trigger_id: &TriggerId,
        limit: u32,
    ) -> Result<Vec<TriggerHistoryEntry>, TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, trigger_id, fired_at, outcome, run_id, details_json, error
             FROM trigger_history
             WHERE trigger_id = ?1
             ORDER BY fired_at DESC
             LIMIT ?2",
        )?;

        let rows = stmt.query_map(params![trigger_id.to_string(), limit], |row| {
            let details_json: Option<String> = row.get(5)?;
            let outcome_str: String = row.get(3)?;
            Ok(TriggerHistoryEntry {
                id: row.get(0)?,
                trigger_id: trigger_id.clone(),
                fired_at: row.get(2)?,
                outcome: outcome_str.parse().unwrap_or(TriggerOutcome::Error),
                run_id: row.get(4)?,
                details: details_json
                    .map(|s| serde_json::from_str(&s))
                    .transpose()
                    .unwrap_or(None),
                error: row.get(6)?,
            })
        })?;

        let mut entries = Vec::new();
        for r in rows {
            entries.push(r?);
        }
        Ok(entries)
    }

    fn count_pending_runs(&self, trigger_id: &TriggerId) -> Result<u32, TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM trigger_history
             WHERE trigger_id = ?1 AND outcome IN ('run_started', 'queued')",
            params![trigger_id.to_string()],
            |row| row.get(0),
        )?;
        Ok(count as u32)
    }

    fn list_enabled_triggers(&self) -> Result<Vec<Trigger>, TriggerStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, pipeline_id, environment, enabled, config_json,
                    run_policy, variable_overrides_json, max_queue_depth, created_at, updated_at
             FROM triggers WHERE enabled = 1",
        )?;

        let rows = stmt.query_map([], |row| Ok(row_to_trigger(row)))?;
        let mut triggers = Vec::new();
        for r in rows {
            triggers.push(r??);
        }
        Ok(triggers)
    }
}

/// Extract the kind tag string from a TriggerKind for storage.
fn kind_tag(kind: &TriggerKind) -> &'static str {
    match kind {
        TriggerKind::Cron { .. } => "cron",
        TriggerKind::Interval { .. } => "interval",
        TriggerKind::FileArrival { .. } => "file_arrival",
        TriggerKind::Webhook { .. } => "webhook",
        TriggerKind::PipelineCompletion { .. } => "pipeline_completion",
    }
}

/// Deserialize a row into a Trigger.
fn row_to_trigger(row: &rusqlite::Row<'_>) -> Result<Trigger, TriggerStoreError> {
    let id_str: String = row.get(0).map_err(TriggerStoreError::Sqlite)?;
    let config_json: String = row.get(5).map_err(TriggerStoreError::Sqlite)?;
    let run_policy_str: String = row.get(6).map_err(TriggerStoreError::Sqlite)?;
    let var_json: Option<String> = row.get(7).map_err(TriggerStoreError::Sqlite)?;

    let kind: TriggerKind = serde_json::from_str(&config_json)?;
    let run_policy: RunPolicy = run_policy_str.parse().unwrap_or(RunPolicy::Queue);
    let variable_overrides = var_json.map(|s| serde_json::from_str(&s)).transpose()?;

    Ok(Trigger {
        id: id_str
            .parse()
            .map_err(|e: uuid::Error| TriggerStoreError::NotFound(e.to_string()))?,
        name: row.get(1).map_err(TriggerStoreError::Sqlite)?,
        pipeline_id: row.get(2).map_err(TriggerStoreError::Sqlite)?,
        environment: row.get(3).map_err(TriggerStoreError::Sqlite)?,
        enabled: row.get::<_, i32>(4).map_err(TriggerStoreError::Sqlite)? != 0,
        kind,
        run_policy,
        variable_overrides,
        max_queue_depth: row.get::<_, i32>(8).map_err(TriggerStoreError::Sqlite)? as u32,
        created_at: row.get(9).map_err(TriggerStoreError::Sqlite)?,
        updated_at: row.get(10).map_err(TriggerStoreError::Sqlite)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn test_trigger() -> Trigger {
        Trigger {
            id: TriggerId::new(),
            name: "test-trigger".to_string(),
            pipeline_id: "pipeline-1".to_string(),
            environment: "dev".to_string(),
            enabled: true,
            kind: TriggerKind::Cron {
                expression: "0 */6 * * *".to_string(),
                timezone: "UTC".to_string(),
            },
            run_policy: RunPolicy::Queue,
            variable_overrides: None,
            max_queue_depth: 3,
            created_at: "2026-04-09T00:00:00Z".to_string(),
            updated_at: "2026-04-09T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn create_and_get_trigger() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let trigger = test_trigger();
        store.create_trigger(&trigger).unwrap();

        let fetched = store.get_trigger(&trigger.id).unwrap();
        assert_eq!(fetched.name, "test-trigger");
        assert_eq!(fetched.pipeline_id, "pipeline-1");
        assert!(fetched.enabled);
    }

    #[test]
    fn list_triggers_with_filters() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();

        let mut t1 = test_trigger();
        t1.name = "t1".to_string();
        t1.pipeline_id = "p1".to_string();
        t1.environment = "dev".to_string();
        store.create_trigger(&t1).unwrap();

        let mut t2 = test_trigger();
        t2.name = "t2".to_string();
        t2.pipeline_id = "p2".to_string();
        t2.environment = "prod".to_string();
        store.create_trigger(&t2).unwrap();

        let all = store.list_triggers(None, None).unwrap();
        assert_eq!(all.len(), 2);

        let p1_only = store.list_triggers(Some("p1"), None).unwrap();
        assert_eq!(p1_only.len(), 1);
        assert_eq!(p1_only[0].name, "t1");

        let prod_only = store.list_triggers(None, Some("prod")).unwrap();
        assert_eq!(prod_only.len(), 1);
        assert_eq!(prod_only[0].name, "t2");
    }

    #[test]
    fn update_trigger() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let mut trigger = test_trigger();
        store.create_trigger(&trigger).unwrap();

        trigger.name = "updated-name".to_string();
        trigger.run_policy = RunPolicy::Skip;
        store.update_trigger(&trigger).unwrap();

        let fetched = store.get_trigger(&trigger.id).unwrap();
        assert_eq!(fetched.name, "updated-name");
        assert_eq!(fetched.run_policy, RunPolicy::Skip);
    }

    #[test]
    fn delete_trigger() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let trigger = test_trigger();
        store.create_trigger(&trigger).unwrap();
        store.delete_trigger(&trigger.id).unwrap();

        let result = store.get_trigger(&trigger.id);
        assert!(result.is_err());
    }

    #[test]
    fn enable_disable() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let trigger = test_trigger();
        store.create_trigger(&trigger).unwrap();

        store.set_enabled(&trigger.id, false).unwrap();
        let fetched = store.get_trigger(&trigger.id).unwrap();
        assert!(!fetched.enabled);

        let enabled = store.list_enabled_triggers().unwrap();
        assert!(enabled.is_empty());

        store.set_enabled(&trigger.id, true).unwrap();
        let enabled = store.list_enabled_triggers().unwrap();
        assert_eq!(enabled.len(), 1);
    }

    #[test]
    fn trigger_state_upsert() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let trigger = test_trigger();
        store.create_trigger(&trigger).unwrap();

        let state = TriggerState {
            trigger_id: trigger.id.clone(),
            last_evaluated_at: Some("2026-04-09T10:00:00Z".to_string()),
            last_fired_at: None,
            next_fire_at: Some("2026-04-09T12:00:00Z".to_string()),
            sensor_state: None,
            consecutive_errors: 0,
        };
        store.upsert_state(&state).unwrap();

        let fetched = store.get_state(&trigger.id).unwrap().unwrap();
        assert_eq!(
            fetched.next_fire_at,
            Some("2026-04-09T12:00:00Z".to_string())
        );
        assert_eq!(fetched.consecutive_errors, 0);
    }

    #[test]
    fn trigger_history_record_and_query() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let trigger = test_trigger();
        store.create_trigger(&trigger).unwrap();

        let entry = TriggerHistoryEntry {
            id: Uuid::new_v4().to_string(),
            trigger_id: trigger.id.clone(),
            fired_at: "2026-04-09T10:00:00Z".to_string(),
            outcome: TriggerOutcome::RunStarted,
            run_id: Some("run-123".to_string()),
            details: None,
            error: None,
        };
        store.record_history(&entry).unwrap();

        let history = store.get_history(&trigger.id, 10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, TriggerOutcome::RunStarted);
        assert_eq!(history[0].run_id, Some("run-123".to_string()));
    }

    #[test]
    fn variable_overrides_roundtrip() {
        let store = SqliteTriggerStore::open_in_memory().unwrap();
        let mut trigger = test_trigger();
        let mut vars = HashMap::new();
        vars.insert("batch_size".to_string(), serde_json::json!(100));
        trigger.variable_overrides = Some(vars);
        store.create_trigger(&trigger).unwrap();

        let fetched = store.get_trigger(&trigger.id).unwrap();
        let overrides = fetched.variable_overrides.unwrap();
        assert_eq!(overrides["batch_size"], serde_json::json!(100));
    }
}
