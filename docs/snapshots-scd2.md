# Snapshots & SCD2

Armillary can maintain **slowly-changing dimension type 2 (SCD2)** history on a sink table. Point a snapshot at a mutable source like `customers` and Armillary preserves every historical version of every row, queryable by `armillary_valid_from` / `armillary_valid_to`. This document is the user-facing guide. For the design rationale and internals, see `planning/28-snapshots-scd2.md`.

Snapshot is a `write_strategy` value on the materialization block introduced in [`incremental-materializations.md`](./incremental-materializations.md). Read that doc first if you haven't — `read_mode`, `write_strategy`, `unique_keys`, and `watermark` all carry over unchanged.

## 1. SCD2 in Sixty Seconds

Most operational tables are **mutable**: a customer's `email` or `plan` column reflects only the *current* value. Once it changes, the previous value is gone. SCD2 fixes that by storing one row per *version* of an entity and adding three columns:

| Column            | Meaning                                                  |
|-------------------|----------------------------------------------------------|
| `armillary_valid_from` | When this version became current                          |
| `armillary_valid_to`   | When this version stopped being current (`NULL` if still current) |
| `armillary_is_current` | Convenience flag — `true` iff `armillary_valid_to IS NULL`     |
| `armillary_scd_id`     | Stable surrogate key — hash of `unique_keys + valid_from` |

A row that has never changed has exactly one version with `armillary_is_current = true` and `armillary_valid_to = NULL`. A row that has changed three times has four versions: three closed (with `armillary_valid_to` set) and one current.

To answer **"what was the customer's email on 2025-03-14?"**, you query the version that was current at that timestamp:

```sql
SELECT email
  FROM customers_history
 WHERE customer_id = 42
   AND '2025-03-14' >= armillary_valid_from
   AND '2025-03-14' <  COALESCE(armillary_valid_to, 'infinity'::timestamptz);
```

This is the only history pattern Armillary ships in v1. SCD6 ("current value" denormalized onto every historical row) is out of scope — `armillary_is_current` plus a view covers most of the use case.

## 2. Configuring a Snapshot

Snapshot lives inside the existing `materialization` block as a `write_strategy: "snapshot"` plus a nested `snapshot:` sub-block:

```json
"materialization": {
  "read_mode": "full",
  "write_strategy": "snapshot",
  "unique_keys": ["customer_id"],
  "snapshot": {
    "change_detection": "check",
    "check_columns": ["email", "plan", "status"],
    "hard_deletes": "ignore"
  }
}
```

| Field                       | Required when                              | Meaning                                                                 |
|-----------------------------|--------------------------------------------|-------------------------------------------------------------------------|
| `unique_keys`               | always                                     | Business key identifying a logical entity across versions               |
| `snapshot.change_detection` | always                                     | `"check"` or `"timestamp"` (see §3)                                     |
| `snapshot.check_columns`    | `change_detection: check`                  | Columns whose change opens a new version. Use `"*"` to track all        |
| `snapshot.updated_at_column`| `change_detection: timestamp`              | Column the source updates whenever a tracked field changes              |
| `snapshot.hard_deletes`     | optional, default `"ignore"`               | What to do with rows present in the target but missing from the source  |

`hard_deletes` has three settings:

- **`ignore`** *(default)* — leave the missing row's current version open. The entity simply hasn't shown up this run.
- **`invalidate`** — close the current version (`armillary_valid_to = now()`). The entity is treated as deleted from "now" forward, but its history is preserved.
- **`delete`** — physically remove **every historical version** of the missing key. Violates strict SCD2 semantics; use only when you know you want it (regulatory deletes, GDPR right-to-be-forgotten).

## 3. `check` vs. `timestamp` — Which to Pick

The two `change_detection` strategies trade off safety against performance.

| Property                       | `check`                                          | `timestamp`                                              |
|--------------------------------|--------------------------------------------------|----------------------------------------------------------|
| How "did this row change?"     | Compare the listed `check_columns` value-by-value | Trust a source-managed `updated_at` column                |
| Cost per row                   | One column read per tracked column               | One column read total                                    |
| Safe if source is buggy?       | Yes — the data itself is the source of truth     | No — a missed `updated_at` write hides the change        |
| Required source contract       | None                                              | Source must update `updated_at` on every tracked change  |
| Combinable with `read_mode: incremental`? | **No** (incoherent — you can't push down a column-set diff) | **Yes** (the performance sweet spot)                     |

The interaction matrix:

| `change_detection` | `read_mode: full`                                                  | `read_mode: incremental`                                                    |
|--------------------|--------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `check`            | Re-read whole source, diff every row by columns (safest default)   | **Rejected at validation.**                                                 |
| `timestamp`        | Re-read whole source, trust `updated_at` to flag candidates        | Push the watermark filter down — only candidate rows are read. **Sweet spot.** |

**Heuristic.** Start with `check` + `read_mode: full`. If the source grows past ~10M rows and most rows don't change between runs, add an `updated_at` column to the source (or use one that exists), switch to `change_detection: timestamp`, and turn on `read_mode: incremental` with a matching `watermark.column`.

### Incremental snapshots: dbt cannot do this

dbt's snapshot materialization always reads the full source on every run. Armillary's orthogonal model lets snapshots run incrementally. With a 100M-row source where 50k rows change between runs, the incremental path reads 50k rows; the full path reads 100M. The user-facing config is one extra block:

```json
"materialization": {
  "read_mode": "incremental",
  "write_strategy": "snapshot",
  "unique_keys": ["customer_id"],
  "watermark":  { "column": "updated_at", "type": "timestamp" },
  "snapshot": {
    "change_detection": "timestamp",
    "updated_at_column": "updated_at",
    "hard_deletes": "ignore"
  }
}
```

The validator enforces two rules unique to this combination:

1. `change_detection` must be `timestamp` (not `check`).
2. `watermark.column` must equal `snapshot.updated_at_column` — one source of truth.

## 4. Sink Support

| Sink                        | Snapshot? | Notes                                                                                          |
|-----------------------------|:---------:|------------------------------------------------------------------------------------------------|
| Postgres                    | ✅        | Native server-side stage-diff-merge. Recommended for any non-trivial dataset                   |
| DuckDB (OpenBoard plugin)   | ✅        | Same algorithm, executed inside the plugin against a local `.duckdb` file                      |
| Parquet (file sink, local)  | ✅        | Read-modify-write with atomic rename. Capped at "fits in RAM" — see §7 caveats                 |
| Parquet on `s3://` / `gs://`/ `az://` | ❌ | No atomic-rename primitive on object stores. Rejected at write time with an actionable error   |
| CSV                         | ❌        | CSV cannot represent the SCD2 metadata columns. Rejected at validation                         |
| Plugin sinks (other)        | depends   | Plugin must declare `[sinks.capabilities.materialization] snapshot = true` in its manifest     |

## 5. Querying Snapshot Tables

The snapshot table has the same business columns as the source plus the four `armillary_*` metadata columns. Common patterns:

**Current state** — what dbt would have given you:

```sql
SELECT customer_id, email, plan
  FROM customers_history
 WHERE armillary_is_current;
```

**Point-in-time lookup** — value as of a specific instant:

```sql
SELECT email
  FROM customers_history
 WHERE customer_id = 42
   AND TIMESTAMP '2025-03-14 00:00:00' >= armillary_valid_from
   AND TIMESTAMP '2025-03-14 00:00:00' <  COALESCE(armillary_valid_to, 'infinity');
```

**Full version history for one entity**, newest first:

```sql
SELECT armillary_valid_from, armillary_valid_to, email, plan, status
  FROM customers_history
 WHERE customer_id = 42
 ORDER BY armillary_valid_from DESC;
```

**Counts of changes per day**:

```sql
SELECT date_trunc('day', armillary_valid_from) AS day,
       count(*)                            AS new_versions
  FROM customers_history
 GROUP BY 1
 ORDER BY 1;
```

**Find rows that have ever held a specific value**:

```sql
SELECT DISTINCT customer_id
  FROM customers_history
 WHERE plan = 'enterprise';
```

If you'd rather not write SQL, the **History viewer** in the sink editor (see §6) renders the same data as a per-key timeline.

## 6. Previewing a Snapshot Run Before It Touches Prod

Armillary ships two read-only inspection tools so you can see what a snapshot would do *before* any write touches the target. Both are scoped per sink node and live in the snapshot sink editor (and the CLI).

### 6.1 Diff preview

The headline differentiator vs dbt and Dagster: **click "Preview diff"** in the sink editor and Armillary runs the upstream pipeline as a dry-run, classifies every staged row against the live target, and shows the result without writing anything.

The output is four counts and a sample of affected keys:

| Bucket      | Meaning                                                              |
|-------------|----------------------------------------------------------------------|
| `unchanged` | Key exists in target with the same tracked value — no version opened |
| `changed`   | Key exists in target with a different tracked value — close + open   |
| `new`       | Key does not exist in target — open                                  |
| `gone`      | Key exists in target but not in this run — `hard_deletes` decides    |

A diff preview never touches the sink. It runs upstream nodes through the executor with `dry_run_no_sinks` set, then reads the target's `armillary_is_current` slice once and compares in memory. The CLI equivalent is:

```sh
armillary snapshot diff <pipeline_id> <sink_node_id>
armillary snapshot diff <pipeline_id> <sink_node_id> --json    # machine-readable
```

**Limits.** The web preview caps the staged sample at 10,000 rows so the editor stays responsive — if your pipeline produces more, the UI shows a banner pointing at the CLI for a full run. Diff results are cached for 5 minutes per `(pipeline, environment, variables)`; editing the pipeline invalidates the cache automatically.

**v1 scope.** The preview button is enabled for postgres sinks. Parquet and DuckDB-via-plugin sinks are tracked under "Deferred" in `planning/28-snapshots-scd2.md`.

### 6.2 History viewer

Below the diff preview, the **History viewer** lets you punch in a unique key and see every version of one entity as a timeline — closed versions in grey, current version in green, with `armillary_valid_from` / `armillary_valid_to` / `armillary_scd_id` and the tracked comparison columns per version.

CLI equivalent:

```sh
armillary snapshot history <pipeline_id> <sink_node_id> --key customer_id=42
armillary snapshot history <pipeline_id> <sink_node_id> --key tenant=acme --key id=42
```

`--key COLUMN=VALUE` is repeatable and must cover every column in the sink's `unique_keys`. Composite keys are bound as text-cast parameters server-side, so the lookup works regardless of the column's physical type.

Same v1 scope as the diff preview: postgres sinks only.

## 7. Performance & Caveats

- **Microsecond timestamps.** `armillary_valid_from` is microsecond-precision, so two versions of the same row from runs less than a second apart are still distinguishable. All three supported sinks (Postgres, DuckDB, Parquet) preserve this precision.
- **Idempotent reruns.** Re-running the same snapshot against an unchanged source produces zero new versions and zero closed versions. Surrogate keys are byte-identical across runs of unchanged rows. Use this property in CI: a second run that opens or closes anything is a bug.
- **Parquet snapshots are bounded by RAM.** The current implementation reads the entire existing target into memory, runs the diff, and writes the new file in one pass. This is correct and simple but caps target size at "fits in RAM". Use Postgres (or wait for the chunked-rewrite work tracked in `planning/28-snapshots-scd2.md`) for larger targets.
- **Parquet snapshots on local paths only.** `s3://` / `gs://` / `az://` are rejected because object stores have no atomic-rename primitive — concurrent writers could corrupt the target. Lifting this requires either a cloud-native staging-prefix layer or an explicit single-writer contract.
- **Backfilling history.** If you start snapshotting a table that already has historical data in a separate audit log, there's no clean way to backfill that history into Armillary. Treat the first snapshot run as "history starts here" and, if necessary, do a one-time manual load.
- **One run at a time per node.** v1 assumes the executor's pipeline-level locking is enough. If you schedule overlapping runs of the same snapshot, the second one will fail at the database level rather than with a clean Armillary error. Defensive sink-side advisory locks are tracked under "Deferred" in the planning doc.

## 8. Common Recipes

### Snapshot a small dimension table (Postgres → Postgres)

```json
"materialization": {
  "write_strategy": "snapshot",
  "unique_keys": ["customer_id"],
  "snapshot": {
    "change_detection": "check",
    "check_columns": "*",
    "hard_deletes": "ignore"
  }
}
```

Tracks every column. Defaults to `read_mode: full`. Missing rows stay open.

### Snapshot a large fact-dimension hybrid with an `updated_at` column

```json
"materialization": {
  "read_mode": "incremental",
  "write_strategy": "snapshot",
  "unique_keys": ["order_id"],
  "watermark":  { "column": "updated_at", "type": "timestamp" },
  "snapshot": {
    "change_detection": "timestamp",
    "updated_at_column": "updated_at",
    "hard_deletes": "ignore"
  }
}
```

Reads only rows updated since the last run; preserves SCD2 history; correct because rows that *didn't* update since the watermark are correctly assumed unchanged.

### GDPR-style hard delete that purges all history

```json
"materialization": {
  "write_strategy": "snapshot",
  "unique_keys": ["user_id"],
  "snapshot": {
    "change_detection": "check",
    "check_columns": ["email", "phone"],
    "hard_deletes": "delete"
  }
}
```

When a `user_id` disappears from the source, *every* historical version of that user is removed from the target. Use with care.

## 9. Where to Read Next

- [`incremental-materializations.md`](./incremental-materializations.md) — the materialization block this builds on
- `planning/28-snapshots-scd2.md` — design rationale, deferred work, and the full task list
- `planning/27-incremental-materializations.md` — design rationale for the orthogonal `read_mode` × `write_strategy` axes
