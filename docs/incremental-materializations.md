# Incremental Materializations

Horizon Flux supports **incremental sink materializations**: a sink declares how rows should land at the destination, and (optionally) how much of the upstream should be read on each run. This document is the user-facing guide. For the design rationale and internals, see `planning/27-incremental-materializations.md`.

## 1. The Mental Model

Two orthogonal axes live on a sink node under `materialization`:

- **`read_mode`** — *how much upstream do we read?*
  - `full` (default) — read everything every run
  - `incremental` — only rows new/changed since the last run, identified by a `watermark` column
- **`write_strategy`** — *how do rows land at the destination?*
  - `append` (default) — insert without dedup
  - `merge` — upsert on `unique_keys`
  - `delete_insert` — delete matching unique keys, then insert
  - `insert_overwrite` — replace whole partitions (requires `partition_column`)
  - `truncate_insert` — truncate target then insert

These axes are independent. Every combination is valid except for the field-presence rules listed in §3. This is the single biggest difference from dbt, where `incremental_strategy` only applies to `materialized='incremental'`.

The minimum config is two lines:

```json
"materialization": {
  "write_strategy": "merge",
  "unique_keys": ["id"]
}
```

Upgrading to incremental is four added lines and **zero changes to transform code**:

```json
"materialization": {
  "read_mode": "incremental",
  "write_strategy": "merge",
  "unique_keys": ["id"],
  "watermark": { "column": "updated_at", "type": "timestamp" }
}
```

Flux pushes the watermark filter down to the source connector automatically — your SQL/Python is identical in `full` and `incremental` modes.

## 2. Strategy Reference & Decision Tree

Use this tree to pick a `write_strategy`. Read mode is a separate decision (§4).

```
Does the target need deduplication?
├── No, upstream is append-only and unique
│   └── append              (fastest, but not idempotent on replay)
│
└── Yes
    ├── There is a stable unique key on each row
    │   ├── Target supports ON CONFLICT / MERGE (Postgres, DuckDB)
    │   │   └── merge       (recommended default for analytics)
    │   └── Target doesn't, or merge is too expensive at width
    │       └── delete_insert
    │
    ├── The source is authoritative for a whole partition
    │   │   (e.g. "today's events", "this region")
    │   └── insert_overwrite + partition_column
    │
    └── The target should always reflect the *current* full source
        └── truncate_insert  (almost always with read_mode: full)
```

### Strategy notes

| Strategy           | Idempotent on replay? | Requires        | Typical use                          |
|--------------------|-----------------------|-----------------|--------------------------------------|
| `append`           | **No**                | —               | Append-only event streams            |
| `merge`            | Yes                   | `unique_keys`   | Most analytics tables (default pick) |
| `delete_insert`    | Yes                   | `unique_keys`   | Reload-a-slice workloads             |
| `insert_overwrite` | Yes (per partition)   | `partition_column` | Date-partitioned warehouse tables |
| `truncate_insert`  | Yes                   | —               | Small dimension tables, full reload  |

> **Idempotency matters.** Flux is at-least-once across the sink-commit ↔ state-save boundary. A crash mid-run can replay rows on the next attempt. `merge` and `delete_insert` are safe under replay; `append` will produce duplicates. Pick accordingly.

## 3. Field Reference & Validation Rules

| Field               | Required when                                          | Rejected when                                           |
|---------------------|--------------------------------------------------------|---------------------------------------------------------|
| `read_mode`         | always (defaults to `full`)                            | —                                                       |
| `write_strategy`    | always (defaults to `append`)                          | —                                                       |
| `watermark`         | `read_mode == incremental`                             | `read_mode == full`                                     |
| `unique_keys`       | `write_strategy ∈ {merge, delete_insert}`              | other strategies                                        |
| `partition_column`  | `write_strategy == insert_overwrite`                   | other strategies                                        |
| `lookback`          | optional under incremental + timestamp watermark       | non-incremental, or non-timestamp watermark             |
| `first_run`         | optional under incremental                             | full read mode                                          |
| `on_schema_change`  | optional, defaults to `append_new_columns`             | —                                                       |

`watermark.type` must be one of `timestamp`, `int64`, or `string`. Timestamps are normalized to UTC nanoseconds; naive Arrow timestamps are assumed UTC and emit a one-line `WARN` on the first batch. `string` watermarks are only safe when the column is provably monotonic (ULIDs, snowflake IDs).

`first_run` controls bootstrap behavior:
- `full` (default) — read everything once and store the max watermark
- `fail` — require an explicit `--bootstrap-incremental` flag

`on_schema_change` controls what happens when the incoming Arrow schema differs from the recorded one:
- `fail` — abort the run with a column-level diff
- `ignore` — drop new columns, keep going
- `append_new_columns` (default) — `ALTER TABLE … ADD COLUMN` for new columns (Postgres today; other sinks log a `WARN` until wired)
- `sync_all_columns` — full reconciliation (additive only on the sinks that currently support evolution)

## 4. Migration Guide: Converting a Pipeline from Full to Incremental

The goal: convert a pipeline that today re-reads the entire source into one that only processes new rows.

### Step 1 — pick the right `write_strategy` first

Before touching read mode, make the write side idempotent. If the sink is currently `append`, switching to incremental on top of `append` will silently drop rows on any replay scenario. Almost always you want:

```json
"materialization": {
  "write_strategy": "merge",
  "unique_keys": ["<your primary key>"]
}
```

Run the pipeline once with this config under `read_mode: full`. The target should now be safe to replay. Verify by re-running and confirming no duplicate rows.

### Step 2 — pick a watermark column

A good watermark column is:
1. **Monotonic** — never decreases for new rows
2. **`NOT NULL`** — flux warns on nulls but they will be silently skipped on the next run
3. **Indexed at the source** — otherwise the pushed-down filter is a full scan

Common picks:
- `updated_at` (timestamp) — best for tables that record row updates
- `created_at` (timestamp) — fine for append-only event tables
- `id` (int64) — works for monotonic auto-increment IDs

### Step 3 — add the four lines

```json
"materialization": {
  "read_mode": "incremental",
  "write_strategy": "merge",
  "unique_keys": ["id"],
  "watermark": { "column": "updated_at", "type": "timestamp" }
}
```

### Step 4 — preview with `incremental plan`

```bash
horizon-flux incremental plan <pipeline>
```

This is a dry run: it prints the resolved read mode, write strategy, watermark column, stored value, projected source filter expression, and the source-side pushdown targets — without touching the sink. Use it to verify the filter you expect is the filter flux will inject.

### Step 5 — first run

The first run with `first_run: full` (the default) is a bootstrap and reads everything. After that, every subsequent run only processes the slice above the stored watermark.

If you'd rather force a deliberate bootstrap step in production:

```json
"first_run": "fail"
```

…then trigger the bootstrap explicitly:

```bash
horizon-flux run <pipeline> --bootstrap-incremental
```

### Step 6 — add `lookback` if your data is mildly out-of-order

If late-arriving rows are common (e.g. async event ingestion), overlap the window:

```json
"watermark": { "column": "updated_at", "type": "timestamp" },
"lookback": "PT1H"
```

This subtracts one hour from the stored watermark before filtering, so rows that arrived up to an hour late are still picked up. Use with `merge` so the overlap doesn't create duplicates.

### Resetting

To force the next run to be a fresh bootstrap:

```bash
horizon-flux incremental reset <pipeline> <node_id> [--env <env>]
```

Or use the **Reset incremental state** button in the sink node editor (asks for confirmation).

To run a one-off full rebuild without changing config:

```bash
horizon-flux run <pipeline> --full-refresh
```

`--full-refresh` skips watermark filter injection but still advances state at the end (re-baseline).

## 5. Troubleshooting

### "My incremental run processed zero rows but I know there's new data"

- Run `horizon-flux incremental status <pipeline>` and check the stored watermark value.
- Run `horizon-flux incremental plan <pipeline>` and read the projected source filter.
- The most common cause is a non-monotonic watermark column: the stored value is from a row whose `updated_at` is *higher* than newer rows because someone updated an old row out-of-order. Switch to `updated_at` if you were using `created_at`, or vice versa.

### "I'm seeing duplicate rows after a crash"

You're using `write_strategy: append`. Append is not idempotent on replay; flux is at-least-once. Switch to `merge` (or `delete_insert`) and re-run with `--full-refresh` to clean up, or `incremental reset` and rebuild.

### "Schema change aborted my run"

You have `on_schema_change: fail`. The error message contains the diff. Either:
- Update the target schema manually and re-run, or
- Switch `on_schema_change` to `append_new_columns` if your sink supports schema evolution (Postgres does today).

### "The watermark column doesn't exist on the source"

Flux validates watermark columns against the source's actual `TableProvider` schema *before* any I/O. If you renamed a column upstream, update the `watermark.column` field. If the column is computed by a transform, the filter cannot be pushed down — flux will fall back to filtering at the executor (still correct, but slower).

### "I get `WatermarkTypeMismatch`"

The Arrow type of the watermark column on the incoming batch doesn't match the declared `watermark.type`. Either:
- Fix the declared type to match the actual column type (e.g. declare `int64` for an `Int32` column — flux widens), or
- Cast the column upstream so the type matches.

### "Plugin sink rejects `merge`"

The plugin's `[sinks.capabilities.materialization]` table in `plugin.toml` doesn't declare `merge = true`. Either pick a strategy the plugin supports, or update the plugin to declare the capability and implement it.

### "Lookback was rejected"

`lookback` is only meaningful with `read_mode: incremental` *and* a `timestamp` watermark. It's rejected at validation time in any other configuration. If you need a "process the last N rows" semantic on an `int64` watermark, encode it in the source query directly.

### "I forgot `unique_keys` on a `merge` strategy"

Validation will reject the pipeline at import time. `unique_keys` is required iff `write_strategy ∈ {merge, delete_insert}`.

### "My state got corrupted / I lost the metadata DB"

The next run is a full rebuild — loud and expensive, but not data-corrupting. If the target uses `merge` or `delete_insert`, the rebuild produces a target equivalent to a fresh full run. If the target uses `append`, the rebuild produces duplicates; recover by truncating the target first.

## 6. Performance Notes

### What to expect

- **First run is a bootstrap.** It reads everything. Plan capacity for it. After that, runs scale with the *new-row* slice, not the table size.
- **Pushdown is the win.** When the source connector supports filter pushdown on the watermark column (Postgres does, Parquet does via DataFusion, REST depends on the API), the source read scans only the relevant slice. Verify with `incremental plan` and your warehouse's query log.
- **Pushdown can be blocked by transforms.** A Python node that doesn't preserve the watermark column blocks pushdown — flux falls back to filtering after the source read, which is correct but reads the full source. If performance matters, keep the watermark column intact through every transform between source and sink.
- **Multiple incremental sinks fanning into one source share a watermark.** The coordinator merges to the *minimum* stored watermark across the fan-out so neither sink is shorted. Splitting the source into two pipelines avoids this if the sinks have wildly different cadences.

### When to schedule full refreshes

Incremental is not CDC. Drift sources include:
- Late-arriving data beyond the `lookback` window
- Source rows updated without bumping the watermark column (a non-monotonic write pattern)
- Manual edits to the source

For drift-prone tables, schedule a periodic `--full-refresh` externally (cron, GitHub Actions, your scheduler of choice). Weekly is a common starting point. There is no built-in scheduling hint in v1; use external scheduling.

### Width and merge cost

Postgres `INSERT ... ON CONFLICT DO UPDATE SET ...` with hundreds of columns generates a lot of SQL. The flux generator is efficient but the warehouse still has to plan it. For very wide tables, consider:
- `delete_insert` instead of `merge` — sometimes faster on wide rows
- Pre-projecting to only the columns the sink actually needs, upstream of the sink

### Receipts: the canonical answer to "what did this run do?"

Every successful sink write returns a `MaterializationReceipt` with rows scanned, rows filtered by watermark, rows inserted/updated/deleted, watermark before/after, schema diff, and duration. The receipt is:
- Persisted in run history
- Emitted on the WebSocket so the canvas badge updates live
- Surfaced in `horizon-flux incremental status` / `incremental plan`
- Returned in API responses (`GET /api/pipelines/:id/runs/:run_id/incremental-stats`)

You should never need to scrape logs or query the warehouse to answer "did my incremental run do what I expected?" — read the receipt.
