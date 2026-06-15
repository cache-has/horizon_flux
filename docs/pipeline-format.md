# Pipeline JSON Format Reference

Complete reference for the Armillary pipeline definition format. Pipelines are JSON files that describe a DAG of source, transform, and sink nodes.

## Top-Level Fields

```json
{
  "name": "My Pipeline",
  "version": 1,
  "default_environment": "dev",
  "code_dir": "transforms/",
  "variables": {},
  "environment_overrides": {},
  "sample_config": null,
  "nodes": [],
  "edges": []
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | **yes** | | Pipeline display name. Must be non-empty and unique. |
| `version` | integer | no | `1` | Schema version, incremented automatically on save. |
| `default_environment` | string | no | `"dev"` | Default environment for execution. |
| `code_dir` | string | no | `null` | Base directory for `code_path` references. Relative to the working directory. |
| `variables` | object | no | `{}` | Pipeline variable declarations. See [Variables](#variables). |
| `environment_overrides` | object | no | `{}` | Per-environment, per-node config overrides. See [Environment Overrides](#environment-overrides). |
| `sample_config` | object | no | `null` | Default preview sampling config. See [Sample Config](#sample-config). |
| `nodes` | array | **yes** | | Array of node definitions. See [Nodes](#nodes). |
| `edges` | array | **yes** | | Array of edge definitions. See [Edges](#edges). |

---

## Nodes

Each node has a `type` field that determines its role and which config fields are relevant.

### Common Fields (All Node Types)

```json
{
  "id": "my_node",
  "name": "My Node",
  "type": "source",
  "position": { "x": 0, "y": 0 },
  "pinned_position": false
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | **yes** | | Unique node identifier. Used in edges, SQL table references, and `code_path`. Must match `[a-zA-Z_][a-zA-Z0-9_]*`. |
| `name` | string | **yes** | | Display name shown on the canvas. |
| `type` | string | **yes** | | One of `"source"`, `"transform"`, `"sink"`. |
| `position` | object | no | `{"x": 0, "y": 0}` | Canvas position. |
| `pinned_position` | boolean | no | `false` | Whether the position is pinned (not affected by auto-layout). |

### Source Nodes

```json
{
  "id": "pg_source",
  "name": "Customer Data",
  "type": "source",
  "connector": "postgresql",
  "config": {
    "connection_string": "{{ env:DB_CONNECTION }}",
    "table": "customers"
  }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connector` | string | **yes** | | Connector type. See [Connectors](#connectors). |
| `config` | object | no | `null` | Connector-specific configuration. |

### Transform Nodes

```json
{
  "id": "filter_active",
  "name": "Filter Active Customers",
  "type": "transform",
  "mode": "sql",
  "code": "SELECT * FROM pg_source WHERE active = 1",
  "materialized": false
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mode` | string | **yes** | | `"sql"` or `"python"`. |
| `code` | string | no | `""` | Inline SQL query or Python code. Ignored when `code_path` is set. |
| `code_path` | string | no | `null` | Path to an external code file, resolved relative to `code_dir`. Supports nested paths like `"silver/usgs/parse.py"`. |
| `materialized` | boolean | no | `false` | Whether output should be cached for preview. |

**SQL transforms** reference upstream nodes by their `id` as table names:
```sql
SELECT o.*, c.name AS customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
```

**Python transforms** receive upstream data as Polars DataFrames:
```python
import polars as pl

def transform(inputs, params):
    df = inputs["orders"]  # upstream node id
    return df.filter(pl.col("amount") > params.get("min_amount", 0))
```

### Sink Nodes

```json
{
  "id": "output_csv",
  "name": "Export Results",
  "type": "sink",
  "connector": "csv",
  "config": {
    "path": "output/results.csv",
    "format": "csv"
  }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connector` | string | **yes** | | Connector type. See [Connectors](#connectors). |
| `config` | object | no | `null` | Connector-specific configuration. |

---

## Edges

Edges define data flow between nodes. Each edge connects one node's output to another node's input.

```json
{ "from": "source_node_id", "to": "transform_node_id" }
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `from` | string | **yes** | Source node `id`. |
| `to` | string | **yes** | Target node `id`. |

Rules:
- The graph must be a valid DAG (no cycles).
- Source nodes cannot be edge targets (no inputs).
- Sink nodes cannot be edge sources (no outputs).
- Every non-source node must have at least one incoming edge.
- Every non-sink node must have at least one outgoing edge.

---

## Variables

Declare typed variables with optional defaults. Override at runtime via CLI (`-V key=value`) or environment-specific overrides.

```json
{
  "variables": {
    "min_amount": {
      "type": "float",
      "default": 100.0
    },
    "target_region": {
      "type": "string",
      "default": "US"
    }
  }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | **yes** | | One of `"string"`, `"integer"`, `"float"`, `"date"`, `"boolean"`. |
| `default` | any | no | `null` | Default value. Type must match `type`. |

### Variable Interpolation

Use `{{ variable_name }}` in SQL code, Python params, and connector configs:

```json
{ "connection_string": "postgresql://{{ db_host }}:5432/mydb" }
```

### Built-in Variables

Always available, no declaration needed:

| Variable | Description | Example |
|----------|-------------|---------|
| `{{ run_date }}` | Current date (ISO format) | `2026-03-30` |
| `{{ run_id }}` | Unique run identifier | `a1b2c3d4-...` |
| `{{ pipeline_name }}` | Pipeline name | `My Pipeline` |
| `{{ environment }}` | Active environment | `dev` |

### Environment Variables

Reference OS environment variables (including `.env` file values):

```json
{ "connection_string": "{{ env:DATABASE_URL }}" }
```

### Secrets

Reference encrypted secrets from the secret store:

```json
{ "password": "{{ secret:db_password }}" }
```

---

## Environment Overrides

Override node configs per environment. Outer key = environment name, inner key = node `id`, value = config fields to merge.

```json
{
  "environment_overrides": {
    "prod": {
      "pg_source": {
        "connection_string": "{{ secret:prod_db_url }}"
      }
    },
    "staging": {
      "pg_source": {
        "connection_string": "{{ env:STAGING_DB_URL }}"
      }
    }
  }
}
```

Overrides are shallow-merged into the node's `config` object before variable interpolation.

---

## Sample Config

Controls how data is sampled during preview execution.

```json
{ "mode": "first_n", "count": 100 }
```

| Mode | Fields | Description |
|------|--------|-------------|
| `first_n` | `count` (integer) | Take the first N rows from each source. Default: 100. |
| `random` | `count` (integer), `seed` (integer) | Random sample of N rows with a fixed seed. |
| `full` | | No sampling — use all data. |

---

## Connectors

### PostgreSQL

**Connector names:** `"postgresql"`, `"postgres"`

**Source config:**

```json
{
  "connection_string": "postgresql://user:pass@host:5432/dbname",
  "table": "my_table",
  "query": null
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connection_string` | string | **yes** | | PostgreSQL connection URL. Supports `{{ secret:... }}` and `{{ env:... }}`. |
| `table` | string | one of table/query | | Table name. Enables filter and projection pushdown. |
| `query` | string | one of table/query | | Raw SQL query. Mutually exclusive with `table`. |

**Sink config:**

```json
{
  "connection_string": "postgresql://user:pass@host:5432/dbname",
  "table": "output_table",
  "write_mode": "truncate_insert",
  "batch_size": 1000,
  "conflict_keys": ["id"],
  "indexes": [["customer_id"], ["region", "tier"]]
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connection_string` | string | **yes** | | PostgreSQL connection URL. |
| `table` | string | **yes** | | Target table name. Auto-created if it doesn't exist. |
| `write_mode` | string | no | `"insert"` | `"insert"`, `"upsert"`, `"truncate_insert"`, or `"append"`. |
| `batch_size` | integer | no | `1000` | Rows per INSERT batch. |
| `conflict_keys` | array | no | `[]` | Column names for upsert ON CONFLICT. Required when `write_mode` is `"upsert"`. |
| `indexes` | array | no | `[]` | Indexes to create after writing. Each entry is an array of column names. |

### CSV / Parquet Files

**Connector names:** `"csv"`, `"parquet"`, `"file"`

**Source config:**

```json
{
  "path": "data/input.csv",
  "format": "csv",
  "options": {
    "delimiter": ",",
    "has_header": true,
    "quote_char": "\""
  }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `path` | string | **yes** | | File path or glob pattern (`data/*.csv`). |
| `format` | string | **yes** | | `"csv"` or `"parquet"`. |
| `options.delimiter` | string | no | `","` | CSV field delimiter (single character). |
| `options.has_header` | boolean | no | `true` | Whether CSV has a header row. |
| `options.quote_char` | string | no | `"\""` | CSV quote character. |
| `options.null_values` | array | no | `[]` | Strings to treat as null. |

**Sink config:**

```json
{
  "path": "output/results.parquet",
  "format": "parquet",
  "options": {
    "compression": "snappy",
    "write_mode": "overwrite"
  }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `path` | string | **yes** | | Output file path. Parent directories are created automatically. |
| `format` | string | **yes** | | `"csv"` or `"parquet"`. |
| `options.compression` | string | no | `"snappy"` | Parquet compression: `"snappy"`, `"gzip"`, `"zstd"`, `"none"`. |
| `options.write_mode` | string | no | `"overwrite"` | `"overwrite"` or `"append"` (CSV only for append). |
| `options.delimiter` | string | no | `","` | CSV field delimiter. |
| `options.has_header` | boolean | no | `true` | CSV: include header row. |

### REST API

**Connector names:** `"rest"`, `"rest_api"`, `"http"` (source only)

```json
{
  "url": "https://api.example.com/data",
  "method": "GET",
  "headers": { "Accept": "application/json" },
  "auth": { "type": "bearer", "token": "{{ secret:api_token }}" },
  "response_format": "json",
  "data_path": "results",
  "pagination": { "type": "offset", "offset_param": "offset", "limit_param": "limit", "limit": 100 },
  "max_pages": 10,
  "rate_limit_ms": 200
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | **yes** | | Request URL. Supports variable interpolation. |
| `method` | string | no | `"GET"` | HTTP method. |
| `headers` | object | no | `{}` | Request headers as key-value pairs. |
| `auth` | object | no | `null` | Authentication. See below. |
| `response_format` | string | no | `"json"` | `"json"`, `"ndjson"`, or `"csv"`. |
| `data_path` | string | no | `null` | Path to the data array in JSON responses. Dot-notation (`data.items`) or JSON Pointer (`/data/items`). |
| `pagination` | object | no | `null` | Pagination config. See below. |
| `schema` | object | no | `{}` | User-defined schema as `{"field": "type"}`. If omitted, schema is inferred. |
| `rate_limit_ms` | integer | no | `null` | Minimum delay between paginated requests (ms). |
| `max_retries` | integer | no | `3` | Retry attempts for failed requests. |
| `max_pages` | integer | no | `null` | Maximum pages to fetch (safety limit). |

**Authentication types:**

| Type | Fields | Description |
|------|--------|-------------|
| `basic` | `username`, `password` | HTTP Basic auth. |
| `bearer` | `token` | Bearer token in Authorization header. |
| `api_key` | `header`, `value` | Custom header with API key. |

**Pagination types:**

| Type | Fields | Description |
|------|--------|-------------|
| `offset` | `offset_param`, `limit_param`, `limit` | Offset-based (`?offset=0&limit=100`). |
| `cursor` | `cursor_param`, `cursor_path` | Cursor-based (extract next cursor from response). |
| `link_header` | | RFC 8288 Link header pagination. |

### Stdout

**Connector name:** `"stdout"` (sink only)

```json
{
  "format": "table",
  "max_rows": 100
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `format` | string | no | `"table"` | `"table"` (psql-style), `"csv"`, `"json"`, or `"ndjson"`. |
| `max_rows` | integer | no | `null` | Maximum rows to display. |

---

## Complete Example

```json
{
  "name": "Customer Revenue Pipeline",
  "version": 1,
  "default_environment": "dev",
  "code_dir": "transforms/",
  "variables": {
    "min_revenue": {
      "type": "float",
      "default": 100.0
    }
  },
  "environment_overrides": {
    "prod": {
      "customers": {
        "connection_string": "{{ secret:prod_db }}"
      }
    }
  },
  "nodes": [
    {
      "id": "customers",
      "name": "Customer Data",
      "type": "source",
      "connector": "postgresql",
      "config": {
        "connection_string": "{{ env:DB_CONNECTION }}",
        "table": "customers"
      }
    },
    {
      "id": "orders",
      "name": "Order Data",
      "type": "source",
      "connector": "csv",
      "config": {
        "path": "data/orders.csv",
        "format": "csv"
      }
    },
    {
      "id": "join_data",
      "name": "Join Customers + Orders",
      "type": "transform",
      "mode": "sql",
      "code": "SELECT c.*, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount >= {{ min_revenue }}"
    },
    {
      "id": "enrich",
      "name": "Add RFM Scores",
      "type": "transform",
      "mode": "python",
      "code_path": "gold/rfm_scoring.py"
    },
    {
      "id": "sink_db",
      "name": "Write to PostgreSQL",
      "type": "sink",
      "connector": "postgresql",
      "config": {
        "connection_string": "{{ env:DB_CONNECTION }}",
        "table": "customer_scores",
        "write_mode": "truncate_insert",
        "indexes": [["customer_id"], ["segment"]]
      }
    },
    {
      "id": "sink_file",
      "name": "Export CSV",
      "type": "sink",
      "connector": "csv",
      "config": {
        "path": "output/customer_scores.csv",
        "format": "csv"
      }
    }
  ],
  "edges": [
    { "from": "customers", "to": "join_data" },
    { "from": "orders", "to": "join_data" },
    { "from": "join_data", "to": "enrich" },
    { "from": "enrich", "to": "sink_db" },
    { "from": "enrich", "to": "sink_file" }
  ]
}
```

## Import / Export

```bash
# Import a pipeline
armillary import pipeline.json

# Import with conflict handling
armillary import pipeline.json --on-conflict rename    # auto-rename if name exists
armillary import pipeline.json --on-conflict overwrite # replace existing

# Export (code_path references are resolved to inline code)
armillary export "My Pipeline" -o pipeline.json
armillary export --all -o ./pipelines/
```

Exported pipelines are self-contained — all `code_path` references are resolved to inline `code` and `code_dir` is removed.
