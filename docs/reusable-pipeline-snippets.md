# Reusable Pipeline Snippets

Snippets let you package a small sub-DAG — a source plus its cleansing
transforms, a parameterized enrichment block, anything you find yourself
copy-pasting between pipelines — into a single reusable file, then call
it from a parent pipeline as if it were one node. This is Layer 2 of the
reuse story in `planning/29-reusable-transforms.md`.

## Defining a snippet

A snippet is a regular pipeline JSON file with three extra top-level
fields: `snippet` (the snippet's name), `params` (a map of parameter
names to types), and `outputs` (internal node IDs exposed to the
caller). The file stem must match the `snippet` field.

```json
{
  "name": "standard_ingest",
  "snippet": "standard_ingest",
  "params": {
    "table": "string",
    "pk": "column"
  },
  "outputs": ["cleansed"],
  "nodes": [
    { "id": "raw", "name": "raw", "type": "source", "connector": "postgresql",
      "config": { "table": "{{ table }}" } },
    { "id": "cleansed", "name": "cleansed", "type": "transform", "mode": "sql",
      "code": "SELECT * FROM raw WHERE {{ pk }} IS NOT NULL" }
  ],
  "edges": [{ "from": "raw", "to": "cleansed" }]
}
```

`{{ param_name }}` references in source/sink `config` values and
transform `code` are substituted at expansion time using the call site's
`params` map.

## Parameter types

Exactly five types are supported:

| Type          | Accepts                          | Substitution                        |
|---------------|----------------------------------|-------------------------------------|
| `string`      | JSON string                      | raw string                          |
| `number`      | JSON number                      | number as text                      |
| `bool`        | JSON boolean                     | `true`/`false`                      |
| `column`      | JSON string                      | raw string (caller supplies ident)  |
| `column_list` | JSON array of strings            | comma-joined list (`a, b, c`)       |

## Calling a snippet

The parent pipeline declares a `snippets_dir` (resolved relative to the
pipeline file) and uses a snippet call-site node. Call-site nodes have
no `type` field — the presence of the `snippet` key is the discriminator:

```json
{
  "name": "orders",
  "snippets_dir": "./snippets",
  "nodes": [
    { "id": "ingest", "name": "ingest", "snippet": "standard_ingest",
      "params": { "table": "orders", "pk": "order_id" } },
    { "id": "summarize", "name": "summarize", "type": "transform", "mode": "sql",
      "code": "SELECT count(*) FROM \"ingest.cleansed\"" }
  ],
  "edges": [
    { "from": "ingest.cleansed", "to": "summarize" }
  ]
}
```

At load time each snippet call-site is replaced by its inner nodes,
with every inner node ID rewritten as `<call_site>.<inner_id>`. Edges
from `"ingest.cleansed"` to the rest of the parent DAG Just Work because
the external alias already matches the namespaced ID.

## Inspecting snippets from the CLI

```
armillary snippet list path/to/pipeline.json
armillary snippet expand path/to/pipeline.json
```

`expand` prints the fully materialized pipeline JSON — useful for
debugging substitution and namespacing.

## Composition with UDFs

Snippets and UDFs (Layer 1) compose naturally. A snippet's inner SQL
transform can call any UDF declared in the parent pipeline's `udfs_dir`:
both are resolved during the same load pass and the expanded transform
still goes through the normal SQL compile path.

## v1 limitations

- **Outputs only, no inputs.** A snippet's inner nodes cannot consume
  data from the parent pipeline — the parent can only read from
  `<call_site>.<output_name>`. If you need a reusable block that
  accepts upstream data, write it as a regular pipeline node for now.
- **No canvas rendering.** Snippet call sites appear in the pipeline
  store but are not rendered as collapsible groups in the frontend
  canvas yet. This is tracked as a follow-up in doc 29.
- **No versioning.** Snippets are resolved by file stem at load time;
  there is no `@version` selector.
