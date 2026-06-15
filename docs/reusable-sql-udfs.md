# Reusable SQL UDFs

Armillary lets you define scalar SQL functions once in a `.sql` file and call
them from any SQL transform in a pipeline. This is Layer 1 of the reuse
story described in `planning/29-reusable-transforms.md`: pure, scalar,
row-level logic that gets inlined into transform SQL at execution time.

## Defining a UDF

Create a directory of `.sql` files. Each file declares one or more
PostgreSQL-style `CREATE FUNCTION` statements with a single SQL expression
as the body:

```sql
-- udfs/normalize_name.sql
CREATE OR REPLACE FUNCTION normalize_name(s VARCHAR) RETURNS VARCHAR
AS $$
    LOWER(TRIM(s))
$$ LANGUAGE SQL IMMUTABLE;
```

Rules:

- The body must be a **single SQL expression**. Multi-statement bodies,
  `RETURN`, `BEGIN`/`END`, and procedural code are rejected at load time.
- Function names must be **single-segment** (no `schema.func`).
- All parameters must be **named**.
- `OR REPLACE`, `LANGUAGE SQL`, and `IMMUTABLE`/`STABLE`/`VOLATILE` are
  parsed but ignored — armillary treats every UDF as immutable.

## Wiring a UDF directory into a pipeline

Set the pipeline-level `udfs_dir` field. Paths are resolved relative to
the process working directory:

```json
{
  "name": "my_pipeline",
  "udfs_dir": "./udfs",
  "nodes": [ ... ],
  "edges": [ ... ]
}
```

When the executor starts a run, it scans `udfs_dir` for `*.sql` files,
parses each one, and builds a registry. Errors at this stage (parse
failures, duplicate names, unsupported bodies) fail the pipeline before
any node executes.

## Calling UDFs from SQL transforms

Once registered, a UDF is callable from any SQL transform in the pipeline:

```sql
SELECT id, normalize_name(customer_name) AS name
FROM customers
```

At execution time the parser walks the SQL AST and replaces every call
to `normalize_name` with its body, substituting `s` with the call-site
expression `customer_name`. The expanded query is then handed to
DataFusion as if you had written it inline. There is **no string
templating** — everything happens on the parsed expression tree.

## Limitations (v1)

- **Scalar only.** UDFs cannot generate dynamic SQL, loop over column
  lists, or abstract join logic. For those cases, see Layers 2 and 3 in
  `planning/29-reusable-transforms.md`.
- **Parameter-name shadowing.** If a UDF parameter shares a name with a
  column reference inside the UDF body, the parameter wins. Use
  distinctive parameter names (`_s`, `arg_name`, etc.) to avoid surprises.
- **Python transforms cannot call UDFs.** UDFs only apply to SQL
  transforms. Python transforms should reuse logic via standard Python
  imports.

## CLI

```sh
armillary udf list path/to/pipeline.json
```

Lists every UDF discovered in the pipeline's `udfs_dir`, with its
signature and source file. Use `--json` for machine-readable output.
