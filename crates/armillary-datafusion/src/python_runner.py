# Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
# SPDX-License-Identifier: MIT OR Apache-2.0

"""
Armillary — Python transform runner.

This script is invoked as a subprocess by the Rust pipeline executor.
It reads a JSON manifest describing inputs, parameters, and user code,
loads Arrow IPC files as Polars DataFrames, executes the user's transform
function, and writes the output as an Arrow IPC file.

Usage: python3 python_runner.py <manifest_path> <output_path>

Manifest format (JSON):
{
    "inputs": {"node_id": "/path/to/input.arrow", ...},
    "params": {"run_date": "2026-01-01", ...},
    "code": "def transform(inputs, params):\n    ...",
    "lineage_path": "/path/to/lineage.json"  (optional)
}

The user's code must define a function:
    def transform(inputs: dict[str, pl.DataFrame], params: dict)
        -> pl.DataFrame | pl.LazyFrame

If a LazyFrame is returned, column-level lineage is extracted before
collecting. The lineage is written to `lineage_path` if provided.

Exit codes:
    0 — success (output written to output_path)
    1 — user code error (traceback on stderr)
    2 — runner infrastructure error (message on stderr)
"""

import json
import sys
import traceback
from typing import Any


# ---------------------------------------------------------------------------
# User-facing decorator: @column_lineage(outputs={...})
# ---------------------------------------------------------------------------

def column_lineage(*, outputs: dict[str, list]) -> Any:
    """Annotate a transform function with explicit column lineage.

    Use this when your transform uses eager Polars operations (DataFrames) and
    automatic lineage cannot be derived from a LazyFrame plan.

    ``outputs`` maps each output column name to a list of upstream column
    references that contribute to it.  Each entry in the list is either:

    * A bare string — interpreted as ``(column_name, "derived")``.
    * A ``(column_name, relationship)`` tuple where *relationship* is one of:
      ``"direct"``, ``"derived"``, ``"cast"``, ``"filter"``, ``"group_by"``,
      ``"aggregate_input"``, ``"join_key"``, ``"join_passthrough"``,
      ``"window_partition"``, ``"window_order"``, ``"window_input"``,
      ``"opaque"``.

    Example::

        @column_lineage(outputs={
            "total": [("price", "derived"), ("qty", "derived")],
            "name": [("name", "direct")],
            "status": ["active_flag"],  # defaults to "derived"
        })
        def transform(inputs, params):
            ...
    """
    # Normalise each entry to (column, relationship) tuples.
    normalised: dict[str, list[tuple[str, str]]] = {}
    for out_col, sources in outputs.items():
        entries: list[tuple[str, str]] = []
        for src in sources:
            if isinstance(src, str):
                entries.append((src, "derived"))
            elif isinstance(src, (tuple, list)) and len(src) == 2:
                entries.append((str(src[0]), str(src[1])))
            else:
                raise TypeError(
                    f"column_lineage: each source must be a string or "
                    f"(column, relationship) tuple, got {src!r} for output '{out_col}'"
                )
        normalised[out_col] = entries

    def decorator(fn: Any) -> Any:
        fn._flux_lineage_annotation = normalised
        return fn

    return decorator


def _annotation_to_lineage(annotation: dict[str, list[tuple[str, str]]]) -> dict:
    """Convert a ``_flux_lineage_annotation`` dict to the lineage sidecar format."""
    edges: list[dict[str, str]] = []
    for out_col, sources in annotation.items():
        for upstream_col, relationship in sources:
            edge: dict[str, str] = {
                "upstream_column": upstream_col,
                "downstream_column": out_col,
                "relationship": relationship,
            }
            edges.append(edge)
    return {
        "edges": edges,
        "confidence": "annotation",
        "warnings": [],
    }


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: python3 python_runner.py <manifest_path> <output_path>", file=sys.stderr)
        return 2

    manifest_path = sys.argv[1]
    output_path = sys.argv[2]

    # Read manifest.
    try:
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
    except Exception as e:
        print(f"RUNNER_ERROR: failed to read manifest: {e}", file=sys.stderr)
        return 2

    # Import polars (after reading manifest so we get a clear error).
    try:
        import polars as pl
    except ImportError:
        print(
            "RUNNER_ERROR: polars is not installed. "
            "Install it with: uv pip install polars",
            file=sys.stderr,
        )
        return 2

    # Load input DataFrames from Arrow IPC files.
    inputs: dict[str, pl.DataFrame] = {}
    for name, path in manifest.get("inputs", {}).items():
        try:
            inputs[name] = pl.read_ipc(path)
        except Exception as e:
            print(f"RUNNER_ERROR: failed to read input '{name}' from {path}: {e}", file=sys.stderr)
            return 2

    params: dict = manifest.get("params", {})
    code: str = manifest.get("code", "")

    # Validate the user code by parsing it first.
    try:
        compiled = compile(code, "<transform>", "exec")
    except SyntaxError as e:
        print(f"SyntaxError in transform code: {e}", file=sys.stderr)
        return 1

    # Execute user code in a clean namespace.
    # Inject polars and the lineage decorator so user code can use both.
    namespace: dict = {"pl": pl, "column_lineage": column_lineage}
    try:
        exec(compiled, namespace)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1

    # Find the transform function.
    transform_fn = namespace.get("transform")
    if transform_fn is None:
        print(
            "USER_ERROR: transform code must define a function named 'transform'. "
            "Example:\n\n"
            "def transform(inputs: dict[str, pl.DataFrame], params: dict) -> pl.DataFrame:\n"
            "    return inputs['my_table'].filter(pl.col('status') == 'active')",
            file=sys.stderr,
        )
        return 1

    if not callable(transform_fn):
        print("USER_ERROR: 'transform' must be a callable function", file=sys.stderr)
        return 1

    # Call the user's transform function.
    try:
        result = transform_fn(inputs, params)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1

    # Accept LazyFrame returns — extract lineage before collecting.
    lineage_path = manifest.get("lineage_path")

    # Check for user-provided lineage annotation (from @column_lineage decorator).
    has_annotation = hasattr(transform_fn, "_flux_lineage_annotation")

    if has_annotation and lineage_path:
        # Annotation takes precedence — write it directly, skip LazyFrame walk.
        try:
            from polars_lineage import write_lineage_json

            annotation_lineage = _annotation_to_lineage(transform_fn._flux_lineage_annotation)
            write_lineage_json(annotation_lineage, lineage_path)
        except Exception as e:
            print(f"WARNING: annotation lineage write failed: {e}", file=sys.stderr)

    if isinstance(result, pl.LazyFrame):
        # Extract column lineage from the LazyFrame plan (only if no annotation).
        if lineage_path and not has_annotation:
            try:
                from polars_lineage import extract_lineage, write_lineage_json

                lineage = extract_lineage(result)
                write_lineage_json(lineage, lineage_path)
            except Exception as e:
                # Lineage extraction failure is non-fatal — log and continue.
                print(
                    f"WARNING: lineage extraction failed: {e}",
                    file=sys.stderr,
                )

        # Collect the LazyFrame into a DataFrame.
        try:
            result = result.collect()
        except Exception:
            traceback.print_exc(file=sys.stderr)
            return 1

    # Validate the result.
    if not isinstance(result, pl.DataFrame):
        actual_type = type(result).__name__
        print(
            f"USER_ERROR: transform function must return a polars DataFrame "
            f"or LazyFrame, got {actual_type}. "
            f"Hint: make sure your function returns a pl.DataFrame or pl.LazyFrame.",
            file=sys.stderr,
        )
        return 1

    # Write output as Arrow IPC.
    try:
        result.write_ipc(output_path)
    except Exception as e:
        print(f"RUNNER_ERROR: failed to write output: {e}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
