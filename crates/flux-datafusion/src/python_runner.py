# Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
# SPDX-License-Identifier: MIT OR Apache-2.0

"""
Horizon Flux — Python transform runner.

This script is invoked as a subprocess by the Rust pipeline executor.
It reads a JSON manifest describing inputs, parameters, and user code,
loads Arrow IPC files as Polars DataFrames, executes the user's transform
function, and writes the output as an Arrow IPC file.

Usage: python3 python_runner.py <manifest_path> <output_path>

Manifest format (JSON):
{
    "inputs": {"node_id": "/path/to/input.arrow", ...},
    "params": {"run_date": "2026-01-01", ...},
    "code": "def transform(inputs, params):\n    ..."
}

The user's code must define a function:
    def transform(inputs: dict[str, pl.DataFrame], params: dict) -> pl.DataFrame

Exit codes:
    0 — success (output written to output_path)
    1 — user code error (traceback on stderr)
    2 — runner infrastructure error (message on stderr)
"""

import json
import sys
import traceback


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
    namespace: dict = {"pl": pl}
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

    # Validate the result.
    if not isinstance(result, pl.DataFrame):
        actual_type = type(result).__name__
        print(
            f"USER_ERROR: transform function must return a polars DataFrame, "
            f"got {actual_type}. "
            f"Hint: make sure your function returns a pl.DataFrame.",
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
