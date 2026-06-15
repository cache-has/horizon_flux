# Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
# SPDX-License-Identifier: MIT OR Apache-2.0

"""
Polars LazyFrame column lineage walker.

Walks a Polars LazyFrame optimized plan via the internal NodeTraverser API
to extract column-level lineage edges. Falls back to opaque (every-to-every)
edges for unsupported operations.

The walker is version-gated: it checks the IR protocol version returned by
NodeTraverser.version() and refuses to walk plans from unsupported versions,
falling back gracefully.

Output format: list of dicts, each with:
    upstream_column, downstream_column, relationship, expression_text (optional)

Supported IR major versions: 12 (Polars 1.x).
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

logger = logging.getLogger("polars_lineage")

# IR major versions we know how to walk.
SUPPORTED_IR_MAJOR = {12}


def extract_lineage(lf: Any) -> dict:
    """Extract column lineage from a Polars LazyFrame.

    Returns a dict with:
        "edges": list of edge dicts
        "confidence": "lazyframe" | "opaque"
        "warnings": list of warning strings

    If the plan cannot be walked (unsupported version, missing API, etc.),
    returns opaque lineage based on input/output schema diff.
    """
    import polars as pl

    warnings: list[str] = []

    # Validate input.
    if not isinstance(lf, pl.LazyFrame):
        return _opaque_result([], _schema_columns(lf.collect_schema()), ["not a LazyFrame"])

    # Check API availability.
    inner = lf._ldf
    if not hasattr(inner, "visit"):
        warnings.append("NodeTraverser API not available in this Polars version")
        return _opaque_result(
            [], _schema_columns(lf.collect_schema()), warnings
        )

    try:
        nt = inner.visit()
    except Exception as e:
        warnings.append(f"visit() failed: {e}")
        return _opaque_result([], _schema_columns(lf.collect_schema()), warnings)

    # Version gate.
    try:
        major, minor = nt.version()
    except Exception as e:
        warnings.append(f"version() failed: {e}")
        return _opaque_result([], _schema_columns(lf.collect_schema()), warnings)

    if major not in SUPPORTED_IR_MAJOR:
        warnings.append(
            f"unsupported IR version ({major}, {minor}); "
            f"supported majors: {SUPPORTED_IR_MAJOR}"
        )
        return _opaque_result([], _schema_columns(lf.collect_schema()), warnings)

    # Walk the optimized plan.
    try:
        edges, walk_warnings = _walk_plan(nt)
        warnings.extend(walk_warnings)
        return {
            "edges": edges,
            "confidence": "lazyframe",
            "warnings": warnings,
        }
    except Exception as e:
        warnings.append(f"plan walk failed: {e}")
        return _opaque_result([], _schema_columns(lf.collect_schema()), warnings)


def _schema_columns(schema: Any) -> list[str]:
    """Extract column names from a Polars schema."""
    return list(schema.names()) if hasattr(schema, "names") else list(schema)


def _opaque_result(
    input_columns: list[str],
    output_columns: list[str],
    warnings: list[str],
) -> dict:
    """Build opaque (every-to-every) lineage result."""
    edges = []
    for out_col in output_columns:
        for in_col in input_columns:
            edges.append({
                "upstream_column": in_col,
                "downstream_column": out_col,
                "relationship": "opaque",
            })
    return {
        "edges": edges,
        "confidence": "opaque",
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Plan walker
# ---------------------------------------------------------------------------

def _walk_plan(nt: Any) -> tuple[list[dict], list[str]]:
    """Walk the optimized plan tree and return (edges, warnings).

    Returns a flat list of edges representing column flow from input schemas
    to the root output schema. Each edge is:
        {upstream_column, downstream_column, relationship, expression_text?}
    """
    warnings: list[str] = []

    # Build column maps bottom-up. Each node produces a map:
    #   output_column -> list of (source_column, relationship, expr_text?)
    # where source_column is a column from the leaf (DataFrameScan/Scan) schemas.

    root_id = nt.get_node()
    col_map = _walk_node(nt, root_id, warnings)

    # Flatten the column map into edges.
    edges: list[dict] = []
    for out_col, sources in col_map.items():
        for src_col, rel, expr_text in sources:
            edge: dict[str, str] = {
                "upstream_column": src_col,
                "downstream_column": out_col,
                "relationship": rel,
            }
            if expr_text:
                edge["expression_text"] = expr_text
            edges.append(edge)

    return edges, warnings


# Type alias: column_name -> [(source_column, relationship, expression_text?)]
ColumnMap = dict[str, list[tuple[str, str, str | None]]]


def _walk_node(nt: Any, node_id: int, warnings: list[str]) -> ColumnMap:
    """Walk a single plan node. Returns the column map for this node."""
    nt.set_node(node_id)
    node = nt.view_current_node()
    node_type = type(node).__name__
    inputs = nt.get_inputs()

    # Recurse into children first.
    child_maps: list[ColumnMap] = []
    for child_id in inputs:
        child_maps.append(_walk_node(nt, child_id, warnings))

    # Navigate back to this node (children may have moved the cursor).
    nt.set_node(node_id)

    handler = _NODE_HANDLERS.get(node_type)
    if handler is not None:
        return handler(nt, node_id, child_maps, warnings)

    # Unknown node type — fall through with passthrough if single child,
    # else opaque.
    if len(child_maps) == 1:
        warnings.append(f"unknown plan node '{node_type}', assuming passthrough")
        return child_maps[0]
    elif len(child_maps) == 0:
        warnings.append(f"unknown leaf node '{node_type}'")
        return _schema_to_identity(nt)
    else:
        warnings.append(f"unknown multi-input node '{node_type}', using opaque")
        return _merge_opaque(nt, child_maps)


def _schema_to_identity(nt: Any) -> ColumnMap:
    """Create an identity column map from the current node's schema."""
    schema = nt.get_schema()
    result: ColumnMap = {}
    for col in schema:
        result[col] = [(col, "direct", None)]
    return result


def _merge_opaque(nt: Any, child_maps: list[ColumnMap]) -> ColumnMap:
    """Create opaque edges from all child columns to all output columns."""
    schema = nt.get_schema()
    all_sources: list[tuple[str, str, str | None]] = []
    for cm in child_maps:
        for sources in cm.values():
            all_sources.extend(sources)

    # Deduplicate source columns.
    seen: set[str] = set()
    unique_sources: list[tuple[str, str, str | None]] = []
    for src_col, _rel, _expr in all_sources:
        if src_col not in seen:
            seen.add(src_col)
            unique_sources.append((src_col, "opaque", None))

    result: ColumnMap = {}
    for col in schema:
        result[col] = list(unique_sources)
    return result


# ---------------------------------------------------------------------------
# Expression walker
# ---------------------------------------------------------------------------

def _walk_expr(nt: Any, expr_node: int) -> list[str]:
    """Walk an expression tree and return all leaf column names."""
    expr = nt.view_expression(expr_node)
    expr_type = type(expr).__name__

    if expr_type == "Column":
        return [expr.name]
    elif expr_type == "Literal" or expr_type == "Len":
        return []

    # For composite expressions, gather children from known attributes.
    columns: list[str] = []

    # BinaryExpr: left, right
    if hasattr(expr, "left") and hasattr(expr, "right"):
        columns.extend(_walk_expr(nt, expr.left))
        columns.extend(_walk_expr(nt, expr.right))
    # Unary wrappers: expr attribute
    if hasattr(expr, "expr") and isinstance(expr.expr, int):
        columns.extend(_walk_expr(nt, expr.expr))
    # Function / Agg: arguments list or input list
    for attr in ("arguments", "input"):
        val = getattr(expr, attr, None)
        if isinstance(val, list):
            for child in val:
                if isinstance(child, int):
                    columns.extend(_walk_expr(nt, child))
    # Ternary: predicate, truthy, falsy
    for attr in ("predicate", "truthy", "falsy"):
        val = getattr(expr, attr, None)
        if isinstance(val, int):
            columns.extend(_walk_expr(nt, val))
    # Window: function, partition_by, order_by
    if hasattr(expr, "function") and isinstance(expr.function, int):
        columns.extend(_walk_expr(nt, expr.function))
    if hasattr(expr, "partition_by") and isinstance(expr.partition_by, list):
        for child in expr.partition_by:
            if isinstance(child, int):
                columns.extend(_walk_expr(nt, child))
    # Sort expression
    if expr_type == "Sort" and hasattr(expr, "expr") and isinstance(expr.expr, int):
        pass  # already handled above via hasattr(expr, "expr")
    # Gather
    if hasattr(expr, "idx") and isinstance(expr.idx, int):
        columns.extend(_walk_expr(nt, expr.idx))

    return columns


def _classify_expr(nt: Any, expr_node: int) -> str:
    """Classify an expression into a relationship kind."""
    expr = nt.view_expression(expr_node)
    expr_type = type(expr).__name__

    if expr_type == "Column":
        return "direct"
    elif expr_type == "Cast":
        return "cast"
    elif expr_type == "Agg":
        return "aggregate_input"
    elif expr_type == "Window":
        return "window_input"
    else:
        return "derived"


def _expr_display(nt: Any, expr_ir: Any) -> str | None:
    """Get a display string for an expression. Returns None for trivial columns."""
    try:
        expr = nt.view_expression(expr_ir.node)
        if type(expr).__name__ == "Column":
            return None
        # Use output_name as a hint — not perfect but useful.
        return expr_ir.output_name
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Node handlers
# ---------------------------------------------------------------------------

def _resolve_exprs(
    nt: Any, node_id: int, child_map: ColumnMap, warnings: list[str]
) -> ColumnMap:
    """Resolve a list of expressions against a child column map.

    Used for Select and HStack nodes.
    """
    nt.set_node(node_id)
    exprs = nt.get_exprs()
    result: ColumnMap = {}

    for expr_ir in exprs:
        out_name = expr_ir.output_name
        cols = _walk_expr(nt, expr_ir.node)
        rel = _classify_expr(nt, expr_ir.node)
        expr_text = _expr_display(nt, expr_ir)

        sources: list[tuple[str, str, str | None]] = []
        if cols:
            for col in _dedupe(cols):
                # Trace through child map to get original source columns.
                if col in child_map:
                    for src_col, _src_rel, _src_expr in child_map[col]:
                        sources.append((src_col, rel, expr_text))
                else:
                    # Column not in child — may be a literal alias or generated.
                    sources.append((col, rel, expr_text))
        # No column refs means this is a pure literal/len — no lineage edges.

        if sources:
            result[out_name] = sources

    return result


def _dedupe(items: list[str]) -> list[str]:
    """Deduplicate a list preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _handle_select(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Select node (.select(), .drop(), .rename())."""
    child_map = child_maps[0] if child_maps else {}
    return _resolve_exprs(nt, node_id, child_map, warnings)


def _handle_hstack(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle HStack node (.with_columns())."""
    child_map = child_maps[0] if child_maps else {}
    # Start with passthrough of all child columns.
    result: ColumnMap = dict(child_map)
    # Overlay new/replaced columns from expressions.
    new_cols = _resolve_exprs(nt, node_id, child_map, warnings)
    result.update(new_cols)
    return result


def _handle_filter(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Filter node (.filter()). Passthrough + filter edges."""
    child_map = child_maps[0] if child_maps else {}
    # All columns pass through unchanged.
    result: ColumnMap = {}
    for col, sources in child_map.items():
        result[col] = list(sources)

    # Extract filter predicate columns and add filter edges.
    nt.set_node(node_id)
    exprs = nt.get_exprs()
    if exprs:
        for expr_ir in exprs:
            filter_cols = _walk_expr(nt, expr_ir.node)
            for out_col in result:
                for fcol in _dedupe(filter_cols):
                    if fcol in child_map:
                        for src_col, _rel, _expr in child_map[fcol]:
                            result[out_col].append((src_col, "filter", None))

    return result


def _handle_sort(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Sort — pure passthrough."""
    return child_maps[0] if child_maps else {}


def _handle_distinct(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Distinct (.unique()) — passthrough."""
    return child_maps[0] if child_maps else {}


def _handle_slice(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Slice (.slice(), .limit(), .head(), .tail()) — passthrough."""
    return child_maps[0] if child_maps else {}


def _handle_scan(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Scan / DataFrameScan — leaf node, identity map."""
    return _schema_to_identity(nt)


def _handle_groupby(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle GroupBy (.group_by().agg())."""
    child_map = child_maps[0] if child_maps else {}

    nt.set_node(node_id)
    node = nt.view_current_node()
    result: ColumnMap = {}

    # Keys — these are group-by columns (PyExprIR objects).
    keys = getattr(node, "keys", [])
    if isinstance(keys, list):
        for key_expr_ir in keys:
            if hasattr(key_expr_ir, "output_name") and hasattr(key_expr_ir, "node"):
                out_name = key_expr_ir.output_name
                key_cols = _walk_expr(nt, key_expr_ir.node)
                sources: list[tuple[str, str, str | None]] = []
                for kcol in _dedupe(key_cols):
                    if kcol in child_map:
                        for src_col, _rel, _expr in child_map[kcol]:
                            sources.append((src_col, "group_by", None))
                    else:
                        sources.append((kcol, "group_by", None))
                result[out_name] = sources

    # Aggregations (also PyExprIR objects).
    aggs = getattr(node, "aggs", [])
    if isinstance(aggs, list):
        for agg_expr_ir in aggs:
            if not hasattr(agg_expr_ir, "output_name"):
                continue
            out_name = agg_expr_ir.output_name
            cols = _walk_expr(nt, agg_expr_ir.node)
            sources = []
            for col in _dedupe(cols):
                if col in child_map:
                    for src_col, _rel, _expr in child_map[col]:
                        sources.append((src_col, "aggregate_input", out_name))
                else:
                    sources.append((col, "aggregate_input", out_name))
            result[out_name] = sources

    return result


def _handle_join(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Join."""
    if len(child_maps) < 2:
        warnings.append("join node with fewer than 2 inputs")
        return child_maps[0] if child_maps else {}

    left_map, right_map = child_maps[0], child_maps[1]

    nt.set_node(node_id)
    node = nt.view_current_node()
    schema = nt.get_schema()

    # Extract join key column names.
    left_on_cols: set[str] = set()
    right_on_cols: set[str] = set()
    left_on = getattr(node, "left_on", [])
    right_on = getattr(node, "right_on", [])
    if isinstance(left_on, list):
        for expr_ir in left_on:
            if hasattr(expr_ir, "node"):
                left_on_cols.update(_walk_expr(nt, expr_ir.node))
    if isinstance(right_on, list):
        for expr_ir in right_on:
            if hasattr(expr_ir, "node"):
                right_on_cols.update(_walk_expr(nt, expr_ir.node))

    result: ColumnMap = {}

    for col in schema:
        sources: list[tuple[str, str, str | None]] = []

        # Check left side.
        if col in left_map:
            rel = "join_key" if col in left_on_cols else "join_passthrough"
            for src_col, _rel, _expr in left_map[col]:
                sources.append((src_col, rel, None))

        # Check right side.
        if col in right_map:
            rel = "join_key" if col in right_on_cols else "join_passthrough"
            for src_col, _rel, _expr in right_map[col]:
                sources.append((src_col, rel, None))

        # Handle suffixed columns (e.g., col_right from join).
        if not sources:
            # Try stripping common suffixes.
            for suffix in ("_right", "_left"):
                base = col[: -len(suffix)] if col.endswith(suffix) else None
                if base and base in right_map:
                    for src_col, _rel, _expr in right_map[base]:
                        sources.append((src_col, "join_passthrough", None))
                if base and base in left_map:
                    for src_col, _rel, _expr in left_map[base]:
                        sources.append((src_col, "join_passthrough", None))

        if sources:
            result[col] = sources
        else:
            # Column we can't trace — opaque.
            result[col] = [(col, "opaque", None)]
            warnings.append(f"join: could not trace column '{col}'")

    return result


def _handle_union(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle Union (pl.concat()). Merge by column name across inputs."""
    schema = nt.get_schema()
    result: ColumnMap = {}

    for col in schema:
        sources: list[tuple[str, str, str | None]] = []
        for cm in child_maps:
            if col in cm:
                sources.extend(cm[col])
        result[col] = sources if sources else [(col, "direct", None)]

    return result


def _handle_map_function(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle MapFunction (.rename(), .explode(), .unpivot(), etc.).

    These are harder to introspect — we do our best for Rename and fall back
    to passthrough/opaque for others.
    """
    child_map = child_maps[0] if child_maps else {}

    nt.set_node(node_id)
    node = nt.view_current_node()
    func = getattr(node, "function", None)

    # func is typically a tuple like ('Rename', {'existing': [...], 'new': [...]})
    if isinstance(func, tuple) and len(func) >= 1:
        func_name = func[0] if isinstance(func[0], str) else str(func[0])

        if func_name == "Rename" and len(func) >= 2:
            rename_info = func[1] if isinstance(func[1], dict) else {}
            existing = rename_info.get("existing", [])
            new = rename_info.get("new", [])
            if existing and new and len(existing) == len(new):
                rename_map = dict(zip(existing, new))
                result: ColumnMap = {}
                for col, sources in child_map.items():
                    new_name = rename_map.get(col, col)
                    result[new_name] = [(src, "direct", None) for src, _, _ in sources]
                return result

        # For Explode, Unpivot, etc. — passthrough with a warning.
        warnings.append(f"MapFunction '{func_name}' uses passthrough lineage")

    # Default: passthrough from child, overlaid with output schema.
    schema = nt.get_schema()
    result = {}
    for col in schema:
        if col in child_map:
            result[col] = child_map[col]
        else:
            # New column from the map function — opaque against all inputs.
            all_sources = []
            for sources in child_map.values():
                all_sources.extend(sources)
            seen: set[str] = set()
            result[col] = []
            for src, _, _ in all_sources:
                if src not in seen:
                    seen.add(src)
                    result[col].append((src, "opaque", None))
            if not result[col]:
                warnings.append(f"MapFunction: no sources for column '{col}'")
    return result


def _handle_simple_projection(
    nt: Any, node_id: int, child_maps: list[ColumnMap], warnings: list[str]
) -> ColumnMap:
    """Handle SimpleProjection — optimizer-inserted column subset."""
    child_map = child_maps[0] if child_maps else {}
    schema = nt.get_schema()
    result: ColumnMap = {}
    for col in schema:
        if col in child_map:
            result[col] = child_map[col]
        else:
            result[col] = [(col, "direct", None)]
    return result


# Registry of node type handlers.
_NODE_HANDLERS: dict[str, Any] = {
    "Select": _handle_select,
    "HStack": _handle_hstack,
    "Filter": _handle_filter,
    "Sort": _handle_sort,
    "Distinct": _handle_distinct,
    "Slice": _handle_slice,
    "Scan": _handle_scan,
    "DataFrameScan": _handle_scan,
    "GroupBy": _handle_groupby,
    "Join": _handle_join,
    "Union": _handle_union,
    "MapFunction": _handle_map_function,
    "SimpleProjection": _handle_simple_projection,
}


# ---------------------------------------------------------------------------
# CLI entry point (for testing)
# ---------------------------------------------------------------------------

def write_lineage_json(lineage: dict, path: str) -> None:
    """Write lineage result to a JSON file."""
    with open(path, "w") as f:
        json.dump(lineage, f, indent=2)
