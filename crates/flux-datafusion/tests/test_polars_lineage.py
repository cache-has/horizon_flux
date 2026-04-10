# Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
# SPDX-License-Identifier: MIT OR Apache-2.0

"""
Golden-file tests for the Polars LazyFrame column lineage walker.

Run with: uv run pytest crates/flux-datafusion/tests/test_polars_lineage.py -v

Requires polars >= 1.39.3.
"""

import json
import sys
import os

import pytest

# Add the source dir so we can import polars_lineage directly.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src"),
)

import polars as pl
from polars_lineage import extract_lineage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def edge_set(lineage: dict) -> set[tuple[str, str, str]]:
    """Extract (upstream, downstream, relationship) tuples from lineage edges."""
    return {
        (e["upstream_column"], e["downstream_column"], e["relationship"])
        for e in lineage["edges"]
    }


def downstream_for(lineage: dict, col: str) -> set[tuple[str, str]]:
    """Get (upstream_column, relationship) pairs for a given downstream column."""
    return {
        (e["upstream_column"], e["relationship"])
        for e in lineage["edges"]
        if e["downstream_column"] == col
    }


# ---------------------------------------------------------------------------
# Select / projection
# ---------------------------------------------------------------------------


def test_select_passthrough():
    """Direct column passthrough via .select()."""
    df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
    lf = df.lazy().select("a", "b")
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    assert downstream_for(lineage, "a") == {("a", "direct")}
    assert downstream_for(lineage, "b") == {("b", "direct")}
    # Column c should not appear.
    assert not any(e["downstream_column"] == "c" for e in lineage["edges"])


def test_select_expression():
    """Derived column via expression in .select()."""
    df = pl.DataFrame({"amount": [100.0], "tax_rate": [0.08]})
    lf = df.lazy().select(
        (pl.col("amount") * (1 + pl.col("tax_rate"))).alias("total")
    )
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    sources = downstream_for(lineage, "total")
    upstream_cols = {col for col, _ in sources}
    assert "amount" in upstream_cols
    assert "tax_rate" in upstream_cols
    assert all(rel == "derived" for _, rel in sources)


def test_select_rename():
    """Column rename via .alias()."""
    df = pl.DataFrame({"old_name": [1]})
    lf = df.lazy().select(pl.col("old_name").alias("new_name"))
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    assert downstream_for(lineage, "new_name") == {("old_name", "direct")}


# ---------------------------------------------------------------------------
# With columns
# ---------------------------------------------------------------------------


def test_with_columns():
    """.with_columns() adds new columns while preserving existing ones."""
    df = pl.DataFrame({"price": [10.0], "qty": [5]})
    lf = df.lazy().with_columns(
        (pl.col("price") * pl.col("qty")).alias("total")
    )
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    # Original columns pass through.
    assert downstream_for(lineage, "price") == {("price", "direct")}
    assert downstream_for(lineage, "qty") == {("qty", "direct")}
    # Derived column.
    total_sources = downstream_for(lineage, "total")
    upstream_cols = {col for col, _ in total_sources}
    assert "price" in upstream_cols
    assert "qty" in upstream_cols


def test_with_columns_cast():
    """Type cast via .with_columns()."""
    df = pl.DataFrame({"x": [1]})
    lf = df.lazy().with_columns(pl.col("x").cast(pl.Float64))
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    sources = downstream_for(lineage, "x")
    assert any(rel == "cast" for _, rel in sources)


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


def test_filter():
    """.filter() produces filter relationship edges."""
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    lf = df.lazy().filter(pl.col("a") > 1)
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    # Both columns should pass through.
    a_rels = {rel for _, rel in downstream_for(lineage, "a")}
    b_rels = {rel for _, rel in downstream_for(lineage, "b")}
    assert "direct" in a_rels
    assert "direct" in b_rels
    # Filter edges should be present (a filters all columns).
    assert "filter" in a_rels or "filter" in b_rels


# ---------------------------------------------------------------------------
# Group by / aggregate
# ---------------------------------------------------------------------------


def test_group_by_agg():
    """GROUP BY with aggregation."""
    df = pl.DataFrame({"cat": ["a", "b"], "val": [1, 2]})
    lf = df.lazy().group_by("cat").agg(pl.col("val").sum().alias("total"))
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    cat_sources = downstream_for(lineage, "cat")
    assert any(rel == "group_by" for _, rel in cat_sources)
    total_sources = downstream_for(lineage, "total")
    assert any(rel == "aggregate_input" for _, rel in total_sources)
    assert any(col == "val" for col, _ in total_sources)


# ---------------------------------------------------------------------------
# Sort / distinct / slice — passthrough
# ---------------------------------------------------------------------------


def test_sort_passthrough():
    """Sort doesn't change column lineage."""
    df = pl.DataFrame({"a": [2, 1], "b": [3, 4]})
    lf = df.lazy().sort("a")
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    assert downstream_for(lineage, "a") == {("a", "direct")}
    assert downstream_for(lineage, "b") == {("b", "direct")}


def test_distinct_passthrough():
    """.unique() doesn't change column lineage."""
    df = pl.DataFrame({"a": [1, 1], "b": [2, 2]})
    lf = df.lazy().unique()
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    assert downstream_for(lineage, "a") == {("a", "direct")}


def test_slice_passthrough():
    """.slice() doesn't change column lineage."""
    df = pl.DataFrame({"a": [1, 2, 3]})
    lf = df.lazy().slice(0, 2)
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    assert downstream_for(lineage, "a") == {("a", "direct")}


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


def test_join():
    """Join produces join_key and join_passthrough edges."""
    left = pl.DataFrame({"id": [1], "val_l": [10]})
    right = pl.DataFrame({"id": [1], "val_r": [20]})
    lf = left.lazy().join(right.lazy(), on="id", how="inner")
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    id_sources = downstream_for(lineage, "id")
    assert any(rel == "join_key" for _, rel in id_sources)

    # Passthrough columns.
    val_l_sources = downstream_for(lineage, "val_l")
    assert any(rel == "join_passthrough" for _, rel in val_l_sources)
    val_r_sources = downstream_for(lineage, "val_r")
    assert any(rel == "join_passthrough" for _, rel in val_r_sources)


# ---------------------------------------------------------------------------
# Union (pl.concat)
# ---------------------------------------------------------------------------


def test_union():
    """pl.concat merges lineage from both inputs."""
    df1 = pl.DataFrame({"x": [1]})
    df2 = pl.DataFrame({"x": [2]})
    lf = pl.concat([df1.lazy(), df2.lazy()])
    lineage = extract_lineage(lf)

    assert lineage["confidence"] == "lazyframe"
    x_sources = downstream_for(lineage, "x")
    assert any(col == "x" for col, _ in x_sources)


# ---------------------------------------------------------------------------
# DataFrame return (should produce opaque or be skipped)
# ---------------------------------------------------------------------------


def test_dataframe_not_lazyframe():
    """A plain DataFrame cannot be walked — should fail gracefully."""
    df = pl.DataFrame({"a": [1]})
    # extract_lineage expects LazyFrame; passing DataFrame should produce opaque.
    lineage = extract_lineage(df)
    assert lineage["confidence"] == "opaque"


# ---------------------------------------------------------------------------
# JSON sidecar write
# ---------------------------------------------------------------------------


def test_write_lineage_json(tmp_path):
    """Lineage JSON sidecar round-trips correctly."""
    from polars_lineage import write_lineage_json

    df = pl.DataFrame({"a": [1], "b": [2]})
    lf = df.lazy().select("a")
    lineage = extract_lineage(lf)

    path = str(tmp_path / "lineage.json")
    write_lineage_json(lineage, path)

    with open(path) as f:
        loaded = json.load(f)

    assert loaded["confidence"] == "lazyframe"
    assert len(loaded["edges"]) > 0
    assert loaded["edges"][0]["upstream_column"] == "a"
    assert loaded["edges"][0]["downstream_column"] == "a"


# ---------------------------------------------------------------------------
# @column_lineage decorator tests (doc 35c)
# ---------------------------------------------------------------------------

# Import the decorator and helpers from the runner module.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src"),
)
from python_runner import column_lineage, _annotation_to_lineage


def test_column_lineage_decorator_attaches_metadata():
    """The decorator attaches normalised annotation metadata to the function."""

    @column_lineage(outputs={
        "total": [("price", "derived"), ("qty", "derived")],
        "name": [("name", "direct")],
    })
    def transform(inputs, params):
        pass

    assert hasattr(transform, "_flux_lineage_annotation")
    annotation = transform._flux_lineage_annotation
    assert annotation["total"] == [("price", "derived"), ("qty", "derived")]
    assert annotation["name"] == [("name", "direct")]


def test_column_lineage_decorator_bare_strings_default_derived():
    """Bare strings in the outputs list default to 'derived' relationship."""

    @column_lineage(outputs={
        "result": ["col_a", "col_b"],
    })
    def transform(inputs, params):
        pass

    annotation = transform._flux_lineage_annotation
    assert annotation["result"] == [("col_a", "derived"), ("col_b", "derived")]


def test_annotation_to_lineage_format():
    """_annotation_to_lineage produces the correct sidecar JSON format."""
    annotation = {
        "total": [("price", "derived"), ("qty", "derived")],
        "name": [("name", "direct")],
    }
    lineage = _annotation_to_lineage(annotation)

    assert lineage["confidence"] == "annotation"
    assert lineage["warnings"] == []
    assert len(lineage["edges"]) == 3

    edges = edge_set(lineage)
    assert ("price", "total", "derived") in edges
    assert ("qty", "total", "derived") in edges
    assert ("name", "name", "direct") in edges


def test_annotation_precedence_over_lazyframe(tmp_path):
    """When @column_lineage is used, its annotation is written to the sidecar
    instead of the LazyFrame-derived lineage, even if the function returns a
    LazyFrame."""
    from polars_lineage import write_lineage_json

    @column_lineage(outputs={
        "a": [("a", "direct")],
    })
    def transform(inputs, params):
        return inputs["data"].lazy().select("a")

    # Simulate what the runner does: check for annotation and write it.
    assert hasattr(transform, "_flux_lineage_annotation")
    lineage = _annotation_to_lineage(transform._flux_lineage_annotation)

    path = str(tmp_path / "lineage.json")
    write_lineage_json(lineage, path)

    with open(path) as f:
        loaded = json.load(f)

    # Should be "annotation", not "lazyframe".
    assert loaded["confidence"] == "annotation"
