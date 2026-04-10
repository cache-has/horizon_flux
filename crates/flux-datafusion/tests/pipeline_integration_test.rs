// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for full pipeline execution with real file connectors.
//!
//! These tests exercise the `PipelineExecutor` with real CSV/Parquet sources
//! and sinks, validating end-to-end data flow through DataFusion SQL transforms
//! and multi-node DAG patterns (fan-out, fan-in, diamond).

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use flux_connectors::default_registry;
use flux_datafusion::{ExecutionOptions, PipelineExecutor, RunStatus};
use flux_engine::edge::Edge;
use flux_engine::node::*;
use flux_engine::pipeline::Pipeline;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_csv(dir: &TempDir, filename: &str, content: &str) -> String {
    let path = dir.path().join(filename);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    path.to_str().unwrap().to_string()
}

fn write_parquet(dir: &TempDir, filename: &str) -> String {
    let path = dir.path().join(filename);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            Arc::new(Float64Array::from(vec![95.5, 87.3, 92.1])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path.to_str().unwrap().to_string()
}

fn csv_source_node(id: &str, path: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Source(SourceConfig {
            connector: "csv".into(),
            config: serde_json::json!({ "path": path, "format": "csv" }),
            cache_row_limit: None,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn parquet_source_node(id: &str, path: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Source(SourceConfig {
            connector: "parquet".into(),
            config: serde_json::json!({ "path": path, "format": "parquet" }),
            cache_row_limit: None,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn sql_node(id: &str, sql: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Transform(TransformConfig {
            mode: TransformMode::Sql,
            code: sql.to_string(),
            code_path: None,
            materialized: false,
            cache_row_limit: None, lineage_annotations: None,
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn csv_sink_node(id: &str, path: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Sink(SinkConfig {
            connector: "csv".into(),
            materialization: None,
            config: serde_json::json!({ "path": path, "format": "csv" }),
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn parquet_sink_node(id: &str, path: &str) -> Node {
    Node {
        id: NodeId::new(id),
        name: id.to_string(),
        kind: NodeKind::Sink(SinkConfig {
            connector: "parquet".into(),
            materialization: None,
            config: serde_json::json!({ "path": path, "format": "parquet" }),
        }),
        position: Position::default(),
        pinned_position: false,
        snippet_parent: None,
        snippet_name: None,
    }
}

fn pipeline(name: &str, nodes: Vec<Node>, edges: Vec<Edge>) -> Pipeline {
    Pipeline {
        name: name.to_string(),
        version: 1,
        default_environment: "dev".to_string(),
        variables: BTreeMap::new(),
        environment_overrides: BTreeMap::new(),
        sample_config: None,
        cache_row_limit: None,
        code_dir: None,
        udfs_dir: None,
        snippets_dir: None,
        snippet: None,
        params: BTreeMap::new(),
        outputs: Vec::new(),
        nodes,
        edges,
    }
}

fn default_opts() -> ExecutionOptions {
    ExecutionOptions::default()
}

// ---------------------------------------------------------------------------
// Full pipeline execution: source -> transform -> sink (real connectors)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_uses_udf_from_udfs_dir() {
    // Source has a messy `name` column; a UDF normalizes it; the sink should
    // see the normalized values. Verifies the full path: pipeline.udfs_dir
    // → registry load → inline → SQL execution → sink output.
    let dir = TempDir::new().unwrap();
    let udfs_dir = dir.path().join("udfs");
    std::fs::create_dir(&udfs_dir).unwrap();
    std::fs::write(
        udfs_dir.join("normalize_name.sql"),
        "CREATE OR REPLACE FUNCTION normalize_name(s VARCHAR) RETURNS VARCHAR \
         AS $$ LOWER(TRIM(s)) $$ LANGUAGE SQL IMMUTABLE;",
    )
    .unwrap();

    let input = write_csv(
        &dir,
        "input.csv",
        "id,name\n1,  Alice  \n2, BOB \n3,Carol\n",
    );
    let output = dir.path().join("out.csv");

    let mut p = pipeline(
        "udf_pipeline",
        vec![
            csv_source_node("src", &input),
            sql_node("xform", "SELECT id, normalize_name(name) AS name FROM src"),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
    );
    p.udfs_dir = Some(udfs_dir.to_string_lossy().into_owned());

    let registry = default_registry().into_provider_registry();
    let (_result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");
    assert_eq!(run.status, RunStatus::Success);

    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("alice"), "got: {content}");
    assert!(content.contains("bob"), "got: {content}");
    assert!(content.contains("carol"), "got: {content}");
    assert!(
        !content.contains("Alice"),
        "Alice should be lowercased: {content}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_sql_transform_csv_sink() {
    let dir = TempDir::new().unwrap();
    let input = write_csv(
        &dir,
        "input.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n",
    );
    let output = dir.path().join("output.csv");

    let p = pipeline(
        "csv_to_csv",
        vec![
            csv_source_node("src", &input),
            sql_node("xform", "SELECT name, score FROM src WHERE score > 90"),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(result.node_stats.len(), 3);

    // Source: 3 rows
    assert_eq!(result.node_stats[0].rows_out, 3);
    // Transform: filtered to 2 (Alice 95.5, Carol 92.1)
    assert_eq!(result.node_stats[1].rows_in, 3);
    assert_eq!(result.node_stats[1].rows_out, 2);
    // Sink: wrote 2
    assert_eq!(result.node_stats[2].rows_in, 2);
    assert_eq!(result.node_stats[2].rows_out, 2);

    // Verify output file
    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("Alice"));
    assert!(content.contains("Carol"));
    assert!(!content.contains("Bob"));
    // header + 2 data rows
    assert_eq!(content.lines().count(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_source_sql_transform_parquet_sink() {
    let dir = TempDir::new().unwrap();
    let input = write_csv(
        &dir,
        "input.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n",
    );
    let output = dir.path().join("output.parquet");

    let p = pipeline(
        "csv_to_parquet",
        vec![
            csv_source_node("src", &input),
            sql_node(
                "xform",
                "SELECT id, name, CAST(score AS DOUBLE) AS score FROM src",
            ),
            parquet_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(result.node_stats[2].rows_out, 3);

    // Read back the Parquet file and verify
    assert!(output.exists());
    let file = std::fs::File::open(&output).unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 1024).unwrap();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_source_sql_transform_csv_sink() {
    let dir = TempDir::new().unwrap();
    let input = write_parquet(&dir, "input.parquet");
    let output = dir.path().join("output.csv");

    let p = pipeline(
        "parquet_to_csv",
        vec![
            parquet_source_node("src", &input),
            sql_node(
                "xform",
                "SELECT name, score * 2 AS doubled FROM src ORDER BY name",
            ),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![Edge::new("src", "xform"), Edge::new("xform", "sink")],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(result.node_stats[1].rows_out, 3);

    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("Alice"));
    assert!(content.contains("191")); // 95.5 * 2
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chained_sql_transforms() {
    let dir = TempDir::new().unwrap();
    let input = write_csv(
        &dir,
        "input.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n4,Dave,78.0\n",
    );
    let output = dir.path().join("output.csv");

    let p = pipeline(
        "chained",
        vec![
            csv_source_node("src", &input),
            sql_node("filter", "SELECT * FROM src WHERE score >= 87"),
            sql_node(
                "project",
                "SELECT name, score FROM filter ORDER BY score DESC",
            ),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![
            Edge::new("src", "filter"),
            Edge::new("filter", "project"),
            Edge::new("project", "sink"),
        ],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    // src=4, filter keeps 3 (>=87: Alice, Bob, Carol), project passes 3
    assert_eq!(result.node_stats[0].rows_out, 4);
    assert_eq!(result.node_stats[1].rows_out, 3);
    assert_eq!(result.node_stats[2].rows_out, 3);

    let content = std::fs::read_to_string(&output).unwrap();
    assert!(!content.contains("Dave"));
    assert_eq!(content.lines().count(), 4); // header + 3
}

// ---------------------------------------------------------------------------
// Multi-node pipelines: fan-out, fan-in, diamond
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_out_one_source_two_transforms_two_sinks() {
    let dir = TempDir::new().unwrap();
    let input = write_csv(
        &dir,
        "input.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n",
    );
    let output_high = dir.path().join("high.csv");
    let output_low = dir.path().join("low.csv");

    // Fan-out: src → high_filter → high_sink
    //                → low_filter  → low_sink
    let p = pipeline(
        "fan_out",
        vec![
            csv_source_node("src", &input),
            sql_node(
                "high_filter",
                "SELECT name, score FROM src WHERE score > 90",
            ),
            sql_node(
                "low_filter",
                "SELECT name, score FROM src WHERE score <= 90",
            ),
            csv_sink_node("high_sink", output_high.to_str().unwrap()),
            csv_sink_node("low_sink", output_low.to_str().unwrap()),
        ],
        vec![
            Edge::new("src", "high_filter"),
            Edge::new("src", "low_filter"),
            Edge::new("high_filter", "high_sink"),
            Edge::new("low_filter", "low_sink"),
        ],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("fan-out pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(result.node_stats.len(), 5);

    // Verify high output: Alice (95.5), Carol (92.1)
    let high_content = std::fs::read_to_string(&output_high).unwrap();
    assert!(high_content.contains("Alice"));
    assert!(high_content.contains("Carol"));
    assert!(!high_content.contains("Bob"));
    assert_eq!(high_content.lines().count(), 3); // header + 2

    // Verify low output: Bob (87.3)
    let low_content = std::fs::read_to_string(&output_low).unwrap();
    assert!(low_content.contains("Bob"));
    assert!(!low_content.contains("Alice"));
    assert_eq!(low_content.lines().count(), 2); // header + 1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_two_sources_join_to_sink() {
    let dir = TempDir::new().unwrap();
    let users_path = write_csv(&dir, "users.csv", "user_id,name\n1,Alice\n2,Bob\n3,Carol\n");
    let scores_path = write_csv(
        &dir,
        "scores.csv",
        "user_id,score\n1,95.5\n2,87.3\n3,92.1\n",
    );
    let output = dir.path().join("joined.csv");

    // Fan-in: users + scores → join → sink
    let p = pipeline(
        "fan_in",
        vec![
            csv_source_node("users", &users_path),
            csv_source_node("scores", &scores_path),
            sql_node(
                "joined",
                "SELECT u.name, s.score \
                 FROM users u JOIN scores s ON u.user_id = s.user_id \
                 ORDER BY s.score DESC",
            ),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![
            Edge::new("users", "joined"),
            Edge::new("scores", "joined"),
            Edge::new("joined", "sink"),
        ],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("fan-in pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);

    // Join produces 3 rows (all match)
    let join_stats = result
        .node_stats
        .iter()
        .find(|s| s.node_id.0 == "joined")
        .unwrap();
    assert_eq!(join_stats.rows_out, 3);

    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("Alice"));
    assert!(content.contains("Bob"));
    assert!(content.contains("Carol"));
    assert_eq!(content.lines().count(), 4); // header + 3
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diamond_pattern_source_two_transforms_merge_sink() {
    let dir = TempDir::new().unwrap();
    let input = write_csv(
        &dir,
        "input.csv",
        "id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n",
    );
    let output = dir.path().join("diamond_out.csv");

    // Diamond: src → names (extract names)
    //               → scores (extract scores)
    //          names + scores → merge (rejoin) → sink
    let p = pipeline(
        "diamond",
        vec![
            csv_source_node("src", &input),
            sql_node("names", "SELECT id, UPPER(name) AS name FROM src"),
            sql_node("scores", "SELECT id, score * 10 AS points FROM src"),
            sql_node(
                "merge",
                "SELECT n.name, s.points \
                 FROM names n JOIN scores s ON n.id = s.id \
                 ORDER BY s.points DESC",
            ),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![
            Edge::new("src", "names"),
            Edge::new("src", "scores"),
            Edge::new("names", "merge"),
            Edge::new("scores", "merge"),
            Edge::new("merge", "sink"),
        ],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("diamond pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);

    // Merge should produce 3 rows
    let merge_stats = result
        .node_stats
        .iter()
        .find(|s| s.node_id.0 == "merge")
        .unwrap();
    assert_eq!(merge_stats.rows_out, 3);

    let content = std::fs::read_to_string(&output).unwrap();
    // Names should be uppercased
    assert!(content.contains("ALICE"));
    assert!(content.contains("BOB"));
    assert!(content.contains("CAROL"));
    // Scores should be multiplied by 10
    assert!(content.contains("955")); // 95.5 * 10
    assert_eq!(content.lines().count(), 4); // header + 3
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_with_union_instead_of_join() {
    let dir = TempDir::new().unwrap();
    let part1 = write_csv(&dir, "part1.csv", "id,name\n1,Alice\n2,Bob\n");
    let part2 = write_csv(&dir, "part2.csv", "id,name\n3,Carol\n4,Dave\n");
    let output = dir.path().join("union_out.csv");

    // Two sources unioned via SQL
    let p = pipeline(
        "union",
        vec![
            csv_source_node("part1", &part1),
            csv_source_node("part2", &part2),
            sql_node(
                "combined",
                "SELECT * FROM part1 UNION ALL SELECT * FROM part2 ORDER BY id",
            ),
            csv_sink_node("sink", output.to_str().unwrap()),
        ],
        vec![
            Edge::new("part1", "combined"),
            Edge::new("part2", "combined"),
            Edge::new("combined", "sink"),
        ],
    );

    let registry = default_registry().into_provider_registry();
    let (result, run) = PipelineExecutor::execute(&p, &registry, &default_opts())
        .await
        .expect("union pipeline should succeed");

    assert_eq!(run.status, RunStatus::Success);
    let combined_stats = result
        .node_stats
        .iter()
        .find(|s| s.node_id.0 == "combined")
        .unwrap();
    assert_eq!(combined_stats.rows_out, 4);

    let content = std::fs::read_to_string(&output).unwrap();
    assert_eq!(content.lines().count(), 5); // header + 4
    assert!(content.contains("Alice"));
    assert!(content.contains("Dave"));
}
