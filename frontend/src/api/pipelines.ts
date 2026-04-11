// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API types matching the Rust backend's serialization format.
 *
 * Backend nodes use `{ from, to }` edges and flatten NodeKind via
 * `#[serde(tag = "type")]`. These types mirror that shape so we can
 * round-trip without loss.
 */

/** Position on the canvas (matches backend `Position`). */
export interface ApiPosition {
  x: number;
  y: number;
}

// ---------------------------------------------------------------------------
// Sink materialization policy (mirrors `flux-engine::materialization`)
// ---------------------------------------------------------------------------

export type ReadMode = 'full' | 'incremental';

export type WriteStrategy =
  | 'append'
  | 'merge'
  | 'delete_insert'
  | 'insert_overwrite'
  | 'truncate_insert'
  | 'snapshot';

export type ChangeDetection = 'check' | 'timestamp';

export type HardDeletes = 'ignore' | 'invalidate' | 'delete';

/**
 * Snapshot-specific sub-block (mirrors `flux-engine::materialization::SnapshotPolicy`).
 * Only meaningful when `write_strategy: 'snapshot'`.
 */
export interface SnapshotPolicy {
  change_detection?: ChangeDetection;
  /** Required for `change_detection: 'check'`. Use `["*"]` to track all columns. */
  check_columns?: string[];
  /** Required for `change_detection: 'timestamp'`. */
  updated_at_column?: string;
  hard_deletes?: HardDeletes;
}

export type WatermarkType = 'timestamp' | 'int64' | 'string';

export type OnSchemaChange = 'fail' | 'ignore' | 'append_new_columns' | 'sync_all_columns';

export type FirstRun = 'full' | 'fail';

export interface Watermark {
  column: string;
  type: WatermarkType;
}

/** Sink materialization block. Sibling of `config` on a sink node. */
export interface MaterializationPolicy {
  read_mode?: ReadMode;
  write_strategy?: WriteStrategy;
  watermark?: Watermark;
  unique_keys?: string[];
  partition_column?: string;
  on_schema_change?: OnSchemaChange;
  first_run?: FirstRun;
  /** ISO 8601 duration. Only meaningful under incremental + timestamp. */
  lookback?: string;
  /** Required iff `write_strategy: 'snapshot'`. */
  snapshot?: SnapshotPolicy;
}

/** Serialized watermark value from a sink write (mirrors backend `WatermarkValue`). */
export interface WatermarkValue {
  value: string;
  type: WatermarkType;
}

/** Schema field summary used inside `SchemaDiff`. */
export interface SchemaField {
  name: string;
  data_type: string;
}

export interface SchemaTypeChange {
  name: string;
  before: string;
  after: string;
}

export interface SchemaDiff {
  added?: SchemaField[];
  removed?: SchemaField[];
  type_changed?: SchemaTypeChange[];
}

/** A sink write receipt (mirrors backend `MaterializationReceipt`). */
export interface MaterializationReceipt {
  write_strategy: WriteStrategy;
  read_mode: ReadMode;
  rows_scanned: number;
  rows_filtered_by_watermark: number;
  rows_written: number;
  rows_inserted: number;
  rows_updated: number;
  rows_deleted: number;
  watermark_before?: WatermarkValue;
  watermark_after?: WatermarkValue;
  schema_diff?: SchemaDiff;
}

// ---------------------------------------------------------------------------
// Test node assertions (mirrors `flux-engine::node::Assertion`)
// ---------------------------------------------------------------------------

/** Severity level for a test node. */
export type TestSeverity = 'error' | 'warn';

/** A single data assertion. Discriminated by `kind`. */
export type ApiAssertion =
  | { kind: 'not_null'; columns: string[] }
  | { kind: 'unique'; columns: string[] }
  | { kind: 'accepted_values'; column: string; values: (string | number | boolean)[] }
  | { kind: 'row_count_between'; min: number; max: number }
  | { kind: 'row_count_equal_to'; count: number }
  | { kind: 'no_duplicates' }
  | { kind: 'column_values_match_regex'; column: string; pattern: string }
  | { kind: 'expression_true'; expression: string }
  | { kind: 'sql'; name: string; query: string };

/** All supported assertion kinds. */
export const ASSERTION_KINDS = [
  'not_null',
  'unique',
  'accepted_values',
  'row_count_between',
  'row_count_equal_to',
  'no_duplicates',
  'column_values_match_regex',
  'expression_true',
  'sql',
] as const;

export type AssertionKind = (typeof ASSERTION_KINDS)[number];

/** Human-readable labels for assertion kinds. */
export const ASSERTION_LABELS: Record<AssertionKind, string> = {
  not_null: 'Not Null',
  unique: 'Unique',
  accepted_values: 'Accepted Values',
  row_count_between: 'Row Count Between',
  row_count_equal_to: 'Row Count Equal To',
  no_duplicates: 'No Duplicates',
  column_values_match_regex: 'Column Values Match Regex',
  expression_true: 'Expression True',
  sql: 'Custom SQL',
};

/** Create a default (empty) assertion for a given kind. */
export function defaultAssertion(kind: AssertionKind): ApiAssertion {
  switch (kind) {
    case 'not_null': return { kind, columns: [] };
    case 'unique': return { kind, columns: [] };
    case 'accepted_values': return { kind, column: '', values: [] };
    case 'row_count_between': return { kind, min: 1, max: 1000000 };
    case 'row_count_equal_to': return { kind, count: 0 };
    case 'no_duplicates': return { kind };
    case 'column_values_match_regex': return { kind, column: '', pattern: '' };
    case 'expression_true': return { kind, expression: '' };
    case 'sql': return { kind, name: '', query: '' };
  }
}

/** A backend pipeline node (tagged union via `type` field). */
export interface ApiNode {
  id: string;
  name: string;
  type: 'source' | 'transform' | 'sink' | 'test';
  position: ApiPosition;
  pinned_position: boolean;
  /** Source/sink fields */
  connector?: string;
  config?: unknown;
  /** Sink-only: optional materialization policy. */
  materialization?: MaterializationPolicy;
  /** Transform fields */
  mode?: 'sql' | 'python';
  code?: string;
  /** Path to external code file. When set, `code` contains the file contents
   *  (populated by the server) and edits are written back to this file on save. */
  code_path?: string;
  materialized?: boolean;
  /** Max rows to cache for preview. Overrides pipeline-level default when set. */
  cache_row_limit?: number;
  /** Test node fields */
  severity?: TestSeverity;
  assertions?: ApiAssertion[];
  max_violations_reported?: number;
  /** Set on nodes produced by snippet expansion: ID of the outermost snippet
   *  call this node belongs to. The frontend uses this to render snippet
   *  expansions as collapsible group nodes. */
  snippet_parent?: string;
  /** Snippet name (matches the snippet definition's `snippet` field) for the
   *  outermost call this node belongs to. Sibling of `snippet_parent`. */
  snippet_name?: string;
}

/** A backend pipeline edge. */
export interface ApiEdge {
  from: string;
  to: string;
}

/** Sample configuration matching the backend's SampleConfig enum. */
export type ApiSampleConfig =
  | { mode: 'first_n'; count: number }
  | { mode: 'random'; count: number; seed: number }
  | { mode: 'full' };

/** Default sample configuration. */
export const DEFAULT_SAMPLE_CONFIG: ApiSampleConfig = { mode: 'first_n', count: 100 };

/** Format a sample config for display (matches backend format_sample_method). */
export function formatSampleConfig(config: ApiSampleConfig): string {
  switch (config.mode) {
    case 'first_n':
      return `first ${config.count}`;
    case 'random':
      return `random ${config.count}`;
    case 'full':
      return 'full';
  }
}

/** Full pipeline definition as returned by the backend. */
export interface ApiPipeline {
  name: string;
  version: number;
  default_environment: string;
  variables: Record<string, unknown>;
  environment_overrides: Record<string, Record<string, unknown>>;
  sample_config?: ApiSampleConfig;
  nodes: ApiNode[];
  edges: ApiEdge[];
}

/** Wrapper returned by GET/PUT /api/pipelines/:id */
export interface ApiPipelineResponse {
  id: string;
  pipeline: ApiPipeline;
  created_at: number;
  updated_at: number;
}

/** Paginated list response. */
export interface ApiPaginatedResponse<T> {
  data: T[];
  total: number;
  limit: number;
  offset: number;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const BASE = '/api/pipelines';

/** Fetch a single pipeline by ID. */
export async function fetchPipeline(id: string): Promise<ApiPipelineResponse> {
  const res = await fetch(`${BASE}/${id}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch pipeline ${id}: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Create a new empty pipeline. */
export async function createPipeline(name: string): Promise<ApiPipelineResponse> {
  const res = await fetch(BASE, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      version: 1,
      default_environment: 'dev',
      variables: {},
      environment_overrides: {},
      nodes: [],
      edges: [],
    }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to create pipeline: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// UDFs (planning doc 29, Layer 1)
// ---------------------------------------------------------------------------

export interface ApiUdfParam {
  name: string;
  data_type: string;
}

export interface ApiUdfInfo {
  name: string;
  signature: string;
  params: ApiUdfParam[];
  return_type: string | null;
  source: string;
}

export interface ApiUdfsResponse {
  udfs: ApiUdfInfo[];
}

/** Fetch reusable SQL UDFs available to a pipeline (for editor autocomplete). */
export async function fetchPipelineUdfs(pipelineId: string): Promise<ApiUdfInfo[]> {
  const res = await fetch(`${BASE}/${pipelineId}/udfs`);
  if (!res.ok) {
    throw new Error(`Failed to fetch UDFs: ${res.status} ${res.statusText}`);
  }
  const body: ApiUdfsResponse = await res.json();
  return body.udfs;
}

/** List pipelines with optional pagination. */
export async function listPipelines(
  limit = 50,
  offset = 0,
): Promise<ApiPaginatedResponse<ApiPipelineResponse>> {
  const res = await fetch(`${BASE}?limit=${limit}&offset=${offset}`);
  if (!res.ok) {
    throw new Error(`Failed to list pipelines: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Snapshot SCD2 read endpoints (planning doc 28)
// ---------------------------------------------------------------------------

export interface SnapshotHistoryVersion {
  flux_scd_id: string;
  flux_valid_from: string;
  flux_valid_to: string | null;
  flux_is_current: boolean;
  comparison: Record<string, string>;
}

export interface SnapshotHistoryResponse {
  node_id: string;
  table: string;
  unique_keys: string[];
  comparison_columns: string[];
  key: Record<string, string>;
  version_count: number;
  versions: SnapshotHistoryVersion[];
}

/**
 * Fetch SCD2 history for a single business key on a snapshot sink.
 * v1 supports postgresql sinks only — other connectors return a 400
 * with an actionable error message.
 */
export async function fetchSnapshotHistory(
  pipelineId: string,
  nodeId: string,
  key: Record<string, string>,
  environment?: string,
): Promise<SnapshotHistoryResponse> {
  const res = await fetch(
    `${BASE}/${pipelineId}/nodes/${encodeURIComponent(nodeId)}/snapshot/history`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, environment }),
    },
  );
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const body = (await res.json()) as { error?: string; details?: string };
      if (body?.error) {
        detail = body.details ? `${body.error} — ${body.details}` : body.error;
      }
    } catch {
      // body wasn't JSON; keep status text
    }
    throw new Error(detail);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Snapshot diff preview (planning doc 28)
// ---------------------------------------------------------------------------

export type SnapshotDiffClassification = 'unchanged' | 'changed' | 'new' | 'gone';

export interface SnapshotDiffSample {
  classification: SnapshotDiffClassification;
  unique_key: string[];
}

export interface SnapshotDiffStats {
  unchanged: number;
  changed: number;
  new_versions: number;
  gone: number;
}

export interface SnapshotDiffResponse {
  node_id: string;
  table: string;
  environment: string;
  unique_keys: string[];
  comparison_columns: string[];
  stats: SnapshotDiffStats;
  sample: SnapshotDiffSample[];
  staged_row_count: number;
  sample_truncated: boolean;
  staged_row_cap: number;
  cached: boolean;
}

/**
 * Fetch a dry-run snapshot diff: runs the upstream DAG with sink writes
 * disabled, then classifies every staged business key against the target's
 * `flux_is_current` slice. v1 supports postgresql sinks only.
 */
export async function fetchSnapshotDiff(
  pipelineId: string,
  nodeId: string,
  options?: { environment?: string; variables?: Record<string, unknown> },
): Promise<SnapshotDiffResponse> {
  const res = await fetch(
    `${BASE}/${pipelineId}/nodes/${encodeURIComponent(nodeId)}/snapshot/diff`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        environment: options?.environment,
        variables: options?.variables ?? {},
      }),
    },
  );
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const body = (await res.json()) as { error?: string; details?: string };
      if (body?.error) {
        detail = body.details ? `${body.error} — ${body.details}` : body.error;
      }
    } catch {
      // body wasn't JSON; keep status text
    }
    throw new Error(detail);
  }
  return res.json();
}

/** Update a pipeline (full replace). */
export async function updatePipeline(
  id: string,
  pipeline: ApiPipeline,
): Promise<ApiPipelineResponse> {
  const res = await fetch(`${BASE}/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pipeline),
  });
  if (!res.ok) {
    throw new Error(`Failed to update pipeline ${id}: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Import / Export
// ---------------------------------------------------------------------------

/** Export a single pipeline as a JSON file download. */
export async function exportPipeline(id: string): Promise<void> {
  const res = await fetch(`${BASE}/${id}/export`);
  if (!res.ok) {
    throw new Error(`Failed to export pipeline: ${res.status} ${res.statusText}`);
  }
  // Trigger browser download from the response.
  const disposition = res.headers.get('content-disposition') ?? '';
  const match = disposition.match(/filename="(.+?)"/);
  const filename = match?.[1] ?? 'pipeline.json';
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/** How to handle name conflicts during import. */
export type ImportConflict = 'reject' | 'rename' | 'overwrite';

/** Response from importing a pipeline. */
export interface ImportPipelineResponse {
  id: string;
  pipeline: ApiPipeline;
  created_at: number;
  updated_at: number;
  warnings: string[];
  connector_warnings: string[];
}

/** Import a pipeline from a JSON definition. */
export async function importPipeline(
  pipeline: unknown,
  onConflict: ImportConflict = 'reject',
): Promise<ImportPipelineResponse> {
  const res = await fetch(`${BASE}/import`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pipeline, on_conflict: onConflict }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Import failed: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Bulk export all pipelines as a JSON file download. */
export async function bulkExportPipelines(): Promise<void> {
  const res = await fetch(`${BASE}/export`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(`Failed to export pipelines: ${res.status} ${res.statusText}`);
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'horizon-flux-pipelines.json';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Version history
// ---------------------------------------------------------------------------

/** Summary of a pipeline version (without full snapshot). */
export interface ApiVersionSummary {
  version: number;
  saved_at: number;
}

/** Full version response including the pipeline snapshot. */
export interface ApiVersionResponse {
  version: number;
  saved_at: number;
  snapshot: ApiPipeline;
}

/** List version history for a pipeline (newest first). */
export async function fetchVersions(
  id: string,
  limit = 50,
  offset = 0,
): Promise<ApiPaginatedResponse<ApiVersionSummary>> {
  const res = await fetch(`${BASE}/${id}/versions?limit=${limit}&offset=${offset}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch versions: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Fetch a specific version snapshot. */
export async function fetchVersion(
  id: string,
  version: number,
): Promise<ApiVersionResponse> {
  const res = await fetch(`${BASE}/${id}/versions/${version}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch version ${version}: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Restore a pipeline to a previous version. Creates a new version with the restored content. */
export async function restoreVersion(
  id: string,
  version: number,
): Promise<ApiPipelineResponse> {
  const res = await fetch(`${BASE}/${id}/versions/${version}`, {
    method: 'POST',
  });
  if (!res.ok) {
    throw new Error(`Failed to restore version ${version}: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Preview & run history
// ---------------------------------------------------------------------------

/** Column info returned by the preview endpoint. */
export interface ApiColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

/** Per-column statistics (tagged union via `kind` field). */
export type ApiColumnStats =
  | { kind: 'numeric'; min: number | null; max: number | null; mean: number | null; null_count: number }
  | { kind: 'string'; min_length: number | null; max_length: number | null; unique_count: number; null_count: number }
  | { kind: 'boolean'; true_count: number; false_count: number; null_count: number }
  | { kind: 'other'; null_count: number };

/** Preview status for a node result. */
export type PreviewStatus = 'cached' | 'no_cache' | 'skipped' | 're_executed';

/** Single node result from a preview run. */
export interface ApiPreviewNodeResponse {
  node_id: string;
  columns: ApiColumnInfo[];
  row_count: number;
  duration_ms: number;
  rows: Record<string, unknown>[];
  column_stats?: ApiColumnStats[];
  status: PreviewStatus;
}

/** Full pipeline preview response. */
export interface ApiPreviewResponse {
  pipeline_name: string;
  execution_order: string[];
  nodes: ApiPreviewNodeResponse[];
  duration_ms: number;
  sample_method?: string;
}

/** Timestamp as returned by the backend (serde SystemTime). */
interface ApiTimestamp {
  secs_since_epoch: number;
  nanos_since_epoch: number;
}

/** Node-level run statistics. */
export interface ApiNodeRunStats {
  node_id: string;
  rows_in: number;
  rows_out: number;
  start_time?: ApiTimestamp;
  end_time?: ApiTimestamp;
  duration_ms: number;
  error?: string;
}

/** Result of a single assertion within a test node run. */
export interface ApiAssertionResult {
  name: string;
  passed: boolean;
  violation_count: number;
  violating_rows?: Record<string, unknown>[];
  message?: string;
}

/** Summary of a test node's results from a pipeline run. */
export interface ApiTestResult {
  node_id: string;
  passed: boolean;
  severity: TestSeverity;
  assertions: ApiAssertionResult[];
}

/** A pipeline run record. */
export interface ApiPipelineRun {
  id: string;
  pipeline_name: string;
  environment: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'cancelled';
  start_time?: number;
  end_time?: number;
  node_stats: ApiNodeRunStats[];
  error?: string;
  test_results?: ApiTestResult[];
  triggered_by?: string;
}

/** Request body for single-node preview. */
export interface ApiNodePreviewRequest {
  node: {
    type: 'source' | 'transform';
    connector?: string;
    config?: unknown;
    mode?: 'sql' | 'python';
    code?: string;
  };
  upstream?: Record<string, Record<string, unknown>[]>;
  sample?: ApiSampleConfig;
  /** Pipeline variables (name → resolved value) to interpolate into the config. */
  variables?: Record<string, unknown>;
}

/** Error thrown by preview API calls, includes HTTP status for timeout detection. */
export class PreviewError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'PreviewError';
    this.status = status;
  }

  get isTimeout(): boolean {
    return this.status === 504;
  }
}

/** Run a full pipeline preview (sample data through all nodes). */
export async function previewPipeline(
  id: string,
  sample?: ApiSampleConfig,
  signal?: AbortSignal,
  reExecuteNode?: string,
): Promise<ApiPreviewResponse> {
  const payload: Record<string, unknown> = {};
  if (sample) payload.sample = sample;
  if (reExecuteNode) payload.re_execute_node = reExecuteNode;
  const res = await fetch(`${BASE}/${id}/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    const msg = body?.error ?? `Preview failed: ${res.status} ${res.statusText}`;
    throw new PreviewError(msg, res.status);
  }
  return res.json();
}

/** Preview a single node's output. */
export async function previewNode(
  request: ApiNodePreviewRequest,
  signal?: AbortSignal,
): Promise<ApiPreviewNodeResponse> {
  const res = await fetch('/api/preview/node', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
    signal,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    const msg = body?.error ?? `Node preview failed: ${res.status} ${res.statusText}`;
    throw new PreviewError(msg, res.status);
  }
  return res.json();
}

/** Response from running a pipeline. */
export interface ApiRunResponse {
  run_id: string;
}

/** Trigger a full pipeline execution. */
export async function runPipeline(
  id: string,
  environment?: string,
  variables?: Record<string, unknown>,
): Promise<ApiRunResponse> {
  const res = await fetch(`${BASE}/${id}/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ environment, variables }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Run failed: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Compute duration in ms from backend timestamps. */
function timestampDurationMs(start?: ApiTimestamp, end?: ApiTimestamp): number {
  if (!start || !end) return 0;
  const startMs = start.secs_since_epoch * 1000 + start.nanos_since_epoch / 1_000_000;
  const endMs = end.secs_since_epoch * 1000 + end.nanos_since_epoch / 1_000_000;
  return Math.round(endMs - startMs);
}

// ---------------------------------------------------------------------------
// Incremental state (doc 27)
// ---------------------------------------------------------------------------

/** A persisted incremental state row (mirrors backend `IncrementalState`). */
export interface ApiIncrementalState {
  pipeline_id: string;
  node_id: string;
  environment: string;
  watermark_column: string;
  watermark_value: string;
  watermark_type: WatermarkType;
  last_run_at_ms: number;
  last_run_id: string;
  rows_processed: number;
  schema_fingerprint?: string;
}

/** Fetch persisted incremental state rows for a pipeline. */
export async function fetchIncrementalState(
  id: string,
  env?: string,
): Promise<ApiIncrementalState[]> {
  const url = env
    ? `${BASE}/${id}/incremental-state?env=${encodeURIComponent(env)}`
    : `${BASE}/${id}/incremental-state`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(
      `Failed to fetch incremental state for ${id}: ${res.status} ${res.statusText}`,
    );
  }
  const body = (await res.json()) as { states: ApiIncrementalState[] };
  return body.states ?? [];
}

/** Per-node incremental stats for a single run (mirrors backend `RunIncrementalStat`). */
export interface ApiRunIncrementalStat {
  node_id: string;
  rows_in: number;
  rows_out: number;
  duration_ms: number;
  receipt: MaterializationReceipt;
}

/** Fetch per-run incremental stats. Returns one entry per sink with a receipt. */
export async function fetchRunIncrementalStats(
  pipelineId: string,
  runId: string,
): Promise<ApiRunIncrementalStat[]> {
  const res = await fetch(
    `${BASE}/${pipelineId}/runs/${encodeURIComponent(runId)}/incremental-stats`,
  );
  if (!res.ok) {
    throw new Error(
      `Failed to fetch incremental stats for ${runId}: ${res.status} ${res.statusText}`,
    );
  }
  const body = (await res.json()) as { nodes?: ApiRunIncrementalStat[] };
  return body.nodes ?? [];
}

/** Reset persisted incremental state for a single sink node. */
export async function resetIncrementalState(
  pipelineId: string,
  nodeId: string,
  env?: string,
): Promise<void> {
  const url = env
    ? `${BASE}/${pipelineId}/nodes/${encodeURIComponent(nodeId)}/incremental/reset?env=${encodeURIComponent(env)}`
    : `${BASE}/${pipelineId}/nodes/${encodeURIComponent(nodeId)}/incremental/reset`;
  const res = await fetch(url, { method: 'POST' });
  // 404 means "no state existed" — treat as success (idempotent reset).
  if (!res.ok && res.status !== 404) {
    throw new Error(
      `Failed to reset incremental state for ${nodeId}: ${res.status} ${res.statusText}`,
    );
  }
}

/** Paginated response wrapper matching the backend `PaginatedResponse<T>`. */
export interface PaginatedRunsResponse {
  data: ApiPipelineRun[];
  total: number;
  limit: number;
  offset: number;
}

/** Fetch run history for a pipeline with pagination metadata. */
export async function fetchPipelineRunsPaginated(
  id: string,
  limit = 10,
  offset = 0,
): Promise<PaginatedRunsResponse> {
  const res = await fetch(`${BASE}/${id}/runs?limit=${limit}&offset=${offset}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch runs: ${res.status} ${res.statusText}`);
  }
  const body: PaginatedRunsResponse = await res.json();
  for (const run of body.data) {
    for (const stat of run.node_stats) {
      stat.duration_ms = timestampDurationMs(stat.start_time, stat.end_time);
    }
  }
  return body;
}

/** Fetch run history for a pipeline. Returns a plain array of runs. */
export async function fetchPipelineRuns(
  id: string,
  limit = 10,
  offset = 0,
): Promise<ApiPipelineRun[]> {
  const resp = await fetchPipelineRunsPaginated(id, limit, offset);
  return resp.data;
}
