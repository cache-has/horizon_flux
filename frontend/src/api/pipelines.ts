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

/** A backend pipeline node (tagged union via `type` field). */
export interface ApiNode {
  id: string;
  name: string;
  type: 'source' | 'transform' | 'sink';
  position: ApiPosition;
  pinned_position: boolean;
  /** Source/sink fields */
  connector?: string;
  config?: unknown;
  /** Transform fields */
  mode?: 'sql' | 'python';
  code?: string;
  /** Path to external code file. When set, `code` contains the file contents
   *  (populated by the server) and edits are written back to this file on save. */
  code_path?: string;
  materialized?: boolean;
  /** Max rows to cache for preview. Overrides pipeline-level default when set. */
  cache_row_limit?: number;
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

/** Fetch run history for a pipeline. Returns a plain array of runs. */
export async function fetchPipelineRuns(
  id: string,
  limit = 10,
  offset = 0,
): Promise<ApiPipelineRun[]> {
  const res = await fetch(`${BASE}/${id}/runs?limit=${limit}&offset=${offset}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch runs: ${res.status} ${res.statusText}`);
  }
  const runs: ApiPipelineRun[] = await res.json();
  // Compute duration_ms from start/end timestamps for each node stat.
  for (const run of runs) {
    for (const stat of run.node_stats) {
      stat.duration_ms = timestampDurationMs(stat.start_time, stat.end_time);
    }
  }
  return runs;
}
