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
  materialized?: boolean;
}

/** A backend pipeline edge. */
export interface ApiEdge {
  from: string;
  to: string;
}

/** Full pipeline definition as returned by the backend. */
export interface ApiPipeline {
  name: string;
  version: number;
  default_environment: string;
  variables: Record<string, unknown>;
  environment_overrides: Record<string, Record<string, unknown>>;
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
// Preview & run history
// ---------------------------------------------------------------------------

/** Column info returned by the preview endpoint. */
export interface ApiColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

/** Single node result from a preview run. */
export interface ApiPreviewNodeResponse {
  node_id: string;
  columns: ApiColumnInfo[];
  row_count: number;
  duration_ms: number;
  rows: Record<string, unknown>[];
}

/** Full pipeline preview response. */
export interface ApiPreviewResponse {
  pipeline_name: string;
  execution_order: string[];
  nodes: ApiPreviewNodeResponse[];
  duration_ms: number;
}

/** Node-level run statistics. */
export interface ApiNodeRunStats {
  node_id: string;
  rows_in: number;
  rows_out: number;
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
  sample?: { max_rows?: number };
}

/** Run a full pipeline preview (sample data through all nodes). */
export async function previewPipeline(
  id: string,
  sample?: { max_rows?: number },
  signal?: AbortSignal,
): Promise<ApiPreviewResponse> {
  const res = await fetch(`${BASE}/${id}/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sample }),
    signal,
  });
  if (!res.ok) {
    throw new Error(`Preview failed: ${res.status} ${res.statusText}`);
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
    throw new Error(`Node preview failed: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Fetch run history for a pipeline. */
export async function fetchPipelineRuns(
  id: string,
  limit = 10,
  offset = 0,
): Promise<ApiPaginatedResponse<ApiPipelineRun>> {
  const res = await fetch(`${BASE}/${id}/runs?limit=${limit}&offset=${offset}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch runs: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
