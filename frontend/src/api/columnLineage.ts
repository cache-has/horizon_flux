// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Column-level lineage API client (planning doc 35).
 *
 * Provides fetch functions for column upstream/downstream traces, impact
 * analysis, search, and per-pipeline column edges.
 */

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

export type RelationshipKind =
  | 'direct'
  | 'derived'
  | 'cast'
  | 'filter'
  | 'join_key'
  | 'join_passthrough'
  | 'group_by'
  | 'aggregate_input'
  | 'window_partition'
  | 'window_order'
  | 'window_input'
  | 'opaque';

export type ConfidenceLevel = 'exact' | 'lazyframe' | 'annotation' | 'opaque';

export interface ColumnKeyDto {
  pipeline_id: string;
  node_id: string;
  column: string;
}

export interface TraceEdgeDto {
  upstream: ColumnKeyDto;
  downstream: ColumnKeyDto;
  relationship: RelationshipKind;
  confidence: ConfidenceLevel;
  expression_text: string | null;
  depth: number;
}

export interface ColumnTraceResponse {
  fingerprint: string;
  column: string;
  edges: TraceEdgeDto[];
  truncated: boolean;
}

export interface ColumnImpactResponse {
  fingerprint: string;
  column: string;
  affected_columns: TraceEdgeDto[];
  affected_pipelines: string[];
  truncated: boolean;
}

export interface ColumnSearchResponse {
  query: string;
  results: ColumnKeyDto[];
}

/** Per-pipeline column edge (from GET /api/pipelines/:id/column-lineage). */
export interface PipelineColumnEdgeDto {
  upstream_column: string;
  upstream_node: string | null;
  upstream_resource: string | null;
  downstream_column: string;
  downstream_node: string | null;
  downstream_resource: string | null;
  relationship: RelationshipKind;
  expression_text: string | null;
  confidence: ConfidenceLevel;
}

export interface PipelineColumnLineageResponse {
  pipeline_id: string;
  environment: string;
  edges: PipelineColumnEdgeDto[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE = '/api/lineage';

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(body || `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

function traceParams(opts?: ColumnTraceOptions): string {
  const p = new URLSearchParams();
  if (opts?.environment) p.set('environment', opts.environment);
  if (opts?.maxDepth != null) p.set('max_depth', String(opts.maxDepth));
  if (opts?.relationships?.length) p.set('relationships', opts.relationships.join(','));
  if (opts?.confidence?.length) p.set('confidence', opts.confidence.join(','));
  const s = p.toString();
  return s ? `?${s}` : '';
}

// ---------------------------------------------------------------------------
// Query options
// ---------------------------------------------------------------------------

export interface ColumnTraceOptions {
  environment?: string;
  maxDepth?: number;
  relationships?: RelationshipKind[];
  confidence?: ConfidenceLevel[];
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Trace upstream lineage for a column identified by resource fingerprint. */
export async function fetchColumnUpstream(
  fingerprint: string,
  column: string,
  opts?: ColumnTraceOptions,
): Promise<ColumnTraceResponse> {
  const res = await fetch(
    `${BASE}/columns/${encodeURIComponent(fingerprint)}/${encodeURIComponent(column)}/upstream${traceParams(opts)}`,
  );
  return jsonOrThrow(res);
}

/** Trace downstream lineage for a column. */
export async function fetchColumnDownstream(
  fingerprint: string,
  column: string,
  opts?: ColumnTraceOptions,
): Promise<ColumnTraceResponse> {
  const res = await fetch(
    `${BASE}/columns/${encodeURIComponent(fingerprint)}/${encodeURIComponent(column)}/downstream${traceParams(opts)}`,
  );
  return jsonOrThrow(res);
}

/** Impact analysis: what breaks if this column is renamed or dropped. */
export async function fetchColumnImpact(
  fingerprint: string,
  column: string,
  opts?: ColumnTraceOptions,
): Promise<ColumnImpactResponse> {
  const res = await fetch(
    `${BASE}/columns/${encodeURIComponent(fingerprint)}/${encodeURIComponent(column)}/impact${traceParams(opts)}`,
  );
  return jsonOrThrow(res);
}

/** Search columns by name across all resources. */
export async function fetchColumnSearch(
  query: string,
  environment?: string,
): Promise<ColumnSearchResponse> {
  const p = new URLSearchParams({ query });
  if (environment) p.set('environment', environment);
  const res = await fetch(`${BASE}/columns/search?${p}`);
  return jsonOrThrow(res);
}

/** Fetch all column lineage edges for a specific pipeline. */
export async function fetchPipelineColumnLineage(
  pipelineId: string,
  environment?: string,
): Promise<PipelineColumnLineageResponse> {
  const p = new URLSearchParams();
  if (environment) p.set('environment', environment);
  const qs = p.toString();
  const res = await fetch(
    `/api/pipelines/${encodeURIComponent(pipelineId)}/column-lineage${qs ? `?${qs}` : ''}`,
  );
  return jsonOrThrow(res);
}
