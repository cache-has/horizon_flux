// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for run-level and failure investigation endpoints.
 *
 * Failure reports and reproduce bundles are scoped under pipelines:
 *   GET /api/pipelines/:id/runs/:runId/nodes/:nodeId/failure-report
 *   GET /api/pipelines/:id/runs/:runId/nodes/:nodeId/failure-report/reproduce
 *
 * Top-level run endpoints (no pipeline ID required):
 *   GET /api/runs/:runId
 *   GET /api/runs/:runId/compare/:otherId
 */

// ---------------------------------------------------------------------------
// Failure report types (mirrors armillary-datafusion::failure_report)
// ---------------------------------------------------------------------------

export interface ApiSchemaField {
  name: string;
  data_type: string;
  nullable: boolean;
}

export interface ApiInputSchema {
  node_id: string;
  fields: ApiSchemaField[];
}

export interface ApiPluginDiagnostics {
  plugin_name?: string;
  exit_code?: number;
  stderr_tail?: string;
  last_protocol_messages: string[];
}

export interface ApiFailureReport {
  run_id: string;
  node_id: string;
  pipeline_name: string;
  environment: string;
  error_chain: string[];
  node_config?: unknown;
  input_schemas: ApiInputSchema[];
  input_sample: Record<string, unknown>[];
  input_total_rows: number;
  executed_sql?: string;
  plugin_diagnostics?: ApiPluginDiagnostics;
  source_query?: string;
  captured_at_ms: number;
}

// ---------------------------------------------------------------------------
// Run comparison types (mirrors armillary-server::api::runs)
// ---------------------------------------------------------------------------

export interface ApiNodeComparison {
  node_id: string;
  duration_ms_a?: number;
  duration_ms_b?: number;
  duration_delta_ms?: number;
  rows_in_a?: number;
  rows_in_b?: number;
  rows_in_delta?: number;
  rows_out_a?: number;
  rows_out_b?: number;
  rows_out_delta?: number;
  error_a?: string;
  error_b?: string;
  presence?: 'only_in_a' | 'only_in_b';
}

export interface ApiTestComparison {
  node_id: string;
  passed_a?: boolean;
  passed_b?: boolean;
  changed: boolean;
}

export interface ApiRunComparison {
  run_id_a: string;
  run_id_b: string;
  pipeline_name_a: string;
  pipeline_name_b: string;
  status_a: string;
  status_b: string;
  duration_ms_a?: number;
  duration_ms_b?: number;
  duration_delta_ms?: number;
  total_rows_out_a: number;
  total_rows_out_b: number;
  total_rows_out_delta: number;
  node_comparisons: ApiNodeComparison[];
  test_comparisons: ApiTestComparison[];
}

// ---------------------------------------------------------------------------
// Single-run response type (mirrors armillary-datafusion::run::PipelineRun)
// ---------------------------------------------------------------------------

/** Timestamp as returned by the backend (serde SystemTime). */
interface ApiTimestamp {
  secs_since_epoch: number;
  nanos_since_epoch: number;
}

/** Node-level run statistics from the top-level run endpoint. */
export interface ApiRunNodeStats {
  node_id: string;
  start_time?: ApiTimestamp;
  end_time?: ApiTimestamp;
  rows_in: number;
  rows_out: number;
  error?: string;
}

/** Test result summary from a run. */
export interface ApiRunTestResult {
  node_id: string;
  passed: boolean;
  severity: 'error' | 'warn';
  assertions: {
    name: string;
    passed: boolean;
    violation_count: number;
    violating_rows?: Record<string, unknown>[];
    message?: string;
  }[];
}

/** Full run record as returned by `GET /api/runs/:runId`. */
export interface ApiRunDetail {
  id: string;
  pipeline_name: string;
  environment: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'cancelled';
  start_time?: ApiTimestamp;
  end_time?: ApiTimestamp;
  node_stats: ApiRunNodeStats[];
  error?: string;
  test_results?: ApiRunTestResult[];
  triggered_by?: string;
}

/** Compute duration in ms from backend timestamps. */
function timestampToMs(ts?: ApiTimestamp): number | undefined {
  if (!ts) return undefined;
  return ts.secs_since_epoch * 1000 + ts.nanos_since_epoch / 1_000_000;
}

/** Compute duration in ms between two timestamps. */
export function timestampDurationMs(start?: ApiTimestamp, end?: ApiTimestamp): number {
  const s = timestampToMs(start);
  const e = timestampToMs(end);
  if (s == null || e == null) return 0;
  return Math.round(e - s);
}

/** Convert a timestamp to a Date. */
export function timestampToDate(ts?: ApiTimestamp): Date | undefined {
  const ms = timestampToMs(ts);
  if (ms == null) return undefined;
  return new Date(ms);
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const PIPELINES_BASE = '/api/pipelines';
const RUNS_BASE = '/api/runs';

/** Fetch the failure report for a specific node in a run. */
export async function fetchFailureReport(
  pipelineId: string,
  runId: string,
  nodeId: string,
): Promise<ApiFailureReport> {
  const url = `${PIPELINES_BASE}/${pipelineId}/runs/${encodeURIComponent(runId)}/nodes/${encodeURIComponent(nodeId)}/failure-report`;
  const res = await fetch(url);
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('No failure report found for this node and run.');
    }
    throw new Error(`Failed to fetch failure report: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/**
 * Download the reproduce-locally bundle for a failed node.
 * Triggers a browser file download of the JSON bundle.
 */
export async function downloadReproduceBundle(
  pipelineId: string,
  runId: string,
  nodeId: string,
): Promise<void> {
  const url = `${PIPELINES_BASE}/${pipelineId}/runs/${encodeURIComponent(runId)}/nodes/${encodeURIComponent(nodeId)}/failure-report/reproduce`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to download reproduce bundle: ${res.status} ${res.statusText}`);
  }
  const disposition = res.headers.get('content-disposition') ?? '';
  const match = disposition.match(/filename="(.+?)"/);
  const filename = match?.[1] ?? `reproduce-${nodeId}.json`;
  const blob = await res.blob();
  const blobUrl = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(blobUrl);
}

/** Fetch a single run by ID (no pipeline ID needed). */
export async function fetchRun(runId: string): Promise<ApiRunDetail> {
  const res = await fetch(`${RUNS_BASE}/${encodeURIComponent(runId)}`);
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('Run not found.');
    }
    throw new Error(`Failed to fetch run: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Compare two runs side-by-side (no pipeline ID needed). */
export async function compareRuns(
  runIdA: string,
  runIdB: string,
): Promise<ApiRunComparison> {
  const res = await fetch(
    `${RUNS_BASE}/${encodeURIComponent(runIdA)}/compare/${encodeURIComponent(runIdB)}`,
  );
  if (!res.ok) {
    throw new Error(`Failed to compare runs: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
