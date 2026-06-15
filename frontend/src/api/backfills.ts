// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for backfill endpoints (planning doc 33).
 *
 * Types mirror the Rust `armillary-engine::backfill` types serialized via serde.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type BackfillStatus = 'pending' | 'running' | 'completed' | 'cancelled' | 'failed';
export type IterationStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'skipped';
export type DateGranularity = 'hour' | 'day' | 'week' | 'month';

export type RangeDefinition =
  | { kind: 'date_range'; start: string; end: string; granularity: DateGranularity; variable_mapping: Record<string, string> }
  | { kind: 'list'; values: string[]; variable_mapping: Record<string, string> }
  | { kind: 'sql'; connection: string; query: string; variable_mapping: Record<string, string> };

export interface BackfillProgress {
  total: number;
  succeeded: number;
  failed: number;
  running: number;
  pending: number;
  skipped: number;
}

export interface Backfill {
  id: string;
  pipeline_id: string;
  environment: string;
  range_definition: RangeDefinition;
  concurrency: number;
  fail_fast: boolean;
  full_refresh: boolean;
  status: BackfillStatus;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  created_by?: string;
}

export interface BackfillIteration {
  backfill_id: string;
  iteration_index: number;
  iteration_key: string;
  variables: Record<string, unknown>;
  status: IterationStatus;
  run_id?: string;
  error?: string;
  started_at?: string;
  completed_at?: string;
}

export interface BackfillResponse {
  id: string;
  pipeline_id: string;
  environment: string;
  range_definition: RangeDefinition;
  concurrency: number;
  fail_fast: boolean;
  full_refresh: boolean;
  status: BackfillStatus;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  created_by?: string;
  progress?: BackfillProgress;
}

export interface BackfillDetailResponse extends Backfill {
  progress: BackfillProgress;
  iterations: BackfillIteration[];
}

export interface CreateBackfillRequest {
  pipeline_id: string;
  environment?: string;
  range_definition: RangeDefinition;
  concurrency?: number;
  fail_fast?: boolean;
  full_refresh?: boolean;
  variables?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const BASE = '/api/backfills';

/** List backfills, optionally filtered by pipeline and/or status. */
export async function listBackfills(
  pipelineId?: string,
  status?: string,
  limit = 50,
): Promise<BackfillResponse[]> {
  const params = new URLSearchParams();
  if (pipelineId) params.set('pipeline_id', pipelineId);
  if (status) params.set('status', status);
  params.set('limit', String(limit));
  const qs = params.toString();
  const res = await fetch(`${BASE}?${qs}`);
  if (!res.ok) {
    throw new Error(`Failed to list backfills: ${res.status} ${res.statusText}`);
  }
  const body = await res.json();
  return body.data;
}

/** Get a single backfill with iterations. */
export async function getBackfill(id: string): Promise<BackfillDetailResponse> {
  const res = await fetch(`${BASE}/${id}`);
  if (!res.ok) {
    throw new Error(`Failed to get backfill ${id}: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Create and start a backfill. */
export async function createBackfill(req: CreateBackfillRequest): Promise<BackfillResponse> {
  const res = await fetch(BASE, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to create backfill: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Resume a failed or cancelled backfill. */
export async function resumeBackfill(id: string): Promise<BackfillResponse> {
  const res = await fetch(`${BASE}/${id}/resume`, { method: 'POST' });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to resume backfill: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Cancel a running backfill. */
export async function cancelBackfill(id: string): Promise<BackfillResponse> {
  const res = await fetch(`${BASE}/${id}/cancel`, { method: 'POST' });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to cancel backfill: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Delete a backfill. */
export async function deleteBackfill(id: string): Promise<void> {
  const res = await fetch(`${BASE}/${id}`, { method: 'DELETE' });
  if (!res.ok && res.status !== 404) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to delete backfill: ${res.status} ${res.statusText}`);
  }
}
