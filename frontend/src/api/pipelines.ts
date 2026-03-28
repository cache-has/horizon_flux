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
