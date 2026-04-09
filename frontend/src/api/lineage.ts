// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for cross-pipeline lineage endpoints (planning doc 31).
 */

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

export interface LineageEdgeDto {
  upstream_pipeline_id: string;
  upstream_node_id: string;
  downstream_pipeline_id: string;
  downstream_node_id: string;
  resource: string;
  source: 'static' | 'observed';
}

export interface LineageBindingDto {
  pipeline_id: string;
  node_id: string;
  direction: 'source' | 'sink';
  resource: string;
}

export interface LineageGraphResponse {
  pipelines: string[];
  edges: LineageEdgeDto[];
  bindings: LineageBindingDto[];
}

export interface LineageDirectionResponse {
  pipeline_id: string;
  direct: LineageEdgeDto[];
  transitive: string[];
}

export interface ImpactAnalysisResponse {
  pipeline_id: string;
  affected_pipelines: string[];
  direct_edges: LineageEdgeDto[];
}

export interface CyclesResponse {
  cycles: string[][];
}

export interface OrphansResponse {
  dangling_sources: LineageBindingDto[];
  orphaned_sinks: LineageBindingDto[];
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

function envParam(environment: string): string {
  return `environment=${encodeURIComponent(environment)}`;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function fetchLineageGraph(
  environment: string,
): Promise<LineageGraphResponse> {
  const res = await fetch(`${BASE}/graph?${envParam(environment)}`);
  return jsonOrThrow(res);
}

export async function fetchUpstream(
  pipelineId: string,
  environment: string,
): Promise<LineageDirectionResponse> {
  const res = await fetch(
    `${BASE}/pipelines/${encodeURIComponent(pipelineId)}/upstream?${envParam(environment)}`,
  );
  return jsonOrThrow(res);
}

export async function fetchDownstream(
  pipelineId: string,
  environment: string,
): Promise<LineageDirectionResponse> {
  const res = await fetch(
    `${BASE}/pipelines/${encodeURIComponent(pipelineId)}/downstream?${envParam(environment)}`,
  );
  return jsonOrThrow(res);
}

export async function fetchImpact(
  pipelineId: string,
  environment: string,
): Promise<ImpactAnalysisResponse> {
  const res = await fetch(
    `${BASE}/pipelines/${encodeURIComponent(pipelineId)}/impact?${envParam(environment)}`,
  );
  return jsonOrThrow(res);
}

export async function fetchCycles(
  environment: string,
): Promise<CyclesResponse> {
  const res = await fetch(`${BASE}/cycles?${envParam(environment)}`);
  return jsonOrThrow(res);
}

export async function fetchOrphans(
  environment: string,
): Promise<OrphansResponse> {
  const res = await fetch(`${BASE}/orphans?${envParam(environment)}`);
  return jsonOrThrow(res);
}
