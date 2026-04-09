// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for resource catalog endpoints (planning doc 34).
 *
 * Types mirror the Rust `flux-engine::catalog` types serialized via serde.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface AnnotationOwner {
  team?: string;
  contact?: string;
}

export interface MergedColumn {
  name: string;
  data_type?: string;
  nullable?: boolean;
  description?: string;
  accepted_values?: string[];
}

export interface PipelineBinding {
  pipeline_id: string;
  node_id: string;
}

export interface SchemaColumn {
  name: string;
  data_type: string;
  nullable: boolean;
}

export interface AutoDerivedFacts {
  resource_type?: string;
  producers: PipelineBinding[];
  consumers: PipelineBinding[];
  schema_columns: SchemaColumn[];
  last_updated?: string;
  row_count?: number;
  size_bytes?: number;
}

export interface CatalogEntry {
  fingerprint: string;
  name: string;
  description?: string;
  owner?: AnnotationOwner;
  tags: string[];
  derived: AutoDerivedFacts;
  columns: MergedColumn[];
  custom: Record<string, unknown>;
  environment?: string;
  annotation_file?: string;
}

export interface MetadataUpdateRequest {
  name?: string;
  description?: string;
  owner?: { team?: string; contact?: string };
  tags?: string[];
  columns?: Record<string, { description?: string; accepted_values?: string[] }>;
  custom?: Record<string, unknown>;
}

export interface DescribeRequest {
  fingerprint?: string;
  all?: boolean;
  environment?: string;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const BASE = '/api/catalog';

/** List catalog resources with optional filters and search. */
export async function listResources(params?: {
  q?: string;
  tag?: string;
  owner?: string;
  environment?: string;
  env?: string;
}): Promise<{ data: CatalogEntry[]; total: number }> {
  const qs = new URLSearchParams();
  if (params?.q) qs.set('q', params.q);
  if (params?.tag) qs.set('tag', params.tag);
  if (params?.owner) qs.set('owner', params.owner);
  if (params?.environment) qs.set('environment', params.environment);
  if (params?.env) qs.set('env', params.env);
  const query = qs.toString();
  const res = await fetch(`${BASE}/resources${query ? `?${query}` : ''}`);
  if (!res.ok) {
    throw new Error(`Failed to list catalog resources: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Get full detail for a single resource. */
export async function getResource(
  fingerprint: string,
  env?: string,
): Promise<CatalogEntry> {
  const qs = new URLSearchParams({ fingerprint });
  if (env) qs.set('env', env);
  const res = await fetch(`${BASE}/resources/detail?${qs}`);
  if (!res.ok) {
    throw new Error(`Failed to get resource: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Create or update annotation metadata for a resource. */
export async function updateMetadata(
  fingerprint: string,
  body: MetadataUpdateRequest,
): Promise<CatalogEntry> {
  const qs = new URLSearchParams({ fingerprint });
  const res = await fetch(`${BASE}/resources/metadata?${qs}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.error ?? `Failed to update metadata: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Scaffold metadata files for resources. */
export async function describeResources(
  body: DescribeRequest,
): Promise<{ created: string[] }> {
  const res = await fetch(`${BASE}/describe`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.error ?? `Failed to scaffold metadata: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Get all unique tags for filter dropdowns. */
export async function listTags(env?: string): Promise<string[]> {
  const qs = env ? `?env=${encodeURIComponent(env)}` : '';
  const res = await fetch(`${BASE}/tags${qs}`);
  if (!res.ok) {
    throw new Error(`Failed to list tags: ${res.status} ${res.statusText}`);
  }
  const body = await res.json();
  return body.tags;
}

/** Get all unique owner teams for filter dropdowns. */
export async function listOwners(env?: string): Promise<string[]> {
  const qs = env ? `?env=${encodeURIComponent(env)}` : '';
  const res = await fetch(`${BASE}/owners${qs}`);
  if (!res.ok) {
    throw new Error(`Failed to list owners: ${res.status} ${res.statusText}`);
  }
  const body = await res.json();
  return body.owners;
}
