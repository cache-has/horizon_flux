// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for SLA compliance endpoints (planning doc 37, sub-feature 3).
 *
 * Types mirror the Rust `flux-engine::sla` types serialized via serde,
 * enriched with catalog metadata by the server.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type SlaStatusKind = 'ok' | 'warning' | 'breach' | 'unknown';

export interface SlaEvaluation {
  fingerprint: string;
  evaluated_at: string;
  status: SlaStatusKind;
  age?: string;
  max_age: string;
  warn_at?: string;
  producer_pipeline?: string;
  last_success_at?: string;
}

export interface SlaStatusEntry {
  fingerprint: string;
  name: string;
  tags: string[];
  owner?: string;
  /** Flattened from SlaEvaluation via #[serde(flatten)]. */
  evaluated_at: string;
  status: SlaStatusKind;
  age?: string;
  max_age: string;
  warn_at?: string;
  producer_pipeline?: string;
  last_success_at?: string;
}

export interface SlaStatusResponse {
  data: SlaStatusEntry[];
  total: number;
}

export interface SlaDetailResponse {
  evaluation: SlaEvaluation;
  history: SlaEvaluation[];
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const BASE = '/api/sla';

/** List current SLA compliance for all resources with SLAs. */
export async function fetchSlaStatus(params?: {
  status?: SlaStatusKind;
  tag?: string;
  owner?: string;
  env?: string;
}): Promise<SlaStatusResponse> {
  const qs = new URLSearchParams();
  if (params?.status) qs.set('status', params.status);
  if (params?.tag) qs.set('tag', params.tag);
  if (params?.owner) qs.set('owner', params.owner);
  if (params?.env) qs.set('env', params.env);
  const query = qs.toString();
  const res = await fetch(`${BASE}/status${query ? `?${query}` : ''}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch SLA status: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Get current SLA status + recent history for a specific resource. */
export async function fetchSlaDetail(fingerprint: string): Promise<SlaDetailResponse> {
  const res = await fetch(`${BASE}/status/${encodeURIComponent(fingerprint)}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch SLA detail: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Get historical evaluations for a resource. */
export async function fetchSlaHistory(
  fingerprint: string,
  limit?: number,
): Promise<SlaEvaluation[]> {
  const qs = new URLSearchParams();
  if (limit != null) qs.set('limit', String(limit));
  const query = qs.toString();
  const res = await fetch(
    `${BASE}/history/${encodeURIComponent(fingerprint)}${query ? `?${query}` : ''}`,
  );
  if (!res.ok) {
    throw new Error(`Failed to fetch SLA history: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
