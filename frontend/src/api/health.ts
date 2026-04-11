// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for the cross-pipeline health dashboard (planning doc 37/38).
 *
 * Types mirror the Rust `flux-server::api::health` response types.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type TimeWindow = '24h' | '7d' | '30d';

export interface StatusCounts {
  total: number;
  success: number;
  failed: number;
  running: number;
  pending: number;
  cancelled: number;
}

export interface RunSummary extends StatusCounts {
  by_environment: Record<string, StatusCounts>;
}

export interface FailingPipeline {
  pipeline_name: string;
  failure_count: number;
  last_failure_at?: string;
  last_error?: string;
}

export interface SlowestPipeline {
  pipeline_name: string;
  avg_duration_ms: number;
  max_duration_ms: number;
  run_count: number;
}

export interface TriggerFailure {
  trigger_id: string;
  trigger_name: string;
  pipeline_id: string;
  consecutive_errors: number;
}

export interface TriggerHealth {
  total: number;
  healthy: number;
  consecutive_failures: TriggerFailure[];
}

export interface SlaBreach {
  fingerprint: string;
  age?: string;
  max_age: string;
  producer_pipeline?: string;
}

export interface SlaSummary {
  total: number;
  ok: number;
  warning: number;
  breach: number;
  unknown: number;
  breaches: SlaBreach[];
}

export interface NotableEvent {
  kind: string;
  pipeline_name?: string;
  description: string;
  at?: string;
}

export interface HealthOverview {
  window: string;
  generated_at: string;
  cached: boolean;
  run_summary: RunSummary;
  top_failing_pipelines: FailingPipeline[];
  slowest_pipelines: SlowestPipeline[];
  trigger_health: TriggerHealth;
  sla_summary: SlaSummary;
  notable_events: NotableEvent[];
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const BASE = '/api/health';

/** Fetch the project-wide health overview for the given time window. */
export async function fetchHealthOverview(window: TimeWindow = '24h'): Promise<HealthOverview> {
  const res = await fetch(`${BASE}/overview?window=${window}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch health overview: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
