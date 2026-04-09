// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * API client for trigger endpoints.
 *
 * Types mirror the Rust `flux-scheduler` types serialized via serde.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type CompletionStatus = 'success' | 'failure' | 'any';
export type RunPolicy = 'queue' | 'skip' | 'reject';
export type TriggerOutcome = 'run_started' | 'queued' | 'skipped' | 'rejected' | 'error';

export type TriggerKind =
  | { kind: 'cron'; expression: string; timezone: string }
  | { kind: 'interval'; every: string; start_at?: string }
  | { kind: 'file_arrival'; path: string; poll_interval: string; variable_mapping?: Record<string, string> }
  | { kind: 'webhook'; path: string; auth: string; variable_mapping?: Record<string, string> }
  | { kind: 'pipeline_completion'; upstream_pipeline: string; environment?: string; on_status: CompletionStatus };

export type TriggerKindName = TriggerKind['kind'];

export const TRIGGER_KIND_LABELS: Record<TriggerKindName, string> = {
  cron: 'Cron',
  interval: 'Interval',
  file_arrival: 'File Arrival',
  webhook: 'Webhook',
  pipeline_completion: 'Pipeline Completion',
};

export interface Trigger {
  id: string;
  name: string;
  pipeline_id: string;
  environment: string;
  enabled: boolean;
  kind: TriggerKind;
  run_policy: RunPolicy;
  variable_overrides?: Record<string, unknown>;
  max_queue_depth: number;
  created_at: string;
  updated_at: string;
}

export interface TriggerState {
  trigger_id: string;
  last_evaluated_at?: string;
  last_fired_at?: string;
  next_fire_at?: string;
  sensor_state?: unknown;
  consecutive_errors: number;
}

export interface TriggerResponse {
  id: string;
  name: string;
  pipeline_id: string;
  environment: string;
  enabled: boolean;
  kind: TriggerKind;
  run_policy: RunPolicy;
  variable_overrides?: Record<string, unknown>;
  max_queue_depth: number;
  created_at: string;
  updated_at: string;
  state?: TriggerState;
}

export interface TriggerHistoryEntry {
  id: string;
  trigger_id: string;
  fired_at: string;
  outcome: TriggerOutcome;
  run_id?: string;
  details?: unknown;
  error?: string;
}

export interface FireResponse {
  outcome: string;
  run_id?: string;
}

export interface CreateTriggerRequest {
  name: string;
  pipeline_id: string;
  environment?: string;
  enabled?: boolean;
  kind: TriggerKind;
  run_policy?: RunPolicy;
  variable_overrides?: Record<string, unknown>;
  max_queue_depth?: number;
}

export interface UpdateTriggerRequest {
  name?: string;
  pipeline_id?: string;
  environment?: string;
  enabled?: boolean;
  kind?: TriggerKind;
  run_policy?: RunPolicy;
  variable_overrides?: Record<string, unknown>;
  max_queue_depth?: number;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

const BASE = '/api/triggers';

/** List triggers, optionally filtered by pipeline and/or environment. */
export async function listTriggers(
  pipelineId?: string,
  environment?: string,
): Promise<Trigger[]> {
  const params = new URLSearchParams();
  if (pipelineId) params.set('pipeline_id', pipelineId);
  if (environment) params.set('environment', environment);
  const qs = params.toString();
  const res = await fetch(`${BASE}${qs ? `?${qs}` : ''}`);
  if (!res.ok) {
    throw new Error(`Failed to list triggers: ${res.status} ${res.statusText}`);
  }
  const body = await res.json();
  return body.data;
}

/** Get a single trigger with its state. */
export async function getTrigger(id: string): Promise<TriggerResponse> {
  const res = await fetch(`${BASE}/${id}`);
  if (!res.ok) {
    throw new Error(`Failed to get trigger ${id}: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Create a new trigger. */
export async function createTrigger(req: CreateTriggerRequest): Promise<TriggerResponse> {
  const res = await fetch(BASE, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to create trigger: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Update a trigger (partial). */
export async function updateTrigger(id: string, req: UpdateTriggerRequest): Promise<TriggerResponse> {
  const res = await fetch(`${BASE}/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to update trigger: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Delete a trigger. */
export async function deleteTrigger(id: string): Promise<void> {
  const res = await fetch(`${BASE}/${id}`, { method: 'DELETE' });
  if (!res.ok && res.status !== 404) {
    throw new Error(`Failed to delete trigger ${id}: ${res.status} ${res.statusText}`);
  }
}

/** Enable a trigger. */
export async function enableTrigger(id: string): Promise<TriggerResponse> {
  const res = await fetch(`${BASE}/${id}/enable`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(`Failed to enable trigger: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Disable a trigger. */
export async function disableTrigger(id: string): Promise<TriggerResponse> {
  const res = await fetch(`${BASE}/${id}/disable`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(`Failed to disable trigger: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Manually fire a trigger. */
export async function fireTrigger(id: string): Promise<FireResponse> {
  const res = await fetch(`${BASE}/${id}/fire`, { method: 'POST' });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(body?.error ?? `Failed to fire trigger: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Get trigger firing history. */
export async function getTriggerHistory(
  id: string,
  limit = 50,
): Promise<TriggerHistoryEntry[]> {
  const res = await fetch(`${BASE}/${id}/history?limit=${limit}`);
  if (!res.ok) {
    throw new Error(`Failed to get trigger history: ${res.status} ${res.statusText}`);
  }
  const body = await res.json();
  return body.data;
}
