// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

export interface PluginSinkDeclaration {
  type: string;
  display_name: string;
  description?: string | null;
  config_schema: string;
  capabilities?: {
    transactional?: boolean;
    upsert?: boolean;
    schema_validation?: boolean;
    [k: string]: unknown;
  };
}

export interface PluginManifest {
  name: string;
  version: string;
  author?: string | null;
  description?: string | null;
  license?: string | null;
  homepage?: string | null;
  armillary_plugin_protocol: number;
  armillary_min_version: string;
  executable: string;
  args?: string[];
  env?: Record<string, string>;
  sinks?: PluginSinkDeclaration[];
}

export type PluginStatus =
  | { status: 'ok' }
  | { status: 'invalid'; error: string };

export interface DiscoveredPlugin {
  name: string;
  directory: string;
  /** Server flattens PluginStatus into the entry, so the discriminator lives here. */
  status: PluginStatus;
  manifest?: PluginManifest | null;
}

export interface PluginListResponse {
  plugins: DiscoveredPlugin[];
}

export async function listPlugins(signal?: AbortSignal): Promise<PluginListResponse> {
  const res = await fetch('/api/plugins', { signal });
  if (!res.ok) throw new Error(`Failed to list plugins: ${res.status}`);
  return res.json();
}

export async function reloadPlugins(): Promise<{ reloaded: boolean; count: number }> {
  const res = await fetch('/api/plugins/reload', { method: 'POST' });
  if (!res.ok) throw new Error(`Failed to reload plugins: ${res.status}`);
  return res.json();
}

export async function getPluginSinkSchema(
  pluginName: string,
  sinkType: string,
  signal?: AbortSignal,
): Promise<Record<string, unknown>> {
  const res = await fetch(
    `/api/plugins/${encodeURIComponent(pluginName)}/sinks/${encodeURIComponent(sinkType)}/schema`,
    { signal },
  );
  if (!res.ok) {
    let message = `Failed to fetch schema: ${res.status}`;
    try {
      const body = await res.json();
      if (body?.error) message = String(body.error);
    } catch {
      /* ignore */
    }
    throw new Error(message);
  }
  return res.json();
}

/** Returns true when the plugin's status is the "ok" variant. */
export function isPluginOk(status: PluginStatus): boolean {
  return status.status === 'ok';
}

/** Extracts an error message from a plugin status, or null if healthy. */
export function pluginStatusError(status: PluginStatus): string | null {
  return status.status === 'invalid' ? status.error : null;
}
