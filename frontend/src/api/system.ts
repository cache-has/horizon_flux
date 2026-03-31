// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

export interface SystemInfo {
  version: string;
  metadata_backend: string;
  data_dir: string;
  config_source: string;
  connection_string?: string;
}

export async function getSystemInfo(): Promise<SystemInfo> {
  const res = await fetch('/api/system/info');
  if (!res.ok) {
    throw new Error(`Failed to get system info: ${res.status}`);
  }
  return res.json();
}
