// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { ApiColumnStats } from '../../api/pipelines';

// ---------------------------------------------------------------------------
// Type classification
// ---------------------------------------------------------------------------

export type ColumnKind = 'numeric' | 'boolean' | 'temporal' | 'string';

export function classifyType(dataType: string): ColumnKind {
  const t = dataType.toLowerCase();
  if (t === 'boolean' || t === 'bool') return 'boolean';
  if (
    /^(u?int\d|float\d|decimal|double|half)/i.test(dataType) ||
    t === 'numeric'
  ) {
    return 'numeric';
  }
  if (/^(date|time|timestamp|duration|interval)/i.test(dataType)) {
    return 'temporal';
  }
  return 'string';
}

// ---------------------------------------------------------------------------
// Cell formatting
// ---------------------------------------------------------------------------

export function formatCell(
  value: unknown,
  kind: ColumnKind,
): { text: string; isNull: boolean } {
  if (value === null || value === undefined) {
    return { text: 'null', isNull: true };
  }

  switch (kind) {
    case 'numeric': {
      const num = Number(value);
      if (Number.isNaN(num)) return { text: String(value), isNull: false };
      return { text: num.toLocaleString(), isNull: false };
    }
    case 'boolean':
      return { text: String(value), isNull: false };
    case 'temporal':
      if (typeof value === 'number') {
        // Assume epoch millis for numeric timestamps
        return { text: new Date(value).toISOString().replace('T', ' ').replace('Z', ''), isNull: false };
      }
      return { text: String(value), isNull: false };
    default:
      return { text: String(value), isNull: false };
  }
}

// ---------------------------------------------------------------------------
// Column stats tooltip
// ---------------------------------------------------------------------------

export function formatColumnStatsTooltip(stats: ApiColumnStats): string {
  switch (stats.kind) {
    case 'numeric': {
      const parts: string[] = [];
      if (stats.min !== null) parts.push(`Min: ${stats.min.toLocaleString()}`);
      if (stats.max !== null) parts.push(`Max: ${stats.max.toLocaleString()}`);
      if (stats.mean !== null) parts.push(`Mean: ${stats.mean.toLocaleString(undefined, { maximumFractionDigits: 2 })}`);
      parts.push(`Nulls: ${stats.null_count}`);
      return parts.join('\n');
    }
    case 'string': {
      const parts: string[] = [];
      if (stats.min_length !== null) parts.push(`Min length: ${stats.min_length}`);
      if (stats.max_length !== null) parts.push(`Max length: ${stats.max_length}`);
      parts.push(`Unique: ${stats.unique_count}`);
      parts.push(`Nulls: ${stats.null_count}`);
      return parts.join('\n');
    }
    case 'boolean':
      return `True: ${stats.true_count}\nFalse: ${stats.false_count}\nNulls: ${stats.null_count}`;
    case 'other':
      return `Nulls: ${stats.null_count}`;
  }
}
