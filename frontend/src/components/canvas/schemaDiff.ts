// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { ApiColumnInfo } from '../../api/pipelines';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ColumnDiffKind = 'unchanged' | 'added' | 'removed' | 'type_changed' | 'renamed';

export interface ColumnDiff {
  column: ApiColumnInfo;
  kind: ColumnDiffKind;
  /** For renamed columns, the original name before the rename. */
  previousName?: string;
  /** For type_changed columns, the original data type. */
  previousType?: string;
}

export interface SchemaDiff {
  /** Diffs for all output columns (in output order). */
  outputDiffs: ColumnDiff[];
  /** Columns that existed in input but are absent from output. */
  removedColumns: ColumnDiff[];
}

// ---------------------------------------------------------------------------
// Diff computation
// ---------------------------------------------------------------------------

/**
 * Compare an input schema (upstream columns) to an output schema (current node
 * columns) and classify each column.
 *
 * Rename detection: when exactly one column was added and one was removed, and
 * they share the same data_type, we treat them as a rename. For multiple
 * candidates we fall back to added/removed to avoid false positives.
 */
export function computeSchemaDiff(
  inputColumns: ApiColumnInfo[],
  outputColumns: ApiColumnInfo[],
): SchemaDiff {
  const inputByName = new Map(inputColumns.map((c) => [c.name, c]));
  const outputByName = new Map(outputColumns.map((c) => [c.name, c]));

  // First pass — classify without rename detection
  const rawOutputDiffs: ColumnDiff[] = outputColumns.map((col) => {
    const inputCol = inputByName.get(col.name);
    if (!inputCol) {
      return { column: col, kind: 'added' as const };
    }
    if (inputCol.data_type !== col.data_type) {
      return {
        column: col,
        kind: 'type_changed' as const,
        previousType: inputCol.data_type,
      };
    }
    return { column: col, kind: 'unchanged' as const };
  });

  const rawRemoved: ColumnDiff[] = inputColumns
    .filter((c) => !outputByName.has(c.name))
    .map((c) => ({ column: c, kind: 'removed' as const }));

  // Rename detection — match added columns to removed columns by type
  const added = rawOutputDiffs.filter((d) => d.kind === 'added');
  const removed = [...rawRemoved];

  const renames = new Map<string, string>(); // addedName → removedName

  // Greedily pair added↔removed columns that share the same data_type.
  // Only pair 1:1 — if multiple candidates share a type, skip to avoid
  // false positives.
  const usedRemoved = new Set<string>();
  for (const addedDiff of added) {
    const candidates = removed.filter(
      (r) =>
        !usedRemoved.has(r.column.name) &&
        r.column.data_type === addedDiff.column.data_type,
    );
    if (candidates.length === 1) {
      renames.set(addedDiff.column.name, candidates[0].column.name);
      usedRemoved.add(candidates[0].column.name);
    }
  }

  // Apply rename classifications
  const outputDiffs: ColumnDiff[] = rawOutputDiffs.map((d) => {
    const prevName = renames.get(d.column.name);
    if (prevName) {
      return { column: d.column, kind: 'renamed' as const, previousName: prevName };
    }
    return d;
  });

  const removedColumns: ColumnDiff[] = rawRemoved.filter(
    (d) => !usedRemoved.has(d.column.name),
  );

  return { outputDiffs, removedColumns };
}
