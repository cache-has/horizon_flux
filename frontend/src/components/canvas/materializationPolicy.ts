// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pure validation logic for sink materialization policies.
//!
//! Extracted from `MaterializationEditor.tsx` so the component file only exports
//! React components (required for Fast Refresh). Mirrors the cross-field rules
//! enforced by `armillary-engine::materialization::validate_policy`.

import type {
  MaterializationPolicy,
  ReadMode,
  WriteStrategy,
  ChangeDetection,
} from '../../api/pipelines';

/** Cheap structural ISO 8601 duration recognizer. Mirrors the Rust helper. */
function isIso8601Duration(s: string): boolean {
  if (s.length < 3 || s[0] !== 'P') return false;
  const rest = s.slice(1);
  if (!/[0-9]/.test(rest)) return false;
  let seenT = false;
  for (const c of rest) {
    if (/[0-9]/.test(c)) continue;
    if ('YMWDHS'.includes(c)) continue;
    if (c === 'T') {
      if (seenT) return false;
      seenT = true;
      continue;
    }
    return false;
  }
  return true;
}

/** Mirror of `validate_policy` in armillary-engine::materialization. */
export function validatePolicy(p: MaterializationPolicy): string[] {
  const errors: string[] = [];
  const readMode: ReadMode = p.read_mode ?? 'full';
  const strategy: WriteStrategy = p.write_strategy ?? 'append';

  if (readMode === 'incremental') {
    if (!p.watermark) {
      errors.push('Incremental read mode requires a watermark.');
    } else if (!p.watermark.column.trim()) {
      errors.push('Watermark column must not be empty.');
    }
  } else if (p.watermark) {
    errors.push('Watermark must not be set when read_mode is full.');
  }

  const needsKeys = strategy === 'merge' || strategy === 'delete_insert' || strategy === 'snapshot';
  const hasKeys = !!p.unique_keys && p.unique_keys.length > 0;
  if (needsKeys && !hasKeys) {
    errors.push(`unique_keys is required for write_strategy "${strategy}".`);
  }
  if (!needsKeys && p.unique_keys && p.unique_keys.length > 0) {
    errors.push(`unique_keys must not be set for write_strategy "${strategy}".`);
  }

  const needsPartition = strategy === 'insert_overwrite';
  if (needsPartition && !p.partition_column) {
    errors.push('partition_column is required for write_strategy "insert_overwrite".');
  }
  if (!needsPartition && p.partition_column) {
    errors.push(`partition_column must not be set for write_strategy "${strategy}".`);
  }

  const lookback = p.lookback ?? 'PT0S';
  if (lookback !== 'PT0S') {
    if (!isIso8601Duration(lookback)) {
      errors.push(`lookback "${lookback}" is not a valid ISO 8601 duration.`);
    }
    const wmIsTimestamp = p.watermark?.type === 'timestamp';
    if (readMode !== 'incremental' || !wmIsTimestamp) {
      errors.push('lookback only applies under incremental read mode with a timestamp watermark.');
    }
  }

  // Snapshot sub-block rules — mirror armillary-engine::materialization::validate_policy.
  if (strategy === 'snapshot') {
    if (!p.snapshot) {
      errors.push('write_strategy "snapshot" requires a snapshot sub-block.');
    } else {
      const snap = p.snapshot;
      const detection: ChangeDetection = snap.change_detection ?? 'check';
      if (detection === 'check') {
        const cols = snap.check_columns ?? [];
        if (cols.length === 0) {
          errors.push('snapshot.check_columns is required and must be non-empty when change_detection is "check".');
        }
        if (readMode === 'incremental') {
          errors.push(
            'change_detection: "check" is incoherent with read_mode: "incremental" — use change_detection: "timestamp" or read_mode: "full".',
          );
        }
      } else if (detection === 'timestamp') {
        if (!snap.updated_at_column || !snap.updated_at_column.trim()) {
          errors.push('snapshot.updated_at_column is required when change_detection is "timestamp".');
        }
        if (readMode === 'incremental' && p.watermark && snap.updated_at_column) {
          if (p.watermark.column !== snap.updated_at_column) {
            errors.push(
              `read_mode "incremental" requires watermark.column ("${p.watermark.column}") to match snapshot.updated_at_column ("${snap.updated_at_column}").`,
            );
          }
        }
      }
    }
  } else if (p.snapshot) {
    errors.push(`snapshot sub-block must not be set for write_strategy "${strategy}".`);
  }

  return errors;
}
