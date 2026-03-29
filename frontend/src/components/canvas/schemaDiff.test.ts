// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect } from 'vitest';
import { computeSchemaDiff } from './schemaDiff';
import type { ApiColumnInfo } from '../../api/pipelines';

function col(name: string, data_type = 'Utf8', nullable = true): ApiColumnInfo {
  return { name, data_type, nullable };
}

describe('computeSchemaDiff', () => {
  it('marks all columns as unchanged when schemas match', () => {
    const input = [col('a', 'Int32'), col('b', 'Utf8')];
    const output = [col('a', 'Int32'), col('b', 'Utf8')];
    const diff = computeSchemaDiff(input, output);

    expect(diff.outputDiffs).toHaveLength(2);
    expect(diff.outputDiffs.every((d) => d.kind === 'unchanged')).toBe(true);
    expect(diff.removedColumns).toHaveLength(0);
  });

  it('detects added columns', () => {
    const input = [col('a', 'Int32')];
    const output = [col('a', 'Int32'), col('b', 'Utf8')];
    const diff = computeSchemaDiff(input, output);

    expect(diff.outputDiffs[0].kind).toBe('unchanged');
    expect(diff.outputDiffs[1].kind).toBe('added');
    expect(diff.removedColumns).toHaveLength(0);
  });

  it('detects removed columns', () => {
    const input = [col('a', 'Int32'), col('b', 'Utf8')];
    const output = [col('a', 'Int32')];
    const diff = computeSchemaDiff(input, output);

    expect(diff.outputDiffs).toHaveLength(1);
    expect(diff.outputDiffs[0].kind).toBe('unchanged');
    expect(diff.removedColumns).toHaveLength(1);
    expect(diff.removedColumns[0].column.name).toBe('b');
    expect(diff.removedColumns[0].kind).toBe('removed');
  });

  it('detects type changes', () => {
    const input = [col('a', 'Int32'), col('b', 'Utf8')];
    const output = [col('a', 'Float64'), col('b', 'Utf8')];
    const diff = computeSchemaDiff(input, output);

    expect(diff.outputDiffs[0].kind).toBe('type_changed');
    expect(diff.outputDiffs[0].previousType).toBe('Int32');
    expect(diff.outputDiffs[1].kind).toBe('unchanged');
  });

  it('detects a rename when one column is removed and one added with the same type', () => {
    const input = [col('old_name', 'Int32'), col('b', 'Utf8')];
    const output = [col('new_name', 'Int32'), col('b', 'Utf8')];
    const diff = computeSchemaDiff(input, output);

    expect(diff.outputDiffs[0].kind).toBe('renamed');
    expect(diff.outputDiffs[0].previousName).toBe('old_name');
    expect(diff.outputDiffs[1].kind).toBe('unchanged');
    expect(diff.removedColumns).toHaveLength(0);
  });

  it('does not detect a rename when multiple candidates share the same type', () => {
    // Two removed Int32 columns, one added Int32 column — ambiguous
    const input = [col('x', 'Int32'), col('y', 'Int32'), col('z', 'Utf8')];
    const output = [col('w', 'Int32'), col('z', 'Utf8')];
    const diff = computeSchemaDiff(input, output);

    // w should be 'added', not renamed (ambiguous source)
    expect(diff.outputDiffs[0].kind).toBe('added');
    expect(diff.removedColumns).toHaveLength(2);
  });

  it('handles empty input schema (source node — all columns are added)', () => {
    const output = [col('a', 'Int32'), col('b', 'Utf8')];
    const diff = computeSchemaDiff([], output);

    expect(diff.outputDiffs.every((d) => d.kind === 'added')).toBe(true);
    expect(diff.removedColumns).toHaveLength(0);
  });

  it('handles empty output schema (all columns removed)', () => {
    const input = [col('a', 'Int32'), col('b', 'Utf8')];
    const diff = computeSchemaDiff(input, []);

    expect(diff.outputDiffs).toHaveLength(0);
    expect(diff.removedColumns).toHaveLength(2);
  });

  it('handles combined add, remove, type change, and rename', () => {
    const input = [
      col('keep', 'Utf8'),
      col('old_name', 'Float64'),
      col('drop_me', 'Boolean'),
      col('retyped', 'Int32'),
    ];
    const output = [
      col('keep', 'Utf8'),
      col('new_name', 'Float64'),
      col('brand_new', 'Utf8'),
      col('retyped', 'Int64'),
    ];
    const diff = computeSchemaDiff(input, output);

    expect(diff.outputDiffs[0]).toMatchObject({ kind: 'unchanged' });
    expect(diff.outputDiffs[1]).toMatchObject({ kind: 'renamed', previousName: 'old_name' });
    expect(diff.outputDiffs[2]).toMatchObject({ kind: 'added' });
    expect(diff.outputDiffs[3]).toMatchObject({ kind: 'type_changed', previousType: 'Int32' });
    // drop_me removed, old_name consumed by rename
    expect(diff.removedColumns).toHaveLength(1);
    expect(diff.removedColumns[0].column.name).toBe('drop_me');
  });
});
