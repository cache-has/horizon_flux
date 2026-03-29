// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useMemo, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { ApiPreviewNodeResponse, ApiColumnInfo, ApiColumnStats } from '../../api/pipelines';
import type { ColumnDiff } from './schemaDiff';
import './PreviewTable.css';

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

/** Short display label for the type badge. */
function typeBadgeLabel(dataType: string): string {
  // Normalize common Arrow type names to short labels
  const map: Record<string, string> = {
    'utf8': 'str',
    'largeutf8': 'str',
    'string': 'str',
    'boolean': 'bool',
    'float16': 'f16',
    'float32': 'f32',
    'float64': 'f64',
    'double': 'f64',
    'int8': 'i8',
    'int16': 'i16',
    'int32': 'i32',
    'int64': 'i64',
    'uint8': 'u8',
    'uint16': 'u16',
    'uint32': 'u32',
    'uint64': 'u64',
  };
  const lower = dataType.toLowerCase();
  if (map[lower]) return map[lower];
  // Timestamp(...) → timestamp
  if (lower.startsWith('timestamp')) return 'timestamp';
  if (lower.startsWith('date')) return 'date';
  if (lower.startsWith('time')) return 'time';
  if (lower.startsWith('duration')) return 'duration';
  if (lower.startsWith('decimal')) return 'decimal';
  return dataType.length > 10 ? dataType.slice(0, 10) : dataType;
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
// Sorting
// ---------------------------------------------------------------------------

interface SortState {
  column: string;
  dir: 'asc' | 'desc';
}

function compareValues(a: unknown, b: unknown): number {
  if (a === null || a === undefined) return 1;
  if (b === null || b === undefined) return -1;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  return String(a).localeCompare(String(b));
}

// ---------------------------------------------------------------------------
// Column resize hook
// ---------------------------------------------------------------------------

const DEFAULT_COL_WIDTH = 120;
const MIN_COL_WIDTH = 50;
const ROW_NUM_WIDTH = 44;
const ROW_HEIGHT = 28;

// ---------------------------------------------------------------------------
// PreviewTable component
// ---------------------------------------------------------------------------

export interface PreviewTableProps {
  preview: ApiPreviewNodeResponse | null;
  loading: boolean;
  sampleMethod?: string;
  /** Per-column diff info (parallel to preview.columns). When provided, column headers are color-coded. */
  columnDiffs?: ColumnDiff[];
}

/** Format a column stats object as a tooltip string. */
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

export function PreviewTable({ preview, loading, sampleMethod, columnDiffs }: PreviewTableProps) {
  const parentRef = useRef<HTMLDivElement>(null);
  const [sort, setSort] = useState<SortState | null>(null);
  const [colWidths, setColWidths] = useState<Record<string, number>>({});
  const resizeRef = useRef<{
    col: string;
    startX: number;
    startW: number;
  } | null>(null);

  // Columns metadata
  const columns: ApiColumnInfo[] = preview?.columns ?? [];
  const columnKinds = useMemo(
    () => columns.map((c) => classifyType(c.data_type)),
    [columns],
  );

  // Sorted rows
  const sortedRows = useMemo(() => {
    if (!preview) return [];
    const rows = [...preview.rows];
    if (sort) {
      rows.sort((a, b) => {
        const cmp = compareValues(a[sort.column], b[sort.column]);
        return sort.dir === 'asc' ? cmp : -cmp;
      });
    }
    return rows;
  }, [preview, sort]);

  // Virtualizer
  const rowVirtualizer = useVirtualizer({
    count: sortedRows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 10,
  });

  // Sort toggle: none → asc → desc → none
  const handleSort = useCallback(
    (colName: string) => {
      setSort((prev) => {
        if (!prev || prev.column !== colName) return { column: colName, dir: 'asc' };
        if (prev.dir === 'asc') return { column: colName, dir: 'desc' };
        return null;
      });
    },
    [],
  );

  // Column resize handlers
  const handleResizePointerDown = useCallback(
    (e: React.PointerEvent, col: string) => {
      e.preventDefault();
      e.stopPropagation();
      const target = e.currentTarget as HTMLElement;
      target.setPointerCapture(e.pointerId);
      resizeRef.current = {
        col,
        startX: e.clientX,
        startW: colWidths[col] ?? DEFAULT_COL_WIDTH,
      };
    },
    [colWidths],
  );

  const handleResizePointerMove = useCallback(
    (e: React.PointerEvent) => {
      const r = resizeRef.current;
      if (!r) return;
      const delta = e.clientX - r.startX;
      const newWidth = Math.max(MIN_COL_WIDTH, r.startW + delta);
      setColWidths((prev) => ({ ...prev, [r.col]: newWidth }));
    },
    [],
  );

  const handleResizePointerUp = useCallback(() => {
    resizeRef.current = null;
  }, []);

  // Loading state
  if (loading) {
    return (
      <div className="preview-table preview-table--loading" data-testid="preview-table-loading">
        <div className="preview-table__skeleton">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="preview-table__skeleton-row" />
          ))}
        </div>
      </div>
    );
  }

  // Empty state
  if (!preview || preview.rows.length === 0) {
    return (
      <span className="preview-table__empty" data-testid="preview-table-empty">
        No preview data available
      </span>
    );
  }

  const totalWidth =
    ROW_NUM_WIDTH +
    columns.reduce((sum, c) => sum + (colWidths[c.name] ?? DEFAULT_COL_WIDTH), 0);

  return (
    <div className="preview-table" data-testid="preview-table">
      {/* Scrollable container */}
      <div
        ref={parentRef}
        className="preview-table__scroll"
      >
        <div
          className="preview-table__inner"
          style={{ minWidth: totalWidth }}
        >
          {/* Header */}
          <div className="preview-table__header" role="row">
            <div
              className="preview-table__row-num preview-table__row-num--header"
              style={{ width: ROW_NUM_WIDTH, minWidth: ROW_NUM_WIDTH }}
            >
              #
            </div>
            {columns.map((col, ci) => {
              const w = colWidths[col.name] ?? DEFAULT_COL_WIDTH;
              const isSorted = sort?.column === col.name;
              const colStats = preview?.column_stats?.[ci];
              const statsTooltip = colStats ? '\n' + formatColumnStatsTooltip(colStats) : '';
              const diff = columnDiffs?.[ci];
              const diffClass = diff && diff.kind !== 'unchanged'
                ? ` preview-table__th--diff-${diff.kind}`
                : '';
              const diffTooltip = diff?.kind === 'renamed'
                ? `\nRenamed from "${diff.previousName}"`
                : diff?.kind === 'type_changed'
                  ? `\nType changed from ${diff.previousType}`
                  : diff?.kind === 'added'
                    ? '\nNew column'
                    : '';
              return (
                <div
                  key={col.name}
                  className={`preview-table__th preview-table__th--${columnKinds[ci]}${diffClass}`}
                  style={{ width: w, minWidth: w }}
                  onClick={() => handleSort(col.name)}
                  role="columnheader"
                  title={`${col.name} (${col.data_type})${diffTooltip}${statsTooltip}\n— click to sort`}
                >
                  <span className="preview-table__col-name">{col.name}</span>
                  <span className={`preview-table__type-badge preview-table__type-badge--${columnKinds[ci]}`}>
                    {typeBadgeLabel(col.data_type)}
                  </span>
                  {isSorted && (
                    <span className="preview-table__sort-indicator" data-testid="sort-indicator">
                      {sort!.dir === 'asc' ? '\u25B2' : '\u25BC'}
                    </span>
                  )}
                  {/* Resize handle */}
                  <div
                    className="preview-table__resize-handle"
                    onPointerDown={(e) => handleResizePointerDown(e, col.name)}
                    onPointerMove={handleResizePointerMove}
                    onPointerUp={handleResizePointerUp}
                    data-testid={`resize-handle-${col.name}`}
                  />
                </div>
              );
            })}
          </div>

          {/* Virtualized rows */}
          <div
            className="preview-table__body"
            style={{ height: rowVirtualizer.getTotalSize() }}
          >
            {rowVirtualizer.getVirtualItems().map((virtualRow) => {
              const row = sortedRows[virtualRow.index];
              return (
                <div
                  key={virtualRow.index}
                  className="preview-table__row"
                  role="row"
                  style={{
                    height: virtualRow.size,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  <div
                    className="preview-table__row-num"
                    style={{ width: ROW_NUM_WIDTH, minWidth: ROW_NUM_WIDTH }}
                  >
                    {virtualRow.index + 1}
                  </div>
                  {columns.map((col, ci) => {
                    const kind = columnKinds[ci];
                    const { text, isNull } = formatCell(row[col.name], kind);
                    const w = colWidths[col.name] ?? DEFAULT_COL_WIDTH;
                    return (
                      <div
                        key={col.name}
                        className={[
                          'preview-table__td',
                          `preview-table__td--${kind}`,
                          isNull ? 'preview-table__td--null' : '',
                        ]
                          .filter(Boolean)
                          .join(' ')}
                        style={{ width: w, minWidth: w }}
                        title={isNull ? undefined : text}
                        role="cell"
                      >
                        {kind === 'boolean' && !isNull ? (
                          <span
                            className={`preview-table__bool-badge preview-table__bool-badge--${String(row[col.name]).toLowerCase()}`}
                          >
                            {text}
                          </span>
                        ) : (
                          text
                        )}
                      </div>
                    );
                  })}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Stats bar */}
      <div className="preview-table__stats" data-testid="preview-table-stats">
        {preview.row_count.toLocaleString()} rows &middot; {columns.length} columns
        &middot; {preview.duration_ms}ms
        {sampleMethod && (
          <>
            {' '}&middot;{' '}
            <span className="preview-table__sample-method" data-testid="sample-method">
              {sampleMethod}
            </span>
          </>
        )}
      </div>
    </div>
  );
}
