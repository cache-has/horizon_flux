// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useMemo, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { ApiPreviewNodeResponse, ApiColumnInfo } from '../../api/pipelines';
import type { ColumnDiff } from './schemaDiff';
import { classifyType, formatCell, formatColumnStatsTooltip } from './previewUtils';
import { useColumnLineageStore, type HighlightedColumn } from '../../stores/columnLineageStore';
import './PreviewTable.css';

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
  /** Error message to display (e.g. timeout). */
  error?: string | null;
  sampleMethod?: string;
  /** Per-column diff info (parallel to preview.columns). When provided, column headers are color-coded. */
  columnDiffs?: ColumnDiff[];
  /** Node ID for column lineage highlighting. When set, hovering columns traces upstream. */
  nodeId?: string;
}

export function PreviewTable({ preview, loading, error, sampleMethod, columnDiffs, nodeId }: PreviewTableProps) {
  const parentRef = useRef<HTMLDivElement>(null);
  const [sort, setSort] = useState<SortState | null>(null);
  const [colWidths, setColWidths] = useState<Record<string, number>>({});
  const resizeRef = useRef<{
    col: string;
    startX: number;
    startW: number;
  } | null>(null);

  // Columns metadata
  const columns = useMemo<ApiColumnInfo[]>(() => preview?.columns ?? [], [preview]);
  const columnKinds = useMemo(
    () => columns.map((c) => classifyType(c.data_type)),
    [columns],
  );

  // Column lineage highlighting
  const highlightedColumns = useColumnLineageStore((s) => s.highlightedColumns);
  const highlightSource = useColumnLineageStore((s) => s.highlightSource);
  const setHighlight = useColumnLineageStore((s) => s.setHighlight);
  const clearHighlight = useColumnLineageStore((s) => s.clearHighlight);

  // Check which columns in THIS node's preview are highlighted
  const highlightedSet = useMemo(() => {
    if (!nodeId) return new Map<string, HighlightedColumn>();
    const map = new Map<string, HighlightedColumn>();
    for (const h of highlightedColumns) {
      if (h.nodeId === nodeId) {
        map.set(h.column, h);
      }
    }
    return map;
  }, [nodeId, highlightedColumns]);

  const isSourceNode = highlightSource?.nodeId === nodeId;

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

  // Error state (e.g. timeout)
  if (error) {
    return (
      <div className="preview-table__error" data-testid="preview-table-error">
        {error}
      </div>
    );
  }

  // Status-aware empty states
  if (preview?.status === 'no_cache') {
    return (
      <div className="preview-table__status-message" data-testid="preview-table-no-cache">
        <span className="preview-table__status-icon">&#x1f4e6;</span>
        Run the pipeline to enable preview
      </div>
    );
  }

  if (preview?.status === 'skipped') {
    return (
      <div className="preview-table__status-message" data-testid="preview-table-skipped">
        Sinks do not produce preview data
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
              const isHighlighted = highlightedSet.has(col.name);
              const isSource = isSourceNode && highlightSource?.column === col.name;
              const highlightInfo = highlightedSet.get(col.name);
              const provenanceTooltip = highlightInfo
                ? `\nLineage: ${highlightInfo.relationship} (${highlightInfo.confidence})${highlightInfo.expression ? `\nExpression: ${highlightInfo.expression}` : ''}`
                : '';
              return (
                <div
                  key={col.name}
                  className={`preview-table__th preview-table__th--${columnKinds[ci]}${diffClass}${isHighlighted ? ' preview-table__th--highlighted' : ''}${isSource ? ' preview-table__th--highlight-source' : ''}`}
                  style={{ width: w, minWidth: w }}
                  onClick={() => handleSort(col.name)}
                  onMouseEnter={() => nodeId && setHighlight(nodeId, col.name)}
                  onMouseLeave={() => clearHighlight()}
                  role="columnheader"
                  title={`${col.name} (${col.data_type})${diffTooltip}${provenanceTooltip}${statsTooltip}\n— click to sort`}
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
                    const cellHighlighted = highlightedSet.has(col.name) || (isSourceNode && highlightSource?.column === col.name);
                    return (
                      <div
                        key={col.name}
                        className={[
                          'preview-table__td',
                          `preview-table__td--${kind}`,
                          isNull ? 'preview-table__td--null' : '',
                          cellHighlighted ? 'preview-table__td--highlighted' : '',
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
        {preview.status && (
          <span
            className={`preview-table__status-badge preview-table__status-badge--${preview.status}`}
            data-testid="preview-status-badge"
          >
            {preview.status === 'cached' ? 'cached' : 're-executed'}
          </span>
        )}
        {' '}{preview.row_count.toLocaleString()} rows &middot; {columns.length} columns
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
