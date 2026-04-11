// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Modal for viewing column-level lineage for a specific node within a pipeline.
 *
 * Shows all columns that flow through the selected node, grouped by
 * upstream/downstream. Clicking a column expands its full lineage trace
 * inline, showing all transformations applied to that column across the
 * pipeline.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  fetchPipelineColumnLineage,
  type PipelineColumnEdgeDto,
  type RelationshipKind,
} from '../../api/columnLineage';
import { ConfidenceBadge } from '../lineage/ConfidenceBadge';
import './ColumnLineageModal.css';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface ColumnLineageModalProps {
  open: boolean;
  pipelineId: string;
  nodeId: string;
  nodeName: string;
  environment: string;
  onClose: () => void;
}

// ---------------------------------------------------------------------------
// Relationship labels & colors (shared with ColumnLineageGraph)
// ---------------------------------------------------------------------------

const RELATIONSHIP_COLORS: Record<RelationshipKind, string> = {
  direct: '#22c55e',
  derived: '#3b82f6',
  cast: '#8b5cf6',
  filter: '#f59e0b',
  join_key: '#ef4444',
  join_passthrough: '#f87171',
  group_by: '#06b6d4',
  aggregate_input: '#0ea5e9',
  window_partition: '#a855f7',
  window_order: '#d946ef',
  window_input: '#ec4899',
  opaque: '#6b7280',
};

const RELATIONSHIP_LABELS: Record<RelationshipKind, string> = {
  direct: 'Direct',
  derived: 'Derived',
  cast: 'Cast',
  filter: 'Filter',
  join_key: 'Join Key',
  join_passthrough: 'Join Pass',
  group_by: 'Group By',
  aggregate_input: 'Agg Input',
  window_partition: 'Win Partition',
  window_order: 'Win Order',
  window_input: 'Win Input',
  opaque: 'Opaque',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** A single output column with its full upstream lineage chain. */
interface ColumnTrace {
  /** Output column name on the selected node. */
  column: string;
  /** Chain of edges from source → ... → this column (ordered by depth). */
  chain: PipelineColumnEdgeDto[];
}

/**
 * Build per-column lineage traces by walking the edges backwards from the
 * selected node to the source(s).
 */
function buildColumnTraces(
  nodeId: string,
  edges: PipelineColumnEdgeDto[],
): ColumnTrace[] {
  // Edges where this node is the downstream (i.e. this node receives data).
  const outputColumns = new Set<string>();
  for (const e of edges) {
    if ((e.downstream_node ?? '') === nodeId) {
      outputColumns.add(e.downstream_column);
    }
  }

  // Build adjacency: for each (node, column), find upstream edges.
  const adj = new Map<string, PipelineColumnEdgeDto[]>();
  for (const e of edges) {
    const key = `${e.downstream_node ?? ''}:${e.downstream_column}`;
    const list = adj.get(key) ?? [];
    list.push(e);
    adj.set(key, list);
  }

  const traces: ColumnTrace[] = [];
  for (const col of [...outputColumns].sort()) {
    const chain: PipelineColumnEdgeDto[] = [];
    const visited = new Set<string>();
    const queue: Array<{ node: string; column: string }> = [
      { node: nodeId, column: col },
    ];

    while (queue.length > 0) {
      const { node, column } = queue.shift()!;
      const key = `${node}:${column}`;
      if (visited.has(key)) continue;
      visited.add(key);

      const upstreamEdges = adj.get(key) ?? [];
      for (const e of upstreamEdges) {
        chain.push(e);
        if (e.upstream_node) {
          queue.push({ node: e.upstream_node, column: e.upstream_column });
        }
      }
    }

    // Sort chain so deepest upstream comes first (source → transform → node).
    chain.reverse();
    traces.push({ column: col, chain });
  }

  return traces;
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function ColumnLineageModal({
  open,
  pipelineId,
  nodeId,
  nodeName,
  environment,
  onClose,
}: ColumnLineageModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const [edges, setEdges] = useState<PipelineColumnEdgeDto[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedColumn, setExpandedColumn] = useState<string | null>(null);

  // Control dialog open/close.
  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (open && !dialog.open) {
      dialog.showModal();
    } else if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  // Fetch pipeline column lineage when opened.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    setEdges([]);
    setExpandedColumn(null);

    fetchPipelineColumnLineage(pipelineId, environment)
      .then((res) => {
        if (!cancelled) setEdges(res.edges);
      })
      .catch((err) => {
        if (!cancelled) setError((err as Error).message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [open, pipelineId, environment]);

  const traces = useMemo(
    () => buildColumnTraces(nodeId, edges),
    [nodeId, edges],
  );

  const toggleColumn = useCallback((col: string) => {
    setExpandedColumn((prev) => (prev === col ? null : col));
  }, []);

  const exportLineage = useCallback(
    (format: 'json' | 'csv') => {
      // Build export payload scoped to the selected node's traces.
      const exportData = traces.map((t) => ({
        column: t.column,
        steps: t.chain.map((e) => ({
          upstream_node: e.upstream_node,
          upstream_column: e.upstream_column,
          upstream_resource: e.upstream_resource,
          downstream_node: e.downstream_node,
          downstream_column: e.downstream_column,
          downstream_resource: e.downstream_resource,
          relationship: e.relationship,
          expression: e.expression_text,
          confidence: e.confidence,
        })),
      }));

      let blob: Blob;
      let filename: string;
      const ts = new Date().toISOString().replace(/[:.]/g, '-');

      if (format === 'json') {
        const payload = {
          pipeline_id: pipelineId,
          node_id: nodeId,
          node_name: nodeName,
          environment,
          exported_at: new Date().toISOString(),
          columns: exportData,
        };
        blob = new Blob([JSON.stringify(payload, null, 2)], {
          type: 'application/json',
        });
        filename = `column-lineage_${nodeName}_${ts}.json`;
      } else {
        const rows = ['pipeline_id,node,upstream_node,upstream_column,downstream_node,downstream_column,relationship,expression,confidence'];
        for (const t of exportData) {
          for (const s of t.steps) {
            const esc = (v: string | null | undefined) => {
              const str = v ?? '';
              return str.includes(',') || str.includes('"')
                ? `"${str.replace(/"/g, '""')}"`
                : str;
            };
            rows.push(
              [
                esc(pipelineId),
                esc(nodeName),
                esc(s.upstream_node),
                esc(s.upstream_column),
                esc(s.downstream_node),
                esc(s.downstream_column),
                esc(s.relationship),
                esc(s.expression),
                esc(s.confidence),
              ].join(','),
            );
          }
        }
        blob = new Blob([rows.join('\n')], { type: 'text/csv' });
        filename = `column-lineage_${nodeName}_${ts}.csv`;
      }

      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
    },
    [traces, pipelineId, nodeId, nodeName, environment],
  );

  return (
    <dialog ref={dialogRef} className="col-modal" onClose={onClose}>
      <div className="col-modal__header">
        <div>
          <h3 className="col-modal__title">Column Lineage</h3>
          <div className="col-modal__subtitle">{nodeName}</div>
        </div>
        <div className="col-modal__header-actions">
          {traces.length > 0 && (
            <>
              <button
                className="col-modal__export-btn"
                onClick={() => exportLineage('json')}
                title="Export as JSON (for audit trails)"
              >
                Export JSON
              </button>
              <button
                className="col-modal__export-btn"
                onClick={() => exportLineage('csv')}
                title="Export as CSV (for spreadsheets)"
              >
                Export CSV
              </button>
            </>
          )}
          <button className="col-modal__close" onClick={onClose}>
            &times;
          </button>
        </div>
      </div>

      <div className="col-modal__body">
        {loading && (
          <div className="col-modal__loading">Loading column lineage...</div>
        )}
        {error && <div className="col-modal__error">{error}</div>}
        {!loading && !error && traces.length === 0 && (
          <div className="col-modal__empty">
            No column lineage available for this node. Run the pipeline to
            derive column-level lineage from SQL transforms.
          </div>
        )}
        {!loading && !error && traces.length > 0 && (
          <div className="col-modal__columns">
            <div className="col-modal__count">
              {traces.length} column{traces.length !== 1 ? 's' : ''}
            </div>
            {traces.map((trace) => (
              <div key={trace.column} className="col-modal__column-group">
                <button
                  className={`col-modal__column-header ${
                    expandedColumn === trace.column
                      ? 'col-modal__column-header--active'
                      : ''
                  }`}
                  onClick={() => toggleColumn(trace.column)}
                >
                  <span className="col-modal__column-name">
                    {trace.column}
                  </span>
                  <span className="col-modal__column-meta">
                    {trace.chain.length} step{trace.chain.length !== 1 ? 's' : ''}
                  </span>
                  <span className="col-modal__column-chevron">
                    {expandedColumn === trace.column ? '\u25BC' : '\u25B6'}
                  </span>
                </button>

                {expandedColumn === trace.column && (
                  <div className="col-modal__trace">
                    {trace.chain.length === 0 && (
                      <div className="col-modal__trace-empty">
                        No upstream transformations traced.
                      </div>
                    )}
                    {trace.chain.map((edge, i) => (
                      <TraceStep key={i} edge={edge} isLast={i === trace.chain.length - 1} />
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </dialog>
  );
}

// ---------------------------------------------------------------------------
// TraceStep — a single edge in the lineage chain
// ---------------------------------------------------------------------------

function TraceStep({
  edge,
  isLast,
}: {
  edge: PipelineColumnEdgeDto;
  isLast: boolean;
}) {
  const color = RELATIONSHIP_COLORS[edge.relationship];
  const label = RELATIONSHIP_LABELS[edge.relationship];
  const upstreamLabel = edge.upstream_resource
    ? edge.upstream_resource.split('/').filter(Boolean).pop() ?? edge.upstream_resource
    : edge.upstream_node ?? '?';

  return (
    <div className="col-modal__step">
      <div className="col-modal__step-connector">
        <div
          className="col-modal__step-dot"
          style={{ borderColor: color }}
        />
        {!isLast && <div className="col-modal__step-line" />}
      </div>
      <div className="col-modal__step-content">
        <div className="col-modal__step-header">
          <code className="col-modal__step-col">{edge.upstream_column}</code>
          <span className="col-modal__step-arrow">&rarr;</span>
          <code className="col-modal__step-col">{edge.downstream_column}</code>
        </div>
        <div className="col-modal__step-meta">
          <span
            className="col-modal__step-relationship"
            style={{ color }}
          >
            {label}
          </span>
          <span className="col-modal__step-node" title={edge.upstream_node ?? undefined}>
            {upstreamLabel}
          </span>
          {edge.confidence && (
            <ConfidenceBadge level={edge.confidence} />
          )}
        </div>
        {edge.expression_text && (
          <code className="col-modal__step-expr">{edge.expression_text}</code>
        )}
      </div>
    </div>
  );
}
