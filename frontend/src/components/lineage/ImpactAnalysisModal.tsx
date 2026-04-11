// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Impact analysis modal — shows what breaks downstream if a column is
 * renamed or dropped. Fetches data from the column impact API and groups
 * results by pipeline.
 */

import { useCallback, useEffect, useState } from 'react';
import type {
  ColumnImpactResponse,
  TraceEdgeDto,
  RelationshipKind,
} from '../../api/columnLineage';
import { fetchColumnImpact } from '../../api/columnLineage';
import { ConfidenceBadge } from './ConfidenceBadge';
import './ConfidenceBadge.css';
import './ImpactAnalysisModal.css';

interface ImpactAnalysisModalProps {
  fingerprint: string;
  column: string;
  /** Close the modal. */
  onClose: () => void;
  /** Navigate to a pipeline on the canvas. */
  onNavigateToPipeline?: (pipelineId: string) => void;
}

interface GroupedImpact {
  pipelineId: string;
  edges: TraceEdgeDto[];
}

/** Group edges by the downstream pipeline they affect. */
function groupByPipeline(edges: TraceEdgeDto[]): GroupedImpact[] {
  const map = new Map<string, TraceEdgeDto[]>();
  for (const edge of edges) {
    const pid = edge.downstream.pipeline_id;
    if (!map.has(pid)) map.set(pid, []);
    map.get(pid)!.push(edge);
  }
  return Array.from(map.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([pipelineId, edges]) => ({ pipelineId, edges }));
}

/** Human-readable label for a relationship kind. */
function relationshipLabel(kind: RelationshipKind): string {
  const labels: Record<RelationshipKind, string> = {
    direct: 'Direct',
    derived: 'Derived',
    cast: 'Cast',
    filter: 'Filter',
    join_key: 'Join Key',
    join_passthrough: 'Join Passthrough',
    group_by: 'Group By',
    aggregate_input: 'Aggregate Input',
    window_partition: 'Window Partition',
    window_order: 'Window Order',
    window_input: 'Window Input',
    opaque: 'Opaque',
  };
  return labels[kind] ?? kind;
}

export function ImpactAnalysisModal({
  fingerprint,
  column,
  onClose,
  onNavigateToPipeline,
}: ImpactAnalysisModalProps) {
  const [data, setData] = useState<ColumnImpactResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchColumnImpact(fingerprint, column)
      .then((resp) => {
        if (!cancelled) {
          setData(resp);
          setLoading(false);
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setError((e as Error).message);
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [fingerprint, column]);

  // Close on Escape
  useEffect(() => {
    const handle = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handle);
    return () => document.removeEventListener('keydown', handle);
  }, [onClose]);

  const handleBackdropClick = useCallback(
    (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) onClose();
    },
    [onClose],
  );

  const grouped = data ? groupByPipeline(data.affected_columns) : [];

  return (
    <div className="impact-modal__backdrop" onClick={handleBackdropClick}>
      <div className="impact-modal">
        <div className="impact-modal__header">
          <h2 className="impact-modal__title">
            Impact Analysis: <code>{column}</code>
          </h2>
          <button className="impact-modal__close" onClick={onClose}>
            &times;
          </button>
        </div>

        <div className="impact-modal__subheader">
          What breaks if <code>{column}</code> on <code>{fingerprint}</code> is
          renamed or dropped?
        </div>

        <div className="impact-modal__body">
          {loading && (
            <div className="impact-modal__loading">Analyzing downstream impact...</div>
          )}

          {error && <div className="impact-modal__error">{error}</div>}

          {data && !loading && grouped.length === 0 && (
            <div className="impact-modal__empty">
              No downstream dependencies found. This column can be safely modified.
            </div>
          )}

          {data && !loading && grouped.length > 0 && (
            <>
              <div className="impact-modal__summary">
                {data.affected_columns.length} affected column(s) across{' '}
                {data.affected_pipelines.length} pipeline(s)
                {data.truncated && (
                  <span className="impact-modal__truncated">
                    {' '}(results truncated — increase max_depth for full trace)
                  </span>
                )}
              </div>

              {grouped.map((group) => (
                <div key={group.pipelineId} className="impact-modal__group">
                  <div className="impact-modal__group-header">
                    <span
                      className="impact-modal__pipeline-link"
                      onClick={() => onNavigateToPipeline?.(group.pipelineId)}
                    >
                      {group.pipelineId}
                    </span>
                    <span className="impact-modal__group-count">
                      {group.edges.length} column(s)
                    </span>
                  </div>
                  <table className="impact-modal__table">
                    <thead>
                      <tr>
                        <th>Column</th>
                        <th>Node</th>
                        <th>Relationship</th>
                        <th>Confidence</th>
                        <th>Depth</th>
                      </tr>
                    </thead>
                    <tbody>
                      {group.edges.map((edge, i) => (
                        <tr key={i}>
                          <td>
                            <code>{edge.downstream.column}</code>
                          </td>
                          <td>{edge.downstream.node_id}</td>
                          <td>{relationshipLabel(edge.relationship)}</td>
                          <td>
                            <ConfidenceBadge level={edge.confidence} />
                          </td>
                          <td>{edge.depth}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ))}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
