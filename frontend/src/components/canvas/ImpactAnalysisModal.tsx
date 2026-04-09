// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import {
  fetchImpact,
  type ImpactAnalysisResponse,
  type LineageEdgeDto,
} from '../../api/lineage';
import { listPipelines } from '../../api/pipelines';
import './ImpactAnalysisModal.css';

export interface ImpactAnalysisModalProps {
  open: boolean;
  pipelineId: string;
  environment: string;
  onClose: () => void;
  onNavigate?: (pipelineId: string) => void;
}

export function ImpactAnalysisModal({
  open,
  pipelineId,
  environment,
  onClose,
  onNavigate,
}: ImpactAnalysisModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const [impact, setImpact] = useState<ImpactAnalysisResponse | null>(null);
  const [names, setNames] = useState<Map<string, string>>(new Map());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Open / close the dialog element
  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (open && !dialog.open) {
      dialog.showModal();
    } else if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  // Fetch impact data when opened
  useEffect(() => {
    if (!open) return;
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError(null);
      setImpact(null);
      try {
        const [impactRes, pipelinesRes] = await Promise.all([
          fetchImpact(pipelineId, environment),
          listPipelines(1000, 0),
        ]);
        if (cancelled) return;
        setImpact(impactRes);
        const nameMap = new Map<string, string>();
        for (const p of pipelinesRes.data) {
          nameMap.set(p.id, p.pipeline.name);
        }
        setNames(nameMap);
      } catch (err) {
        if (!cancelled) setError((err as Error).message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [open, pipelineId, environment]);

  const handleNavigate = useCallback(
    (targetId: string) => {
      onClose();
      onNavigate?.(targetId);
    },
    [onClose, onNavigate],
  );

  // Group direct edges by downstream pipeline
  const edgesByPipeline = new Map<string, LineageEdgeDto[]>();
  if (impact) {
    for (const edge of impact.direct_edges) {
      const existing = edgesByPipeline.get(edge.downstream_pipeline_id) ?? [];
      existing.push(edge);
      edgesByPipeline.set(edge.downstream_pipeline_id, existing);
    }
  }

  return (
    <dialog ref={dialogRef} className="impact-modal" onClose={onClose}>
      <div className="impact-modal__header">
        <h3 className="impact-modal__title">Impact Analysis</h3>
        <button className="impact-modal__close" onClick={onClose}>
          &times;
        </button>
      </div>

      <div className="impact-modal__body">
        {loading && (
          <div className="impact-modal__loading">Analyzing impact...</div>
        )}
        {error && <div className="impact-modal__error">{error}</div>}
        {!loading && !error && impact && impact.affected_pipelines.length === 0 && (
          <div className="impact-modal__empty">
            No downstream pipelines would be affected by changes to this
            pipeline's sinks.
          </div>
        )}
        {!loading && !error && impact && impact.affected_pipelines.length > 0 && (
          <>
            <div className="impact-modal__count">
              {impact.affected_pipelines.length} pipeline
              {impact.affected_pipelines.length !== 1 ? 's' : ''} affected
            </div>
            <ul className="impact-modal__list">
              {impact.affected_pipelines.map((id) => {
                const edges = edgesByPipeline.get(id);
                const resource = edges?.[0]?.resource;
                return (
                  <li key={id} className="impact-modal__item">
                    <div>
                      <div className="impact-modal__item-name">
                        {names.get(id) ?? id}
                      </div>
                      {resource && (
                        <div
                          className="impact-modal__item-resource"
                          title={resource}
                        >
                          via {resource}
                        </div>
                      )}
                    </div>
                    {onNavigate && (
                      <button
                        className="impact-modal__item-nav"
                        onClick={() => handleNavigate(id)}
                      >
                        Open
                      </button>
                    )}
                  </li>
                );
              })}
            </ul>
          </>
        )}
      </div>
    </dialog>
  );
}
