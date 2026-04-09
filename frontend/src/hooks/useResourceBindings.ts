// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect, useRef } from 'react';
import { fetchLineageGraph } from '../api/lineage';
import { usePipelineStore } from '../stores/pipelineStore';
import { useEnvironmentStore } from '../stores/environmentStore';

/**
 * Fetches lineage bindings for the current pipeline and injects
 * `resourceFingerprint` into source/sink node data. Re-fetches when
 * the pipeline ID or active environment changes.
 */
export function useResourceBindings(): void {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);
  const lastRunCompletedAt = usePipelineStore((s) => s.lastRunCompletedAt);
  const prevKey = useRef<string>('');

  useEffect(() => {
    if (!pipelineId || !activeEnvironment) return;

    const key = `${pipelineId}:${activeEnvironment}:${lastRunCompletedAt}`;
    if (key === prevKey.current) return;
    prevKey.current = key;

    let cancelled = false;

    fetchLineageGraph(activeEnvironment)
      .then((graph) => {
        if (cancelled) return;

        // Build nodeId → fingerprint map for the current pipeline
        const nodeFingerprints = new Map<string, string>();
        for (const binding of graph.bindings) {
          if (binding.pipeline_id === pipelineId) {
            nodeFingerprints.set(binding.node_id, binding.resource);
          }
        }

        if (nodeFingerprints.size === 0) return;

        const store = usePipelineStore.getState();
        const needsUpdate = store.nodes.some(
          (n) => n.data.resourceFingerprint !== nodeFingerprints.get(n.id),
        );
        if (!needsUpdate) return;

        store.setNodes((current) =>
          current.map((n) => {
            const fp = nodeFingerprints.get(n.id);
            if (fp === n.data.resourceFingerprint) return n;
            return {
              ...n,
              data: { ...n.data, resourceFingerprint: fp },
            };
          }),
        );
      })
      .catch(() => {
        // Lineage fetch failure is non-fatal — badges just won't appear.
      });

    return () => {
      cancelled = true;
    };
  }, [pipelineId, activeEnvironment, lastRunCompletedAt]);
}
