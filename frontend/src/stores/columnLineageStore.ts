// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import type {
  PipelineColumnEdgeDto,
  ColumnTraceResponse,
  ColumnImpactResponse,
  ColumnTraceOptions,
  RelationshipKind,
  ConfidenceLevel,
} from '../api/columnLineage';
import * as api from '../api/columnLineage';

// ---------------------------------------------------------------------------
// Highlighted column state — drives inline canvas highlighting
// ---------------------------------------------------------------------------

export interface HighlightedColumn {
  nodeId: string;
  column: string;
  relationship: RelationshipKind;
  confidence: ConfidenceLevel;
  expression?: string | null;
}

interface ColumnLineageState {
  /** Column edges keyed by pipeline ID. */
  edgesByPipeline: Record<string, PipelineColumnEdgeDto[]>;
  /** Loading state per pipeline. */
  loadingPipelines: Set<string>;
  /** Currently highlighted columns on the canvas (upstream of hovered column). */
  highlightedColumns: HighlightedColumn[];
  /** The column currently being hovered (source of highlight). */
  highlightSource: { nodeId: string; column: string } | null;
}

interface ColumnLineageActions {
  /** Fetch column edges for a pipeline and cache them. */
  fetchPipelineEdges(pipelineId: string, environment?: string): Promise<void>;
  /** Invalidate cached edges for a pipeline (e.g. after WebSocket event). */
  invalidatePipeline(pipelineId: string): void;
  /** Set highlighted columns when hovering a column in the preview table. */
  setHighlight(nodeId: string, column: string): void;
  /** Clear all column highlights. */
  clearHighlight(): void;
  /** Fetch upstream trace for a column by resource fingerprint. */
  fetchUpstream(
    fingerprint: string,
    column: string,
    opts?: ColumnTraceOptions,
  ): Promise<ColumnTraceResponse>;
  /** Fetch downstream trace. */
  fetchDownstream(
    fingerprint: string,
    column: string,
    opts?: ColumnTraceOptions,
  ): Promise<ColumnTraceResponse>;
  /** Fetch impact analysis. */
  fetchImpact(
    fingerprint: string,
    column: string,
    opts?: ColumnTraceOptions,
  ): Promise<ColumnImpactResponse>;
  /** Handle WebSocket column_lineage_updated event. */
  handleLineageUpdated(pipelineId: string): void;
}

export const useColumnLineageStore = create<ColumnLineageState & ColumnLineageActions>(
  (set, get) => ({
    edgesByPipeline: {},
    loadingPipelines: new Set(),
    highlightedColumns: [],
    highlightSource: null,

    async fetchPipelineEdges(pipelineId, environment?) {
      const { loadingPipelines } = get();
      if (loadingPipelines.has(pipelineId)) return;
      set({ loadingPipelines: new Set([...loadingPipelines, pipelineId]) });
      try {
        const resp = await api.fetchPipelineColumnLineage(pipelineId, environment);
        set((s) => ({
          edgesByPipeline: { ...s.edgesByPipeline, [pipelineId]: resp.edges },
          loadingPipelines: new Set([...s.loadingPipelines].filter((id) => id !== pipelineId)),
        }));
      } catch {
        set((s) => ({
          loadingPipelines: new Set([...s.loadingPipelines].filter((id) => id !== pipelineId)),
        }));
      }
    },

    invalidatePipeline(pipelineId) {
      set((s) => {
        const next = { ...s.edgesByPipeline };
        delete next[pipelineId];
        return { edgesByPipeline: next };
      });
    },

    setHighlight(nodeId, column) {
      const { edgesByPipeline } = get();

      // Walk backward through edges to find all upstream columns
      const highlighted: HighlightedColumn[] = [];
      const visited = new Set<string>();
      const queue: Array<{ nodeId: string; column: string }> = [{ nodeId, column }];

      // Collect all edges from all pipelines (in practice, canvas shows one pipeline)
      const allEdges = Object.values(edgesByPipeline).flat();

      while (queue.length > 0) {
        const current = queue.shift()!;
        const key = `${current.nodeId}:${current.column}`;
        if (visited.has(key)) continue;
        visited.add(key);

        // Find edges where this column is downstream
        for (const edge of allEdges) {
          if (
            edge.downstream_node === current.nodeId &&
            edge.downstream_column === current.column &&
            edge.upstream_node
          ) {
            const upKey = `${edge.upstream_node}:${edge.upstream_column}`;
            if (!visited.has(upKey)) {
              highlighted.push({
                nodeId: edge.upstream_node,
                column: edge.upstream_column,
                relationship: edge.relationship,
                confidence: edge.confidence,
                expression: edge.expression_text,
              });
              queue.push({ nodeId: edge.upstream_node, column: edge.upstream_column });
            }
          }
        }
      }

      set({ highlightedColumns: highlighted, highlightSource: { nodeId, column } });
    },

    clearHighlight() {
      set({ highlightedColumns: [], highlightSource: null });
    },

    async fetchUpstream(fingerprint, column, opts?) {
      return api.fetchColumnUpstream(fingerprint, column, opts);
    },

    async fetchDownstream(fingerprint, column, opts?) {
      return api.fetchColumnDownstream(fingerprint, column, opts);
    },

    async fetchImpact(fingerprint, column, opts?) {
      return api.fetchColumnImpact(fingerprint, column, opts);
    },

    handleLineageUpdated(pipelineId) {
      // Invalidate the cache so next render re-fetches
      get().invalidatePipeline(pipelineId);
    },
  }),
);
