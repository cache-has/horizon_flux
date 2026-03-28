// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import {
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
  type Connection,
  type NodeChange,
  type EdgeChange,
} from '@xyflow/react';

import type { PipelineNode, PipelineEdge } from '../types/pipeline';
import type {
  ApiPipeline,
  ApiPipelineResponse,
  ApiNode,
  ApiEdge,
} from '../api/pipelines';
import {
  fetchPipeline,
  updatePipeline,
} from '../api/pipelines';

// ---------------------------------------------------------------------------
// Conversion helpers (backend <-> React Flow)
// ---------------------------------------------------------------------------

/** Map a backend node role string to our NodeRole type. */
function apiNodeToReactFlow(apiNode: ApiNode): PipelineNode {
  return {
    id: apiNode.id,
    type: 'pipeline',
    position: { x: apiNode.position.x, y: apiNode.position.y },
    data: {
      label: apiNode.name,
      role: apiNode.type,
      status: 'idle',
      pinnedPosition: apiNode.pinned_position,
      envOverridden: false,
    },
  };
}

/** Map a backend edge to a React Flow edge. */
function apiEdgeToReactFlow(apiEdge: ApiEdge): PipelineEdge {
  return {
    id: `e-${apiEdge.from}-${apiEdge.to}`,
    source: apiEdge.from,
    target: apiEdge.to,
    type: 'pipeline',
  };
}

/**
 * Build an updated ApiPipeline from the current React Flow nodes/edges,
 * preserving the original backend node configs (connector, code, etc.).
 */
export function buildApiPipeline(
  basePipeline: ApiPipeline,
  nodes: PipelineNode[],
  edges: PipelineEdge[],
): ApiPipeline {
  const originalNodeMap = new Map(basePipeline.nodes.map((n) => [n.id, n]));

  const apiNodes: ApiNode[] = nodes.map((rfNode) => {
    const orig = originalNodeMap.get(rfNode.id);
    return {
      // Spread original backend fields (connector, config, mode, code, etc.)
      ...(orig ?? {
        id: rfNode.id,
        name: rfNode.data.label,
        type: rfNode.data.role,
      }),
      // Overwrite position and pinning from current React Flow state
      id: rfNode.id,
      name: rfNode.data.label,
      type: rfNode.data.role,
      position: { x: rfNode.position.x, y: rfNode.position.y },
      pinned_position: rfNode.data.pinnedPosition,
    } as ApiNode;
  });

  const apiEdges: ApiEdge[] = edges.map((rfEdge) => ({
    from: rfEdge.source,
    to: rfEdge.target,
  }));

  return {
    ...basePipeline,
    nodes: apiNodes,
    edges: apiEdges,
  };
}

// ---------------------------------------------------------------------------
// Store types
// ---------------------------------------------------------------------------

export interface PipelineStoreState {
  /** The ID of the currently loaded pipeline (null if none). */
  pipelineId: string | null;
  /** The raw backend pipeline definition (used to preserve node configs on save). */
  apiPipeline: ApiPipeline | null;
  /** React Flow nodes. */
  nodes: PipelineNode[];
  /** React Flow edges. */
  edges: PipelineEdge[];
  /** Whether the pipeline has unsaved position changes. */
  dirty: boolean;
  /** Whether an API operation is in flight. */
  loading: boolean;
  /** Last error message from API operations. */
  error: string | null;
  /** Whether the initial simulation has run for the current pipeline. */
  simulationHasRun: boolean;
}

export interface PipelineStoreActions {
  /** Load a pipeline from the backend by ID. */
  loadPipeline: (id: string) => Promise<void>;
  /** Load a pipeline from an already-fetched API response (avoids duplicate fetch). */
  loadFromResponse: (response: ApiPipelineResponse) => void;
  /** Apply React Flow node changes (drag, select, remove, etc.). */
  onNodesChange: (changes: NodeChange<PipelineNode>[]) => void;
  /** Apply React Flow edge changes (select, remove, etc.). */
  onEdgesChange: (changes: EdgeChange<PipelineEdge>[]) => void;
  /** Handle a new connection. */
  onConnect: (connection: Connection) => void;
  /** Update nodes directly (used by force layout). */
  setNodes: (updater: PipelineNode[] | ((current: PipelineNode[]) => PipelineNode[])) => void;
  /** Pin a node's position (called after manual drag). */
  pinNode: (nodeId: string) => void;
  /** Unpin a node's position. */
  unpinNode: (nodeId: string) => void;
  /** Unpin all nodes. */
  unpinAll: () => void;
  /** Mark positions as needing save (called when simulation settles or drag ends). */
  markDirty: () => void;
  /** Save current positions to the backend (debounced externally). */
  savePositions: () => Promise<void>;
  /** Record that the initial simulation has run. */
  markSimulationRun: () => void;
}

export type PipelineStore = PipelineStoreState & PipelineStoreActions;

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/** Debounce timer for auto-saving positions. */
let saveTimer: ReturnType<typeof setTimeout> | null = null;
const SAVE_DEBOUNCE_MS = 1000;

function debouncedSave(saveFn: () => Promise<void>) {
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    saveFn().catch((err) => console.error('Auto-save positions failed:', err));
  }, SAVE_DEBOUNCE_MS);
}

export const usePipelineStore = create<PipelineStore>((set, get) => ({
  // State
  pipelineId: null,
  apiPipeline: null,
  nodes: [],
  edges: [],
  dirty: false,
  loading: false,
  error: null,
  simulationHasRun: false,

  // Actions
  loadPipeline: async (id: string) => {
    set({ loading: true, error: null });
    try {
      const response = await fetchPipeline(id);
      get().loadFromResponse(response);
    } catch (err) {
      set({ loading: false, error: (err as Error).message });
    }
  },

  loadFromResponse: (response: ApiPipelineResponse) => {
    const { pipeline } = response;
    const nodes = pipeline.nodes.map(apiNodeToReactFlow);
    const edges = pipeline.edges.map(apiEdgeToReactFlow);

    // Determine if all nodes have non-zero positions (i.e., previously saved)
    const hasPositions = pipeline.nodes.some(
      (n) => n.position.x !== 0 || n.position.y !== 0,
    );

    set({
      pipelineId: response.id,
      apiPipeline: pipeline,
      nodes,
      edges,
      dirty: false,
      loading: false,
      error: null,
      // If nodes already have saved positions, skip initial simulation
      simulationHasRun: hasPositions,
    });
  },

  onNodesChange: (changes: NodeChange<PipelineNode>[]) => {
    set((state) => ({
      nodes: applyNodeChanges(changes, state.nodes),
    }));
  },

  onEdgesChange: (changes: EdgeChange<PipelineEdge>[]) => {
    set((state) => ({
      edges: applyEdgeChanges(changes, state.edges),
    }));
  },

  onConnect: (connection: Connection) => {
    set((state) => ({
      edges: addEdge({ ...connection, type: 'pipeline' }, state.edges),
    }));
  },

  setNodes: (updater) => {
    set((state) => ({
      nodes: typeof updater === 'function' ? updater(state.nodes) : updater,
    }));
  },

  pinNode: (nodeId: string) => {
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, pinnedPosition: true } }
          : n,
      ),
      dirty: true,
    }));
    debouncedSave(get().savePositions);
  },

  unpinNode: (nodeId: string) => {
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === nodeId
          ? { ...n, data: { ...n.data, pinnedPosition: false } }
          : n,
      ),
    }));
  },

  unpinAll: () => {
    set((state) => ({
      nodes: state.nodes.map((n) => ({
        ...n,
        data: { ...n.data, pinnedPosition: false },
      })),
    }));
  },

  markDirty: () => {
    set({ dirty: true });
    debouncedSave(get().savePositions);
  },

  savePositions: async () => {
    const { pipelineId, apiPipeline, nodes, edges, dirty } = get();
    if (!pipelineId || !apiPipeline || !dirty) return;

    try {
      const updated = buildApiPipeline(apiPipeline, nodes, edges);
      const response = await updatePipeline(pipelineId, updated);
      set({
        apiPipeline: response.pipeline,
        dirty: false,
      });
    } catch (err) {
      console.error('Failed to save positions:', err);
      set({ error: (err as Error).message });
    }
  },

  markSimulationRun: () => {
    set({ simulationHasRun: true });
  },
}));
