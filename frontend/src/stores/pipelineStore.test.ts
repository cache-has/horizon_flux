// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeEach } from 'vitest';
import { usePipelineStore, buildApiPipeline } from './pipelineStore';
import type { ApiPipelineResponse, ApiPipeline } from '../api/pipelines';
import type { PipelineNode, PipelineEdge } from '../types/pipeline';

const makeResponse = (
  overrides: Partial<ApiPipelineResponse> = {},
): ApiPipelineResponse => ({
  id: 'test-1',
  pipeline: {
    name: 'test-pipeline',
    version: 1,
    default_environment: 'dev',
    variables: {},
    environment_overrides: {},
    nodes: [
      {
        id: 'src',
        name: 'Source',
        type: 'source',
        connector: 'csv',
        config: { path: '/data.csv' },
        position: { x: 100, y: 200 },
        pinned_position: false,
      },
      {
        id: 'tx',
        name: 'Transform',
        type: 'transform',
        mode: 'sql',
        code: 'SELECT *',
        materialized: false,
        position: { x: 400, y: 200 },
        pinned_position: true,
      },
    ],
    edges: [{ from: 'src', to: 'tx' }],
  },
  created_at: 1000,
  updated_at: 2000,
  ...overrides,
});

beforeEach(() => {
  // Reset store between tests
  usePipelineStore.setState({
    pipelineId: null,
    apiPipeline: null,
    nodes: [],
    edges: [],
    dirty: false,
    loading: false,
    error: null,
    simulationHasRun: false,
  });
});

describe('pipelineStore', () => {
  describe('loadFromResponse', () => {
    it('populates nodes and edges from API response', () => {
      const response = makeResponse();
      usePipelineStore.getState().loadFromResponse(response);

      const state = usePipelineStore.getState();
      expect(state.pipelineId).toBe('test-1');
      expect(state.nodes).toHaveLength(2);
      expect(state.edges).toHaveLength(1);
      expect(state.loading).toBe(false);
      expect(state.error).toBeNull();
    });

    it('converts backend node fields to React Flow format', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());

      const nodes = usePipelineStore.getState().nodes;
      const src = nodes.find((n) => n.id === 'src')!;
      expect(src.type).toBe('pipeline');
      expect(src.position).toEqual({ x: 100, y: 200 });
      expect(src.data.label).toBe('Source');
      expect(src.data.role).toBe('source');
      expect(src.data.status).toBe('idle');
      expect(src.data.pinnedPosition).toBe(false);
    });

    it('preserves pinned_position from backend', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());

      const tx = usePipelineStore.getState().nodes.find((n) => n.id === 'tx')!;
      expect(tx.data.pinnedPosition).toBe(true);
    });

    it('converts backend edges to React Flow format', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());

      const edge = usePipelineStore.getState().edges[0];
      expect(edge.id).toBe('e-src-tx');
      expect(edge.source).toBe('src');
      expect(edge.target).toBe('tx');
    });

    it('sets simulationHasRun=true when nodes have non-zero positions', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());
      expect(usePipelineStore.getState().simulationHasRun).toBe(true);
    });

    it('sets simulationHasRun=false when all positions are zero', () => {
      const response = makeResponse();
      response.pipeline.nodes.forEach((n) => {
        n.position = { x: 0, y: 0 };
      });
      usePipelineStore.getState().loadFromResponse(response);
      expect(usePipelineStore.getState().simulationHasRun).toBe(false);
    });
  });

  describe('pinNode / unpinNode / unpinAll', () => {
    beforeEach(() => {
      usePipelineStore.getState().loadFromResponse(makeResponse());
    });

    it('pins a node', () => {
      usePipelineStore.getState().pinNode('src');
      const src = usePipelineStore.getState().nodes.find((n) => n.id === 'src')!;
      expect(src.data.pinnedPosition).toBe(true);
    });

    it('marks dirty when pinning', () => {
      usePipelineStore.getState().pinNode('src');
      expect(usePipelineStore.getState().dirty).toBe(true);
    });

    it('unpins a node', () => {
      usePipelineStore.getState().unpinNode('tx');
      const tx = usePipelineStore.getState().nodes.find((n) => n.id === 'tx')!;
      expect(tx.data.pinnedPosition).toBe(false);
    });

    it('unpins all nodes', () => {
      usePipelineStore.getState().pinNode('src');
      usePipelineStore.getState().unpinAll();
      const nodes = usePipelineStore.getState().nodes;
      expect(nodes.every((n) => !n.data.pinnedPosition)).toBe(true);
    });
  });

  describe('setNodes', () => {
    it('updates nodes with a function updater', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());
      usePipelineStore.getState().setNodes((current) =>
        current.map((n) => ({ ...n, position: { x: 999, y: 999 } })),
      );
      const nodes = usePipelineStore.getState().nodes;
      expect(nodes[0].position).toEqual({ x: 999, y: 999 });
    });

    it('updates nodes with a direct value', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());
      usePipelineStore.getState().setNodes([]);
      expect(usePipelineStore.getState().nodes).toEqual([]);
    });
  });

  describe('markDirty', () => {
    it('sets dirty flag', () => {
      usePipelineStore.getState().loadFromResponse(makeResponse());
      usePipelineStore.getState().markDirty();
      expect(usePipelineStore.getState().dirty).toBe(true);
    });
  });

  describe('markSimulationRun', () => {
    it('sets simulationHasRun flag', () => {
      expect(usePipelineStore.getState().simulationHasRun).toBe(false);
      usePipelineStore.getState().markSimulationRun();
      expect(usePipelineStore.getState().simulationHasRun).toBe(true);
    });
  });
});

describe('buildApiPipeline', () => {
  it('preserves original backend node fields while updating positions', () => {
    const base: ApiPipeline = makeResponse().pipeline;

    const rfNodes: PipelineNode[] = [
      {
        id: 'src',
        type: 'pipeline',
        position: { x: 500, y: 600 },
        data: {
          label: 'Source',
          role: 'source',
          status: 'idle',
          pinnedPosition: true,
          envOverridden: false,
        },
      },
      {
        id: 'tx',
        type: 'pipeline',
        position: { x: 800, y: 300 },
        data: {
          label: 'Transform',
          role: 'transform',
          status: 'idle',
          pinnedPosition: false,
          envOverridden: false,
        },
      },
    ];

    const rfEdges: PipelineEdge[] = [
      { id: 'e-src-tx', source: 'src', target: 'tx' },
    ];

    const result = buildApiPipeline(base, rfNodes, rfEdges);

    // Positions updated
    expect(result.nodes[0].position).toEqual({ x: 500, y: 600 });
    expect(result.nodes[1].position).toEqual({ x: 800, y: 300 });

    // Pinning updated
    expect(result.nodes[0].pinned_position).toBe(true);
    expect(result.nodes[1].pinned_position).toBe(false);

    // Original backend fields preserved
    expect(result.nodes[0].connector).toBe('csv');
    expect(result.nodes[0].config).toEqual({ path: '/data.csv' });
    expect(result.nodes[1].mode).toBe('sql');
    expect(result.nodes[1].code).toBe('SELECT *');

    // Pipeline-level fields preserved
    expect(result.name).toBe('test-pipeline');
    expect(result.version).toBe(1);
  });

  it('converts React Flow edges back to backend format', () => {
    const base = makeResponse().pipeline;
    const rfEdges: PipelineEdge[] = [
      { id: 'e-src-tx', source: 'src', target: 'tx' },
    ];

    const result = buildApiPipeline(base, [], rfEdges);
    expect(result.edges).toEqual([{ from: 'src', to: 'tx' }]);
  });
});
