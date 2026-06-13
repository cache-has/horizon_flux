// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Column-level lineage graph view. Renders a column lineage trace as a
 * ReactFlow graph where each column is a node and lineage edges connect them.
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  type Node,
  type Edge,
  type NodeTypes,
  Position,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import type {
  TraceEdgeDto,
  RelationshipKind,
  ConfidenceLevel,
} from '../../api/columnLineage';
import { fetchColumnUpstream, fetchColumnDownstream } from '../../api/columnLineage';
import { ConfidenceBadge } from './ConfidenceBadge';
import './ConfidenceBadge.css';
import './ColumnLineageGraph.css';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface ColumnLineageGraphProps {
  fingerprint: string;
  column: string;
  direction?: 'upstream' | 'downstream' | 'both';
  environment?: string;
  onClose?: () => void;
  onNavigateToPipeline?: (pipelineId: string) => void;
}

// ---------------------------------------------------------------------------
// Edge colors by relationship kind
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

const ALL_RELATIONSHIPS: RelationshipKind[] = Object.keys(
  RELATIONSHIP_COLORS,
) as RelationshipKind[];

// ---------------------------------------------------------------------------
// Layout helper — position nodes in layers by depth
// ---------------------------------------------------------------------------

interface ColumnNode {
  id: string;
  pipelineId: string;
  nodeId: string;
  column: string;
  confidence?: ConfidenceLevel;
  isRoot: boolean;
  depth: number;
}

function buildGraph(
  rootFingerprint: string,
  rootColumn: string,
  upstreamEdges: TraceEdgeDto[],
  downstreamEdges: TraceEdgeDto[],
  activeRelationships: Set<RelationshipKind>,
): { nodes: Node[]; edges: Edge[] } {
  const columnNodes = new Map<string, ColumnNode>();
  const flowEdges: Edge[] = [];

  // Root node — show short resource name instead of full fingerprint
  const rootId = `root:${rootFingerprint}:${rootColumn}`;
  const rootLabel = rootFingerprint.split('/').filter(Boolean).pop() ?? rootFingerprint;
  columnNodes.set(rootId, {
    id: rootId,
    pipelineId: '',
    nodeId: rootLabel,
    column: rootColumn,
    isRoot: true,
    depth: 0,
  });

  // Process upstream (negative depth)
  for (const edge of upstreamEdges) {
    if (!activeRelationships.has(edge.relationship)) continue;

    const upId =
      `${edge.upstream.pipeline_id}:${edge.upstream.node_id}:${edge.upstream.column}`;
    const downId =
      edge.depth === 1
        ? rootId
        : `${edge.downstream.pipeline_id}:${edge.downstream.node_id}:${edge.downstream.column}`;

    if (!columnNodes.has(upId)) {
      columnNodes.set(upId, {
        id: upId,
        pipelineId: edge.upstream.pipeline_id,
        nodeId: edge.upstream.node_id,
        column: edge.upstream.column,
        confidence: edge.confidence,
        isRoot: false,
        depth: -edge.depth,
      });
    }

    if (!columnNodes.has(downId) && downId !== rootId) {
      columnNodes.set(downId, {
        id: downId,
        pipelineId: edge.downstream.pipeline_id,
        nodeId: edge.downstream.node_id,
        column: edge.downstream.column,
        confidence: edge.confidence,
        isRoot: false,
        depth: -(edge.depth - 1),
      });
    }

    flowEdges.push({
      id: `e-${upId}-${downId}-${edge.relationship}`,
      source: upId,
      target: downId,
      type: 'default',
      animated: true,
      label: RELATIONSHIP_LABELS[edge.relationship],
      style: {
        stroke: RELATIONSHIP_COLORS[edge.relationship],
        strokeWidth: 2,
      },
      labelStyle: {
        fill: RELATIONSHIP_COLORS[edge.relationship],
        fontSize: 10,
      },
      labelBgStyle: {
        fill: '#020617',
        fillOpacity: 0.8,
      },
    });
  }

  // Process downstream (positive depth)
  for (const edge of downstreamEdges) {
    if (!activeRelationships.has(edge.relationship)) continue;

    const downId =
      `${edge.downstream.pipeline_id}:${edge.downstream.node_id}:${edge.downstream.column}`;
    const upId =
      edge.depth === 1
        ? rootId
        : `${edge.upstream.pipeline_id}:${edge.upstream.node_id}:${edge.upstream.column}`;

    if (!columnNodes.has(downId)) {
      columnNodes.set(downId, {
        id: downId,
        pipelineId: edge.downstream.pipeline_id,
        nodeId: edge.downstream.node_id,
        column: edge.downstream.column,
        confidence: edge.confidence,
        isRoot: false,
        depth: edge.depth,
      });
    }

    if (!columnNodes.has(upId) && upId !== rootId) {
      columnNodes.set(upId, {
        id: upId,
        pipelineId: edge.upstream.pipeline_id,
        nodeId: edge.upstream.node_id,
        column: edge.upstream.column,
        confidence: edge.confidence,
        isRoot: false,
        depth: edge.depth - 1,
      });
    }

    flowEdges.push({
      id: `e-${upId}-${downId}-${edge.relationship}`,
      source: upId,
      target: downId,
      type: 'default',
      animated: true,
      label: RELATIONSHIP_LABELS[edge.relationship],
      style: {
        stroke: RELATIONSHIP_COLORS[edge.relationship],
        strokeWidth: 2,
      },
      labelStyle: {
        fill: RELATIONSHIP_COLORS[edge.relationship],
        fontSize: 10,
      },
      labelBgStyle: {
        fill: '#020617',
        fillOpacity: 0.8,
      },
    });
  }

  // Layout: group by depth, stack vertically within each depth
  const byDepth = new Map<number, ColumnNode[]>();
  for (const node of columnNodes.values()) {
    const d = node.depth;
    if (!byDepth.has(d)) byDepth.set(d, []);
    byDepth.get(d)!.push(node);
  }

  const COL_WIDTH = 300;
  const ROW_HEIGHT = 80;
  const depths = [...byDepth.keys()].sort((a, b) => a - b);
  const minDepth = depths[0] ?? 0;

  const nodes: Node[] = [];
  for (const [depth, cols] of byDepth.entries()) {
    const x = (depth - minDepth) * COL_WIDTH;
    cols.forEach((col, i) => {
      nodes.push({
        id: col.id,
        position: { x, y: i * ROW_HEIGHT },
        data: {
          label: col.column,
          nodeId: col.nodeId,
          pipelineId: col.pipelineId,
          confidence: col.confidence,
          isRoot: col.isRoot,
        },
        type: 'columnNode',
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      });
    });
  }

  return { nodes, edges: flowEdges };
}

// ---------------------------------------------------------------------------
// Custom node component
// ---------------------------------------------------------------------------

function ColumnNodeComponent({ data }: { data: Record<string, unknown> }) {
  const label = data.label as string;
  const nodeId = data.nodeId as string;
  const confidence = data.confidence as ConfidenceLevel | undefined;
  const isRoot = data.isRoot as boolean;

  return (
    <div className={`col-lineage-node ${isRoot ? 'col-lineage-node--root' : ''}`}>
      <div className="col-lineage-node__column">{label}</div>
      <div className="col-lineage-node__meta">
        <span className="col-lineage-node__node-id">{nodeId}</span>
        {confidence && <ConfidenceBadge level={confidence} />}
      </div>
    </div>
  );
}

const nodeTypes: NodeTypes = { columnNode: ColumnNodeComponent };

// ---------------------------------------------------------------------------
// Inner component (needs ReactFlowProvider above)
// ---------------------------------------------------------------------------

function ColumnLineageGraphInner({
  fingerprint,
  column,
  direction = 'both',
  environment,
  onClose,
}: ColumnLineageGraphProps) {
  const [upstreamEdges, setUpstreamEdges] = useState<TraceEdgeDto[]>([]);
  const [downstreamEdges, setDownstreamEdges] = useState<TraceEdgeDto[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeRelationships, setActiveRelationships] = useState<Set<RelationshipKind>>(
    () => new Set(ALL_RELATIONSHIPS),
  );

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);

      const fetches: Promise<void>[] = [];
      const traceOpts = environment ? { environment } : undefined;

      if (direction === 'upstream' || direction === 'both') {
        fetches.push(
          fetchColumnUpstream(fingerprint, column, traceOpts).then((r) => {
            if (!cancelled) setUpstreamEdges(r.edges);
          }),
        );
      }

      if (direction === 'downstream' || direction === 'both') {
        fetches.push(
          fetchColumnDownstream(fingerprint, column, traceOpts).then((r) => {
            if (!cancelled) setDownstreamEdges(r.edges);
          }),
        );
      }

      try {
        await Promise.all(fetches);
      } catch (e) {
        if (!cancelled) setError((e as Error).message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [fingerprint, column, direction, environment]);

  const toggleRelationship = useCallback((kind: RelationshipKind) => {
    setActiveRelationships((prev) => {
      const next = new Set(prev);
      if (next.has(kind)) {
        next.delete(kind);
      } else {
        next.add(kind);
      }
      return next;
    });
  }, []);

  const { nodes, edges } = useMemo(
    () =>
      buildGraph(
        fingerprint,
        column,
        upstreamEdges,
        downstreamEdges,
        activeRelationships,
      ),
    [fingerprint, column, upstreamEdges, downstreamEdges, activeRelationships],
  );

  if (loading) {
    return (
      <div className="col-lineage-graph col-lineage-graph--loading">
        Loading column lineage...
      </div>
    );
  }

  if (error) {
    return (
      <div className="col-lineage-graph col-lineage-graph--error">
        Error: {error}
      </div>
    );
  }

  return (
    <div className="col-lineage-graph">
      <div className="col-lineage-graph__toolbar">
        <span className="col-lineage-graph__title">
          Column Lineage: <code>{column}</code>
        </span>
        {onClose && (
          <button className="col-lineage-graph__close" onClick={onClose}>
            &times;
          </button>
        )}
      </div>

      <div className="col-lineage-graph__filters">
        {ALL_RELATIONSHIPS.map((kind) => (
          <label key={kind} className="col-lineage-graph__filter-item">
            <input
              type="checkbox"
              checked={activeRelationships.has(kind)}
              onChange={() => toggleRelationship(kind)}
            />
            <span
              className="col-lineage-graph__filter-dot"
              style={{ background: RELATIONSHIP_COLORS[kind] }}
            />
            {RELATIONSHIP_LABELS[kind]}
          </label>
        ))}
      </div>

      <div className="col-lineage-graph__canvas">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          fitView
          fitViewOptions={{ padding: 0.3 }}
          panOnDrag
          zoomOnScroll
          minZoom={0.2}
          maxZoom={2}
          nodesConnectable={false}
          elementsSelectable
        >
          <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
          <Controls />
          <MiniMap
            nodeStrokeColor="#334155"
            nodeColor="#1e293b"
            maskColor="rgba(0, 0, 0, 0.3)"
            pannable
            zoomable
          />
        </ReactFlow>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Public component with its own ReactFlowProvider
// ---------------------------------------------------------------------------

export function ColumnLineageGraph(props: ColumnLineageGraphProps) {
  return (
    <ReactFlowProvider>
      <ColumnLineageGraphInner {...props} />
    </ReactFlowProvider>
  );
}
