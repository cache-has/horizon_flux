// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  BaseEdge,
  getBezierPath,
  type Node,
  type Edge,
  type NodeTypes,
  type EdgeTypes,
  type EdgeProps,
  type NodeMouseHandler,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { useForceLayout } from '../../hooks/useForceLayout';
import { fetchLineageGraph, fetchCycles, fetchOrphans } from '../../api/lineage';
import { listPipelines, type ApiPipelineResponse } from '../../api/pipelines';
import { useEnvironmentStore } from '../../stores/environmentStore';
import { ProjectPipelineNode, type ProjectNodeData } from './ProjectPipelineNode';
import type { LineageEdgeDto } from '../../api/lineage';
import './ProjectLineageView.css';

// ---------------------------------------------------------------------------
// Custom edge showing resource name
// ---------------------------------------------------------------------------

interface LineageEdgeData extends Record<string, unknown> {
  resources: string[];
  observed: boolean;
}

function LineageEdgeComponent(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data } = props;
  const edgeData = data as LineageEdgeData | undefined;
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });

  const label = edgeData?.resources?.[0] ?? '';
  const shortLabel = label.length > 40 ? '...' + label.slice(-37) : label;

  return (
    <>
      <BaseEdge
        path={edgePath}
        style={{
          stroke: edgeData?.observed ? '#d97706' : '#94a3b8',
          strokeWidth: 2,
          strokeDasharray: edgeData?.observed ? '6 4' : undefined,
        }}
      />
      {shortLabel && (
        <foreignObject
          x={labelX - 100}
          y={labelY - 12}
          width={200}
          height={24}
          requiredExtensions="http://www.w3.org/1999/xhtml"
        >
          <div className="project-lineage__edge-label" title={label}>
            {shortLabel}
          </div>
        </foreignObject>
      )}
    </>
  );
}

const nodeTypes: NodeTypes = {
  projectPipeline: ProjectPipelineNode,
};

const edgeTypes: EdgeTypes = {
  lineage: LineageEdgeComponent,
};

// ---------------------------------------------------------------------------
// Inner component (needs ReactFlowProvider above)
// ---------------------------------------------------------------------------

function ProjectLineageViewInner({
  onBack,
  onNavigateToPipeline,
}: {
  onBack: () => void;
  onNavigateToPipeline: (id: string) => void;
}) {
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);

  const [rfNodes, setRfNodes] = useState<Node<ProjectNodeData>[]>([]);
  const [rfEdges, setRfEdges] = useState<Edge<LineageEdgeData>[]>([]);
  const [loading, setLoading] = useState(true);
  const [cycleCount, setCycleCount] = useState(0);
  const [orphanCount, setOrphanCount] = useState(0);

  // Fetch graph data
  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      try {
        const [graph, pipelinesRes, cyclesRes, orphansRes] = await Promise.all([
          fetchLineageGraph(activeEnvironment),
          listPipelines(1000, 0),
          fetchCycles(activeEnvironment),
          fetchOrphans(activeEnvironment),
        ]);
        if (cancelled) return;

        const pipelineMap = new Map<string, ApiPipelineResponse>();
        for (const p of pipelinesRes.data) {
          pipelineMap.set(p.id, p);
        }

        // Build nodes — include all pipelines that appear in lineage OR exist
        const pipelineIds = new Set<string>(graph.pipelines);
        for (const p of pipelinesRes.data) {
          pipelineIds.add(p.id);
        }

        const nodes: Node<ProjectNodeData>[] = Array.from(pipelineIds).map(
          (id, i) => {
            const p = pipelineMap.get(id);
            return {
              id,
              type: 'projectPipeline',
              position: { x: (i % 5) * 300, y: Math.floor(i / 5) * 150 },
              data: {
                label: p?.pipeline.name ?? id,
                nodeCount: p?.pipeline.nodes.length ?? 0,
                pinnedPosition: false,
              },
            };
          },
        );

        // Build edges — deduplicate by upstream+downstream pair, collect resources
        const edgeMap = new Map<string, { dto: LineageEdgeDto; resources: Set<string>; observed: boolean }>();
        for (const e of graph.edges) {
          const key = `${e.upstream_pipeline_id}->${e.downstream_pipeline_id}`;
          const existing = edgeMap.get(key);
          if (existing) {
            existing.resources.add(e.resource);
            if (e.source === 'observed') existing.observed = true;
          } else {
            edgeMap.set(key, {
              dto: e,
              resources: new Set([e.resource]),
              observed: e.source === 'observed',
            });
          }
        }

        const edges: Edge<LineageEdgeData>[] = Array.from(edgeMap.entries()).map(
          ([key, { dto, resources, observed }]) => ({
            id: key,
            source: dto.upstream_pipeline_id,
            target: dto.downstream_pipeline_id,
            type: 'lineage',
            data: {
              resources: Array.from(resources),
              observed,
            },
          }),
        );

        setRfNodes(nodes);
        setRfEdges(edges);
        setCycleCount(cyclesRes.cycles.length);
        setOrphanCount(
          orphansRes.dangling_sources.length + orphansRes.orphaned_sinks.length,
        );
      } catch {
        // Data unavailable
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [activeEnvironment]);

  // Force layout — ProjectNodeData satisfies ForceLayoutNodeData
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const setNodesForLayout = setRfNodes as any;

  useForceLayout(
    rfNodes,
    rfEdges,
    setNodesForLayout,
    { enabled: rfNodes.length > 0 },
  );

  // Double-click pipeline node → navigate to that pipeline
  const handleNodeDoubleClick: NodeMouseHandler = useCallback(
    (_event, node) => {
      onNavigateToPipeline(node.id);
    },
    [onNavigateToPipeline],
  );

  const hasData = rfNodes.length > 0;

  return (
    <div className="project-lineage">
      <div className="project-lineage__toolbar">
        <button className="project-lineage__back-btn" onClick={onBack}>
          Back
        </button>
        <span className="project-lineage__title">Project Lineage</span>
      </div>

      {!loading && !hasData && (
        <div className="project-lineage__empty">
          <div className="project-lineage__empty-title">No pipelines found</div>
          <div className="project-lineage__empty-hint">
            Create pipelines to see their cross-pipeline lineage here.
          </div>
        </div>
      )}

      {hasData && (
        <ReactFlow
          nodes={rfNodes}
          edges={rfEdges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodeDoubleClick={handleNodeDoubleClick}
          fitView
          fitViewOptions={{ padding: 0.3 }}
          panOnDrag
          zoomOnScroll
          minZoom={0.2}
          maxZoom={2}
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
        >
          <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
          <Controls />
          <MiniMap
            nodeColor={() => '#0891b2'}
            maskColor="rgba(0, 0, 0, 0.15)"
            pannable
            zoomable
          />
        </ReactFlow>
      )}

      {hasData && (cycleCount > 0 || orphanCount > 0) && (
        <div className="project-lineage__info">
          {cycleCount > 0 && (
            <span className="project-lineage__badge project-lineage__badge--warn">
              {cycleCount} cycle{cycleCount !== 1 ? 's' : ''} detected
            </span>
          )}
          {orphanCount > 0 && (
            <span className="project-lineage__badge project-lineage__badge--info">
              {orphanCount} orphan{orphanCount !== 1 ? 's' : ''}
            </span>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Public component with its own ReactFlowProvider
// ---------------------------------------------------------------------------

export function ProjectLineageView({
  onBack,
  onNavigateToPipeline,
}: {
  onBack: () => void;
  onNavigateToPipeline: (id: string) => void;
}) {
  return (
    <ReactFlowProvider>
      <ProjectLineageViewInner
        onBack={onBack}
        onNavigateToPipeline={onNavigateToPipeline}
      />
    </ReactFlowProvider>
  );
}
