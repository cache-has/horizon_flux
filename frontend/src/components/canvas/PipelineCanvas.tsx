// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback } from 'react';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  type OnConnect,
  type OnNodesChange,
  type OnEdgesChange,
  type NodeTypes,
  addEdge,
  useNodesState,
  useEdgesState,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import type { PipelineNode, PipelineEdge } from '../../types/pipeline';
import { PipelineNodeComponent } from './PipelineNode';
import { useForceLayout } from '../../hooks/useForceLayout';
import './PipelineCanvas.css';

const nodeTypes: NodeTypes = {
  pipeline: PipelineNodeComponent,
};

/** Default edge options: animated Bezier curves. */
const defaultEdgeOptions = {
  type: 'default',
  animated: false,
};

// Demo nodes for initial canvas — will be replaced by backend data
const initialNodes: PipelineNode[] = [
  {
    id: 'source-1',
    type: 'pipeline',
    position: { x: 100, y: 200 },
    data: {
      label: 'CSV Import',
      role: 'source',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: false,
    },
  },
  {
    id: 'transform-1',
    type: 'pipeline',
    position: { x: 400, y: 200 },
    data: {
      label: 'Filter Rows',
      role: 'transform',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: false,
    },
  },
  {
    id: 'sink-1',
    type: 'pipeline',
    position: { x: 700, y: 200 },
    data: {
      label: 'PostgreSQL',
      role: 'sink',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: false,
    },
  },
];

const initialEdges: PipelineEdge[] = [
  { id: 'e-source-1-transform-1', source: 'source-1', target: 'transform-1' },
  { id: 'e-transform-1-sink-1', source: 'transform-1', target: 'sink-1' },
];

function PipelineCanvasInner() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState<PipelineNode>(initialNodes);
  const [edges, setEdges, onEdgesChange] =
    useEdgesState<PipelineEdge>(initialEdges);

  useForceLayout(nodes, edges, setNodes);

  const onConnect: OnConnect = useCallback(
    (connection) => {
      setEdges((eds: PipelineEdge[]) => addEdge(connection, eds));
    },
    [setEdges],
  );

  return (
    <div className="pipeline-canvas">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange as OnNodesChange}
        onEdgesChange={onEdgesChange as OnEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        fitView
        fitViewOptions={{ padding: 0.3 }}
        selectionOnDrag
        panOnDrag={[1, 2]}
        selectNodesOnDrag={false}
        selectionMode={1}
        deleteKeyCode="Delete"
        multiSelectionKeyCode="Shift"
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
        <Controls />
        <MiniMap
          nodeColor={(node) => {
            const data = node.data as PipelineNode['data'];
            switch (data.role) {
              case 'source':
                return '#3b82f6';
              case 'transform':
                return '#a855f7';
              case 'sink':
                return '#22c55e';
              default:
                return '#6b7280';
            }
          }}
          maskColor="rgba(0, 0, 0, 0.15)"
          pannable
          zoomable
        />
      </ReactFlow>
    </div>
  );
}

/** PipelineCanvas wraps the inner component with ReactFlowProvider. */
export function PipelineCanvas() {
  return (
    <ReactFlowProvider>
      <PipelineCanvasInner />
    </ReactFlowProvider>
  );
}
