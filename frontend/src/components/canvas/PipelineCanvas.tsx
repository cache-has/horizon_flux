// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useState } from 'react';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  Panel,
  type OnConnect,
  type OnNodesChange,
  type OnEdgesChange,
  type NodeTypes,
  type EdgeTypes,
  type Node,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import type { PipelineNode } from '../../types/pipeline';
import { PipelineNodeComponent } from './PipelineNode';
import { PipelineEdgeComponent, EdgeMarkerDefs } from './PipelineEdge';
import { useForceLayout } from '../../hooks/useForceLayout';
import { usePipelineStore } from '../../stores/pipelineStore';
import './PipelineCanvas.css';

const nodeTypes: NodeTypes = {
  pipeline: PipelineNodeComponent,
};

const edgeTypes: EdgeTypes = {
  pipeline: PipelineEdgeComponent,
};

/** Default edge options: use our custom pipeline edge. */
const defaultEdgeOptions = {
  type: 'pipeline',
  animated: false,
};

function PipelineCanvasInner() {
  const nodes = usePipelineStore((s) => s.nodes);
  const edges = usePipelineStore((s) => s.edges);
  const setNodes = usePipelineStore((s) => s.setNodes);
  const onNodesChange = usePipelineStore((s) => s.onNodesChange);
  const onEdgesChange = usePipelineStore((s) => s.onEdgesChange);
  const onConnect = usePipelineStore((s) => s.onConnect);
  const pinNode = usePipelineStore((s) => s.pinNode);
  const unpinAll = usePipelineStore((s) => s.unpinAll);
  const markDirty = usePipelineStore((s) => s.markDirty);
  const simulationHasRun = usePipelineStore((s) => s.simulationHasRun);
  const markSimulationRun = usePipelineStore((s) => s.markSimulationRun);

  const [unpinOnRelayout, setUnpinOnRelayout] = useState(false);

  /** Called when simulation settles — save positions. */
  const handleSettled = useCallback(() => {
    markSimulationRun();
    markDirty();
  }, [markSimulationRun, markDirty]);

  // The force layout hook uses Node<PipelineNodeData> (type?: string)
  // while our store uses PipelineNode (type: 'pipeline'). The shapes are
  // compatible at runtime, so we bridge the variance gap with a cast.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const setNodesForLayout = setNodes as any;

  const { rerun, rerunAll } = useForceLayout(nodes, edges, setNodesForLayout, {
    // Skip simulation if pipeline was loaded with saved positions
    enabled: !simulationHasRun,
    onSettled: handleSettled,
  });

  /** When a user finishes dragging a node, pin it and save. */
  const handleNodeDragStop = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      pinNode(node.id);
    },
    [pinNode],
  );

  /** Re-layout button handler. */
  const handleRelayout = useCallback(() => {
    if (unpinOnRelayout) {
      unpinAll();
      rerunAll();
    } else {
      rerun();
    }
    // After re-layout, positions will be saved via onSettled
  }, [unpinOnRelayout, unpinAll, rerun, rerunAll]);

  const handleConnect: OnConnect = useCallback(
    (connection) => {
      onConnect(connection);
    },
    [onConnect],
  );

  return (
    <div className="pipeline-canvas">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange as OnNodesChange}
        onEdgesChange={onEdgesChange as OnEdgesChange}
        onConnect={handleConnect}
        onNodeDragStop={handleNodeDragStop}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
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
        <EdgeMarkerDefs />
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
        <Panel position="top-right" className="relayout-panel">
          <label className="relayout-checkbox">
            <input
              type="checkbox"
              checked={unpinOnRelayout}
              onChange={(e) => setUnpinOnRelayout(e.target.checked)}
            />
            Unpin all
          </label>
          <button
            className="relayout-button"
            onClick={handleRelayout}
            title="Re-run force-directed layout"
          >
            Re-layout
          </button>
        </Panel>
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
