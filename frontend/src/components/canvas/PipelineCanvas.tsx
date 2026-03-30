// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState, useRef } from 'react';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  Panel,
  useReactFlow,
  type OnConnect,
  type OnNodesChange,
  type OnEdgesChange,
  type NodeTypes,
  type EdgeTypes,
  type Node,
  type Edge,
  type NodeMouseHandler,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import type { PipelineNode, PipelineEdge, NodeRole } from '../../types/pipeline';
import { PipelineNodeComponent } from './PipelineNode';
import { PipelineEdgeComponent, EdgeMarkerDefs } from './PipelineEdge';
import { useForceLayout } from '../../hooks/useForceLayout';
import { useConnectionValidation } from '../../hooks/useConnectionValidation';
import { usePipelineStore } from '../../stores/pipelineStore';
import {
  CanvasContextMenu,
  type ContextMenuState,
} from './CanvasContextMenu';
import { ConfirmDialog } from './ConfirmDialog';
import { NodePalette, PALETTE_DRAG_TYPE } from './NodePalette';
import type { PaletteItem } from './NodePalette';
import { SidePanel } from './SidePanel';
import { NodeEditorModal } from './NodeEditorModal';
import { EnvironmentManagementPanel } from './EnvironmentManagementPanel';
import { CanvasToolbar } from './CanvasToolbar';
import { useEnvironmentStore } from '../../stores/environmentStore';
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
  const unpinNode = usePipelineStore((s) => s.unpinNode);
  const unpinAll = usePipelineStore((s) => s.unpinAll);
  const markDirty = usePipelineStore((s) => s.markDirty);
  const simulationHasRun = usePipelineStore((s) => s.simulationHasRun);
  const markSimulationRun = usePipelineStore((s) => s.markSimulationRun);
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId);
  const setEditingNodeId = usePipelineStore((s) => s.setEditingNodeId);
  const deleteNodes = usePipelineStore((s) => s.deleteNodes);
  const deleteEdges = usePipelineStore((s) => s.deleteEdges);
  const duplicateNode = usePipelineStore((s) => s.duplicateNode);

  const selectedNodeId = usePipelineStore((s) => s.selectedNodeId);
  const undo = usePipelineStore((s) => s.undo);
  const redo = usePipelineStore((s) => s.redo);
  const { screenToFlowPosition } = useReactFlow();

  const isValidConnection = useConnectionValidation(edges);
  const [unpinOnRelayout, setUnpinOnRelayout] = useState(false);

  // Keyboard shortcuts: Escape to close side panel, Cmd/Ctrl+Z undo, Cmd/Ctrl+Shift+Z redo
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape' && selectedNodeId) {
        setSelectedNodeId(null);
        return;
      }

      const mod = e.metaKey || e.ctrlKey;
      if (mod && e.key === 'z' && !e.shiftKey) {
        e.preventDefault();
        undo();
      } else if (mod && e.key === 'z' && e.shiftKey) {
        e.preventDefault();
        redo();
      }
    }
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [selectedNodeId, setSelectedNodeId, undo, redo]);

  // Sync environment overrides → node envOverridden badges
  const tableOverrides = useEnvironmentStore((s) => s.tableOverrides);
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);
  useEffect(() => {
    const overrideTables = new Set(tableOverrides.map((o) => o.table_name));
    const currentNodes = usePipelineStore.getState().nodes;
    const needsUpdate = currentNodes.some(
      (n) => n.data.envOverridden !== overrideTables.has(n.data.label),
    );
    if (needsUpdate) {
      usePipelineStore.getState().setNodes((prev) =>
        prev.map((n) => ({
          ...n,
          data: {
            ...n.data,
            envOverridden: overrideTables.has(n.data.label),
          },
        })),
      );
    }
  }, [tableOverrides, activeEnvironment]);

  // Context menu state
  const [contextMenu, setContextMenu] = useState<ContextMenuState>(null);

  // Delete confirmation dialog state
  const [deleteConfirm, setDeleteConfirm] = useState<{
    nodeIds?: string[];
    edgeIds?: string[];
    message: string;
  } | null>(null);

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

  // -------------------------------------------------------------------------
  // Node interaction handlers
  // -------------------------------------------------------------------------

  /** Single click: select node for side panel. */
  const handleNodeClick: NodeMouseHandler = useCallback(
    (_event, node) => {
      setSelectedNodeId(node.id);
    },
    [setSelectedNodeId],
  );

  /** Double click: open modal editor. */
  const handleNodeDoubleClick: NodeMouseHandler = useCallback(
    (_event, node) => {
      setEditingNodeId(node.id);
    },
    [setEditingNodeId],
  );

  /** Click on empty canvas: deselect. */
  const handlePaneClick = useCallback(() => {
    setSelectedNodeId(null);
    setContextMenu(null);
  }, [setSelectedNodeId]);

  // -------------------------------------------------------------------------
  // Delete handling with confirmation
  // -------------------------------------------------------------------------

  /** Intercept delete key to show confirmation. */
  const handleNodesDelete = useCallback(
    (deletedNodes: Node[]) => {
      const ids = deletedNodes.map((n) => n.id);
      const count = ids.length;
      setDeleteConfirm({
        nodeIds: ids,
        message:
          count === 1
            ? `Delete "${(deletedNodes[0].data as PipelineNode['data']).label}"?`
            : `Delete ${count} selected nodes?`,
      });
    },
    [],
  );

  const handleEdgesDelete = useCallback(
    (deletedEdges: Edge[]) => {
      const ids = deletedEdges.map((e) => e.id);
      setDeleteConfirm({
        edgeIds: ids,
        message:
          ids.length === 1
            ? 'Delete this connection?'
            : `Delete ${ids.length} connections?`,
      });
    },
    [],
  );

  const confirmDelete = useCallback(() => {
    if (deleteConfirm?.nodeIds) {
      deleteNodes(deleteConfirm.nodeIds);
    }
    if (deleteConfirm?.edgeIds) {
      deleteEdges(deleteConfirm.edgeIds);
    }
    setDeleteConfirm(null);
  }, [deleteConfirm, deleteNodes, deleteEdges]);

  const cancelDelete = useCallback(() => {
    setDeleteConfirm(null);
  }, []);

  // -------------------------------------------------------------------------
  // Context menu handlers
  // -------------------------------------------------------------------------

  const handleNodeContextMenu: NodeMouseHandler = useCallback(
    (event, node) => {
      event.preventDefault();
      const data = node.data as PipelineNode['data'];

      // If multiple nodes are selected, show multi-select menu
      const selectedNodes = usePipelineStore
        .getState()
        .nodes.filter((n) => n.selected);
      if (selectedNodes.length > 1 && node.selected) {
        setContextMenu({
          kind: 'multi',
          nodeIds: selectedNodes.map((n) => n.id),
          x: event.clientX,
          y: event.clientY,
        });
        return;
      }

      const envStore = useEnvironmentStore.getState();
      setContextMenu({
        kind: 'node',
        nodeId: node.id,
        nodeRole: data.role,
        nodeLabel: data.label,
        isPinned: data.pinnedPosition,
        hasOverride: envStore.hasOverride(data.label),
        activeEnvironment: envStore.activeEnvironment,
        x: event.clientX,
        y: event.clientY,
      });
    },
    [],
  );

  const handleEdgeContextMenu = useCallback(
    (event: React.MouseEvent, edge: Edge) => {
      event.preventDefault();
      setContextMenu({
        kind: 'edge',
        edgeId: edge.id,
        x: event.clientX,
        y: event.clientY,
      });
    },
    [],
  );

  const handlePaneContextMenu = useCallback(
    (event: React.MouseEvent | MouseEvent) => {
      event.preventDefault();
      const canvasPos = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });
      setContextMenu({
        kind: 'canvas',
        canvasX: canvasPos.x,
        canvasY: canvasPos.y,
        x: event.clientX,
        y: event.clientY,
      });
    },
    [screenToFlowPosition],
  );

  const closeContextMenu = useCallback(() => {
    setContextMenu(null);
  }, []);

  // -------------------------------------------------------------------------
  // Node palette state & drag-and-drop from palette onto canvas
  // -------------------------------------------------------------------------

  const [paletteCollapsed, setPaletteCollapsed] = useState(false);
  const reactFlowWrapper = useRef<HTMLDivElement>(null);

  const handleDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const handleDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const raw = event.dataTransfer.getData(PALETTE_DRAG_TYPE);
      if (!raw) return;

      const item: PaletteItem = JSON.parse(raw);
      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      const newId = `${item.role}-${Date.now()}`;
      const label =
        item.subtype.charAt(0).toUpperCase() + item.subtype.slice(1);

      const newNode: PipelineNode = {
        id: newId,
        type: 'pipeline',
        position,
        data: {
          label: `New ${label}`,
          role: item.role,
          status: 'idle',
          pinnedPosition: false,
          envOverridden: false,
        },
      };
      usePipelineStore.getState().pushSnapshot();
      usePipelineStore.getState().setNodes((current) => [...current, newNode]);
      usePipelineStore.getState().markDirty();
    },
    [screenToFlowPosition],
  );

  /** Handle actions dispatched from the context menu. */
  const handleContextAction = useCallback(
    (action: string, payload?: Record<string, unknown>) => {
      switch (action) {
        case 'edit-node':
          setEditingNodeId(payload?.nodeId as string);
          break;
        case 'rename-node':
          // Rename will be handled by side panel / inline editor (doc 11)
          setSelectedNodeId(payload?.nodeId as string);
          break;
        case 'duplicate-node':
          duplicateNode(payload?.nodeId as string);
          break;
        case 'pin-node':
          pinNode(payload?.nodeId as string);
          break;
        case 'unpin-node':
          unpinNode(payload?.nodeId as string);
          break;
        case 'delete-node': {
          const nodeId = payload?.nodeId as string;
          const node = usePipelineStore
            .getState()
            .nodes.find((n) => n.id === nodeId);
          setDeleteConfirm({
            nodeIds: [nodeId],
            message: `Delete "${node?.data.label ?? nodeId}"?`,
          });
          break;
        }
        case 'delete-nodes': {
          const nodeIds = payload?.nodeIds as string[];
          setDeleteConfirm({
            nodeIds,
            message: `Delete ${nodeIds.length} selected nodes?`,
          });
          break;
        }
        case 'delete-edge':
          setDeleteConfirm({
            edgeIds: [payload?.edgeId as string],
            message: 'Delete this connection?',
          });
          break;
        case 'view-edge-metadata':
          // Edge metadata viewing is handled by the existing edge click/hover tooltip
          break;
        case 'add-node': {
          const role = payload?.role as NodeRole;
          const x = payload?.x as number;
          const y = payload?.y as number;
          const connector = payload?.connector as string | undefined;
          const mode = payload?.mode as string | undefined;
          const newId = `${role}-${Date.now()}`;
          const labelParts = [
            connector ?? mode ?? role,
          ];
          const label =
            labelParts[0].charAt(0).toUpperCase() + labelParts[0].slice(1);

          const newNode: PipelineNode = {
            id: newId,
            type: 'pipeline',
            position: { x, y },
            data: {
              label: `New ${label}`,
              role,
              status: 'idle',
              pinnedPosition: false,
              envOverridden: false,
            },
          };
          usePipelineStore.getState().pushSnapshot();
          usePipelineStore.getState().setNodes((current) => [...current, newNode]);
          usePipelineStore.getState().markDirty();
          break;
        }
        case 'create-transform-from-selected': {
          // Create a new transform node with all selected nodes as inputs
          const nodeIds = payload?.nodeIds as string[];
          usePipelineStore.getState().pushSnapshot();
          const store = usePipelineStore.getState();
          const selectedNodes = store.nodes.filter((n) =>
            nodeIds.includes(n.id),
          );
          // Place the new transform to the right of selected nodes
          const maxX = Math.max(...selectedNodes.map((n) => n.position.x));
          const avgY =
            selectedNodes.reduce((sum, n) => sum + n.position.y, 0) /
            selectedNodes.length;
          const newId = `transform-${Date.now()}`;
          const newNode: PipelineNode = {
            id: newId,
            type: 'pipeline',
            position: { x: maxX + 250, y: avgY },
            data: {
              label: 'New Transform',
              role: 'transform',
              status: 'idle',
              pinnedPosition: false,
              envOverridden: false,
            },
          };
          // Create edges from all selected nodes to the new transform
          const newEdges: PipelineEdge[] = nodeIds.map((srcId) => ({
            id: `e-${srcId}-${newId}`,
            source: srcId,
            target: newId,
            type: 'pipeline' as const,
          }));
          store.setNodes((current) => [...current, newNode]);
          usePipelineStore.setState((state) => ({
            edges: [...state.edges, ...newEdges],
            dirty: true,
          }));
          break;
        }
        case 'create-dev-override': {
          const env = payload?.environment as string;
          const label = payload?.nodeLabel as string;
          if (env && label) {
            useEnvironmentStore.getState().addTableOverride(env, label);
          }
          break;
        }
        case 'remove-override': {
          const env = payload?.environment as string;
          const label = payload?.nodeLabel as string;
          if (env && label) {
            useEnvironmentStore.getState().removeTableOverride(env, label);
          }
          break;
        }
        case 'view-preview':
          // Will be implemented in a later planning phase
          break;
      }
    },
    [
      setEditingNodeId,
      setSelectedNodeId,
      duplicateNode,
      pinNode,
      unpinNode,
    ],
  );

  return (
    <div className="pipeline-canvas" ref={reactFlowWrapper}>
      <NodePalette
        collapsed={paletteCollapsed}
        onToggle={() => setPaletteCollapsed((c) => !c)}
      />
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange as OnNodesChange}
        onEdgesChange={onEdgesChange as OnEdgesChange}
        onConnect={handleConnect}
        onNodeDragStop={handleNodeDragStop}
        onNodeClick={handleNodeClick}
        onNodeDoubleClick={handleNodeDoubleClick}
        onPaneClick={handlePaneClick}
        onNodesDelete={handleNodesDelete}
        onEdgesDelete={handleEdgesDelete}
        onNodeContextMenu={handleNodeContextMenu}
        onEdgeContextMenu={handleEdgeContextMenu}
        onPaneContextMenu={handlePaneContextMenu}
        onDragOver={handleDragOver}
        onDrop={handleDrop}
        isValidConnection={isValidConnection}
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
        <Panel position="top-right" className="toolbar-panel">
          <CanvasToolbar />
          <div className="relayout-panel">
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
          </div>
        </Panel>
      </ReactFlow>

      <CanvasContextMenu
        state={contextMenu}
        onClose={closeContextMenu}
        onAction={handleContextAction}
      />

      <ConfirmDialog
        open={deleteConfirm !== null}
        title="Confirm Delete"
        message={deleteConfirm?.message ?? ''}
        onConfirm={confirmDelete}
        onCancel={cancelDelete}
      />

      <SidePanel />
      <EnvironmentManagementPanel />
      <NodeEditorModal />
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
