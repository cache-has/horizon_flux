// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect, useRef, useCallback } from 'react';
import {
  useReactFlow,
  useNodesInitialized,
  type Node,
  type Edge,
} from '@xyflow/react';
import {
  forceSimulation,
  forceLink,
  forceManyBody,
  forceX,
  forceY,
  type Simulation,
  type SimulationNodeDatum,
  type SimulationLinkDatum,
} from 'd3-force';

import type { PipelineNodeData } from '../types/pipeline';

/** d3-force simulation node that carries React Flow node data. */
export interface SimNode extends SimulationNodeDatum {
  id: string;
  /** Measured width from React Flow (falls back to default). */
  width: number;
  /** Measured height from React Flow (falls back to default). */
  height: number;
  /** Topological depth in the DAG (0 = source, 1 = first transform, etc.). */
  depth: number;
}

const DEFAULT_NODE_WIDTH = 180;
const DEFAULT_NODE_HEIGHT = 60;
const COLUMN_SPACING = 250;

/**
 * Compute the topological depth of each node in the DAG.
 * Nodes with no incoming edges get depth 0. Each subsequent layer
 * gets depth = max(parent depths) + 1.
 */
export function computeDepths(
  nodes: Node[],
  edges: Edge[],
): Map<string, number> {
  const depths = new Map<string, number>();
  const incomingMap = new Map<string, string[]>();

  for (const node of nodes) {
    incomingMap.set(node.id, []);
  }
  for (const edge of edges) {
    const incoming = incomingMap.get(edge.target);
    if (incoming) {
      incoming.push(edge.source);
    }
  }

  // BFS / Kahn-style traversal
  const queue: string[] = [];
  for (const [id, parents] of incomingMap) {
    if (parents.length === 0) {
      depths.set(id, 0);
      queue.push(id);
    }
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentDepth = depths.get(current)!;

    for (const edge of edges) {
      if (edge.source === current) {
        const childDepth = depths.get(edge.target);
        const newDepth = currentDepth + 1;
        if (childDepth === undefined || newDepth > childDepth) {
          depths.set(edge.target, newDepth);
        }
        // Only enqueue when all parents have been processed
        const parents = incomingMap.get(edge.target) ?? [];
        const allParentsProcessed = parents.every((p) => depths.has(p));
        if (allParentsProcessed) {
          queue.push(edge.target);
        }
      }
    }
  }

  // Fallback: any unreached node gets depth 0
  for (const node of nodes) {
    if (!depths.has(node.id)) {
      depths.set(node.id, 0);
    }
  }

  return depths;
}

/** Rectangular collision force using quadtree approach. */
function collideRect(padding = 10) {
  let nodes: SimNode[] = [];

  function force(alpha: number) {
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i];
        const b = nodes[j];

        const ax = a.x ?? 0;
        const ay = a.y ?? 0;
        const bx = b.x ?? 0;
        const by = b.y ?? 0;

        const dx = ax - bx;
        const dy = ay - by;

        const overlapX =
          (a.width + b.width) / 2 + padding - Math.abs(dx);
        const overlapY =
          (a.height + b.height) / 2 + padding - Math.abs(dy);

        if (overlapX > 0 && overlapY > 0) {
          // Push apart along the axis of least overlap
          const strength = alpha * 0.5;
          if (overlapX < overlapY) {
            const shift = overlapX * strength * 0.5;
            const sign = dx > 0 ? 1 : -1;
            if (a.fx == null) a.x = ax + sign * shift;
            if (b.fx == null) b.x = bx - sign * shift;
          } else {
            const shift = overlapY * strength * 0.5;
            const sign = dy > 0 ? 1 : -1;
            if (a.fy == null) a.y = ay + sign * shift;
            if (b.fy == null) b.y = by - sign * shift;
          }
        }
      }
    }
  }

  force.initialize = (n: SimNode[]) => {
    nodes = n;
  };

  return force;
}

export interface UseForceLayoutOptions {
  /** Whether the force layout is enabled. Defaults to true. */
  enabled?: boolean;
  /** Called when the simulation settles (alpha below threshold). */
  onSettled?: () => void;
}

/**
 * Custom hook that integrates d3-force simulation with React Flow.
 *
 * Runs simulation when nodes are initialized and when the graph structure
 * changes (node/edge count). Respects pinned nodes via fx/fy.
 */
export function useForceLayout(
  nodes: Node<PipelineNodeData>[],
  edges: Edge[],
  setNodes: (updater: (current: Node<PipelineNodeData>[]) => Node<PipelineNodeData>[]) => void,
  options: UseForceLayoutOptions = {},
) {
  const { enabled = true, onSettled } = options;
  const { fitView } = useReactFlow();
  const nodesInitialized = useNodesInitialized();

  const simulationRef = useRef<Simulation<SimNode, SimulationLinkDatum<SimNode>> | null>(null);
  const runningRef = useRef(false);
  const rafRef = useRef<number | null>(null);
  const onSettledRef = useRef(onSettled);
  useEffect(() => {
    onSettledRef.current = onSettled;
  }, [onSettled]);

  // Track structural identity to detect when to restart simulation
  const structureKeyRef = useRef('');

  const stopSimulation = useCallback(() => {
    runningRef.current = false;
    if (rafRef.current != null) {
      cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    }
    if (simulationRef.current) {
      simulationRef.current.stop();
    }
  }, []);

  const runSimulation = useCallback(
    (
      rfNodes: Node<PipelineNodeData>[],
      rfEdges: Edge[],
      ignorePin = false,
    ) => {
      stopSimulation();

      const depths = computeDepths(rfNodes, rfEdges);

      const simNodes: SimNode[] = rfNodes.map((node) => {
        const isPinned = !ignorePin && node.data.pinnedPosition;
        return {
          id: node.id,
          x: node.position.x,
          y: node.position.y,
          fx: isPinned ? node.position.x : undefined,
          fy: isPinned ? node.position.y : undefined,
          width: node.measured?.width ?? DEFAULT_NODE_WIDTH,
          height: node.measured?.height ?? DEFAULT_NODE_HEIGHT,
          depth: depths.get(node.id) ?? 0,
        };
      });

      const simLinks: SimulationLinkDatum<SimNode>[] = rfEdges.map(
        (edge) => ({
          source: edge.source,
          target: edge.target,
        }),
      );

      const sim = forceSimulation<SimNode>(simNodes)
        .force(
          'charge',
          forceManyBody<SimNode>().strength(-800).distanceMax(500),
        )
        .force(
          'link',
          forceLink<SimNode, SimulationLinkDatum<SimNode>>(simLinks)
            .id((d) => d.id)
            .distance(150)
            .strength(0.1),
        )
        .force(
          'x',
          forceX<SimNode>()
            .x((d) => d.depth * COLUMN_SPACING)
            .strength(0.3),
        )
        .force('y', forceY<SimNode>().y(0).strength(0.05))
        .force('collide', collideRect(10) as never)
        .alphaDecay(0.02)
        .alphaTarget(0)
        .stop();

      simulationRef.current = sim;
      runningRef.current = true;

      const tick = () => {
        sim.tick();

        setNodes((current) =>
          current.map((node) => {
            const simNode = simNodes.find((sn) => sn.id === node.id);
            if (!simNode) return node;
            return {
              ...node,
              position: {
                x: simNode.fx ?? simNode.x ?? node.position.x,
                y: simNode.fy ?? simNode.y ?? node.position.y,
              },
            };
          }),
        );

        if (sim.alpha() < (sim.alphaMin?.() ?? 0.001)) {
          runningRef.current = false;
          // Final fitView once settled
          requestAnimationFrame(() => fitView({ padding: 0.3 }));
          // Notify that simulation has settled
          onSettledRef.current?.();
          return;
        }

        if (runningRef.current) {
          rafRef.current = requestAnimationFrame(tick);
        }
      };

      rafRef.current = requestAnimationFrame(tick);
    },
    [setNodes, fitView, stopSimulation],
  );

  // Track structural changes and run simulation when structure changes
  useEffect(() => {
    if (!enabled || !nodesInitialized) return;

    // Build a structural key from node IDs + edge source/target pairs
    const nodeIds = nodes
      .map((n) => n.id)
      .sort()
      .join(',');
    const edgeKeys = edges
      .map((e) => `${e.source}->${e.target}`)
      .sort()
      .join(',');
    const key = `${nodeIds}|${edgeKeys}`;

    if (key === structureKeyRef.current) return;
    structureKeyRef.current = key;

    runSimulation(nodes, edges);
  }, [enabled, nodesInitialized, nodes, edges, runSimulation]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      stopSimulation();
    };
  }, [stopSimulation]);

  return {
    /** Check whether the simulation is currently running (call in event handlers, not render). */
    getIsRunning: () => runningRef.current,
    /** Manually re-run the simulation, respecting pinned positions. */
    rerun: () => runSimulation(nodes, edges),
    /** Re-run the simulation, ignoring all pinned positions (full re-layout). */
    rerunAll: () => runSimulation(nodes, edges, true),
    /** Stop the simulation immediately. */
    stop: stopSimulation,
  };
}
