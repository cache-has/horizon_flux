// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback } from 'react';
import type { Connection, Edge } from '@xyflow/react';

/**
 * Check whether adding an edge from `source` to `target` would create a cycle
 * in the DAG defined by `edges`. Uses BFS from `target` following existing
 * outgoing edges — if we can reach `source`, connecting source→target would
 * close a loop.
 */
export function wouldCreateCycle(
  source: string,
  target: string,
  edges: Edge[],
): boolean {
  // Build adjacency list (outgoing edges)
  const adjacency = new Map<string, string[]>();
  for (const edge of edges) {
    const targets = adjacency.get(edge.source);
    if (targets) {
      targets.push(edge.target);
    } else {
      adjacency.set(edge.source, [edge.target]);
    }
  }

  // BFS from target: can we reach source?
  const visited = new Set<string>();
  const queue = [target];

  while (queue.length > 0) {
    const current = queue.shift()!;
    if (current === source) return true;
    if (visited.has(current)) continue;
    visited.add(current);

    const neighbors = adjacency.get(current);
    if (neighbors) {
      for (const neighbor of neighbors) {
        if (!visited.has(neighbor)) {
          queue.push(neighbor);
        }
      }
    }
  }

  return false;
}

/**
 * Hook that returns an `isValidConnection` callback for React Flow.
 * Prevents self-connections and cycles.
 */
export function useConnectionValidation(edges: Edge[]) {
  return useCallback(
    (connection: Connection | { source: string | null; target: string | null }): boolean => {
      const { source, target } = connection;

      // Both ends must be defined
      if (!source || !target) return false;

      // Prevent self-connections
      if (source === target) return false;

      // Prevent duplicate edges
      const duplicate = edges.some(
        (e) => e.source === source && e.target === target,
      );
      if (duplicate) return false;

      // Prevent cycles
      if (wouldCreateCycle(source, target, edges)) return false;

      return true;
    },
    [edges],
  );
}
