// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect } from 'vitest';
import { computeDepths } from './useForceLayout';
import type { Node, Edge } from '@xyflow/react';

function makeNode(id: string): Node {
  return { id, position: { x: 0, y: 0 }, data: {} };
}

function makeEdge(source: string, target: string): Edge {
  return { id: `e-${source}-${target}`, source, target };
}

describe('computeDepths', () => {
  it('assigns depth 0 to nodes with no incoming edges', () => {
    const nodes = [makeNode('a'), makeNode('b')];
    const depths = computeDepths(nodes, []);

    expect(depths.get('a')).toBe(0);
    expect(depths.get('b')).toBe(0);
  });

  it('computes linear chain depths correctly', () => {
    const nodes = [makeNode('a'), makeNode('b'), makeNode('c')];
    const edges = [makeEdge('a', 'b'), makeEdge('b', 'c')];
    const depths = computeDepths(nodes, edges);

    expect(depths.get('a')).toBe(0);
    expect(depths.get('b')).toBe(1);
    expect(depths.get('c')).toBe(2);
  });

  it('computes diamond DAG depths correctly', () => {
    // a -> b, a -> c, b -> d, c -> d
    const nodes = [
      makeNode('a'),
      makeNode('b'),
      makeNode('c'),
      makeNode('d'),
    ];
    const edges = [
      makeEdge('a', 'b'),
      makeEdge('a', 'c'),
      makeEdge('b', 'd'),
      makeEdge('c', 'd'),
    ];
    const depths = computeDepths(nodes, edges);

    expect(depths.get('a')).toBe(0);
    expect(depths.get('b')).toBe(1);
    expect(depths.get('c')).toBe(1);
    expect(depths.get('d')).toBe(2);
  });

  it('handles multiple root nodes', () => {
    const nodes = [
      makeNode('r1'),
      makeNode('r2'),
      makeNode('child'),
    ];
    const edges = [makeEdge('r1', 'child'), makeEdge('r2', 'child')];
    const depths = computeDepths(nodes, edges);

    expect(depths.get('r1')).toBe(0);
    expect(depths.get('r2')).toBe(0);
    expect(depths.get('child')).toBe(1);
  });

  it('handles empty graph', () => {
    const depths = computeDepths([], []);
    expect(depths.size).toBe(0);
  });
});
