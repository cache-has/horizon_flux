// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect } from 'vitest';
import { wouldCreateCycle } from './useConnectionValidation';
import type { Edge } from '@xyflow/react';

function makeEdge(source: string, target: string): Edge {
  return { id: `e-${source}-${target}`, source, target };
}

describe('wouldCreateCycle', () => {
  it('returns false for a valid new edge in a linear chain', () => {
    // a -> b, adding b -> c
    const edges = [makeEdge('a', 'b')];
    expect(wouldCreateCycle('b', 'c', edges)).toBe(false);
  });

  it('detects a direct back-edge cycle', () => {
    // a -> b, adding b -> a
    const edges = [makeEdge('a', 'b')];
    expect(wouldCreateCycle('b', 'a', edges)).toBe(true);
  });

  it('detects an indirect cycle through multiple hops', () => {
    // a -> b -> c, adding c -> a
    const edges = [makeEdge('a', 'b'), makeEdge('b', 'c')];
    expect(wouldCreateCycle('c', 'a', edges)).toBe(true);
  });

  it('returns false when no edges exist', () => {
    expect(wouldCreateCycle('a', 'b', [])).toBe(false);
  });

  it('handles diamond DAG without false positive', () => {
    // a -> b, a -> c, b -> d, c -> d — adding a -> d is valid (no cycle)
    const edges = [
      makeEdge('a', 'b'),
      makeEdge('a', 'c'),
      makeEdge('b', 'd'),
      makeEdge('c', 'd'),
    ];
    expect(wouldCreateCycle('a', 'd', edges)).toBe(false);
  });

  it('detects cycle in diamond DAG', () => {
    // a -> b, a -> c, b -> d, c -> d — adding d -> a is a cycle
    const edges = [
      makeEdge('a', 'b'),
      makeEdge('a', 'c'),
      makeEdge('b', 'd'),
      makeEdge('c', 'd'),
    ];
    expect(wouldCreateCycle('d', 'a', edges)).toBe(true);
  });

  it('does not consider disconnected nodes as cyclic', () => {
    // a -> b, c is isolated — adding c -> a is fine
    const edges = [makeEdge('a', 'b')];
    expect(wouldCreateCycle('c', 'a', edges)).toBe(false);
  });
});
