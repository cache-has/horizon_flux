// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { Node, Edge } from '@xyflow/react';

/** The three fundamental node roles in a pipeline DAG. */
export type NodeRole = 'source' | 'transform' | 'sink';

/** Execution status of a pipeline node. */
export type NodeStatus = 'idle' | 'running' | 'success' | 'error';

/** Application-specific data stored on each React Flow node. */
export interface PipelineNodeData extends Record<string, unknown> {
  label: string;
  role: NodeRole;
  status: NodeStatus;
  /** Whether this node's position has been manually pinned by the user. */
  pinnedPosition: boolean;
  /** Whether config is overridden in the current environment. */
  envOverridden: boolean;
}

/** A pipeline node is a React Flow Node carrying our custom data. */
export type PipelineNode = Node<PipelineNodeData, 'pipeline'>;

/** A pipeline edge is a standard React Flow Edge (Bezier by default). */
export type PipelineEdge = Edge;
