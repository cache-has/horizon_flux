// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { Node, Edge } from '@xyflow/react';

/** The three fundamental node roles in a pipeline DAG. */
export type NodeRole = 'source' | 'transform' | 'sink';

/** Execution status of a pipeline node. */
export type NodeStatus = 'idle' | 'running' | 'success' | 'error';

/** Column descriptor shown in hover tooltip. */
export interface SchemaColumn {
  name: string;
  dataType: string;
}

/** Application-specific data stored on each React Flow node. */
export interface PipelineNodeData extends Record<string, unknown> {
  label: string;
  role: NodeRole;
  status: NodeStatus;
  /** Whether this node's position has been manually pinned by the user. */
  pinnedPosition: boolean;
  /** Whether config is overridden in the current environment. */
  envOverridden: boolean;
  /** Row count from last execution (undefined if never run). */
  rowCount?: number;
  /** Schema columns from last execution. */
  schemaSummary?: SchemaColumn[];
  /** Duration of last execution in milliseconds. */
  lastRunDurationMs?: number;
  /** Error message from the last execution (set when status is 'error'). */
  errorMessage?: string;
}

/** A pipeline node is a React Flow Node carrying our custom data. */
export type PipelineNode = Node<PipelineNodeData, 'pipeline'>;

/** Application-specific data stored on each React Flow edge. */
export interface PipelineEdgeData extends Record<string, unknown> {
  /** Row count that flowed through this connection in the last run. */
  rowCount?: number;
  /** Data volume in bytes transferred during the last run. */
  dataVolumeBytes?: number;
  /** Elapsed time in milliseconds for the last transfer. */
  elapsedMs?: number;
  /** Schema columns flowing through this connection. */
  schemaSummary?: SchemaColumn[];
  /** Whether the connection is actively transferring data (triggers animation). */
  animated?: boolean;
}

/** A pipeline edge is a React Flow Edge carrying our custom data. */
export type PipelineEdge = Edge<PipelineEdgeData, 'pipeline'>;
