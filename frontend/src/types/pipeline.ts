// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { Node, Edge } from '@xyflow/react';
import type {
  MaterializationPolicy,
  MaterializationReceipt,
} from '../api/pipelines';

/** The fundamental node roles in a pipeline DAG. */
export type NodeRole = 'source' | 'transform' | 'sink' | 'test';

/** Execution status of a pipeline node. */
export type NodeStatus = 'idle' | 'running' | 'success' | 'error';

/** Column descriptor shown in hover tooltip. */
export interface SchemaColumn {
  name: string;
  dataType: string;
}

/** Group node data for collapsed snippet expansions (`type === 'snippetGroup'`). */
export interface SnippetGroupData extends Record<string, unknown> {
  /** Display name (the snippet definition's `snippet` field). */
  snippetName: string;
  /** The call-site ID in the parent pipeline. Equals the React Flow node id. */
  callSiteId: string;
  /** IDs of the inner nodes that belong to this group. */
  childIds: string[];
  /** Whether the group is currently collapsed (true) or expanded (false). */
  collapsed: boolean;
}

/** A snippet-group React Flow node. */
export type SnippetGroupNode = Node<SnippetGroupData, 'snippetGroup'>;

/** Application-specific data stored on each React Flow node. */
export interface PipelineNodeData extends Record<string, unknown> {
  label: string;
  role: NodeRole;
  status: NodeStatus;
  /** If set, this node is part of an expanded snippet (call-site id of the
   *  outermost snippet call). Used for collapsible-group rendering. */
  snippetParent?: string;
  /** Snippet name for the outermost call this node belongs to. */
  snippetName?: string;
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
  /** Sink-only: declared materialization policy (drives the incremental badge). */
  materializationPolicy?: MaterializationPolicy;
  /** Sink-only: latest run's materialization receipt — feeds the badge tooltip. */
  materializationReceipt?: MaterializationReceipt;
  /** Resource fingerprint from lineage bindings (source/sink nodes only). */
  resourceFingerprint?: string;
}

/** A pipeline node is a React Flow Node carrying our custom data. */
export type PipelineNode = Node<PipelineNodeData, 'pipeline'>;

/** A node displayed on the canvas — either a regular pipeline node or a
 *  collapsed snippet-group placeholder. */
export type CanvasNode = PipelineNode | SnippetGroupNode;

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
