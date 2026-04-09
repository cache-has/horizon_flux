// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { Handle, Position, type NodeProps } from '@xyflow/react';
import './ProjectPipelineNode.css';

export interface ProjectNodeData extends Record<string, unknown> {
  label: string;
  nodeCount: number;
  pinnedPosition: boolean;
}

export function ProjectPipelineNode({ data, selected }: NodeProps) {
  const { label, nodeCount } = data as ProjectNodeData;

  return (
    <div className={`project-node${selected ? ' project-node--selected' : ''}`}>
      <Handle type="target" position={Position.Left} />
      <div className="project-node__name" title={label}>
        {label}
      </div>
      <div className="project-node__meta">
        <span>{nodeCount} node{nodeCount !== 1 ? 's' : ''}</span>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
