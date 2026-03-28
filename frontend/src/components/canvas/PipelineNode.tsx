// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { PipelineNode } from '../../types/pipeline';
import './PipelineNode.css';

const roleIcons: Record<string, string> = {
  source: '\u{1F4E5}',
  transform: '\u{2699}\u{FE0F}',
  sink: '\u{1F4E4}',
};

const statusIndicators: Record<string, { className: string; label: string }> = {
  idle: { className: 'status-idle', label: 'Idle' },
  running: { className: 'status-running', label: 'Running' },
  success: { className: 'status-success', label: 'Success' },
  error: { className: 'status-error', label: 'Error' },
};

export const PipelineNodeComponent = memo(function PipelineNodeComponent({
  data,
}: NodeProps<PipelineNode>) {
  const icon = roleIcons[data.role] ?? '?';
  const status = statusIndicators[data.status] ?? statusIndicators.idle;

  return (
    <div className={`pipeline-node pipeline-node--${data.role}`}>
      {data.role !== 'source' && (
        <Handle type="target" position={Position.Left} />
      )}
      <div className="pipeline-node__header">
        <span className="pipeline-node__icon">{icon}</span>
        <span className="pipeline-node__label">{data.label}</span>
        <span
          className={`pipeline-node__status ${status.className}`}
          title={status.label}
        />
      </div>
      {data.envOverridden && (
        <span className="pipeline-node__env-badge" title="Environment override active" />
      )}
      {data.role !== 'sink' && (
        <Handle type="source" position={Position.Right} />
      )}
    </div>
  );
});
