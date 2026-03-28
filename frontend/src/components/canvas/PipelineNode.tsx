// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { memo, useState } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { PipelineNode } from '../../types/pipeline';
import './PipelineNode.css';

const roleIcons: Record<string, string> = {
  source: '\u{1F4E5}',
  transform: '\u{2699}\u{FE0F}',
  sink: '\u{1F4E4}',
};

interface StatusMeta {
  className: string;
  label: string;
  icon: string | null;
}

const statusIndicators: Record<string, StatusMeta> = {
  idle: { className: 'status-idle', label: 'Idle', icon: null },
  running: { className: 'status-running', label: 'Running', icon: null },
  success: { className: 'status-success', label: 'Success', icon: '\u2713' },
  error: { className: 'status-error', label: 'Error', icon: '\u2717' },
};

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export const PipelineNodeComponent = memo(function PipelineNodeComponent({
  data,
}: NodeProps<PipelineNode>) {
  const icon = roleIcons[data.role] ?? '?';
  const status = statusIndicators[data.status] ?? statusIndicators.idle;
  const [hovered, setHovered] = useState(false);

  const hasStats =
    data.rowCount != null ||
    data.schemaSummary != null ||
    data.lastRunDurationMs != null;

  return (
    <div
      className={`pipeline-node pipeline-node--${data.role}${hovered ? ' pipeline-node--hovered' : ''}`}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      {data.role !== 'source' && (
        <Handle type="target" position={Position.Left} />
      )}
      <div className="pipeline-node__header">
        <span className="pipeline-node__icon">{icon}</span>
        <span className="pipeline-node__label">{data.label}</span>
        <span
          className={`pipeline-node__status ${status.className}`}
          title={status.label}
        >
          {status.icon}
        </span>
      </div>
      {hovered && hasStats && (
        <div className="pipeline-node__tooltip">
          {data.rowCount != null && (
            <div className="pipeline-node__tooltip-row">
              <span className="pipeline-node__tooltip-key">Rows</span>
              <span>{data.rowCount.toLocaleString()}</span>
            </div>
          )}
          {data.lastRunDurationMs != null && (
            <div className="pipeline-node__tooltip-row">
              <span className="pipeline-node__tooltip-key">Duration</span>
              <span>{formatDuration(data.lastRunDurationMs)}</span>
            </div>
          )}
          {data.schemaSummary != null && data.schemaSummary.length > 0 && (
            <div className="pipeline-node__tooltip-schema">
              <span className="pipeline-node__tooltip-key">Schema</span>
              <ul className="pipeline-node__tooltip-columns">
                {data.schemaSummary.slice(0, 6).map((col) => (
                  <li key={col.name}>
                    {col.name}{' '}
                    <span className="pipeline-node__tooltip-type">
                      {col.dataType}
                    </span>
                  </li>
                ))}
                {data.schemaSummary.length > 6 && (
                  <li className="pipeline-node__tooltip-more">
                    +{data.schemaSummary.length - 6} more
                  </li>
                )}
              </ul>
            </div>
          )}
        </div>
      )}
      {data.envOverridden && (
        <span
          className="pipeline-node__env-badge"
          title="Environment override active"
        />
      )}
      {data.role !== 'sink' && (
        <Handle type="source" position={Position.Right} />
      )}
    </div>
  );
});
