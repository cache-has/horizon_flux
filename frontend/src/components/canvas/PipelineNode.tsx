// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { memo, useState } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { PipelineNode } from '../../types/pipeline';
import { useEnvironmentStore } from '../../stores/environmentStore';
import { roleIcon } from '../icons';
import './PipelineNode.css';

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

/** Small colored dot indicating environment resolution status. */
function EnvironmentBadge({ envOverridden }: { envOverridden: boolean }) {
  const activeEnv = useEnvironmentStore((s) => s.activeEnvironment);
  const tooltip = envOverridden
    ? `Resolving from: ${activeEnv} (override)`
    : 'Resolving from: prod (fallthrough)';
  const className = envOverridden
    ? 'pipeline-node__env-badge pipeline-node__env-badge--override'
    : 'pipeline-node__env-badge pipeline-node__env-badge--fallthrough';

  return <span className={className} title={tooltip} />;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export const PipelineNodeComponent = memo(function PipelineNodeComponent({
  data,
}: NodeProps<PipelineNode>) {
  const icon = roleIcon[data.role] ?? null;
  const status = statusIndicators[data.status] ?? statusIndicators.idle;
  const [hovered, setHovered] = useState(false);

  const hasStats =
    data.rowCount != null ||
    data.schemaSummary != null ||
    data.lastRunDurationMs != null ||
    data.errorMessage != null;

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
          title={data.errorMessage ?? status.label}
        >
          {status.icon}
        </span>
      </div>
      {hovered && hasStats && (
        <div className="pipeline-node__tooltip">
          {data.errorMessage != null && (
            <div className="pipeline-node__tooltip-error">
              {data.errorMessage}
            </div>
          )}
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
      <EnvironmentBadge envOverridden={data.envOverridden} />
      {data.role !== 'sink' && (
        <Handle type="source" position={Position.Right} />
      )}
    </div>
  );
});
