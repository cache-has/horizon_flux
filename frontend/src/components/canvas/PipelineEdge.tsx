// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { memo, useState, useCallback, useRef, useEffect } from 'react';
import {
  BaseEdge,
  getBezierPath,
  type EdgeProps,
} from '@xyflow/react';
import type { PipelineEdge, SchemaColumn } from '../../types/pipeline';
import './PipelineEdge.css';

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

/** Unique marker ID so multiple edges don't collide. */
const ARROW_MARKER_ID = 'pipeline-edge-arrow';

/**
 * SVG marker definitions rendered once at the top of the SVG layer.
 * Import and render inside <ReactFlow> as a child to inject into the SVG defs.
 */
export function EdgeMarkerDefs() {
  return (
    <svg style={{ position: 'absolute', width: 0, height: 0 }}>
      <defs>
        <marker
          id={ARROW_MARKER_ID}
          markerWidth="8"
          markerHeight="8"
          refX="7"
          refY="4"
          orient="auto"
          markerUnits="strokeWidth"
        >
          <path d="M 0 0 L 8 4 L 0 8 z" fill="var(--border, #e5e4e7)" />
        </marker>
        <marker
          id={`${ARROW_MARKER_ID}-selected`}
          markerWidth="8"
          markerHeight="8"
          refX="7"
          refY="4"
          orient="auto"
          markerUnits="strokeWidth"
        >
          <path d="M 0 0 L 8 4 L 0 8 z" fill="var(--accent, #a855f7)" />
        </marker>
      </defs>
    </svg>
  );
}

interface TooltipData {
  rowCount?: number;
  dataVolumeBytes?: number;
  elapsedMs?: number;
  schemaSummary?: SchemaColumn[];
}

function EdgeTooltip({ data, x, y }: { data: TooltipData; x: number; y: number }) {
  const hasData =
    data.rowCount != null ||
    data.dataVolumeBytes != null ||
    data.elapsedMs != null ||
    (data.schemaSummary != null && data.schemaSummary.length > 0);

  if (!hasData) return null;

  return (
    <div
      className="pipeline-edge-tooltip"
      style={{ left: x, top: y }}
    >
      {data.rowCount != null && (
        <div className="pipeline-edge-tooltip__row">
          <span className="pipeline-edge-tooltip__key">Rows</span>
          <span>{data.rowCount.toLocaleString()}</span>
        </div>
      )}
      {data.dataVolumeBytes != null && (
        <div className="pipeline-edge-tooltip__row">
          <span className="pipeline-edge-tooltip__key">Volume</span>
          <span>{formatBytes(data.dataVolumeBytes)}</span>
        </div>
      )}
      {data.elapsedMs != null && (
        <div className="pipeline-edge-tooltip__row">
          <span className="pipeline-edge-tooltip__key">Elapsed</span>
          <span>{formatDuration(data.elapsedMs)}</span>
        </div>
      )}
      {data.schemaSummary != null && data.schemaSummary.length > 0 && (
        <div className="pipeline-edge-tooltip__schema">
          <span className="pipeline-edge-tooltip__key">Schema</span>
          <ul className="pipeline-edge-tooltip__columns">
            {data.schemaSummary.slice(0, 6).map((col) => (
              <li key={col.name}>
                {col.name}{' '}
                <span className="pipeline-edge-tooltip__type">{col.dataType}</span>
              </li>
            ))}
            {data.schemaSummary.length > 6 && (
              <li className="pipeline-edge-tooltip__more">
                +{data.schemaSummary.length - 6} more
              </li>
            )}
          </ul>
        </div>
      )}
    </div>
  );
}

export const PipelineEdgeComponent = memo(function PipelineEdgeComponent({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
}: EdgeProps<PipelineEdge>) {
  const [hovered, setHovered] = useState(false);
  const [pinned, setPinned] = useState(false);
  const pathRef = useRef<SVGGElement>(null);
  const animated = data?.animated ?? false;

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const markerId = selected
    ? `url(#${ARROW_MARKER_ID}-selected)`
    : `url(#${ARROW_MARKER_ID})`;

  const handleMouseEnter = useCallback(() => {
    setHovered(true);
  }, []);

  const handleMouseLeave = useCallback(() => {
    setHovered(false);
  }, []);

  const handleClick = useCallback(() => {
    setPinned((prev) => !prev);
  }, []);

  // Close pinned tooltip when clicking elsewhere
  useEffect(() => {
    if (!pinned) return;
    const handleOutsideClick = (e: MouseEvent) => {
      if (pathRef.current && !pathRef.current.contains(e.target as Node)) {
        setPinned(false);
      }
    };
    document.addEventListener('mousedown', handleOutsideClick);
    return () => document.removeEventListener('mousedown', handleOutsideClick);
  }, [pinned]);

  const showTooltip = hovered || pinned;
  const tooltipData: TooltipData = {
    rowCount: data?.rowCount,
    dataVolumeBytes: data?.dataVolumeBytes,
    elapsedMs: data?.elapsedMs,
    schemaSummary: data?.schemaSummary,
  };

  return (
    <g ref={pathRef}>
      {/* Invisible wider hit area for easier hover/click */}
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={16}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
        className="pipeline-edge__hitarea"
      />
      {/* Visible edge */}
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerId}
        className={[
          'pipeline-edge__path',
          selected ? 'pipeline-edge__path--selected' : '',
          animated ? 'pipeline-edge__path--animated' : '',
        ]
          .filter(Boolean)
          .join(' ')}
      />
      {/* Animated dots during execution */}
      {animated && (
        <circle r="3" className="pipeline-edge__dot">
          <animateMotion dur="1.5s" repeatCount="indefinite" path={edgePath} />
        </circle>
      )}
      {/* Tooltip (rendered via foreignObject so we can use HTML) */}
      {showTooltip && (
        <foreignObject
          x={labelX - 100}
          y={labelY - 80}
          width={220}
          height={200}
          className="pipeline-edge__tooltip-wrapper"
          style={{ overflow: 'visible', pointerEvents: 'none' }}
        >
          <EdgeTooltip data={tooltipData} x={0} y={0} />
        </foreignObject>
      )}
    </g>
  );
});
