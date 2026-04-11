// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import {
  fetchRun,
  timestampDurationMs,
  timestampToDate,
  type ApiRunDetail,
  type ApiRunNodeStats,
  type ApiRunTestResult,
} from '../../api/runs';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { ApiNode } from '../../api/pipelines';
import type { NodeRole } from '../../types/pipeline';
import './RunDetailPanel.css';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function formatTime(date: Date | undefined): string {
  if (!date) return '—';
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function statusClass(status: string): string {
  return `run-detail-panel__status run-detail-panel__status--${status}`;
}

function nodeRole(nodeId: string, apiNodes: ApiNode[]): NodeRole {
  const n = apiNodes.find((n) => n.id === nodeId);
  return (n?.type ?? 'transform') as NodeRole;
}

function nodeName(nodeId: string, apiNodes: ApiNode[]): string {
  const n = apiNodes.find((n) => n.id === nodeId);
  return n?.name ?? nodeId;
}

// ---------------------------------------------------------------------------
// Gantt timeline
// ---------------------------------------------------------------------------

interface GanttProps {
  run: ApiRunDetail;
  apiNodes: ApiNode[];
  selectedNodeId: string | null;
  onSelectNode: (nodeId: string) => void;
}

function GanttTimeline({ run, apiNodes, selectedNodeId, onSelectNode }: GanttProps) {
  if (run.node_stats.length === 0) {
    return <span className="run-detail-panel__empty">No node execution data</span>;
  }

  // Compute the overall time window from all node stats
  const allStartMs: number[] = [];
  const allEndMs: number[] = [];
  for (const stat of run.node_stats) {
    if (stat.start_time) {
      allStartMs.push(stat.start_time.secs_since_epoch * 1000 + stat.start_time.nanos_since_epoch / 1_000_000);
    }
    if (stat.end_time) {
      allEndMs.push(stat.end_time.secs_since_epoch * 1000 + stat.end_time.nanos_since_epoch / 1_000_000);
    }
  }

  if (allStartMs.length === 0) {
    return <span className="run-detail-panel__empty">No timing data available</span>;
  }

  const windowStart = Math.min(...allStartMs);
  const windowEnd = Math.max(...allEndMs);
  const windowDuration = windowEnd - windowStart || 1; // avoid division by zero

  return (
    <div className="run-detail-panel__gantt" data-testid="gantt-timeline">
      {run.node_stats.map((stat) => {
        const startMs = stat.start_time
          ? stat.start_time.secs_since_epoch * 1000 + stat.start_time.nanos_since_epoch / 1_000_000
          : windowStart;
        const endMs = stat.end_time
          ? stat.end_time.secs_since_epoch * 1000 + stat.end_time.nanos_since_epoch / 1_000_000
          : windowEnd;
        const durationMs = timestampDurationMs(stat.start_time, stat.end_time);

        const leftPct = ((startMs - windowStart) / windowDuration) * 100;
        const widthPct = ((endMs - startMs) / windowDuration) * 100;

        const role = nodeRole(stat.node_id, apiNodes);
        const barClass = stat.error
          ? 'run-detail-panel__gantt-bar run-detail-panel__gantt-bar--error'
          : `run-detail-panel__gantt-bar run-detail-panel__gantt-bar--${role}`;
        const isSelected = stat.node_id === selectedNodeId;

        return (
          <div
            key={stat.node_id}
            className={`run-detail-panel__gantt-row${isSelected ? ' run-detail-panel__gantt-row--selected' : ''}`}
            onClick={() => onSelectNode(stat.node_id)}
            title={`${nodeName(stat.node_id, apiNodes)}: ${formatDuration(durationMs)}`}
          >
            <span className="run-detail-panel__gantt-label">
              {nodeName(stat.node_id, apiNodes)}
            </span>
            <div className="run-detail-panel__gantt-track">
              <div
                className={barClass}
                style={{
                  left: `${leftPct}%`,
                  width: `${Math.max(widthPct, 1)}%`,
                }}
              />
            </div>
            <span className="run-detail-panel__gantt-duration">
              {formatDuration(durationMs)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Node detail panel (type-specific content)
// ---------------------------------------------------------------------------

interface NodeDetailProps {
  stat: ApiRunNodeStats;
  apiNode: ApiNode | undefined;
  testResult: ApiRunTestResult | undefined;
  onJumpToNode: (nodeId: string) => void;
  onViewFailureReport?: (runId: string, nodeId: string) => void;
  runId: string;
}

function NodeDetail({ stat, apiNode, testResult, onJumpToNode, onViewFailureReport, runId }: NodeDetailProps) {
  const role = (apiNode?.type ?? 'transform') as NodeRole;
  const durationMs = timestampDurationMs(stat.start_time, stat.end_time);

  return (
    <div className="run-detail-panel__node-detail">
      <div className="run-detail-panel__node-header">
        <span className="run-detail-panel__node-name">
          {apiNode?.name ?? stat.node_id}
        </span>
        <span className={`run-detail-panel__node-role run-detail-panel__node-role--${role}`}>
          {role}
        </span>
      </div>

      {/* Common stats */}
      <div className="run-detail-panel__kv">
        <span className="run-detail-panel__kv-key">Duration</span>
        <span className="run-detail-panel__kv-value">{formatDuration(durationMs)}</span>
      </div>
      <div className="run-detail-panel__kv">
        <span className="run-detail-panel__kv-key">Rows in</span>
        <span className="run-detail-panel__kv-value">{stat.rows_in.toLocaleString()}</span>
      </div>
      <div className="run-detail-panel__kv">
        <span className="run-detail-panel__kv-key">Rows out</span>
        <span className="run-detail-panel__kv-value">{stat.rows_out.toLocaleString()}</span>
      </div>

      {/* Type-specific content */}
      {role === 'source' && apiNode && (
        <>
          <div className="run-detail-panel__kv">
            <span className="run-detail-panel__kv-key">Connector</span>
            <span className="run-detail-panel__kv-value">{apiNode.connector ?? '—'}</span>
          </div>
        </>
      )}

      {role === 'transform' && apiNode && (
        <>
          <div className="run-detail-panel__kv">
            <span className="run-detail-panel__kv-key">Mode</span>
            <span className="run-detail-panel__kv-value">{(apiNode.mode ?? 'sql').toUpperCase()}</span>
          </div>
        </>
      )}

      {role === 'sink' && apiNode && (
        <>
          <div className="run-detail-panel__kv">
            <span className="run-detail-panel__kv-key">Connector</span>
            <span className="run-detail-panel__kv-value">{apiNode.connector ?? '—'}</span>
          </div>
        </>
      )}

      {role === 'test' && testResult && (
        <>
          <div className="run-detail-panel__kv">
            <span className="run-detail-panel__kv-key">Result</span>
            <span
              className="run-detail-panel__kv-value"
              style={{ color: testResult.passed ? '#16a34a' : '#ef4444' }}
            >
              {testResult.passed ? 'Passed' : 'Failed'}
            </span>
          </div>
          <div className="run-detail-panel__kv">
            <span className="run-detail-panel__kv-key">Assertions</span>
            <span className="run-detail-panel__kv-value">
              {testResult.assertions.filter((a) => a.passed).length}/{testResult.assertions.length} passed
            </span>
          </div>
        </>
      )}

      {/* Error */}
      {stat.error && (
        <div className="run-detail-panel__error-msg">{stat.error}</div>
      )}

      {/* Actions */}
      <div className="run-detail-panel__actions">
        <button
          className="run-detail-panel__action-btn"
          onClick={() => onJumpToNode(stat.node_id)}
        >
          Jump to node
        </button>
        {stat.error && onViewFailureReport && (
          <button
            className="run-detail-panel__action-btn"
            onClick={() => onViewFailureReport(runId, stat.node_id)}
          >
            View failure report
          </button>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main RunDetailPanel
// ---------------------------------------------------------------------------

interface RunDetailPanelProps {
  runId: string;
  open: boolean;
  onClose: () => void;
  onJumpToNode: (nodeId: string) => void;
  onViewFailureReport?: (runId: string, nodeId: string) => void;
  onCompare?: (runId: string) => void;
  onShowLineage?: () => void;
}

export function RunDetailPanel({
  runId,
  open,
  onClose,
  onJumpToNode,
  onViewFailureReport,
  onCompare,
  onShowLineage,
}: RunDetailPanelProps) {
  const [run, setRun] = useState<ApiRunDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  const apiPipeline = usePipelineStore((s) => s.apiPipeline);
  const apiNodes = apiPipeline?.nodes ?? [];

  useEffect(() => {
    if (!open || !runId) return;
    setLoading(true);
    setError(null);
    setSelectedNodeId(null);

    fetchRun(runId)
      .then((r) => {
        setRun(r);
        // Auto-select the first failed node if the run failed
        if (r.status === 'failed') {
          const failedNode = r.node_stats.find((s) => s.error);
          if (failedNode) setSelectedNodeId(failedNode.node_id);
        }
      })
      .catch((e) => setError((e as Error).message))
      .finally(() => setLoading(false));
  }, [runId, open]);

  const handleSelectNode = useCallback((nodeId: string) => {
    setSelectedNodeId((prev) => (prev === nodeId ? null : nodeId));
  }, []);

  const handleJumpToFailingNode = useCallback(() => {
    if (!run) return;
    const failedNode = run.node_stats.find((s) => s.error);
    if (failedNode) {
      onJumpToNode(failedNode.node_id);
    }
  }, [run, onJumpToNode]);

  const selectedStat = run?.node_stats.find((s) => s.node_id === selectedNodeId) ?? null;
  const selectedApiNode = apiNodes.find((n) => n.id === selectedNodeId);
  const selectedTestResult = run?.test_results?.find((t) => t.node_id === selectedNodeId);

  const totalDuration = run ? timestampDurationMs(run.start_time, run.end_time) : 0;
  const startDate = run ? timestampToDate(run.start_time) : undefined;

  return (
    <div
      className={`run-detail-panel${open ? ' run-detail-panel--open' : ''}`}
      data-testid="run-detail-panel"
    >
      <div className="run-detail-panel__header">
        <span className="run-detail-panel__title">
          Run {runId.slice(0, 8)}...
        </span>
        <button
          className="run-detail-panel__close"
          onClick={onClose}
          aria-label="Close panel"
          title="Close"
        >
          &times;
        </button>
      </div>

      <div className="run-detail-panel__body">
        {loading && (
          <div className="run-detail-panel__loading">Loading run details...</div>
        )}
        {error && (
          <div className="run-detail-panel__section">
            <div className="run-detail-panel__error-msg">{error}</div>
          </div>
        )}
        {run && !loading && (
          <>
            {/* Run summary */}
            <div className="run-detail-panel__section">
              <div className="run-detail-panel__section-title">Summary</div>
              <div className="run-detail-panel__kv">
                <span className="run-detail-panel__kv-key">Status</span>
                <span className={statusClass(run.status)}>{run.status}</span>
              </div>
              <div className="run-detail-panel__kv">
                <span className="run-detail-panel__kv-key">Pipeline</span>
                <span className="run-detail-panel__kv-value">{run.pipeline_name}</span>
              </div>
              <div className="run-detail-panel__kv">
                <span className="run-detail-panel__kv-key">Environment</span>
                <span className="run-detail-panel__kv-value">{run.environment}</span>
              </div>
              <div className="run-detail-panel__kv">
                <span className="run-detail-panel__kv-key">Started</span>
                <span className="run-detail-panel__kv-value">{formatTime(startDate)}</span>
              </div>
              <div className="run-detail-panel__kv">
                <span className="run-detail-panel__kv-key">Duration</span>
                <span className="run-detail-panel__kv-value">{formatDuration(totalDuration)}</span>
              </div>
              {run.triggered_by && (
                <div className="run-detail-panel__kv">
                  <span className="run-detail-panel__kv-key">Triggered by</span>
                  <span className="run-detail-panel__kv-value">{run.triggered_by}</span>
                </div>
              )}
              {run.error && (
                <div className="run-detail-panel__error-msg">{run.error}</div>
              )}
            </div>

            {/* Gantt timeline */}
            <div className="run-detail-panel__section">
              <div className="run-detail-panel__section-title">Execution Timeline</div>
              <GanttTimeline
                run={run}
                apiNodes={apiNodes}
                selectedNodeId={selectedNodeId}
                onSelectNode={handleSelectNode}
              />
            </div>

            {/* Selected node detail */}
            {selectedStat && (
              <NodeDetail
                stat={selectedStat}
                apiNode={selectedApiNode}
                testResult={selectedTestResult}
                onJumpToNode={onJumpToNode}
                onViewFailureReport={onViewFailureReport}
                runId={runId}
              />
            )}
          </>
        )}
      </div>

      {/* Footer actions */}
      {run && !loading && (
        <div className="run-detail-panel__footer">
          {run.status === 'failed' && (
            <button
              className="run-detail-panel__action-btn run-detail-panel__action-btn--primary"
              onClick={handleJumpToFailingNode}
            >
              Jump to failing node
            </button>
          )}
          {onCompare && (
            <button
              className="run-detail-panel__action-btn"
              onClick={() => onCompare(runId)}
            >
              Compare with...
            </button>
          )}
          {onShowLineage && (
            <button
              className="run-detail-panel__action-btn"
              onClick={onShowLineage}
              title="Show upstream dependencies and downstream impact in lineage view"
            >
              Show Lineage
            </button>
          )}
        </div>
      )}
    </div>
  );
}
