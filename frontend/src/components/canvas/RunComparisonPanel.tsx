// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import {
  compareRuns,
  type ApiRunComparison,
} from '../../api/runs';
import {
  fetchPipelineRuns,
  type ApiPipelineRun,
} from '../../api/pipelines';
import { usePipelineStore } from '../../stores/pipelineStore';
import './RunComparisonPanel.css';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDuration(ms: number | undefined): string {
  if (ms == null) return '—';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function formatDelta(delta: number | undefined): { text: string; cls: string } {
  if (delta == null) return { text: '—', cls: 'run-compare-panel__delta--zero' };
  if (delta === 0) return { text: '0', cls: 'run-compare-panel__delta--zero' };
  const sign = delta > 0 ? '+' : '';
  const cls = delta > 0 ? 'run-compare-panel__delta--positive' : 'run-compare-panel__delta--negative';
  return { text: `${sign}${delta.toLocaleString()}`, cls };
}

function formatDeltaDuration(delta: number | undefined): { text: string; cls: string } {
  if (delta == null) return { text: '—', cls: 'run-compare-panel__delta--zero' };
  if (delta === 0) return { text: '0', cls: 'run-compare-panel__delta--zero' };
  const sign = delta > 0 ? '+' : '';
  const cls = delta > 0 ? 'run-compare-panel__delta--positive' : 'run-compare-panel__delta--negative';
  return { text: `${sign}${formatDuration(Math.abs(delta))}`, cls };
}

function statusClass(status: string): string {
  return `run-compare-panel__status run-compare-panel__status--${status}`;
}

function formatRunDate(run: ApiPipelineRun): string {
  if (!run.start_time) return '—';
  return new Date(run.start_time).toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

// ---------------------------------------------------------------------------
// Run picker (shown when user hasn't selected the comparison target yet)
// ---------------------------------------------------------------------------

interface RunPickerProps {
  currentRunId: string;
  onSelect: (runId: string) => void;
}

function RunPicker({ currentRunId, onSelect }: RunPickerProps) {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const [runs, setRuns] = useState<ApiPipelineRun[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!pipelineId) return;
    setLoading(true);
    fetchPipelineRuns(pipelineId, 20, 0)
      .then((r) => setRuns(r.filter((run) => run.id !== currentRunId)))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [pipelineId, currentRunId]);

  if (loading) {
    return <div className="run-compare-panel__loading">Loading runs...</div>;
  }

  if (runs.length === 0) {
    return <div className="run-compare-panel__loading">No other runs to compare with</div>;
  }

  return (
    <div className="run-compare-panel__picker">
      <div className="run-compare-panel__picker-label">
        Select a run to compare with:
      </div>
      <ul className="run-compare-panel__run-list">
        {runs.map((run) => (
          <li
            key={run.id}
            className="run-compare-panel__run-item"
            onClick={() => onSelect(run.id)}
          >
            <span className={statusClass(run.status)}>{run.status}</span>
            <span>{run.id.slice(0, 8)}...</span>
            <span style={{ color: 'var(--text, #64748b)', fontSize: 11 }}>
              {formatRunDate(run)}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Comparison results
// ---------------------------------------------------------------------------

interface ComparisonResultsProps {
  comparison: ApiRunComparison;
  apiNodeNames: Map<string, string>;
}

function ComparisonResults({ comparison, apiNodeNames }: ComparisonResultsProps) {
  const durDelta = formatDeltaDuration(comparison.duration_delta_ms);
  const rowsDelta = formatDelta(comparison.total_rows_out_delta);

  return (
    <>
      {/* Overall summary */}
      <div className="run-compare-panel__section">
        <div className="run-compare-panel__section-title">Overall</div>
        <table className="run-compare-panel__table">
          <thead>
            <tr>
              <th></th>
              <th>Run A</th>
              <th>Run B</th>
              <th>Delta</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Status</td>
              <td><span className={statusClass(comparison.status_a)}>{comparison.status_a}</span></td>
              <td><span className={statusClass(comparison.status_b)}>{comparison.status_b}</span></td>
              <td></td>
            </tr>
            <tr>
              <td>Duration</td>
              <td>{formatDuration(comparison.duration_ms_a)}</td>
              <td>{formatDuration(comparison.duration_ms_b)}</td>
              <td className={durDelta.cls}>{durDelta.text}</td>
            </tr>
            <tr>
              <td>Total rows</td>
              <td>{comparison.total_rows_out_a.toLocaleString()}</td>
              <td>{comparison.total_rows_out_b.toLocaleString()}</td>
              <td className={rowsDelta.cls}>{rowsDelta.text}</td>
            </tr>
          </tbody>
        </table>
      </div>

      {/* Per-node comparison */}
      <div className="run-compare-panel__section">
        <div className="run-compare-panel__section-title">
          Node Comparison ({comparison.node_comparisons.length})
        </div>
        <table className="run-compare-panel__table" data-testid="node-comparison-table">
          <thead>
            <tr>
              <th>Node</th>
              <th>Dur A</th>
              <th>Dur B</th>
              <th>Rows A</th>
              <th>Rows B</th>
              <th>Delta</th>
            </tr>
          </thead>
          <tbody>
            {comparison.node_comparisons.map((nc) => {
              const durD = formatDeltaDuration(nc.duration_delta_ms);
              const rowsD = formatDelta(nc.rows_out_delta);
              return (
                <tr key={nc.node_id}>
                  <td>{apiNodeNames.get(nc.node_id) ?? nc.node_id}</td>
                  <td>{formatDuration(nc.duration_ms_a)}</td>
                  <td>{formatDuration(nc.duration_ms_b)}</td>
                  <td>{nc.rows_out_a?.toLocaleString() ?? '—'}</td>
                  <td>{nc.rows_out_b?.toLocaleString() ?? '—'}</td>
                  <td>
                    <span className={durD.cls} title="Duration delta">{durD.text}</span>
                    {' / '}
                    <span className={rowsD.cls} title="Rows delta">{rowsD.text}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Test comparison */}
      {comparison.test_comparisons.length > 0 && (
        <div className="run-compare-panel__section">
          <div className="run-compare-panel__section-title">
            Test Results ({comparison.test_comparisons.length})
          </div>
          <table className="run-compare-panel__table" data-testid="test-comparison-table">
            <thead>
              <tr>
                <th>Test</th>
                <th>Run A</th>
                <th>Run B</th>
                <th>Changed</th>
              </tr>
            </thead>
            <tbody>
              {comparison.test_comparisons.map((tc) => (
                <tr
                  key={tc.node_id}
                  className={tc.changed ? 'run-compare-panel__test-changed' : ''}
                >
                  <td>{apiNodeNames.get(tc.node_id) ?? tc.node_id}</td>
                  <td>
                    <span className={tc.passed_a ? 'run-compare-panel__test-pass' : 'run-compare-panel__test-fail'}>
                      {tc.passed_a == null ? '—' : tc.passed_a ? 'pass' : 'fail'}
                    </span>
                  </td>
                  <td>
                    <span className={tc.passed_b ? 'run-compare-panel__test-pass' : 'run-compare-panel__test-fail'}>
                      {tc.passed_b == null ? '—' : tc.passed_b ? 'pass' : 'fail'}
                    </span>
                  </td>
                  <td>{tc.changed ? 'Yes' : ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Main RunComparisonPanel
// ---------------------------------------------------------------------------

interface RunComparisonPanelProps {
  /** The "base" run ID (Run A). */
  runId: string;
  open: boolean;
  onClose: () => void;
}

export function RunComparisonPanel({
  runId,
  open,
  onClose,
}: RunComparisonPanelProps) {
  const [otherRunId, setOtherRunId] = useState<string | null>(null);
  const [comparison, setComparison] = useState<ApiRunComparison | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const apiPipeline = usePipelineStore((s) => s.apiPipeline);
  const apiNodeNames = new Map<string, string>();
  for (const n of apiPipeline?.nodes ?? []) {
    apiNodeNames.set(n.id, n.name);
  }

  // Reset state when panel opens with a new run
  useEffect(() => {
    if (!open) return;
    setOtherRunId(null);
    setComparison(null);
    setError(null);
  }, [runId, open]);

  // Fetch comparison when other run is selected
  useEffect(() => {
    if (!open || !otherRunId) return;
    setLoading(true);
    setError(null);

    compareRuns(runId, otherRunId)
      .then((c) => setComparison(c))
      .catch((e) => setError((e as Error).message))
      .finally(() => setLoading(false));
  }, [runId, otherRunId, open]);

  const handleSelectOther = useCallback((id: string) => {
    setOtherRunId(id);
  }, []);

  return (
    <div
      className={`run-compare-panel${open ? ' run-compare-panel--open' : ''}`}
      data-testid="run-comparison-panel"
    >
      <div className="run-compare-panel__header">
        <span className="run-compare-panel__title">
          Compare Runs
        </span>
        <button
          className="run-compare-panel__close"
          onClick={onClose}
          aria-label="Close panel"
          title="Close"
        >
          &times;
        </button>
      </div>

      <div className="run-compare-panel__body">
        {!otherRunId && (
          <RunPicker
            currentRunId={runId}
            onSelect={handleSelectOther}
          />
        )}

        {loading && (
          <div className="run-compare-panel__loading">Comparing runs...</div>
        )}

        {error && (
          <div className="run-compare-panel__section">
            <div className="run-compare-panel__error-msg">{error}</div>
          </div>
        )}

        {comparison && !loading && (
          <ComparisonResults
            comparison={comparison}
            apiNodeNames={apiNodeNames}
          />
        )}
      </div>
    </div>
  );
}
