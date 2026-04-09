// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Snapshot SCD2 diff preview panel (planning doc 28).
//!
//! Sibling to the `flux snapshot diff` CLI and the `SnapshotHistoryPanel`.
//! Runs the upstream pipeline as a dry-run on the server, classifies every
//! staged row against the snapshot target's current versions, and shows the
//! four counts (unchanged / changed / new / gone) plus a sample of affected
//! keys with classification badges. The headline differentiator vs.
//! dbt/Dagster — engineers can see exactly what a snapshot run would do
//! before any write touches prod.
//!
//! v1 is postgresql-only; non-postgres sinks return an actionable 400 from
//! the server which is surfaced inline. The server caps staged rows
//! materialized for the diff to avoid OOMing on huge sources; when that cap
//! is hit a banner explains it and points at the CLI for a full run.

import { useState } from 'react';
import {
  fetchSnapshotDiff,
  type SnapshotDiffResponse,
  type SnapshotDiffSample,
} from '../../api/pipelines';

export interface SnapshotDiffPanelProps {
  pipelineId?: string;
  nodeId?: string;
  environment?: string;
  /** Whether the canvas has unsaved changes — disables the preview button. */
  dirty?: boolean;
}

export function SnapshotDiffPanel({
  pipelineId,
  nodeId,
  environment,
  dirty,
}: SnapshotDiffPanelProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<SnapshotDiffResponse | null>(null);

  const canSubmit = !!pipelineId && !!nodeId && !dirty && !loading;

  const onPreview = async () => {
    if (!pipelineId || !nodeId) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetchSnapshotDiff(pipelineId, nodeId, { environment });
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  if (!pipelineId || !nodeId) {
    return (
      <div className="snapshot-diff-panel">
        <h4>Snapshot diff preview</h4>
        <p className="muted">Save the pipeline first to preview a snapshot diff.</p>
      </div>
    );
  }

  return (
    <div className="snapshot-diff-panel">
      <h4>Snapshot diff preview</h4>
      <p className="muted">
        Runs the upstream pipeline as a dry-run (no writes) and shows which rows would be
        inserted, closed, or left alone. The headline thing dbt and Dagster don't do.
      </p>

      <button type="button" disabled={!canSubmit} onClick={onPreview}>
        {loading
          ? 'Running dry-run…'
          : `Preview diff against ${environment ?? '(default env)'}`}
      </button>
      {dirty && (
        <p className="muted snapshot-diff-dirty-hint">
          Save the pipeline before previewing — unsaved edits aren't included in the dry-run.
        </p>
      )}

      {error && (
        <div className="snapshot-diff-error" role="alert">
          {error}
        </div>
      )}

      {result && <SnapshotDiffResult result={result} />}
    </div>
  );
}

function SnapshotDiffResult({ result }: { result: SnapshotDiffResponse }) {
  return (
    <div className="snapshot-diff-result">
      <div className="snapshot-diff-summary">
        Diffed against <code>{result.table}</code> (<code>{result.environment}</code>)
        {result.cached && <span className="snapshot-diff-cached"> · cached</span>}
      </div>

      <div className="snapshot-diff-counts">
        <DiffCount label="unchanged" count={result.stats.unchanged} kind="unchanged" />
        <DiffCount label="changed" count={result.stats.changed} kind="changed" />
        <DiffCount label="new" count={result.stats.new_versions} kind="new" />
        <DiffCount label="gone" count={result.stats.gone} kind="gone" />
      </div>

      {result.sample_truncated && (
        <div className="snapshot-diff-truncated" role="note">
          Diff computed against the first {result.staged_row_count.toLocaleString()} staged rows
          (cap is {result.staged_row_cap.toLocaleString()}). Use{' '}
          <code>flux snapshot diff</code> from the CLI for a full run.
        </div>
      )}

      {result.sample.length === 0 ? (
        <div className="snapshot-diff-empty">
          No rows would change — every staged row matches the current target version.
        </div>
      ) : (
        <table className="snapshot-diff-sample">
          <thead>
            <tr>
              <th>classification</th>
              <th>{result.unique_keys.join(', ')}</th>
            </tr>
          </thead>
          <tbody>
            {result.sample.map((s, i) => (
              <SampleRow key={i} sample={s} />
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function DiffCount({
  label,
  count,
  kind,
}: {
  label: string;
  count: number;
  kind: 'unchanged' | 'changed' | 'new' | 'gone';
}) {
  return (
    <div className={`snapshot-diff-count snapshot-diff-count-${kind}`}>
      <div className="snapshot-diff-count-value">{count.toLocaleString()}</div>
      <div className="snapshot-diff-count-label">{label}</div>
    </div>
  );
}

function SampleRow({ sample }: { sample: SnapshotDiffSample }) {
  return (
    <tr>
      <td>
        <span
          className={`snapshot-diff-badge snapshot-diff-badge-${sample.classification}`}
        >
          {sample.classification}
        </span>
      </td>
      <td>
        <code>{sample.unique_key.join(', ')}</code>
      </td>
    </tr>
  );
}
