// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Snapshot SCD2 history viewer (planning doc 28).
//!
//! Sibling to the `armillary snapshot history` CLI: given a business key, query
//! every historical version of that row from the snapshot target and render
//! a timeline showing valid_from / valid_to / is_current and the tracked
//! comparison-column values per version. Lets engineers tired of the
//! dbt/Dagster opaque-history experience answer "what did this customer's
//! plan look like on 2025-03-14?" without leaving the canvas.
//!
//! v1 is postgresql-only — non-postgres connectors return an actionable
//! 400 from the server and we surface it inline.

import { useState } from 'react';
import {
  fetchSnapshotHistory,
  type MaterializationPolicy,
  type SnapshotHistoryResponse,
} from '../../api/pipelines';

export interface SnapshotHistoryPanelProps {
  pipelineId?: string;
  nodeId?: string;
  environment?: string;
  policy: MaterializationPolicy;
}

export function SnapshotHistoryPanel({
  pipelineId,
  nodeId,
  environment,
  policy,
}: SnapshotHistoryPanelProps) {
  const uniqueKeys = policy.unique_keys ?? [];
  const [keyValues, setKeyValues] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<SnapshotHistoryResponse | null>(null);

  const canSubmit =
    !!pipelineId &&
    !!nodeId &&
    uniqueKeys.length > 0 &&
    uniqueKeys.every((k) => (keyValues[k] ?? '').trim().length > 0) &&
    !loading;

  const onLookup = async () => {
    if (!pipelineId || !nodeId) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const trimmed: Record<string, string> = {};
      for (const k of uniqueKeys) trimmed[k] = (keyValues[k] ?? '').trim();
      const res = await fetchSnapshotHistory(pipelineId, nodeId, trimmed, environment);
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  if (uniqueKeys.length === 0) {
    return (
      <div className="snapshot-history-panel">
        <h4>Snapshot history</h4>
        <p className="muted">
          Define <code>unique_keys</code> on the materialization above to look up version history.
        </p>
      </div>
    );
  }

  if (!pipelineId || !nodeId) {
    return (
      <div className="snapshot-history-panel">
        <h4>Snapshot history</h4>
        <p className="muted">Save the pipeline first to query snapshot history.</p>
      </div>
    );
  }

  return (
    <div className="snapshot-history-panel">
      <h4>Snapshot history</h4>
      <p className="muted">
        Look up every SCD2 version of a single row in this snapshot target. Reads from postgres
        directly — no upstream execution, no writes.
      </p>

      <div className="snapshot-history-key-form">
        {uniqueKeys.map((k) => (
          <label key={k} className="snapshot-history-key-field">
            <span>{k}</span>
            <input
              type="text"
              value={keyValues[k] ?? ''}
              onChange={(e) => setKeyValues({ ...keyValues, [k]: e.target.value })}
              placeholder={`value for ${k}`}
            />
          </label>
        ))}
        <button type="button" disabled={!canSubmit} onClick={onLookup}>
          {loading ? 'Loading…' : 'Show history'}
        </button>
      </div>

      {error && (
        <div className="snapshot-history-error" role="alert">
          {error}
        </div>
      )}

      {result && <SnapshotHistoryTimeline result={result} />}
    </div>
  );
}

function SnapshotHistoryTimeline({ result }: { result: SnapshotHistoryResponse }) {
  if (result.version_count === 0) {
    return (
      <div className="snapshot-history-empty">
        No rows in <code>{result.table}</code> match that key.
      </div>
    );
  }
  return (
    <div className="snapshot-history-timeline">
      <div className="snapshot-history-summary">
        {result.version_count} version{result.version_count === 1 ? '' : 's'} in{' '}
        <code>{result.table}</code>
      </div>
      <ol>
        {result.versions.map((v) => (
          <li
            key={v.armillary_scd_id}
            className={v.armillary_is_current ? 'version current' : 'version closed'}
          >
            <div className="version-header">
              <span className="version-badge">
                {v.armillary_is_current ? 'current' : 'closed'}
              </span>
              <code className="version-scd-id">{v.armillary_scd_id}</code>
            </div>
            <div className="version-validity">
              <span>
                <strong>from</strong> {v.armillary_valid_from}
              </span>
              <span>
                <strong>to</strong> {v.armillary_valid_to ?? '—'}
              </span>
            </div>
            {result.comparison_columns.length > 0 && (
              <table className="version-comparison">
                <tbody>
                  {result.comparison_columns.map((col) => (
                    <tr key={col}>
                      <th>{col}</th>
                      <td>{v.comparison[col] ?? ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </li>
        ))}
      </ol>
    </div>
  );
}
