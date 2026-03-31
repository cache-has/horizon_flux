// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import { usePipelineStore } from '../../stores/pipelineStore';
import {
  fetchVersions,
  fetchVersion,
  restoreVersion,
  type ApiVersionSummary,
  type ApiPipeline,
} from '../../api/pipelines';
import { VersionDiffModal } from './VersionDiffModal';
import { VersionViewModal } from './VersionViewModal';
import './VersionHistoryPanel.css';

interface VersionHistoryPanelProps {
  open: boolean;
  onClose: () => void;
}

type DiffState = {
  leftVersion: number;
  rightVersion: number;
  leftLabel: string;
  rightLabel: string;
  leftJson: string;
  rightJson: string;
};

function formatTimestamp(ms: number): string {
  return new Date(ms).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

/** Serialize a pipeline snapshot to deterministic pretty JSON for diffing. */
function snapshotToJson(pipeline: ApiPipeline): string {
  return JSON.stringify(pipeline, null, 2);
}

export function VersionHistoryPanel({ open, onClose }: VersionHistoryPanelProps) {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const apiPipeline = usePipelineStore((s) => s.apiPipeline);
  const loadPipeline = usePipelineStore((s) => s.loadPipeline);

  const [versions, setVersions] = useState<ApiVersionSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [restoring, setRestoring] = useState<number | null>(null);

  // Two-version selection for comparison
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [diffState, setDiffState] = useState<DiffState | null>(null);
  const [diffLoading, setDiffLoading] = useState(false);

  // Single-version view state
  const [viewState, setViewState] = useState<{ label: string; json: string } | null>(null);
  const [viewLoading, setViewLoading] = useState<number | null>(null);

  // Load versions when panel opens
  useEffect(() => {
    if (!open || !pipelineId || pipelineId === 'demo') return;
    setLoading(true);
    setError(null);
    setSelected(new Set());
    fetchVersions(pipelineId, 100, 0)
      .then((res) => setVersions(res.data))
      .catch((err) => setError((err as Error).message))
      .finally(() => setLoading(false));
  }, [open, pipelineId]);

  const toggleSelect = useCallback((version: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(version)) {
        next.delete(version);
      } else {
        // Max two selections
        if (next.size >= 2) {
          // Replace the oldest selection
          const [first] = next;
          next.delete(first);
        }
        next.add(version);
      }
      return next;
    });
  }, []);

  const handleCompare = useCallback(async () => {
    if (!pipelineId || selected.size < 1) return;
    setDiffLoading(true);

    try {
      const sorted = [...selected].sort((a, b) => a - b);

      if (sorted.length === 1) {
        // Compare selected version against current
        const ver = sorted[0];
        const res = await fetchVersion(pipelineId, ver);
        setDiffState({
          leftVersion: ver,
          rightVersion: apiPipeline?.version ?? ver,
          leftLabel: `v${ver}`,
          rightLabel: 'Current',
          leftJson: snapshotToJson(res.snapshot),
          rightJson: apiPipeline ? snapshotToJson(apiPipeline) : '',
        });
      } else {
        // Compare two selected versions
        const [v1, v2] = sorted;
        const [res1, res2] = await Promise.all([
          fetchVersion(pipelineId, v1),
          fetchVersion(pipelineId, v2),
        ]);
        setDiffState({
          leftVersion: v1,
          rightVersion: v2,
          leftLabel: `v${v1}`,
          rightLabel: `v${v2}`,
          leftJson: snapshotToJson(res1.snapshot),
          rightJson: snapshotToJson(res2.snapshot),
        });
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setDiffLoading(false);
    }
  }, [pipelineId, selected, apiPipeline]);

  const handleRestore = useCallback(
    async (version: number) => {
      if (!pipelineId) return;
      if (!window.confirm(`Restore pipeline to version ${version}? This creates a new version.`))
        return;

      setRestoring(version);
      try {
        await restoreVersion(pipelineId, version);
        await loadPipeline(pipelineId);
        // Refresh version list
        const res = await fetchVersions(pipelineId, 100, 0);
        setVersions(res.data);
        setSelected(new Set());
      } catch (err) {
        setError((err as Error).message);
      } finally {
        setRestoring(null);
      }
    },
    [pipelineId, loadPipeline],
  );

  const handleView = useCallback(
    async (version: number) => {
      if (!pipelineId) return;
      setViewLoading(version);
      try {
        // For the current version, use the in-memory pipeline; otherwise fetch
        if (version === apiPipeline?.version && apiPipeline) {
          setViewState({ label: `v${version} (current)`, json: snapshotToJson(apiPipeline) });
        } else {
          const res = await fetchVersion(pipelineId, version);
          setViewState({ label: `v${version}`, json: snapshotToJson(res.snapshot) });
        }
      } catch (err) {
        setError((err as Error).message);
      } finally {
        setViewLoading(null);
      }
    },
    [pipelineId, apiPipeline],
  );

  const closeDiff = useCallback(() => setDiffState(null), []);
  const closeView = useCallback(() => setViewState(null), []);

  const currentVersion = apiPipeline?.version;

  return (
    <>
      <div className={`version-panel${open ? ' version-panel--open' : ''}`}>
        <div className="version-panel__header">
          <h3 className="version-panel__title">Version History</h3>
          <button className="version-panel__close" onClick={onClose} title="Close">
            &times;
          </button>
        </div>

        <div className="version-panel__actions">
          <button
            className="version-panel__compare-btn"
            disabled={selected.size === 0 || diffLoading}
            onClick={handleCompare}
            title={
              selected.size === 0
                ? 'Select 1-2 versions to compare'
                : selected.size === 1
                  ? 'Compare selected version with current'
                  : 'Compare two selected versions'
            }
          >
            {diffLoading
              ? 'Loading...'
              : selected.size <= 1
                ? 'Compare with Current'
                : 'Compare Selected'}
          </button>
        </div>

        <div className="version-panel__body">
          {loading && <div className="version-panel__loading">Loading...</div>}
          {error && <p className="version-panel__error">{error}</p>}

          {!loading && versions.length === 0 && !error && (
            <div className="version-panel__empty">No version history available.</div>
          )}

          {!loading &&
            versions.map((v) => {
              const isCurrent = v.version === currentVersion;
              const isSelected = selected.has(v.version);
              return (
                <div
                  key={v.version}
                  className={`version-panel__item${isSelected ? ' version-panel__item--selected' : ''}`}
                >
                  <label className="version-panel__checkbox-label">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => toggleSelect(v.version)}
                      className="version-panel__checkbox"
                    />
                  </label>
                  <div className="version-panel__item-info">
                    <span className="version-panel__version">
                      v{v.version}
                      {isCurrent && (
                        <span className="version-panel__current-badge">current</span>
                      )}
                    </span>
                    <span className="version-panel__time">
                      {formatTimestamp(v.saved_at)}
                    </span>
                  </div>
                  <button
                    className="version-panel__view-btn"
                    onClick={() => handleView(v.version)}
                    disabled={viewLoading !== null}
                  >
                    {viewLoading === v.version ? '...' : 'View'}
                  </button>
                  {!isCurrent && (
                    <button
                      className="version-panel__restore-btn"
                      onClick={() => handleRestore(v.version)}
                      disabled={restoring !== null}
                    >
                      {restoring === v.version ? 'Restoring...' : 'Restore'}
                    </button>
                  )}
                </div>
              );
            })}
        </div>
      </div>

      {diffState && (
        <VersionDiffModal
          leftLabel={diffState.leftLabel}
          rightLabel={diffState.rightLabel}
          leftJson={diffState.leftJson}
          rightJson={diffState.rightJson}
          onClose={closeDiff}
        />
      )}

      {viewState && (
        <VersionViewModal
          versionLabel={viewState.label}
          json={viewState.json}
          onClose={closeView}
        />
      )}
    </>
  );
}
