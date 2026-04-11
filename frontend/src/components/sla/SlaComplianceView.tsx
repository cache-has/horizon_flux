// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * SLA Compliance Dashboard — shows a grid of resources with SLA status,
 * filterable by status/tag/owner, sortable by most overdue (planning doc 38).
 */

import { useEffect, useMemo } from 'react';
import { useSlaStore, sortSlaEntries } from '../../stores/slaStore';
import type { SlaStatusKind } from '../../api/sla';
import type { SlaStatusEntry } from '../../api/sla';
import type { SlaSortField } from '../../stores/slaStore';
import './SlaComplianceView.css';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface SlaComplianceViewProps {
  onBack: () => void;
  onNavigateToPipeline?: (id: string) => void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const STATUS_LABELS: Record<SlaStatusKind, string> = {
  ok: 'OK',
  warning: 'Warning',
  breach: 'Breach',
  unknown: 'No Data',
};

function formatDuration(iso?: string): string {
  if (!iso) return '\u2014';
  // Simple PT..H..M..S display
  const match = iso.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/);
  if (!match) return iso;
  const parts: string[] = [];
  if (match[1]) parts.push(`${match[1]}h`);
  if (match[2]) parts.push(`${match[2]}m`);
  if (match[3]) parts.push(`${match[3]}s`);
  return parts.join(' ') || '0s';
}

function formatTimestamp(iso?: string): string {
  if (!iso) return '\u2014';
  return new Date(iso).toLocaleString();
}

// ---------------------------------------------------------------------------
// Main View
// ---------------------------------------------------------------------------

export function SlaComplianceView({ onBack, onNavigateToPipeline }: SlaComplianceViewProps) {
  const selectedFingerprint = useSlaStore((s) => s.selectedFingerprint);

  if (selectedFingerprint) {
    return (
      <SlaHistoryView
        onBack={() => useSlaStore.getState().clearSelection()}
        onNavigateToPipeline={onNavigateToPipeline}
      />
    );
  }

  return <SlaStatusList onBack={onBack} onNavigateToPipeline={onNavigateToPipeline} />;
}

// ---------------------------------------------------------------------------
// Status List (main dashboard grid)
// ---------------------------------------------------------------------------

function SlaStatusList({ onBack, onNavigateToPipeline }: SlaComplianceViewProps) {
  const entries = useSlaStore((s) => s.entries);
  const total = useSlaStore((s) => s.total);
  const loading = useSlaStore((s) => s.loading);
  const error = useSlaStore((s) => s.error);
  const statusFilter = useSlaStore((s) => s.statusFilter);
  const tagFilter = useSlaStore((s) => s.tagFilter);
  const ownerFilter = useSlaStore((s) => s.ownerFilter);
  const sortField = useSlaStore((s) => s.sortField);
  const sortAsc = useSlaStore((s) => s.sortAsc);
  const fetchStatus = useSlaStore((s) => s.fetchStatus);
  const setStatusFilter = useSlaStore((s) => s.setStatusFilter);
  const setTagFilter = useSlaStore((s) => s.setTagFilter);
  const setOwnerFilter = useSlaStore((s) => s.setOwnerFilter);
  const setSortField = useSlaStore((s) => s.setSortField);
  const selectResource = useSlaStore((s) => s.selectResource);

  useEffect(() => {
    void fetchStatus();
  }, [fetchStatus]);

  // Re-fetch when filters change.
  useEffect(() => {
    void fetchStatus();
  }, [statusFilter, tagFilter, ownerFilter, fetchStatus]);

  const sorted = useMemo(
    () => sortSlaEntries(entries, sortField, sortAsc),
    [entries, sortField, sortAsc],
  );

  // Derive unique tags and owners for filter dropdowns.
  const [allTags, allOwners] = useMemo(() => {
    const tags = new Set<string>();
    const owners = new Set<string>();
    for (const e of entries) {
      for (const t of e.tags) tags.add(t);
      if (e.owner) owners.add(e.owner);
    }
    return [[...tags].sort(), [...owners].sort()];
  }, [entries]);

  // Summary counts.
  const counts = useMemo(() => {
    const c = { ok: 0, warning: 0, breach: 0, unknown: 0 };
    for (const e of entries) c[e.status]++;
    return c;
  }, [entries]);

  const sortIndicator = (field: SlaSortField) => {
    if (sortField !== field) return '';
    return sortAsc ? ' \u25B2' : ' \u25BC';
  };

  return (
    <div className="sla-view">
      <div className="sla-view__toolbar">
        <button className="sla-view__back-btn" onClick={onBack}>Back</button>
        <span className="sla-view__title">SLA Compliance</span>
        <span className="sla-view__count">{total} resources</span>
      </div>

      {/* Summary badges */}
      <div className="sla-view__summary">
        <SummaryBadge label="Breach" count={counts.breach} status="breach" />
        <SummaryBadge label="Warning" count={counts.warning} status="warning" />
        <SummaryBadge label="OK" count={counts.ok} status="ok" />
        <SummaryBadge label="No Data" count={counts.unknown} status="unknown" />
      </div>

      {/* Filters */}
      <div className="sla-view__filters">
        <span className="sla-view__filter-label">Status:</span>
        <select
          className="sla-view__filter-select"
          value={statusFilter ?? ''}
          onChange={(e) => setStatusFilter((e.target.value || null) as SlaStatusKind | null)}
        >
          <option value="">All</option>
          <option value="breach">Breach</option>
          <option value="warning">Warning</option>
          <option value="ok">OK</option>
          <option value="unknown">No Data</option>
        </select>

        <span className="sla-view__filter-label">Tag:</span>
        <select
          className="sla-view__filter-select"
          value={tagFilter ?? ''}
          onChange={(e) => setTagFilter(e.target.value || null)}
        >
          <option value="">All</option>
          {allTags.map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>

        <span className="sla-view__filter-label">Owner:</span>
        <select
          className="sla-view__filter-select"
          value={ownerFilter ?? ''}
          onChange={(e) => setOwnerFilter(e.target.value || null)}
        >
          <option value="">All</option>
          {allOwners.map((o) => (
            <option key={o} value={o}>{o}</option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div className="sla-view__body">
        {error && <div className="sla-view__error">{error}</div>}
        {loading && entries.length === 0 && (
          <div className="sla-view__empty">Loading SLA data...</div>
        )}
        {!loading && sorted.length === 0 && (
          <div className="sla-view__empty">
            No resources with SLAs found. Declare freshness SLAs in resource annotation YAML files.
          </div>
        )}

        {sorted.length > 0 && (
          <table className="sla-view__table">
            <thead>
              <tr>
                <th className="sla-view__th" onClick={() => setSortField('name')}>
                  Resource{sortIndicator('name')}
                </th>
                <th className="sla-view__th" onClick={() => setSortField('status')}>
                  Status{sortIndicator('status')}
                </th>
                <th className="sla-view__th" onClick={() => setSortField('age')}>
                  Age{sortIndicator('age')}
                </th>
                <th className="sla-view__th" onClick={() => setSortField('max_age')}>
                  Max Age{sortIndicator('max_age')}
                </th>
                <th className="sla-view__th">Producer</th>
                <th className="sla-view__th">Last Success</th>
                <th className="sla-view__th">Tags</th>
                <th className="sla-view__th">Owner</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((entry) => (
                <SlaRow
                  key={entry.fingerprint}
                  entry={entry}
                  onSelect={() => selectResource(entry.fingerprint)}
                  onNavigateToPipeline={onNavigateToPipeline}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary Badge
// ---------------------------------------------------------------------------

function SummaryBadge({
  label,
  count,
  status,
}: {
  label: string;
  count: number;
  status: SlaStatusKind;
}) {
  return (
    <div className={`sla-view__summary-badge sla-view__summary-badge--${status}`}>
      <span className="sla-view__summary-count">{count}</span>
      <span className="sla-view__summary-label">{label}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Table Row
// ---------------------------------------------------------------------------

function SlaRow({
  entry,
  onSelect,
  onNavigateToPipeline,
}: {
  entry: SlaStatusEntry;
  onSelect: () => void;
  onNavigateToPipeline?: (id: string) => void;
}) {
  return (
    <tr className="sla-view__row" onClick={onSelect}>
      <td className="sla-view__td sla-view__td--name">
        <div className="sla-view__entry-name">{entry.name}</div>
        <div className="sla-view__entry-fp">{entry.fingerprint}</div>
      </td>
      <td className="sla-view__td">
        <span className={`sla-view__status-badge sla-view__status-badge--${entry.status}`}>
          {STATUS_LABELS[entry.status]}
        </span>
      </td>
      <td className="sla-view__td sla-view__td--mono">{formatDuration(entry.age)}</td>
      <td className="sla-view__td sla-view__td--mono">{formatDuration(entry.max_age)}</td>
      <td className="sla-view__td">
        {entry.producer_pipeline ? (
          <span
            className="sla-view__pipeline-link"
            onClick={(e) => {
              e.stopPropagation();
              onNavigateToPipeline?.(entry.producer_pipeline!);
            }}
          >
            {entry.producer_pipeline}
          </span>
        ) : (
          '\u2014'
        )}
      </td>
      <td className="sla-view__td">{formatTimestamp(entry.last_success_at)}</td>
      <td className="sla-view__td">
        {entry.tags.map((t) => (
          <span key={t} className="sla-view__tag">{t}</span>
        ))}
        {entry.tags.length === 0 && '\u2014'}
      </td>
      <td className="sla-view__td">{entry.owner ?? '\u2014'}</td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// SLA History View (per-resource)
// ---------------------------------------------------------------------------

function SlaHistoryView({
  onBack,
  onNavigateToPipeline,
}: {
  onBack: () => void;
  onNavigateToPipeline?: (id: string) => void;
}) {
  const fingerprint = useSlaStore((s) => s.selectedFingerprint)!;
  const history = useSlaStore((s) => s.selectedHistory);
  const historyLoading = useSlaStore((s) => s.historyLoading);
  const entries = useSlaStore((s) => s.entries);

  // Find the current entry for header info.
  const current = entries.find((e) => e.fingerprint === fingerprint);

  return (
    <div className="sla-view">
      <div className="sla-view__toolbar">
        <button className="sla-view__back-btn" onClick={onBack}>Back to Dashboard</button>
        <span className="sla-view__title">{current?.name ?? fingerprint}</span>
        {current && (
          <span className={`sla-view__status-badge sla-view__status-badge--${current.status}`}>
            {STATUS_LABELS[current.status]}
          </span>
        )}
      </div>

      {/* Current status summary */}
      {current && (
        <div className="sla-view__detail-summary">
          <div className="sla-view__detail-item">
            <span className="sla-view__detail-label">Current Age</span>
            <span className="sla-view__detail-value">{formatDuration(current.age)}</span>
          </div>
          <div className="sla-view__detail-item">
            <span className="sla-view__detail-label">Max Age</span>
            <span className="sla-view__detail-value">{formatDuration(current.max_age)}</span>
          </div>
          {current.warn_at && (
            <div className="sla-view__detail-item">
              <span className="sla-view__detail-label">Warn At</span>
              <span className="sla-view__detail-value">{formatDuration(current.warn_at)}</span>
            </div>
          )}
          <div className="sla-view__detail-item">
            <span className="sla-view__detail-label">Last Success</span>
            <span className="sla-view__detail-value">{formatTimestamp(current.last_success_at)}</span>
          </div>
          {current.producer_pipeline && (
            <div className="sla-view__detail-item">
              <span className="sla-view__detail-label">Producer</span>
              <span
                className="sla-view__pipeline-link"
                onClick={() => onNavigateToPipeline?.(current.producer_pipeline!)}
              >
                {current.producer_pipeline}
              </span>
            </div>
          )}
        </div>
      )}

      {/* History table */}
      <div className="sla-view__body">
        <h3 className="sla-view__section-title">Evaluation History</h3>
        {historyLoading && <div className="sla-view__empty">Loading history...</div>}
        {!historyLoading && history.length === 0 && (
          <div className="sla-view__empty">No evaluation history available.</div>
        )}
        {history.length > 0 && (
          <table className="sla-view__table">
            <thead>
              <tr>
                <th className="sla-view__th">Evaluated At</th>
                <th className="sla-view__th">Status</th>
                <th className="sla-view__th">Age</th>
                <th className="sla-view__th">Max Age</th>
                <th className="sla-view__th">Last Success</th>
              </tr>
            </thead>
            <tbody>
              {history.map((eval_, i) => (
                <tr key={`${eval_.evaluated_at}-${i}`} className="sla-view__row sla-view__row--static">
                  <td className="sla-view__td">{formatTimestamp(eval_.evaluated_at)}</td>
                  <td className="sla-view__td">
                    <span className={`sla-view__status-badge sla-view__status-badge--${eval_.status}`}>
                      {STATUS_LABELS[eval_.status]}
                    </span>
                  </td>
                  <td className="sla-view__td sla-view__td--mono">{formatDuration(eval_.age)}</td>
                  <td className="sla-view__td sla-view__td--mono">{formatDuration(eval_.max_age)}</td>
                  <td className="sla-view__td">{formatTimestamp(eval_.last_success_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
