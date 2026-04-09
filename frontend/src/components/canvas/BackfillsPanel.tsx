// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useBackfillStore } from '../../stores/backfillStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useEnvironmentStore } from '../../stores/environmentStore';
import type {
  BackfillResponse,
  BackfillIteration,
  CreateBackfillRequest,
  RangeDefinition,
  DateGranularity,
  BackfillProgress,
} from '../../api/backfills';
import './SystemInfoPanel.css';
import './BackfillsPanel.css';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const STATUS_LABELS: Record<string, string> = {
  pending: 'Pending',
  running: 'Running',
  completed: 'Completed',
  cancelled: 'Cancelled',
  failed: 'Failed',
  succeeded: 'Succeeded',
  skipped: 'Skipped',
};

function formatTime(iso?: string): string {
  if (!iso) return '—';
  const dt = new Date(iso);
  return dt.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function progressPercent(p?: BackfillProgress): number {
  if (!p || p.total === 0) return 0;
  return Math.round(((p.succeeded + p.failed + p.skipped) / p.total) * 100);
}

function rangeLabel(rd: RangeDefinition): string {
  switch (rd.kind) {
    case 'date_range':
      return `${rd.start} .. ${rd.end} (${rd.granularity})`;
    case 'list':
      return rd.values.length <= 5
        ? rd.values.join(', ')
        : `${rd.values.slice(0, 3).join(', ')} +${rd.values.length - 3} more`;
    case 'sql':
      return `SQL: ${rd.query.slice(0, 60)}${rd.query.length > 60 ? '...' : ''}`;
  }
}

// ---------------------------------------------------------------------------
// Backfill Editor (Create form)
// ---------------------------------------------------------------------------

interface BackfillEditorProps {
  pipelineId: string;
  environment: string;
  onSave: (req: CreateBackfillRequest) => Promise<void>;
  onCancel: () => void;
}

function BackfillEditor({ pipelineId, environment, onSave, onCancel }: BackfillEditorProps) {
  const [rangeKind, setRangeKind] = useState<'date_range' | 'list' | 'sql'>('date_range');
  const [concurrency, setConcurrency] = useState(1);
  const [failFast, setFailFast] = useState(false);
  const [fullRefresh, setFullRefresh] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Date range fields
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [granularity, setGranularity] = useState<DateGranularity>('day');
  const [dateVarStart, setDateVarStart] = useState('run_date');
  const [dateVarEnd, setDateVarEnd] = useState('next_date');

  // List fields
  const [listValues, setListValues] = useState('');
  const [listVarName, setListVarName] = useState('value');

  // SQL fields
  const [sqlConnection, setSqlConnection] = useState('metadata');
  const [sqlQuery, setSqlQuery] = useState('');
  const [sqlVarMapping, setSqlVarMapping] = useState('');

  const buildRange = useCallback((): RangeDefinition => {
    switch (rangeKind) {
      case 'date_range': {
        const mapping: Record<string, string> = {};
        if (dateVarStart.trim()) mapping[dateVarStart.trim()] = '$iteration.start';
        if (dateVarEnd.trim()) mapping[dateVarEnd.trim()] = '$iteration.end';
        return { kind: 'date_range', start: startDate, end: endDate, granularity, variable_mapping: mapping };
      }
      case 'list': {
        const values = listValues.split(',').map((v) => v.trim()).filter(Boolean);
        const mapping: Record<string, string> = {};
        if (listVarName.trim()) mapping[listVarName.trim()] = '$iteration.value';
        return { kind: 'list', values, variable_mapping: mapping };
      }
      case 'sql': {
        const mapping: Record<string, string> = {};
        for (const line of sqlVarMapping.split('\n')) {
          const [k, v] = line.split('=').map((s) => s.trim());
          if (k && v) mapping[k] = v;
        }
        return { kind: 'sql', connection: sqlConnection, query: sqlQuery, variable_mapping: mapping };
      }
    }
  }, [rangeKind, startDate, endDate, granularity, dateVarStart, dateVarEnd, listValues, listVarName, sqlConnection, sqlQuery, sqlVarMapping]);

  const canSave = useMemo(() => {
    switch (rangeKind) {
      case 'date_range':
        return startDate.trim().length > 0 && endDate.trim().length > 0;
      case 'list':
        return listValues.trim().length > 0;
      case 'sql':
        return sqlQuery.trim().length > 0;
    }
  }, [rangeKind, startDate, endDate, listValues, sqlQuery]);

  const handleSave = useCallback(async () => {
    if (!canSave) return;
    setSaving(true);
    setError(null);
    try {
      await onSave({
        pipeline_id: pipelineId,
        environment,
        range_definition: buildRange(),
        concurrency,
        fail_fast: failFast,
        full_refresh: fullRefresh,
      });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [canSave, onSave, pipelineId, environment, buildRange, concurrency, failFast, fullRefresh]);

  return (
    <div className="trigger-editor-overlay" onClick={onCancel}>
      <div className="trigger-editor" onClick={(e) => e.stopPropagation()}>
        <h3 className="trigger-editor__title">Start Backfill</h3>

        {/* Range kind selector */}
        <div className="trigger-editor__field">
          <label className="trigger-editor__label">Range Type</label>
          <select
            className="trigger-editor__select"
            value={rangeKind}
            onChange={(e) => setRangeKind(e.target.value as 'date_range' | 'list' | 'sql')}
          >
            <option value="date_range">Date Range</option>
            <option value="list">List</option>
            <option value="sql">SQL Query</option>
          </select>
        </div>

        {/* Date range fields */}
        {rangeKind === 'date_range' && (
          <>
            <div className="backfills-panel__row-pair">
              <div className="trigger-editor__field">
                <label className="trigger-editor__label">Start Date</label>
                <input
                  className="trigger-editor__input trigger-editor__input--mono"
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                />
              </div>
              <div className="trigger-editor__field">
                <label className="trigger-editor__label">End Date</label>
                <input
                  className="trigger-editor__input trigger-editor__input--mono"
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                />
              </div>
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Granularity</label>
              <select
                className="trigger-editor__select"
                value={granularity}
                onChange={(e) => setGranularity(e.target.value as DateGranularity)}
              >
                <option value="hour">Hourly</option>
                <option value="day">Daily</option>
                <option value="week">Weekly</option>
                <option value="month">Monthly</option>
              </select>
            </div>
            <div className="backfills-panel__row-pair">
              <div className="trigger-editor__field">
                <label className="trigger-editor__label">Start Variable</label>
                <input
                  className="trigger-editor__input"
                  value={dateVarStart}
                  onChange={(e) => setDateVarStart(e.target.value)}
                  placeholder="run_date"
                />
              </div>
              <div className="trigger-editor__field">
                <label className="trigger-editor__label">End Variable</label>
                <input
                  className="trigger-editor__input"
                  value={dateVarEnd}
                  onChange={(e) => setDateVarEnd(e.target.value)}
                  placeholder="next_date"
                />
              </div>
            </div>
            <div className="trigger-editor__help">
              Variables are set per iteration: {dateVarStart || 'run_date'}=$iteration.start, {dateVarEnd || 'next_date'}=$iteration.end
            </div>
          </>
        )}

        {/* List fields */}
        {rangeKind === 'list' && (
          <>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Values (comma-separated)</label>
              <input
                className="trigger-editor__input trigger-editor__input--mono"
                value={listValues}
                onChange={(e) => setListValues(e.target.value)}
                placeholder="US, EU, APAC"
              />
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Variable Name</label>
              <input
                className="trigger-editor__input"
                value={listVarName}
                onChange={(e) => setListVarName(e.target.value)}
                placeholder="region"
              />
              <div className="trigger-editor__help">
                Each value is passed as ${listVarName || 'value'}=$iteration.value
              </div>
            </div>
          </>
        )}

        {/* SQL fields */}
        {rangeKind === 'sql' && (
          <>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Connection</label>
              <input
                className="trigger-editor__input"
                value={sqlConnection}
                onChange={(e) => setSqlConnection(e.target.value)}
                placeholder="metadata"
              />
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Query</label>
              <textarea
                className="trigger-editor__input trigger-editor__input--mono backfills-panel__textarea"
                value={sqlQuery}
                onChange={(e) => setSqlQuery(e.target.value)}
                placeholder="SELECT DISTINCT tenant_id FROM tenants WHERE active = true"
                rows={3}
              />
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Variable Mapping (one per line: name=$iteration.column)</label>
              <textarea
                className="trigger-editor__input trigger-editor__input--mono backfills-panel__textarea"
                value={sqlVarMapping}
                onChange={(e) => setSqlVarMapping(e.target.value)}
                placeholder="tenant_id=$iteration.tenant_id"
                rows={2}
              />
            </div>
          </>
        )}

        {/* Options */}
        <div className="trigger-editor__field">
          <label className="trigger-editor__label">Concurrency</label>
          <input
            className="trigger-editor__input"
            type="number"
            min={1}
            max={32}
            value={concurrency}
            onChange={(e) => setConcurrency(Number(e.target.value))}
          />
          <div className="trigger-editor__help">Number of iterations to run in parallel</div>
        </div>

        <div className="backfills-panel__checkboxes">
          <label className="backfills-panel__checkbox-label">
            <input
              type="checkbox"
              checked={failFast}
              onChange={(e) => setFailFast(e.target.checked)}
            />
            Fail fast (stop on first failure)
          </label>
          <label className="backfills-panel__checkbox-label">
            <input
              type="checkbox"
              checked={fullRefresh}
              onChange={(e) => setFullRefresh(e.target.checked)}
            />
            Full refresh (ignore incremental state)
          </label>
        </div>

        {error && <div className="trigger-editor__error">{error}</div>}

        <div className="trigger-editor__footer">
          <button className="trigger-editor__btn" onClick={onCancel}>Cancel</button>
          <button
            className="trigger-editor__btn trigger-editor__btn--primary"
            onClick={handleSave}
            disabled={!canSave || saving}
          >
            {saving ? 'Starting...' : 'Start Backfill'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Iteration Table (detail view drill-down)
// ---------------------------------------------------------------------------

function IterationTable({ iterations }: { iterations: BackfillIteration[] }) {
  return (
    <div className="backfills-panel__iterations">
      <table className="backfills-panel__iter-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Key</th>
            <th>Status</th>
            <th>Run</th>
            <th>Time</th>
          </tr>
        </thead>
        <tbody>
          {iterations.map((it) => (
            <tr key={it.iteration_index} className={`backfills-panel__iter-row--${it.status}`}>
              <td>{it.iteration_index}</td>
              <td className="backfills-panel__iter-key">{it.iteration_key}</td>
              <td>
                <span className={`backfills-panel__iter-badge backfills-panel__iter-badge--${it.status}`}>
                  {STATUS_LABELS[it.status] ?? it.status}
                </span>
              </td>
              <td className="backfills-panel__iter-run">
                {it.run_id ? it.run_id.slice(0, 8) : '—'}
              </td>
              <td className="backfills-panel__iter-time">{formatTime(it.started_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {iterations.some((it) => it.error) && (
        <div className="backfills-panel__errors">
          <h4 className="backfills-panel__errors-title">Errors</h4>
          {iterations
            .filter((it) => it.error)
            .map((it) => (
              <div key={it.iteration_index} className="backfills-panel__error-entry">
                <span className="backfills-panel__error-key">{it.iteration_key}:</span>
                <span>{it.error}</span>
              </div>
            ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Backfill Detail View
// ---------------------------------------------------------------------------

function BackfillDetail({ onBack }: { onBack: () => void }) {
  const detail = useBackfillStore((s) => s.detail);
  const detailLoading = useBackfillStore((s) => s.detailLoading);
  const resumeBackfill = useBackfillStore((s) => s.resumeBackfill);
  const cancelBackfill = useBackfillStore((s) => s.cancelBackfill);
  const [acting, setActing] = useState(false);

  if (detailLoading || !detail) {
    return (
      <div>
        <button className="backfills-panel__back-btn" onClick={onBack}>&larr; Back</button>
        <div className="system-info-panel__loading">Loading...</div>
      </div>
    );
  }

  const p = detail.progress;
  const pct = progressPercent(p);
  const isRunning = detail.status === 'running';
  const canResume = detail.status === 'failed' || detail.status === 'cancelled';

  const handleResume = async () => {
    setActing(true);
    try {
      await resumeBackfill(detail.id);
    } finally {
      setActing(false);
    }
  };

  const handleCancel = async () => {
    if (!window.confirm('Cancel this backfill?')) return;
    setActing(true);
    try {
      await cancelBackfill(detail.id);
    } finally {
      setActing(false);
    }
  };

  return (
    <div>
      <button className="backfills-panel__back-btn" onClick={onBack}>&larr; Back</button>

      <div className="backfills-panel__detail-header">
        <span className={`backfills-panel__status-badge backfills-panel__status-badge--${detail.status}`}>
          {STATUS_LABELS[detail.status]}
        </span>
        <span className="backfills-panel__detail-id">{detail.id.slice(0, 8)}</span>
      </div>

      <div className="backfills-panel__detail-meta">
        {rangeLabel(detail.range_definition)}
      </div>
      <div className="backfills-panel__detail-meta">
        Concurrency: {detail.concurrency} &middot; {detail.full_refresh ? 'Full refresh' : 'Incremental'}
        {detail.fail_fast ? ' \u00b7 Fail fast' : ''}
      </div>

      {/* Progress bar */}
      <div className="backfills-panel__progress">
        <div className="backfills-panel__progress-bar">
          <div
            className="backfills-panel__progress-fill backfills-panel__progress-fill--succeeded"
            style={{ width: `${p.total ? (p.succeeded / p.total) * 100 : 0}%` }}
          />
          <div
            className="backfills-panel__progress-fill backfills-panel__progress-fill--failed"
            style={{ width: `${p.total ? (p.failed / p.total) * 100 : 0}%` }}
          />
          <div
            className="backfills-panel__progress-fill backfills-panel__progress-fill--running"
            style={{ width: `${p.total ? (p.running / p.total) * 100 : 0}%` }}
          />
        </div>
        <div className="backfills-panel__progress-label">
          {pct}% &mdash; {p.succeeded} succeeded, {p.failed} failed, {p.running} running, {p.pending} pending
          {p.skipped > 0 ? `, ${p.skipped} skipped` : ''}
        </div>
      </div>

      {/* Actions */}
      <div className="triggers-panel__actions">
        {canResume && (
          <button className="triggers-panel__action-btn" onClick={handleResume} disabled={acting}>
            Resume
          </button>
        )}
        {isRunning && (
          <button className="triggers-panel__action-btn triggers-panel__action-btn--danger" onClick={handleCancel} disabled={acting}>
            Cancel
          </button>
        )}
      </div>

      {/* Iterations table */}
      <IterationTable iterations={detail.iterations} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Backfill Card (list view)
// ---------------------------------------------------------------------------

function BackfillCard({
  backfill,
  onSelect,
}: {
  backfill: BackfillResponse;
  onSelect: (id: string) => void;
}) {
  const deleteBackfill = useBackfillStore((s) => s.deleteBackfill);
  const pct = progressPercent(backfill.progress);
  const isRunning = backfill.status === 'running';

  const handleDelete = useCallback(async () => {
    if (!window.confirm('Delete this backfill?')) return;
    await deleteBackfill(backfill.id);
  }, [deleteBackfill, backfill.id]);

  return (
    <li
      className="triggers-panel__item backfills-panel__card"
      onClick={() => onSelect(backfill.id)}
    >
      <div className="triggers-panel__item-header">
        <span className={`backfills-panel__status-badge backfills-panel__status-badge--${backfill.status}`}>
          {STATUS_LABELS[backfill.status]}
        </span>
        <span className="triggers-panel__name">{backfill.id.slice(0, 8)}</span>
        <span className="triggers-panel__kind-badge">
          {backfill.range_definition.kind.replace('_', ' ')}
        </span>
      </div>

      <div className="triggers-panel__detail-mono">{rangeLabel(backfill.range_definition)}</div>
      <div className="backfills-panel__card-time">{formatTime(backfill.created_at)}</div>

      {/* Mini progress bar */}
      {backfill.progress && (
        <div className="backfills-panel__mini-progress">
          <div className="backfills-panel__progress-bar backfills-panel__progress-bar--mini">
            <div
              className="backfills-panel__progress-fill backfills-panel__progress-fill--succeeded"
              style={{ width: `${backfill.progress.total ? (backfill.progress.succeeded / backfill.progress.total) * 100 : 0}%` }}
            />
            <div
              className="backfills-panel__progress-fill backfills-panel__progress-fill--failed"
              style={{ width: `${backfill.progress.total ? (backfill.progress.failed / backfill.progress.total) * 100 : 0}%` }}
            />
          </div>
          <span className="backfills-panel__mini-label">
            {isRunning ? `${pct}%` : `${backfill.progress.succeeded}/${backfill.progress.total}`}
          </span>
        </div>
      )}

      <div className="triggers-panel__actions" onClick={(e) => e.stopPropagation()}>
        {!isRunning && (
          <button
            className="triggers-panel__action-btn triggers-panel__action-btn--danger"
            onClick={handleDelete}
          >
            Delete
          </button>
        )}
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------------
// BackfillsPanel (pipeline-level, mounted in PipelineCanvas)
// ---------------------------------------------------------------------------

interface BackfillsPanelProps {
  open: boolean;
  onClose: () => void;
}

export function BackfillsPanel({ open, onClose }: BackfillsPanelProps) {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);
  const backfills = useBackfillStore((s) => s.backfills);
  const loading = useBackfillStore((s) => s.loading);
  const error = useBackfillStore((s) => s.error);
  const fetchBackfills = useBackfillStore((s) => s.fetchBackfills);
  const fetchDetail = useBackfillStore((s) => s.fetchDetail);
  const clearDetail = useBackfillStore((s) => s.clearDetail);
  const createBackfill = useBackfillStore((s) => s.createBackfill);

  const [editorOpen, setEditorOpen] = useState(false);
  const [viewingId, setViewingId] = useState<string | null>(null);

  // Fetch backfills on open
  useEffect(() => {
    if (open && pipelineId) {
      void fetchBackfills(pipelineId);
    }
  }, [open, pipelineId, fetchBackfills]);

  // Auto-refresh running backfills
  useEffect(() => {
    if (!open || !pipelineId) return;
    const hasRunning = backfills.some((b) => b.status === 'running');
    if (!hasRunning) return;
    const interval = setInterval(() => fetchBackfills(pipelineId), 5000);
    return () => clearInterval(interval);
  }, [open, pipelineId, backfills, fetchBackfills]);

  // Fetch detail when selected, auto-refresh if running
  useEffect(() => {
    if (!viewingId) return;
    void fetchDetail(viewingId);
    const interval = setInterval(() => fetchDetail(viewingId), 3000);
    return () => clearInterval(interval);
  }, [viewingId, fetchDetail]);

  const handleNew = useCallback(() => {
    setEditorOpen(true);
  }, []);

  const handleEditorSave = useCallback(
    async (req: CreateBackfillRequest) => {
      await createBackfill(req);
      setEditorOpen(false);
    },
    [createBackfill],
  );

  const handleEditorCancel = useCallback(() => {
    setEditorOpen(false);
  }, []);

  const handleSelect = useCallback((id: string) => {
    setViewingId(id);
  }, []);

  const handleBack = useCallback(() => {
    setViewingId(null);
    clearDetail();
    if (pipelineId) void fetchBackfills(pipelineId);
  }, [clearDetail, fetchBackfills, pipelineId]);

  if (!open) return null;

  return (
    <>
      <div className="system-info-panel system-info-panel--open">
        <div className="system-info-panel__header">
          <h3 className="system-info-panel__title">Backfills</h3>
          <button className="system-info-panel__close" onClick={onClose} title="Close">
            &times;
          </button>
        </div>

        <div className="system-info-panel__body">
          {viewingId ? (
            <BackfillDetail onBack={handleBack} />
          ) : (
            <>
              <button className="triggers-panel__add-btn" onClick={handleNew}>
                + Start Backfill
              </button>

              {loading && backfills.length === 0 && (
                <div className="system-info-panel__loading">Loading backfills...</div>
              )}
              {error && <p className="system-info-panel__error">{error}</p>}
              {!loading && backfills.length === 0 && !error && (
                <p className="triggers-panel__empty">
                  No backfills for this pipeline. Start one to rerun the pipeline across a range of parameters.
                </p>
              )}

              <ul className="triggers-panel__list">
                {backfills.map((b) => (
                  <BackfillCard key={b.id} backfill={b} onSelect={handleSelect} />
                ))}
              </ul>
            </>
          )}
        </div>
      </div>

      {editorOpen && pipelineId && (
        <BackfillEditor
          pipelineId={pipelineId}
          environment={activeEnvironment}
          onSave={handleEditorSave}
          onCancel={handleEditorCancel}
        />
      )}
    </>
  );
}
