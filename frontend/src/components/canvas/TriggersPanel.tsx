// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTriggerStore } from '../../stores/triggerStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useEnvironmentStore } from '../../stores/environmentStore';
import type {
  TriggerResponse,
  TriggerKind,
  TriggerKindName,
  TriggerHistoryEntry,
  RunPolicy,
  CompletionStatus,
  CreateTriggerRequest,
  UpdateTriggerRequest,
} from '../../api/triggers';
import { TRIGGER_KIND_LABELS } from '../../api/triggers';
import { listPipelines, type ApiPipelineResponse } from '../../api/pipelines';
import './SystemInfoPanel.css';
import './TriggersPanel.css';

// ---------------------------------------------------------------------------
// Cron validation — lightweight client-side check
// ---------------------------------------------------------------------------

/** Validate a 5-field cron expression. Returns an error message or null. */
function validateCron(expr: string): string | null {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) return 'Expected 5 fields: minute hour day month weekday';
  const ranges = [
    { name: 'minute', min: 0, max: 59 },
    { name: 'hour', min: 0, max: 23 },
    { name: 'day', min: 1, max: 31 },
    { name: 'month', min: 1, max: 12 },
    { name: 'weekday', min: 0, max: 7 },
  ];
  for (let i = 0; i < 5; i++) {
    const field = parts[i];
    const { name } = ranges[i];
    // Allow *, */N, N, N-M, N-M/S, comma-separated lists
    if (!/^(\*|(\d+(-\d+)?)(\/\d+)?)([,](\*|(\d+(-\d+)?)(\/\d+)?))*$/.test(field) && field !== '*') {
      return `Invalid ${name} field: "${field}"`;
    }
  }
  return null;
}

/** Describe a cron expression in plain English (simplified). */
function describeCron(expr: string): string | null {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) return null;
  const [min, hour, dom, mon, dow] = parts;
  const pieces: string[] = [];
  if (min === '0' && hour === '*' && dom === '*' && mon === '*' && dow === '*') {
    return 'Every hour at :00';
  }
  if (min.startsWith('*/')) {
    pieces.push(`Every ${min.slice(2)} minutes`);
  } else if (hour.startsWith('*/')) {
    pieces.push(`Every ${hour.slice(2)} hours at :${min.padStart(2, '0')}`);
  } else if (hour !== '*' && min !== '*') {
    pieces.push(`At ${hour.padStart(2, '0')}:${min.padStart(2, '0')}`);
  }
  if (dom !== '*') pieces.push(`on day ${dom}`);
  if (mon !== '*') pieces.push(`of month ${mon}`);
  if (dow !== '*') pieces.push(`on weekday ${dow}`);
  return pieces.length > 0 ? pieces.join(' ') : null;
}

// ---------------------------------------------------------------------------
// Trigger Editor Modal
// ---------------------------------------------------------------------------

interface TriggerEditorProps {
  editing: TriggerResponse | null;
  pipelineId: string;
  environment: string;
  onSave: (req: CreateTriggerRequest | UpdateTriggerRequest, isNew: boolean) => Promise<void>;
  onCancel: () => void;
}

function TriggerEditor({ editing, pipelineId, environment, onSave, onCancel }: TriggerEditorProps) {
  const isNew = editing === null;
  const [name, setName] = useState(editing?.name ?? '');
  const [kindName, setKindName] = useState<TriggerKindName>(editing?.kind.kind ?? 'cron');
  const [runPolicy, setRunPolicy] = useState<RunPolicy>(editing?.run_policy ?? 'queue');
  const [maxQueueDepth, setMaxQueueDepth] = useState(editing?.max_queue_depth ?? 3);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Kind-specific fields
  const [cronExpr, setCronExpr] = useState(
    editing?.kind.kind === 'cron' ? editing.kind.expression : '0 */6 * * *',
  );
  const [cronTz, setCronTz] = useState(
    editing?.kind.kind === 'cron' ? editing.kind.timezone : 'UTC',
  );
  const [intervalEvery, setIntervalEvery] = useState(
    editing?.kind.kind === 'interval' ? editing.kind.every : 'PT30M',
  );
  const [filePath, setFilePath] = useState(
    editing?.kind.kind === 'file_arrival' ? editing.kind.path : '',
  );
  const [filePollInterval, setFilePollInterval] = useState(
    editing?.kind.kind === 'file_arrival' ? editing.kind.poll_interval : 'PT1M',
  );
  const [webhookPath, setWebhookPath] = useState(
    editing?.kind.kind === 'webhook' ? editing.kind.path : '',
  );
  const [upstreamPipeline, setUpstreamPipeline] = useState(
    editing?.kind.kind === 'pipeline_completion' ? editing.kind.upstream_pipeline : '',
  );
  const [onStatus, setOnStatus] = useState<CompletionStatus>(
    editing?.kind.kind === 'pipeline_completion' ? editing.kind.on_status : 'success',
  );

  // Pipeline list for completion trigger
  const [pipelines, setPipelines] = useState<ApiPipelineResponse[]>([]);
  useEffect(() => {
    if (kindName === 'pipeline_completion') {
      listPipelines(100, 0).then((res) => setPipelines(res.data)).catch(() => {});
    }
  }, [kindName]);

  const cronError = useMemo(() => (kindName === 'cron' ? validateCron(cronExpr) : null), [kindName, cronExpr]);
  const cronDesc = useMemo(() => (kindName === 'cron' ? describeCron(cronExpr) : null), [kindName, cronExpr]);

  const buildKind = useCallback((): TriggerKind => {
    switch (kindName) {
      case 'cron':
        return { kind: 'cron', expression: cronExpr.trim(), timezone: cronTz };
      case 'interval':
        return { kind: 'interval', every: intervalEvery.trim() };
      case 'file_arrival':
        return { kind: 'file_arrival', path: filePath.trim(), poll_interval: filePollInterval };
      case 'webhook':
        return { kind: 'webhook', path: webhookPath.trim() || `/triggers/${name.toLowerCase().replace(/\s+/g, '-')}`, auth: 'token' };
      case 'pipeline_completion':
        return { kind: 'pipeline_completion', upstream_pipeline: upstreamPipeline, on_status: onStatus };
    }
  }, [kindName, cronExpr, cronTz, intervalEvery, filePath, filePollInterval, webhookPath, name, upstreamPipeline, onStatus]);

  const canSave = name.trim().length > 0 && (kindName !== 'cron' || cronError === null);

  const handleSave = useCallback(async () => {
    if (!canSave) return;
    setSaving(true);
    setError(null);
    try {
      const kind = buildKind();
      if (isNew) {
        await onSave(
          {
            name: name.trim(),
            pipeline_id: pipelineId,
            environment,
            kind,
            run_policy: runPolicy,
            max_queue_depth: maxQueueDepth,
          } satisfies CreateTriggerRequest,
          true,
        );
      } else {
        await onSave(
          {
            name: name.trim(),
            kind,
            run_policy: runPolicy,
            max_queue_depth: maxQueueDepth,
          } satisfies UpdateTriggerRequest,
          false,
        );
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [canSave, buildKind, isNew, name, pipelineId, environment, runPolicy, maxQueueDepth, onSave]);

  return (
    <div className="trigger-editor-overlay" onClick={onCancel}>
      <div className="trigger-editor" onClick={(e) => e.stopPropagation()}>
        <h3 className="trigger-editor__title">{isNew ? 'New Trigger' : 'Edit Trigger'}</h3>

        {/* Name */}
        <div className="trigger-editor__field">
          <label className="trigger-editor__label">Name</label>
          <input
            className="trigger-editor__input"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. Hourly refresh"
            autoFocus
          />
        </div>

        {/* Kind selector */}
        <div className="trigger-editor__field">
          <label className="trigger-editor__label">Kind</label>
          <select
            className="trigger-editor__select"
            value={kindName}
            onChange={(e) => setKindName(e.target.value as TriggerKindName)}
            disabled={!isNew}
          >
            {(Object.keys(TRIGGER_KIND_LABELS) as TriggerKindName[]).map((k) => (
              <option key={k} value={k}>{TRIGGER_KIND_LABELS[k]}</option>
            ))}
          </select>
        </div>

        {/* Kind-specific fields */}
        {kindName === 'cron' && (
          <>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Cron Expression</label>
              <input
                className="trigger-editor__input trigger-editor__input--mono"
                value={cronExpr}
                onChange={(e) => setCronExpr(e.target.value)}
                placeholder="0 */6 * * *"
              />
              <div className="trigger-editor__help">5-field Unix cron: minute hour day month weekday</div>
              {cronError && <div className="trigger-editor__error">{cronError}</div>}
              {!cronError && cronDesc && <div className="trigger-editor__cron-preview">{cronDesc}</div>}
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Timezone</label>
              <input
                className="trigger-editor__input"
                value={cronTz}
                onChange={(e) => setCronTz(e.target.value)}
                placeholder="UTC"
              />
            </div>
          </>
        )}

        {kindName === 'interval' && (
          <div className="trigger-editor__field">
            <label className="trigger-editor__label">Interval (ISO 8601)</label>
            <input
              className="trigger-editor__input trigger-editor__input--mono"
              value={intervalEvery}
              onChange={(e) => setIntervalEvery(e.target.value)}
              placeholder="PT30M"
            />
            <div className="trigger-editor__help">ISO 8601 duration, e.g. PT30M (30 min), PT1H (1 hour), P1D (1 day)</div>
          </div>
        )}

        {kindName === 'file_arrival' && (
          <>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Path / Glob</label>
              <input
                className="trigger-editor__input trigger-editor__input--mono"
                value={filePath}
                onChange={(e) => setFilePath(e.target.value)}
                placeholder="s3://bucket/incoming/*.parquet"
              />
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Poll Interval</label>
              <input
                className="trigger-editor__input trigger-editor__input--mono"
                value={filePollInterval}
                onChange={(e) => setFilePollInterval(e.target.value)}
                placeholder="PT1M"
              />
            </div>
          </>
        )}

        {kindName === 'webhook' && (
          <div className="trigger-editor__field">
            <label className="trigger-editor__label">Endpoint Path</label>
            <input
              className="trigger-editor__input trigger-editor__input--mono"
              value={webhookPath}
              onChange={(e) => setWebhookPath(e.target.value)}
              placeholder="/triggers/my-webhook"
            />
            <div className="trigger-editor__help">Leave blank to auto-generate from name</div>
          </div>
        )}

        {kindName === 'pipeline_completion' && (
          <>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">Upstream Pipeline</label>
              <select
                className="trigger-editor__select"
                value={upstreamPipeline}
                onChange={(e) => setUpstreamPipeline(e.target.value)}
              >
                <option value="">Select a pipeline...</option>
                {pipelines.map((p) => (
                  <option key={p.id} value={p.pipeline.name}>
                    {p.pipeline.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="trigger-editor__field">
              <label className="trigger-editor__label">On Status</label>
              <select
                className="trigger-editor__select"
                value={onStatus}
                onChange={(e) => setOnStatus(e.target.value as CompletionStatus)}
              >
                <option value="success">Success</option>
                <option value="failure">Failure</option>
                <option value="any">Any</option>
              </select>
            </div>
          </>
        )}

        {/* Run Policy */}
        <div className="trigger-editor__field">
          <label className="trigger-editor__label">Run Policy</label>
          <select
            className="trigger-editor__select"
            value={runPolicy}
            onChange={(e) => setRunPolicy(e.target.value as RunPolicy)}
          >
            <option value="queue">Queue (enqueue behind running)</option>
            <option value="skip">Skip (silently skip if running)</option>
            <option value="reject">Reject (fail if running)</option>
          </select>
        </div>

        {/* Max Queue Depth */}
        {runPolicy === 'queue' && (
          <div className="trigger-editor__field">
            <label className="trigger-editor__label">Max Queue Depth</label>
            <input
              className="trigger-editor__input"
              type="number"
              min={1}
              max={100}
              value={maxQueueDepth}
              onChange={(e) => setMaxQueueDepth(Number(e.target.value))}
            />
          </div>
        )}

        {error && <div className="trigger-editor__error">{error}</div>}

        <div className="trigger-editor__footer">
          <button className="trigger-editor__btn" onClick={onCancel}>Cancel</button>
          <button
            className="trigger-editor__btn trigger-editor__btn--primary"
            onClick={handleSave}
            disabled={!canSave || saving}
          >
            {saving ? 'Saving...' : isNew ? 'Create' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Trigger History Section (collapsible per-trigger)
// ---------------------------------------------------------------------------

function TriggerHistorySection({ triggerId, onViewRunDetail }: { triggerId: string; onViewRunDetail?: (runId: string) => void }) {
  const [expanded, setExpanded] = useState(false);
  const history = useTriggerStore((s) => s.history.get(triggerId));
  const fetchHistory = useTriggerStore((s) => s.fetchHistory);

  useEffect(() => {
    if (expanded) void fetchHistory(triggerId, 20);
  }, [expanded, triggerId, fetchHistory]);

  return (
    <div className="triggers-panel__history">
      <button
        className="triggers-panel__history-toggle"
        onClick={() => setExpanded((e) => !e)}
      >
        {expanded ? '▾ History' : '▸ History'}
      </button>
      {expanded && history && (
        <ul className="triggers-panel__history-list">
          {history.length === 0 && (
            <li className="triggers-panel__history-entry">No firings yet</li>
          )}
          {history.map((h) => (
            <HistoryRow key={h.id} entry={h} onViewRunDetail={onViewRunDetail} />
          ))}
        </ul>
      )}
    </div>
  );
}

function HistoryRow({ entry, onViewRunDetail }: { entry: TriggerHistoryEntry; onViewRunDetail?: (runId: string) => void }) {
  const dt = new Date(entry.fired_at);
  const time = dt.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  return (
    <li className="triggers-panel__history-entry">
      <span className={`triggers-panel__history-outcome triggers-panel__history-outcome--${entry.outcome}`}>
        {entry.outcome.replace('_', ' ')}
      </span>
      <span>{time}</span>
      {entry.run_id && onViewRunDetail && (
        <span
          className="triggers-panel__history-run-link"
          onClick={() => onViewRunDetail(entry.run_id!)}
          title={`View run ${entry.run_id}`}
        >
          {entry.run_id.slice(0, 8)}
        </span>
      )}
      {entry.error && <span title={entry.error}>⚠</span>}
    </li>
  );
}

// ---------------------------------------------------------------------------
// Trigger Card
// ---------------------------------------------------------------------------

function TriggerCard({
  trigger,
  onEdit,
  onViewRunDetail,
}: {
  trigger: TriggerResponse;
  onEdit: (t: TriggerResponse) => void;
  onViewRunDetail?: (runId: string) => void;
}) {
  const enableTrigger = useTriggerStore((s) => s.enableTrigger);
  const disableTrigger = useTriggerStore((s) => s.disableTrigger);
  const fireTrigger = useTriggerStore((s) => s.fireTrigger);
  const deleteTrigger = useTriggerStore((s) => s.deleteTrigger);
  const [firing, setFiring] = useState(false);
  const [copied, setCopied] = useState(false);

  const hasErrors = (trigger.state?.consecutive_errors ?? 0) >= 3;

  const handleFire = useCallback(async () => {
    setFiring(true);
    try {
      await fireTrigger(trigger.id);
    } catch {
      // Error handling via store
    } finally {
      setFiring(false);
    }
  }, [fireTrigger, trigger.id]);

  const handleDelete = useCallback(async () => {
    if (!window.confirm(`Delete trigger "${trigger.name}"?`)) return;
    await deleteTrigger(trigger.id);
  }, [deleteTrigger, trigger.id, trigger.name]);

  const handleToggle = useCallback(async () => {
    if (trigger.enabled) {
      await disableTrigger(trigger.id);
    } else {
      await enableTrigger(trigger.id);
    }
  }, [trigger.enabled, trigger.id, enableTrigger, disableTrigger]);

  // Webhook URL + token display
  const webhookUrl = trigger.kind.kind === 'webhook'
    ? `${location.origin}/triggers/webhook/${trigger.id}`
    : null;
  const webhookToken = trigger.kind.kind === 'webhook' && trigger.state?.sensor_state
    ? (trigger.state.sensor_state as { token?: string })?.token
    : null;

  const handleCopyWebhook = useCallback(() => {
    if (!webhookUrl) return;
    const curlCmd = webhookToken
      ? `curl -X POST ${webhookUrl} -H "Authorization: Bearer ${webhookToken}" -H "Content-Type: application/json" -d '{}'`
      : `curl -X POST ${webhookUrl} -H "Content-Type: application/json" -d '{}'`;
    navigator.clipboard.writeText(curlCmd).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [webhookUrl, webhookToken]);

  // Kind-specific detail line
  const kindDetail = useMemo(() => {
    switch (trigger.kind.kind) {
      case 'cron':
        return trigger.kind.expression + (trigger.kind.timezone !== 'UTC' ? ` (${trigger.kind.timezone})` : '');
      case 'interval':
        return trigger.kind.every;
      case 'file_arrival':
        return trigger.kind.path;
      case 'webhook':
        return trigger.kind.path;
      case 'pipeline_completion':
        return `${trigger.kind.upstream_pipeline} → ${trigger.kind.on_status}`;
    }
  }, [trigger.kind]);

  const statusDotClass = hasErrors
    ? 'triggers-panel__status-dot--error'
    : trigger.enabled
      ? 'triggers-panel__status-dot--active'
      : 'triggers-panel__status-dot--paused';

  const itemClass = [
    'triggers-panel__item',
    !trigger.enabled && 'triggers-panel__item--disabled',
    hasErrors && 'triggers-panel__item--warning',
  ].filter(Boolean).join(' ');

  return (
    <li className={itemClass}>
      <div className="triggers-panel__item-header">
        <span className={`triggers-panel__status-dot ${statusDotClass}`} />
        <span className="triggers-panel__name">{trigger.name}</span>
        <span className="triggers-panel__kind-badge">
          {TRIGGER_KIND_LABELS[trigger.kind.kind]}
        </span>
      </div>

      <div className="triggers-panel__detail-mono">{kindDetail}</div>

      {trigger.state?.next_fire_at && (
        <div className="triggers-panel__next-fire">
          Next: {new Date(trigger.state.next_fire_at).toLocaleString()}
        </div>
      )}

      {hasErrors && (
        <div className="triggers-panel__health-warning">
          {trigger.state!.consecutive_errors} consecutive errors
        </div>
      )}

      {/* Webhook URL + token display */}
      {webhookUrl && (
        <div className="triggers-panel__webhook-url">
          <code title={webhookUrl}>{webhookUrl}</code>
          <button
            className="triggers-panel__copy-btn"
            onClick={handleCopyWebhook}
            title="Copy curl command"
          >
            {copied ? 'Copied!' : 'Copy'}
          </button>
        </div>
      )}
      {webhookToken && (
        <div className="triggers-panel__detail-mono" title={webhookToken}>
          Token: {webhookToken.slice(0, 8)}...
        </div>
      )}

      <div className="triggers-panel__actions">
        <button className="triggers-panel__action-btn" onClick={handleToggle}>
          {trigger.enabled ? 'Disable' : 'Enable'}
        </button>
        <button
          className="triggers-panel__action-btn"
          onClick={handleFire}
          disabled={firing}
        >
          {firing ? 'Firing...' : 'Fire'}
        </button>
        <button className="triggers-panel__action-btn" onClick={() => onEdit(trigger)}>
          Edit
        </button>
        <button
          className="triggers-panel__action-btn triggers-panel__action-btn--danger"
          onClick={handleDelete}
        >
          Delete
        </button>
      </div>

      <TriggerHistorySection triggerId={trigger.id} onViewRunDetail={onViewRunDetail} />
    </li>
  );
}

// ---------------------------------------------------------------------------
// TriggersPanel (pipeline-level, mounted in PipelineCanvas)
// ---------------------------------------------------------------------------

interface TriggersPanelProps {
  open: boolean;
  onClose: () => void;
  onViewRunDetail?: (runId: string) => void;
}

export function TriggersPanel({ open, onClose, onViewRunDetail }: TriggersPanelProps) {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);
  const triggers = useTriggerStore((s) => s.triggers);
  const loading = useTriggerStore((s) => s.loading);
  const error = useTriggerStore((s) => s.error);
  const fetchTriggers = useTriggerStore((s) => s.fetchTriggers);
  const createTrigger = useTriggerStore((s) => s.createTrigger);
  const updateTrigger = useTriggerStore((s) => s.updateTrigger);

  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState<TriggerResponse | null>(null);

  useEffect(() => {
    if (open && pipelineId) {
      void fetchTriggers(pipelineId);
    }
  }, [open, pipelineId, fetchTriggers]);

  const handleNew = useCallback(() => {
    setEditing(null);
    setEditorOpen(true);
  }, []);

  const handleEdit = useCallback((t: TriggerResponse) => {
    setEditing(t);
    setEditorOpen(true);
  }, []);

  const handleEditorSave = useCallback(
    async (req: CreateTriggerRequest | UpdateTriggerRequest, isNew: boolean) => {
      if (isNew) {
        await createTrigger(req as CreateTriggerRequest);
      } else if (editing) {
        await updateTrigger(editing.id, req as UpdateTriggerRequest);
      }
      setEditorOpen(false);
      setEditing(null);
    },
    [createTrigger, updateTrigger, editing],
  );

  const handleEditorCancel = useCallback(() => {
    setEditorOpen(false);
    setEditing(null);
  }, []);

  if (!open) return null;

  return (
    <>
      <div className="system-info-panel system-info-panel--open">
        <div className="system-info-panel__header">
          <h3 className="system-info-panel__title">Triggers</h3>
          <button className="system-info-panel__close" onClick={onClose} title="Close">
            &times;
          </button>
        </div>

        <div className="system-info-panel__body">
          <button className="triggers-panel__add-btn" onClick={handleNew}>
            + New Trigger
          </button>

          {loading && triggers.length === 0 && (
            <div className="system-info-panel__loading">Loading triggers...</div>
          )}
          {error && <p className="system-info-panel__error">{error}</p>}
          {!loading && triggers.length === 0 && !error && (
            <p className="triggers-panel__empty">
              No triggers for this pipeline. Create one to schedule or automate runs.
            </p>
          )}

          <ul className="triggers-panel__list">
            {triggers.map((t) => (
              <TriggerCard key={t.id} trigger={t} onEdit={handleEdit} onViewRunDetail={onViewRunDetail} />
            ))}
          </ul>
        </div>
      </div>

      {editorOpen && pipelineId && (
        <TriggerEditor
          editing={editing}
          pipelineId={pipelineId}
          environment={activeEnvironment}
          onSave={handleEditorSave}
          onCancel={handleEditorCancel}
        />
      )}
    </>
  );
}
