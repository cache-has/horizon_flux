// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Sink materialization policy editor.
//!
//! Mirrors the orthogonal `read_mode` × `write_strategy` model and the field
//! rules enforced by `flux-engine::materialization::validate_policy`. Surfaces
//! validation errors inline so the user sees the same diagnostics the backend
//! would emit on import.
//!
//! Doc 27 calls this "JSON Schema-driven", but the materialization block has
//! cross-field rules (e.g. `unique_keys` required iff `merge`/`delete_insert`)
//! that pure JSON Schema can't express cleanly. We hand-render the form so the
//! conditional structure is obvious.

import { useMemo, useState } from 'react';
import {
  resetIncrementalState,
  type MaterializationPolicy,
  type ReadMode,
  type WriteStrategy,
  type WatermarkType,
  type OnSchemaChange,
  type FirstRun,
  type SnapshotPolicy,
  type ChangeDetection,
  type HardDeletes,
} from '../../api/pipelines';
import { ConfirmDialog } from './ConfirmDialog';
import './connector-editor.css';
import { validatePolicy } from './materializationPolicy';

export interface MaterializationEditorProps {
  policy: MaterializationPolicy | undefined;
  onChange: (next: MaterializationPolicy | undefined) => void;
  /** Pipeline ID — required to enable the "Reset incremental state" button. */
  pipelineId?: string;
  /** Sink node ID — required to enable the "Reset incremental state" button. */
  nodeId?: string;
  /** Environment for the reset; defaults to backend default. */
  environment?: string;
}

const READ_MODES: { value: ReadMode; label: string }[] = [
  { value: 'full', label: 'Full (read everything every run)' },
  { value: 'incremental', label: 'Incremental (only new/changed rows)' },
];

const WRITE_STRATEGIES: { value: WriteStrategy; label: string; needsKeys?: boolean; needsPartition?: boolean }[] = [
  { value: 'append', label: 'Append' },
  { value: 'merge', label: 'Merge (upsert on unique_keys)', needsKeys: true },
  { value: 'delete_insert', label: 'Delete + Insert', needsKeys: true },
  { value: 'insert_overwrite', label: 'Insert Overwrite (partition replace)', needsPartition: true },
  { value: 'truncate_insert', label: 'Truncate + Insert' },
  { value: 'snapshot', label: 'Snapshot (SCD2 history)', needsKeys: true },
];

const CHANGE_DETECTIONS: { value: ChangeDetection; label: string }[] = [
  { value: 'check', label: 'Check (compare tracked columns)' },
  { value: 'timestamp', label: 'Timestamp (trust updated_at column)' },
];

const HARD_DELETES: { value: HardDeletes; label: string }[] = [
  { value: 'ignore', label: 'Ignore (leave current version)' },
  { value: 'invalidate', label: 'Invalidate (close current version)' },
  { value: 'delete', label: 'Delete (remove all history)' },
];

const DEFAULT_SNAPSHOT: SnapshotPolicy = {
  change_detection: 'check',
  check_columns: [],
  hard_deletes: 'ignore',
};

const SCHEMA_CHANGE: OnSchemaChange[] = ['fail', 'ignore', 'append_new_columns', 'sync_all_columns'];
const FIRST_RUN: FirstRun[] = ['full', 'fail'];
const WATERMARK_TYPES: WatermarkType[] = ['timestamp', 'int64', 'string'];

const DEFAULT_POLICY: MaterializationPolicy = {
  read_mode: 'full',
  write_strategy: 'append',
};

export function MaterializationEditor({
  policy,
  onChange,
  pipelineId,
  nodeId,
  environment,
}: MaterializationEditorProps) {
  const enabled = policy !== undefined;
  const p: MaterializationPolicy = policy ?? DEFAULT_POLICY;
  const errors = useMemo(() => (enabled ? validatePolicy(p) : []), [enabled, p]);
  const [confirmReset, setConfirmReset] = useState(false);
  const [resetState, setResetState] = useState<
    { kind: 'idle' } | { kind: 'busy' } | { kind: 'ok' } | { kind: 'err'; message: string }
  >({ kind: 'idle' });
  const canReset =
    enabled &&
    (p.read_mode ?? 'full') === 'incremental' &&
    !!pipelineId &&
    !!nodeId;

  const handleReset = async () => {
    if (!pipelineId || !nodeId) return;
    setConfirmReset(false);
    setResetState({ kind: 'busy' });
    try {
      await resetIncrementalState(pipelineId, nodeId, environment);
      setResetState({ kind: 'ok' });
    } catch (e) {
      setResetState({ kind: 'err', message: (e as Error).message });
    }
  };

  const update = (patch: Partial<MaterializationPolicy>) => {
    onChange({ ...p, ...patch });
  };

  const readMode: ReadMode = p.read_mode ?? 'full';
  const strategy: WriteStrategy = p.write_strategy ?? 'append';
  const stratMeta = WRITE_STRATEGIES.find((s) => s.value === strategy);

  return (
    <div className="connector-editor__section">
      <div className="connector-editor__section-title">Materialization</div>

      <div className="connector-editor__field">
        <label className="connector-editor__label">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => onChange(e.target.checked ? { ...DEFAULT_POLICY } : undefined)}
          />
          {' '}Enable materialization policy
        </label>
        <small className="connector-editor__hint">
          When disabled, the sink uses legacy append/overwrite behavior.
        </small>
      </div>

      {enabled && (
        <>
          <div className="connector-editor__field">
            <label className="connector-editor__label">Read Mode</label>
            <select
              className="connector-editor__select"
              value={readMode}
              onChange={(e) => {
                const next = e.target.value as ReadMode;
                const patch: Partial<MaterializationPolicy> = { read_mode: next };
                if (next === 'full') {
                  patch.watermark = undefined;
                  patch.lookback = undefined;
                  patch.first_run = undefined;
                }
                update(patch);
              }}
            >
              {READ_MODES.map((m) => (
                <option key={m.value} value={m.value}>{m.label}</option>
              ))}
            </select>
          </div>

          <div className="connector-editor__field">
            <label className="connector-editor__label">Write Strategy</label>
            <select
              className="connector-editor__select"
              value={strategy}
              onChange={(e) => {
                const next = e.target.value as WriteStrategy;
                const meta = WRITE_STRATEGIES.find((s) => s.value === next);
                const patch: Partial<MaterializationPolicy> = { write_strategy: next };
                if (!meta?.needsKeys) patch.unique_keys = undefined;
                if (!meta?.needsPartition) patch.partition_column = undefined;
                if (next === 'snapshot') {
                  patch.snapshot = p.snapshot ?? { ...DEFAULT_SNAPSHOT };
                } else {
                  patch.snapshot = undefined;
                }
                update(patch);
              }}
            >
              {WRITE_STRATEGIES.map((s) => (
                <option key={s.value} value={s.value}>{s.label}</option>
              ))}
            </select>
          </div>

          {stratMeta?.needsKeys && (
            <div className="connector-editor__field">
              <label className="connector-editor__label">Unique Keys *</label>
              <input
                className="connector-editor__input"
                type="text"
                value={(p.unique_keys ?? []).join(', ')}
                onChange={(e) => {
                  const keys = e.target.value
                    .split(',')
                    .map((k) => k.trim())
                    .filter((k) => k.length > 0);
                  update({ unique_keys: keys.length > 0 ? keys : undefined });
                }}
                placeholder="id, tenant_id"
              />
              <small className="connector-editor__hint">Comma-separated column names.</small>
            </div>
          )}

          {stratMeta?.needsPartition && (
            <div className="connector-editor__field">
              <label className="connector-editor__label">Partition Column *</label>
              <input
                className="connector-editor__input"
                type="text"
                value={p.partition_column ?? ''}
                onChange={(e) =>
                  update({ partition_column: e.target.value.trim() || undefined })
                }
                placeholder="event_date"
              />
            </div>
          )}

          {strategy === 'snapshot' && (() => {
            const snap: SnapshotPolicy = p.snapshot ?? DEFAULT_SNAPSHOT;
            const detection: ChangeDetection = snap.change_detection ?? 'check';
            const updateSnap = (patch: Partial<SnapshotPolicy>) => {
              update({ snapshot: { ...snap, ...patch } });
            };
            return (
              <div
                className="connector-editor__field"
                style={{
                  borderLeft: '2px solid #4b5563',
                  paddingLeft: '0.75em',
                  marginLeft: '0.25em',
                }}
                data-testid="snapshot-subblock"
              >
                <div
                  className="connector-editor__label"
                  style={{ fontWeight: 600, marginBottom: '0.4em' }}
                >
                  Snapshot (SCD2)
                </div>

                <div className="connector-editor__field">
                  <label className="connector-editor__label">Change Detection</label>
                  <select
                    className="connector-editor__select"
                    value={detection}
                    onChange={(e) => {
                      const next = e.target.value as ChangeDetection;
                      const patch: Partial<SnapshotPolicy> = { change_detection: next };
                      if (next === 'check') {
                        patch.updated_at_column = undefined;
                        if (!snap.check_columns) patch.check_columns = [];
                      } else {
                        patch.check_columns = undefined;
                      }
                      updateSnap(patch);
                    }}
                  >
                    {CHANGE_DETECTIONS.map((c) => (
                      <option key={c.value} value={c.value}>{c.label}</option>
                    ))}
                  </select>
                </div>

                {detection === 'check' && (
                  <div className="connector-editor__field">
                    <label className="connector-editor__label">Check Columns *</label>
                    <input
                      className="connector-editor__input"
                      type="text"
                      value={(snap.check_columns ?? []).join(', ')}
                      onChange={(e) => {
                        const cols = e.target.value
                          .split(',')
                          .map((c) => c.trim())
                          .filter((c) => c.length > 0);
                        updateSnap({ check_columns: cols });
                      }}
                      placeholder="email, plan, status"
                    />
                    <small className="connector-editor__hint">
                      Comma-separated. Use <code>*</code> to track every column.
                    </small>
                  </div>
                )}

                {detection === 'timestamp' && (
                  <div className="connector-editor__field">
                    <label className="connector-editor__label">Updated-At Column *</label>
                    <input
                      className="connector-editor__input"
                      type="text"
                      value={snap.updated_at_column ?? ''}
                      onChange={(e) =>
                        updateSnap({ updated_at_column: e.target.value.trim() || undefined })
                      }
                      placeholder="updated_at"
                    />
                    <small className="connector-editor__hint">
                      Source column flux trusts to flag changed rows.
                    </small>
                  </div>
                )}

                <div className="connector-editor__field">
                  <label className="connector-editor__label">Hard Deletes</label>
                  <select
                    className="connector-editor__select"
                    value={snap.hard_deletes ?? 'ignore'}
                    onChange={(e) =>
                      updateSnap({ hard_deletes: e.target.value as HardDeletes })
                    }
                  >
                    {HARD_DELETES.map((h) => (
                      <option key={h.value} value={h.value}>{h.label}</option>
                    ))}
                  </select>
                  <small className="connector-editor__hint">
                    What to do with rows present in the target but missing from the source.
                  </small>
                </div>
              </div>
            );
          })()}

          {readMode === 'incremental' && (
            <>
              <div className="connector-editor__field">
                <label className="connector-editor__label">Watermark Column *</label>
                <input
                  className="connector-editor__input"
                  type="text"
                  value={p.watermark?.column ?? ''}
                  onChange={(e) =>
                    update({
                      watermark: {
                        column: e.target.value,
                        type: p.watermark?.type ?? 'timestamp',
                      },
                    })
                  }
                  placeholder="updated_at"
                />
              </div>
              <div className="connector-editor__field">
                <label className="connector-editor__label">Watermark Type</label>
                <select
                  className="connector-editor__select"
                  value={p.watermark?.type ?? 'timestamp'}
                  onChange={(e) =>
                    update({
                      watermark: {
                        column: p.watermark?.column ?? '',
                        type: e.target.value as WatermarkType,
                      },
                    })
                  }
                >
                  {WATERMARK_TYPES.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
              </div>

              {p.watermark?.type === 'timestamp' && (
                <div className="connector-editor__field">
                  <label className="connector-editor__label">Lookback (ISO 8601)</label>
                  <input
                    className="connector-editor__input"
                    type="text"
                    value={p.lookback ?? ''}
                    onChange={(e) =>
                      update({ lookback: e.target.value.trim() || undefined })
                    }
                    placeholder="PT1H"
                  />
                  <small className="connector-editor__hint">
                    Subtracted from the stored watermark before filtering. Leave blank for none.
                  </small>
                </div>
              )}

              <div className="connector-editor__field">
                <label className="connector-editor__label">First Run</label>
                <select
                  className="connector-editor__select"
                  value={p.first_run ?? 'full'}
                  onChange={(e) => update({ first_run: e.target.value as FirstRun })}
                >
                  {FIRST_RUN.map((f) => (
                    <option key={f} value={f}>{f}</option>
                  ))}
                </select>
              </div>
            </>
          )}

          <div className="connector-editor__field">
            <label className="connector-editor__label">On Schema Change</label>
            <select
              className="connector-editor__select"
              value={p.on_schema_change ?? 'append_new_columns'}
              onChange={(e) =>
                update({ on_schema_change: e.target.value as OnSchemaChange })
              }
            >
              {SCHEMA_CHANGE.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          {errors.length > 0 && (
            <div className="connector-editor__error">
              <ul style={{ margin: 0, paddingLeft: '1.2em' }}>
                {errors.map((e, i) => (
                  <li key={i}>{e}</li>
                ))}
              </ul>
            </div>
          )}

          {canReset && (
            <div className="connector-editor__field">
              <button
                type="button"
                className="connector-editor__test-btn"
                style={{ borderColor: '#ef4444', color: '#ef4444' }}
                onClick={() => setConfirmReset(true)}
                disabled={resetState.kind === 'busy'}
                data-testid="reset-incremental-state-btn"
              >
                {resetState.kind === 'busy' ? 'Resetting…' : 'Reset incremental state'}
              </button>
              <small className="connector-editor__hint">
                Clears the stored watermark for this sink. The next run will be a full bootstrap.
              </small>
              {resetState.kind === 'ok' && (
                <small className="connector-editor__hint" style={{ color: '#10b981' }}>
                  Incremental state reset.
                </small>
              )}
              {resetState.kind === 'err' && (
                <small className="connector-editor__hint" style={{ color: '#ef4444' }}>
                  {resetState.message}
                </small>
              )}
            </div>
          )}
        </>
      )}

      <ConfirmDialog
        open={confirmReset}
        title="Reset incremental state?"
        message="The stored watermark for this sink will be cleared. The next run will read all rows from the source as a fresh bootstrap. This cannot be undone."
        confirmLabel="Reset"
        cancelLabel="Cancel"
        destructive
        onConfirm={handleReset}
        onCancel={() => setConfirmReset(false)}
      />
    </div>
  );
}
