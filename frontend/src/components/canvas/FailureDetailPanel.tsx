// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import {
  fetchFailureReport,
  downloadReproduceBundle,
  type ApiFailureReport,
  type ApiInputSchema,
} from '../../api/runs';
import './FailureDetailPanel.css';

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function ErrorChain({ chain }: { chain: string[] }) {
  return (
    <ul className="failure-panel__error-chain">
      {chain.map((msg, i) => (
        <li key={i} className="failure-panel__error-item">
          {i > 0 && <span className="failure-panel__error-depth">caused by:</span>}
          {msg}
        </li>
      ))}
    </ul>
  );
}

function SchemaSection({ schemas }: { schemas: ApiInputSchema[] }) {
  if (schemas.length === 0) return null;
  return (
    <>
      {schemas.map((s) => (
        <div key={s.node_id} className="failure-panel__schema-group">
          <div className="failure-panel__schema-source">
            from {s.node_id}
          </div>
          <ul className="failure-panel__schema-list">
            {s.fields.map((f) => (
              <li key={f.name} className="failure-panel__schema-item">
                <span className="failure-panel__schema-name">{f.name}</span>
                <span className="failure-panel__schema-meta">
                  <span className="failure-panel__schema-type">{f.data_type}</span>
                  {f.nullable && (
                    <span className="failure-panel__schema-nullable">?</span>
                  )}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </>
  );
}

function SqlDisplay({ sql, label }: { sql: string; label: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(sql).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    });
  }, [sql]);

  return (
    <div>
      <div className="failure-panel__section-title">
        {label}
        <button className="failure-panel__copy-btn" onClick={handleCopy}>
          {copied ? 'copied' : 'copy'}
        </button>
      </div>
      <pre className="failure-panel__sql">{sql}</pre>
    </div>
  );
}

function InputSampleTable({
  sample,
  totalRows,
}: {
  sample: Record<string, unknown>[];
  totalRows: number;
}) {
  const [copyFormat, setCopyFormat] = useState<'json' | 'csv'>('json');
  const [copied, setCopied] = useState(false);

  if (sample.length === 0) return null;

  const columns = Object.keys(sample[0]);

  const handleCopy = useCallback(() => {
    let text: string;
    if (copyFormat === 'json') {
      text = JSON.stringify(sample, null, 2);
    } else {
      const header = columns.join(',');
      const rows = sample.map((row) =>
        columns
          .map((col) => {
            const val = row[col];
            const s = val == null ? '' : String(val);
            return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
          })
          .join(','),
      );
      text = [header, ...rows].join('\n');
    }
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    });
  }, [sample, columns, copyFormat]);

  return (
    <div>
      <div className="failure-panel__section-title">
        Input Sample
        <span>
          <button
            className="failure-panel__copy-btn"
            onClick={() => { setCopyFormat('json'); handleCopy(); }}
            style={{ marginRight: 8 }}
          >
            {copied && copyFormat === 'json' ? 'copied' : 'copy JSON'}
          </button>
          <button
            className="failure-panel__copy-btn"
            onClick={() => { setCopyFormat('csv'); handleCopy(); }}
          >
            {copied && copyFormat === 'csv' ? 'copied' : 'copy CSV'}
          </button>
        </span>
      </div>
      <div className="failure-panel__sample-wrap">
        <table className="failure-panel__sample-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sample.map((row, i) => (
              <tr key={i}>
                {columns.map((col) => (
                  <td key={col} title={String(row[col] ?? '')}>
                    {row[col] == null ? <em>null</em> : String(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalRows > sample.length && (
        <div className="failure-panel__sample-info">
          Showing {sample.length} of {totalRows.toLocaleString()} total input rows
        </div>
      )}
    </div>
  );
}

function PluginDiagnosticsSection({
  diag,
}: {
  diag: NonNullable<ApiFailureReport['plugin_diagnostics']>;
}) {
  return (
    <div>
      {diag.plugin_name && (
        <div className="failure-panel__plugin-field">
          <div className="failure-panel__plugin-label">Plugin</div>
          <div className="failure-panel__plugin-value">{diag.plugin_name}</div>
        </div>
      )}
      {diag.exit_code != null && (
        <div className="failure-panel__plugin-field">
          <div className="failure-panel__plugin-label">Exit Code</div>
          <div className="failure-panel__plugin-value">{diag.exit_code}</div>
        </div>
      )}
      {diag.stderr_tail && (
        <div className="failure-panel__plugin-field">
          <div className="failure-panel__plugin-label">stderr</div>
          <pre className="failure-panel__plugin-stderr">{diag.stderr_tail}</pre>
        </div>
      )}
      {diag.last_protocol_messages.length > 0 && (
        <div className="failure-panel__plugin-field">
          <div className="failure-panel__plugin-label">Protocol Messages</div>
          <pre className="failure-panel__plugin-stderr">
            {diag.last_protocol_messages.join('\n')}
          </pre>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------

export interface FailureDetailPanelProps {
  /** Pipeline ID for API calls. */
  pipelineId: string;
  /** Run ID that contains the failure. */
  runId: string;
  /** Node ID of the failed node. */
  nodeId: string;
  /** Whether the panel is visible. */
  open: boolean;
  /** Close the panel. */
  onClose: () => void;
  /** Navigate the canvas to a specific node. */
  onJumpToNode: (nodeId: string) => void;
  /** Navigate to the project lineage view. */
  onShowLineage?: () => void;
}

export function FailureDetailPanel({
  pipelineId,
  runId,
  nodeId,
  open,
  onClose,
  onJumpToNode,
  onShowLineage,
}: FailureDetailPanelProps) {
  const [report, setReport] = useState<ApiFailureReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [downloading, setDownloading] = useState(false);

  // Collapsed section state
  const [sectionsOpen, setSectionsOpen] = useState<Record<string, boolean>>({
    errors: true,
    schema: true,
    sample: true,
    sql: false,
    plugin: true,
  });

  const toggleSection = useCallback((key: string) => {
    setSectionsOpen((prev) => ({ ...prev, [key]: !prev[key] }));
  }, []);

  useEffect(() => {
    if (!open || !pipelineId || !runId || !nodeId) return;

    let cancelled = false;
    setLoading(true);
    setError(null);
    setReport(null);

    fetchFailureReport(pipelineId, runId, nodeId)
      .then((r) => {
        if (!cancelled) setReport(r);
      })
      .catch((err) => {
        if (!cancelled) setError((err as Error).message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [open, pipelineId, runId, nodeId]);

  const handleDownload = useCallback(async () => {
    setDownloading(true);
    try {
      await downloadReproduceBundle(pipelineId, runId, nodeId);
    } catch (err) {
      console.error('Failed to download reproduce bundle:', err);
    } finally {
      setDownloading(false);
    }
  }, [pipelineId, runId, nodeId]);

  const handleJump = useCallback(() => {
    onJumpToNode(nodeId);
    onClose();
  }, [nodeId, onJumpToNode, onClose]);

  const capturedAt = report
    ? new Date(report.captured_at_ms).toLocaleString()
    : '';

  return (
    <div
      className={`failure-panel${open ? ' failure-panel--open' : ''}`}
      data-testid="failure-detail-panel"
    >
      <div className="failure-panel__header">
        <span className="failure-panel__title">
          Failure: {nodeId}
        </span>
        <button
          className="failure-panel__close"
          onClick={onClose}
          aria-label="Close failure panel"
        >
          &times;
        </button>
      </div>

      <div className="failure-panel__body">
        {loading && (
          <div className="failure-panel__loading">Loading failure report...</div>
        )}

        {error && (
          <div className="failure-panel__error">{error}</div>
        )}

        {report && (
          <>
            {/* Context */}
            <div className="failure-panel__section">
              <div className="failure-panel__context">
                <span>
                  <span className="failure-panel__context-label">Pipeline </span>
                  <span className="failure-panel__context-value">{report.pipeline_name}</span>
                </span>
                <span>
                  <span className="failure-panel__context-label">Env </span>
                  <span className="failure-panel__context-value">{report.environment}</span>
                </span>
                <span>
                  <span className="failure-panel__context-label">Captured </span>
                  <span className="failure-panel__context-value">{capturedAt}</span>
                </span>
              </div>
            </div>

            {/* Error chain */}
            <div className="failure-panel__section">
              <div
                className="failure-panel__section-title"
                onClick={() => toggleSection('errors')}
              >
                Error Chain ({report.error_chain.length})
                <span>{sectionsOpen.errors ? '\u25BC' : '\u25B6'}</span>
              </div>
              {sectionsOpen.errors && <ErrorChain chain={report.error_chain} />}
            </div>

            {/* Input schemas */}
            {report.input_schemas.length > 0 && (
              <div className="failure-panel__section">
                <div
                  className="failure-panel__section-title"
                  onClick={() => toggleSection('schema')}
                >
                  Input Schema ({report.input_schemas.reduce((n, s) => n + s.fields.length, 0)} columns)
                  <span>{sectionsOpen.schema ? '\u25BC' : '\u25B6'}</span>
                </div>
                {sectionsOpen.schema && <SchemaSection schemas={report.input_schemas} />}
              </div>
            )}

            {/* Executed SQL */}
            {report.executed_sql && (
              <div className="failure-panel__section">
                <div onClick={() => toggleSection('sql')} style={{ cursor: 'pointer' }}>
                  {sectionsOpen.sql ? (
                    <SqlDisplay sql={report.executed_sql} label="Executed SQL" />
                  ) : (
                    <div className="failure-panel__section-title">
                      Executed SQL
                      <span>{'\u25B6'}</span>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Source query */}
            {report.source_query && (
              <div className="failure-panel__section">
                <SqlDisplay sql={report.source_query} label="Source Query" />
              </div>
            )}

            {/* Input sample */}
            {report.input_sample.length > 0 && (
              <div className="failure-panel__section">
                <div onClick={() => toggleSection('sample')} style={{ cursor: 'pointer' }}>
                  {sectionsOpen.sample ? (
                    <InputSampleTable
                      sample={report.input_sample}
                      totalRows={report.input_total_rows}
                    />
                  ) : (
                    <div className="failure-panel__section-title">
                      Input Sample ({report.input_sample.length} rows)
                      <span>{'\u25B6'}</span>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Plugin diagnostics */}
            {report.plugin_diagnostics && (
              <div className="failure-panel__section">
                <div
                  className="failure-panel__section-title"
                  onClick={() => toggleSection('plugin')}
                >
                  Plugin Diagnostics
                  <span>{sectionsOpen.plugin ? '\u25BC' : '\u25B6'}</span>
                </div>
                {sectionsOpen.plugin && (
                  <PluginDiagnosticsSection diag={report.plugin_diagnostics} />
                )}
              </div>
            )}
          </>
        )}
      </div>

      {/* Actions footer */}
      {report && (
        <div className="failure-panel__actions">
          <button
            className="failure-panel__action-btn failure-panel__action-btn--primary"
            onClick={handleJump}
          >
            Jump to Node
          </button>
          <button
            className="failure-panel__action-btn"
            onClick={handleDownload}
            disabled={downloading}
          >
            {downloading ? 'Downloading...' : 'Reproduce Locally'}
          </button>
          {onShowLineage && (
            <button
              className="failure-panel__action-btn"
              onClick={onShowLineage}
              title="Show this pipeline's upstream dependencies and downstream impact in the project lineage view"
            >
              Show Lineage
            </button>
          )}
        </div>
      )}
    </div>
  );
}
