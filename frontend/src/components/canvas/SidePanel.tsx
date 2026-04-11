// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { PipelineNode } from '../../types/pipeline';
import { roleIcon } from '../iconMaps';
import type { ApiNode, ApiColumnInfo, ApiSampleConfig, ApiTestResult, ApiAssertionResult } from '../../api/pipelines';
import {
  previewPipeline,
  updatePipeline,
  fetchPipelineRunsPaginated,
  fetchRunIncrementalStats,
  listPipelines,
  type ApiPreviewNodeResponse,
  type ApiNodeRunStats,
  type ApiPipelineRun,
  type MaterializationReceipt,
} from '../../api/pipelines';
import { buildApiPipeline } from '../../stores/pipelineStore';
import { useEnvironmentStore } from '../../stores/environmentStore';
import { PreviewTable } from './PreviewTable';
import { SampleConfigDropdown } from './SampleConfigDropdown';
import { SchemaDiffViewer } from './SchemaDiffViewer';
import { computeSchemaDiff, type SchemaDiff } from './schemaDiff';
import {
  fetchUpstream,
  fetchDownstream,
  type LineageDirectionResponse,
} from '../../api/lineage';
import { ColumnLineageModal } from './ColumnLineageModal';
import './SidePanel.css';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// roleIcon imported from ../icons

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function maskConnectionString(value: unknown): string {
  if (typeof value !== 'string') return String(value ?? '');
  // Mask password portion of connection strings
  return value.replace(/:([^@/]+)@/, ':****@');
}

function truncateCode(code: string, lines: number): string {
  const split = code.split('\n');
  if (split.length <= lines) return code;
  return split.slice(0, lines).join('\n') + '\n...';
}

// ---------------------------------------------------------------------------
// Schema List sub-component
// ---------------------------------------------------------------------------

interface SchemaListProps {
  preview: ApiPreviewNodeResponse | null;
  collapsible?: boolean;
  schemaDiff?: SchemaDiff | null;
}

function SchemaList({ preview, collapsible = true, schemaDiff }: SchemaListProps) {
  const [collapsed, setCollapsed] = useState(false);
  const [copiedCol, setCopiedCol] = useState<string | null>(null);

  if (!preview || preview.columns.length === 0) {
    return <span className="side-panel__empty">No schema available</span>;
  }

  const handleCopy = (name: string) => {
    navigator.clipboard.writeText(name).then(() => {
      setCopiedCol(name);
      setTimeout(() => setCopiedCol(null), 1200);
    });
  };

  const diffByName = new Map(
    schemaDiff?.outputDiffs.map((d) => [d.column.name, d]) ?? [],
  );

  return (
    <div>
      {collapsible && (
        <button
          className="side-panel__schema-toggle"
          onClick={() => setCollapsed((c) => !c)}
        >
          <span className={`side-panel__schema-arrow${collapsed ? '' : ' side-panel__schema-arrow--open'}`}>
            &#9654;
          </span>
          {preview.columns.length} column{preview.columns.length !== 1 ? 's' : ''}
        </button>
      )}
      {!collapsed && (
        <ul className="side-panel__schema-list">
          {preview.columns.map((col) => {
            const diff = diffByName.get(col.name);
            const diffClass = diff && diff.kind !== 'unchanged'
              ? ` side-panel__schema-item--diff-${diff.kind}`
              : '';
            const diffLabel = diff?.kind === 'added'
              ? ' (new)'
              : diff?.kind === 'renamed'
                ? ` (was ${diff.previousName})`
                : diff?.kind === 'type_changed'
                  ? ` (was ${diff.previousType})`
                  : '';
            return (
              <li
                key={col.name}
                className={`side-panel__schema-item${diffClass}`}
                onClick={() => handleCopy(col.name)}
                title={`Click to copy "${col.name}"${diffLabel}`}
              >
                <span className="side-panel__schema-name">
                  {col.name}
                  {diffLabel && (
                    <span className={`side-panel__schema-diff-label side-panel__schema-diff-label--${diff!.kind}`}>
                      {diffLabel}
                    </span>
                  )}
                  {copiedCol === col.name && (
                    <span className="side-panel__schema-copied">copied</span>
                  )}
                </span>
                <span className="side-panel__schema-meta">
                  <span className="side-panel__schema-type">{col.data_type}</span>
                  {col.nullable && (
                    <span className="side-panel__schema-nullable">?</span>
                  )}
                </span>
              </li>
            );
          })}
          {/* Removed columns */}
          {schemaDiff?.removedColumns.map((d) => (
            <li
              key={`removed-${d.column.name}`}
              className="side-panel__schema-item side-panel__schema-item--diff-removed"
              title={`"${d.column.name}" was removed`}
            >
              <span className="side-panel__schema-name">
                <s>{d.column.name}</s>
                <span className="side-panel__schema-diff-label side-panel__schema-diff-label--removed">
                  {' '}(removed)
                </span>
              </span>
              <span className="side-panel__schema-meta">
                <span className="side-panel__schema-type">{d.column.data_type}</span>
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Run Stats sub-component
// ---------------------------------------------------------------------------

interface RunStatsProps {
  stats: ApiNodeRunStats | null;
  role: string;
  receipt?: MaterializationReceipt | null;
  onViewFailureReport?: () => void;
}

function formatWatermark(wm: { value: string; type: string } | undefined): string {
  if (!wm) return '—';
  return wm.value;
}

function RunStats({ stats, role, receipt, onViewFailureReport }: RunStatsProps) {
  if (!stats) {
    return <span className="side-panel__empty">No run data</span>;
  }
  return (
    <div>
      {role !== 'sink' && (
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Rows out</span>
          <span className="side-panel__kv-value">{stats.rows_out.toLocaleString()}</span>
        </div>
      )}
      {role === 'transform' && (
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Rows in</span>
          <span className="side-panel__kv-value">{stats.rows_in.toLocaleString()}</span>
        </div>
      )}
      {role === 'sink' && (
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Rows written</span>
          <span className="side-panel__kv-value">{stats.rows_out.toLocaleString()}</span>
        </div>
      )}
      <div className="side-panel__kv">
        <span className="side-panel__kv-key">Duration</span>
        <span className="side-panel__kv-value">{formatDuration(stats.duration_ms)}</span>
      </div>
      {stats.error && (
        <>
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Error</span>
            <span className="side-panel__kv-value" style={{ color: '#ef4444' }}>
              {stats.error}
            </span>
          </div>
          {onViewFailureReport && (
            <button
              className="side-panel__action-btn"
              style={{ marginTop: 6, width: '100%' }}
              onClick={onViewFailureReport}
            >
              View Failure Report
            </button>
          )}
        </>
      )}
      {role === 'sink' && receipt && (
        <div data-testid="run-stats-receipt">
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Read mode</span>
            <span className="side-panel__kv-value">{receipt.read_mode}</span>
          </div>
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Strategy</span>
            <span className="side-panel__kv-value">{receipt.write_strategy}</span>
          </div>
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Rows scanned</span>
            <span className="side-panel__kv-value">
              {receipt.rows_scanned.toLocaleString()}
            </span>
          </div>
          {receipt.rows_filtered_by_watermark > 0 && (
            <div className="side-panel__kv">
              <span className="side-panel__kv-key">Filtered by watermark</span>
              <span className="side-panel__kv-value">
                {receipt.rows_filtered_by_watermark.toLocaleString()}
              </span>
            </div>
          )}
          {receipt.rows_inserted > 0 && (
            <div className="side-panel__kv">
              <span className="side-panel__kv-key">Inserted</span>
              <span className="side-panel__kv-value">
                {receipt.rows_inserted.toLocaleString()}
              </span>
            </div>
          )}
          {receipt.rows_updated > 0 && (
            <div className="side-panel__kv">
              <span className="side-panel__kv-key">Updated</span>
              <span className="side-panel__kv-value">
                {receipt.rows_updated.toLocaleString()}
              </span>
            </div>
          )}
          {receipt.rows_deleted > 0 && (
            <div className="side-panel__kv">
              <span className="side-panel__kv-key">Deleted</span>
              <span className="side-panel__kv-value">
                {receipt.rows_deleted.toLocaleString()}
              </span>
            </div>
          )}
          {receipt.read_mode === 'incremental' && (
            <>
              <div className="side-panel__kv">
                <span className="side-panel__kv-key">Watermark before</span>
                <span className="side-panel__kv-value">
                  {formatWatermark(receipt.watermark_before)}
                </span>
              </div>
              <div className="side-panel__kv">
                <span className="side-panel__kv-key">Watermark after</span>
                <span className="side-panel__kv-value">
                  {formatWatermark(receipt.watermark_after)}
                </span>
              </div>
            </>
          )}
          {receipt.schema_diff && (
            <div className="side-panel__kv" style={{ flexDirection: 'column', alignItems: 'flex-start' }}>
              <span className="side-panel__kv-key">Schema diff</span>
              <SchemaDiffViewer diff={receipt.schema_diff} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Source node content
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Cache row limit inline editor
// ---------------------------------------------------------------------------

interface CacheRowLimitProps {
  value: number | undefined;
  onChange: (value: number | undefined) => void;
}

function CacheRowLimitInput({ value, onChange }: CacheRowLimitProps) {
  const [draft, setDraft] = useState(value?.toString() ?? '');
  const [editing, setEditing] = useState(false);

  const commit = useCallback(() => {
    setEditing(false);
    const parsed = parseInt(draft, 10);
    if (!draft.trim()) {
      onChange(undefined); // Clear override → fall back to pipeline default
    } else if (!Number.isNaN(parsed) && parsed > 0) {
      onChange(parsed);
    } else {
      setDraft(value?.toString() ?? '');
    }
  }, [draft, value, onChange]);

  const display = value != null ? value.toLocaleString() : 'default';

  if (!editing) {
    return (
      <span
        className="side-panel__kv-value"
        style={{ cursor: 'pointer' }}
        onClick={() => { setDraft(value?.toString() ?? ''); setEditing(true); }}
        title="Click to edit cache row limit"
      >
        {display}
      </span>
    );
  }

  return (
    <input
      className="side-panel__cache-limit-input"
      type="text"
      inputMode="numeric"
      value={draft}
      placeholder="default"
      autoFocus
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === 'Enter') commit();
        if (e.key === 'Escape') { setDraft(value?.toString() ?? ''); setEditing(false); }
      }}
    />
  );
}

// ---------------------------------------------------------------------------
// Node content components
// ---------------------------------------------------------------------------

interface NodeContentProps {
  node: PipelineNode;
  apiNode: ApiNode | undefined;
  preview: ApiPreviewNodeResponse | null;
  previewLoading: boolean;
  previewError?: string | null;
  sampleMethod?: string;
  runStats: ApiNodeRunStats | null;
  receipt?: MaterializationReceipt | null;
  testResult?: ApiTestResult | null;
  recentRuns?: ApiPipelineRun[];
  upstreamNames: string[];
  schemaDiff?: SchemaDiff | null;
  sampleConfig?: ApiSampleConfig;
  onSampleConfigChange?: (config: ApiSampleConfig) => void;
  reExecute: boolean;
  onReExecuteChange?: (value: boolean) => void;
  onCacheRowLimitChange?: (value: number | undefined) => void;
  onMaterializedChange?: (value: boolean) => void;
  feedsSink?: boolean;
  latestRunId?: string;
  onViewFailureReport?: (runId: string, nodeId: string) => void;
}

function SourceContent({ node, apiNode, preview, previewLoading, previewError, sampleMethod, runStats, sampleConfig, onSampleConfigChange, onCacheRowLimitChange, latestRunId, onViewFailureReport }: NodeContentProps) {
  const connector = apiNode?.connector ?? 'unknown';
  const config = apiNode?.config as Record<string, unknown> | undefined;

  return (
    <>
      <div className="side-panel__section">
        <div className="side-panel__section-title">Configuration</div>
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Connector</span>
          <span className="side-panel__kv-value">{connector}</span>
        </div>
        {onCacheRowLimitChange && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Cache limit</span>
            <CacheRowLimitInput
              value={apiNode?.cache_row_limit}
              onChange={onCacheRowLimitChange}
            />
          </div>
        )}
        {config?.connection_string != null && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Connection</span>
            <span className="side-panel__kv-value">
              {maskConnectionString(config.connection_string)}
            </span>
          </div>
        )}
        {config?.path != null && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Path</span>
            <span className="side-panel__kv-value">{String(config.path)}</span>
          </div>
        )}
        {config?.query != null && (
          <div className="side-panel__code-preview">
            {truncateCode(String(config.query), 3)}
          </div>
        )}
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Last Run</div>
        <RunStats stats={runStats} role="source" onViewFailureReport={runStats?.error && latestRunId && onViewFailureReport ? () => onViewFailureReport(latestRunId, node.id) : undefined} />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">
          Preview
          {onSampleConfigChange && (
            <SampleConfigDropdown value={sampleConfig} onChange={onSampleConfigChange} />
          )}
        </div>
        <PreviewTable preview={preview} loading={previewLoading} error={previewError} sampleMethod={sampleMethod} nodeId={node.id} />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Schema</div>
        <SchemaList preview={preview} />
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Transform node content
// ---------------------------------------------------------------------------

function TransformContent({
  node,
  apiNode,
  preview,
  previewLoading,
  previewError,
  sampleMethod,
  runStats,
  upstreamNames,
  schemaDiff,
  sampleConfig,
  onSampleConfigChange,
  reExecute,
  onReExecuteChange,
  onCacheRowLimitChange,
  onMaterializedChange,
  feedsSink,
  latestRunId,
  onViewFailureReport,
}: NodeContentProps) {
  const mode = apiNode?.mode ?? 'sql';
  const code = apiNode?.code ?? '';
  const isMaterialized = apiNode?.materialized ?? false;

  return (
    <>
      <div className="side-panel__section">
        <div className="side-panel__section-title">Transform</div>
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Mode</span>
          <span className="side-panel__kv-value">{mode.toUpperCase()}</span>
        </div>
        {onMaterializedChange && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Materialized</span>
            <label className="side-panel__toggle side-panel__toggle--inline" title="When enabled, this node's output is cached for preview and downstream use">
              <input
                type="checkbox"
                checked={isMaterialized}
                onChange={(e) => onMaterializedChange(e.target.checked)}
              />
              {isMaterialized ? 'Yes' : 'No'}
            </label>
          </div>
        )}
        {feedsSink && !isMaterialized && (
          <div className="side-panel__hint">
            This transform feeds a sink — consider enabling materialization for preview
          </div>
        )}
        {onCacheRowLimitChange && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Cache limit</span>
            <CacheRowLimitInput
              value={apiNode?.cache_row_limit}
              onChange={onCacheRowLimitChange}
            />
          </div>
        )}
        {code && (
          <div className="side-panel__code-preview">{truncateCode(code, 5)}</div>
        )}
      </div>

      {upstreamNames.length > 0 && (
        <div className="side-panel__section">
          <div className="side-panel__section-title">Inputs</div>
          {upstreamNames.map((name) => (
            <div key={name} className="side-panel__kv">
              <span className="side-panel__kv-value">{name}</span>
            </div>
          ))}
        </div>
      )}

      <div className="side-panel__section">
        <div className="side-panel__section-title">Last Run</div>
        <RunStats stats={runStats} role="transform" onViewFailureReport={runStats?.error && latestRunId && onViewFailureReport ? () => onViewFailureReport(latestRunId, node.id) : undefined} />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">
          Preview
          {onSampleConfigChange && (
            <SampleConfigDropdown value={sampleConfig} onChange={onSampleConfigChange} />
          )}
        </div>
        {onReExecuteChange && (
          <label className="side-panel__toggle" title="Re-execute this node against cached upstream data to preview code changes">
            <input
              type="checkbox"
              checked={reExecute}
              onChange={(e) => onReExecuteChange(e.target.checked)}
            />
            Re-execute
          </label>
        )}
        <PreviewTable
          preview={preview}
          loading={previewLoading}
          error={previewError}
          sampleMethod={sampleMethod}
          columnDiffs={schemaDiff?.outputDiffs}
          nodeId={node.id}
        />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Output Schema</div>
        <SchemaList preview={preview} schemaDiff={schemaDiff} />
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Sink node content
// ---------------------------------------------------------------------------

function SinkContent({ node, apiNode, runStats, receipt, latestRunId, onViewFailureReport }: NodeContentProps) {
  const connector = apiNode?.connector ?? 'unknown';
  const config = apiNode?.config as Record<string, unknown> | undefined;

  return (
    <>
      <div className="side-panel__section">
        <div className="side-panel__section-title">Configuration</div>
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Connector</span>
          <span className="side-panel__kv-value">{connector}</span>
        </div>
        {config?.table != null && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Table</span>
            <span className="side-panel__kv-value">{String(config.table)}</span>
          </div>
        )}
        {config?.write_mode != null && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Write mode</span>
            <span className="side-panel__kv-value">{String(config.write_mode)}</span>
          </div>
        )}
        {config?.path != null && (
          <div className="side-panel__kv">
            <span className="side-panel__kv-key">Path</span>
            <span className="side-panel__kv-value">{String(config.path)}</span>
          </div>
        )}
      </div>

      {node.data.envOverridden && (
        <div className="side-panel__section">
          <div className="side-panel__section-title">Environment</div>
          <span className="side-panel__env-badge">Override active</span>
        </div>
      )}

      <div className="side-panel__section">
        <div className="side-panel__section-title">Last Run</div>
        <RunStats stats={runStats} role="sink" receipt={receipt} onViewFailureReport={runStats?.error && latestRunId && onViewFailureReport ? () => onViewFailureReport(latestRunId, node.id) : undefined} />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Preview</div>
        <span className="side-panel__empty">Sinks do not produce preview data</span>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Assertion result row sub-component
// ---------------------------------------------------------------------------

function AssertionResultRow({ result }: { result: ApiAssertionResult }) {
  const [expanded, setExpanded] = useState(false);
  const hasViolations = (result.violating_rows?.length ?? 0) > 0;

  return (
    <div className="side-panel__assertion-row">
      <div
        className="side-panel__assertion-header"
        onClick={hasViolations ? () => setExpanded(!expanded) : undefined}
        style={{ cursor: hasViolations ? 'pointer' : 'default' }}
      >
        <span className={`side-panel__assertion-icon ${result.passed ? 'side-panel__assertion-icon--pass' : 'side-panel__assertion-icon--fail'}`}>
          {result.passed ? '\u2713' : '\u2717'}
        </span>
        <span className="side-panel__assertion-name">{result.name}</span>
        {!result.passed && (
          <span className="side-panel__assertion-count">
            {result.violation_count.toLocaleString()}
          </span>
        )}
        {hasViolations && (
          <span className={`side-panel__schema-arrow${expanded ? ' side-panel__schema-arrow--open' : ''}`}>
            &#9654;
          </span>
        )}
      </div>
      {result.message && !result.passed && (
        <div className="side-panel__assertion-message">{result.message}</div>
      )}
      {expanded && hasViolations && (
        <div className="side-panel__table-wrap">
          <table className="side-panel__table">
            <thead>
              <tr>
                {Object.keys(result.violating_rows![0]).map((col) => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.violating_rows!.map((row, i) => (
                <tr key={i}>
                  {Object.values(row).map((val, j) => (
                    <td key={j}>{val == null ? 'NULL' : String(val)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Test trend mini-bar
// ---------------------------------------------------------------------------

function TestTrendBar({ nodeId, runs }: { nodeId: string; runs: ApiPipelineRun[] }) {
  // Only include runs that have test results for this node
  const relevant = runs
    .filter((r) => r.test_results?.some((tr) => tr.node_id === nodeId))
    .slice(0, 10)
    .reverse(); // oldest first for left-to-right display

  if (relevant.length === 0) {
    return <span className="side-panel__empty">No test history</span>;
  }

  return (
    <div className="side-panel__test-trend" title="Recent test results (oldest to newest)">
      {relevant.map((run) => {
        const tr = run.test_results!.find((t) => t.node_id === nodeId)!;
        const cls = tr.passed
          ? 'side-panel__trend-dot--pass'
          : tr.severity === 'warn'
            ? 'side-panel__trend-dot--warn'
            : 'side-panel__trend-dot--fail';
        return (
          <span
            key={run.id}
            className={`side-panel__trend-dot ${cls}`}
            title={`${run.id.slice(0, 8)} — ${tr.passed ? 'passed' : 'failed'}`}
          />
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Test node content
// ---------------------------------------------------------------------------

function TestContent({ apiNode, testResult, recentRuns, node }: NodeContentProps) {
  const severity = (apiNode as Record<string, unknown> | undefined)?.severity as string | undefined;
  const assertions = (apiNode as Record<string, unknown> | undefined)?.assertions as Array<Record<string, unknown>> | undefined;

  return (
    <>
      <div className="side-panel__section">
        <div className="side-panel__section-title">Configuration</div>
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Severity</span>
          <span className={`side-panel__kv-value ${severity === 'warn' ? 'side-panel__severity--warn' : ''}`}>
            {severity ?? 'error'}
          </span>
        </div>
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Assertions</span>
          <span className="side-panel__kv-value">{assertions?.length ?? 0}</span>
        </div>
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Last Run Results</div>
        {!testResult ? (
          <span className="side-panel__empty">No test results yet</span>
        ) : (
          <>
            <div className="side-panel__kv" style={{ marginBottom: 8 }}>
              <span className="side-panel__kv-key">Status</span>
              <span
                className="side-panel__kv-value"
                style={{ color: testResult.passed ? '#16a34a' : testResult.severity === 'warn' ? '#d97706' : '#ef4444' }}
              >
                {testResult.passed ? 'Passed' : testResult.severity === 'warn' ? 'Warning' : 'Failed'}
              </span>
            </div>
            {testResult.assertions.map((a, i) => (
              <AssertionResultRow key={i} result={a} />
            ))}
          </>
        )}
      </div>

      {recentRuns && recentRuns.length > 0 && (
        <div className="side-panel__section">
          <div className="side-panel__section-title">Test History</div>
          <TestTrendBar nodeId={node.id} runs={recentRuns} />
        </div>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Inline editable name
// ---------------------------------------------------------------------------

interface InlineNameProps {
  name: string;
  onRename: (newName: string) => void;
}

function InlineNameInner({ name, onRename }: InlineNameProps) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (editing) inputRef.current?.select();
  }, [editing]);

  const commit = useCallback(() => {
    const trimmed = draft.trim();
    if (trimmed && trimmed !== name) {
      onRename(trimmed);
    } else {
      setDraft(name);
    }
    setEditing(false);
  }, [draft, name, onRename]);

  if (editing) {
    return (
      <input
        ref={inputRef}
        className="side-panel__name-input"
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') commit();
          if (e.key === 'Escape') {
            setDraft(name);
            setEditing(false);
          }
        }}
      />
    );
  }

  return (
    <span
      className="side-panel__name-display"
      onClick={() => setEditing(true)}
      title="Click to rename"
    >
      {name}
    </span>
  );
}

/** Wrapper that resets InlineNameInner state when `name` changes via key. */
function InlineName({ name, onRename }: InlineNameProps) {
  return <InlineNameInner key={name} name={name} onRename={onRename} />;
}

// ---------------------------------------------------------------------------
// Lineage section (cross-pipeline upstream/downstream)
// ---------------------------------------------------------------------------

function LineageSection({
  pipelineId,
  environment,
  onNavigate,
}: {
  pipelineId: string;
  environment: string;
  onNavigate?: (id: string) => void;
}) {
  const [collapsed, setCollapsed] = useState(true);
  const [upstream, setUpstream] = useState<LineageDirectionResponse | null>(null);
  const [downstream, setDownstream] = useState<LineageDirectionResponse | null>(null);
  const [names, setNames] = useState<Map<string, string>>(new Map());
  const [loading, setLoading] = useState(false);
  const fetchedRef = useRef(false);

  useEffect(() => {
    if (collapsed || fetchedRef.current) return;
    fetchedRef.current = true;

    async function load() {
      setLoading(true);
      try {
        const [up, down, pipelines] = await Promise.all([
          fetchUpstream(pipelineId, environment),
          fetchDownstream(pipelineId, environment),
          listPipelines(1000, 0),
        ]);
        setUpstream(up);
        setDownstream(down);
        const nameMap = new Map<string, string>();
        for (const p of pipelines.data) {
          nameMap.set(p.id, p.pipeline.name);
        }
        setNames(nameMap);
      } catch {
        // Lineage data unavailable — silently degrade
      } finally {
        setLoading(false);
      }
    }

    load();
  }, [collapsed, pipelineId, environment]);

  // Reset when pipeline changes — use a ref to track previous pipelineId
  // so we avoid calling setState synchronously in an effect.
  const prevPipelineIdRef = useRef(pipelineId);
  if (prevPipelineIdRef.current !== pipelineId) {
    prevPipelineIdRef.current = pipelineId;
    fetchedRef.current = false;
    setUpstream(null);
    setDownstream(null);
    setCollapsed(true);
  }

  const upstreamIds = upstream?.transitive ?? [];
  const downstreamIds = downstream?.transitive ?? [];
  const hasLineage = upstreamIds.length > 0 || downstreamIds.length > 0;

  return (
    <div className="side-panel__section">
      <div
        className="side-panel__section-title side-panel__section-title--clickable"
        onClick={() => setCollapsed((c) => !c)}
      >
        Lineage {collapsed ? '\u25B6' : '\u25BC'}
      </div>
      {!collapsed && (
        <>
          {loading && (
            <span className="side-panel__empty">Loading lineage...</span>
          )}
          {!loading && !hasLineage && (
            <span className="side-panel__empty">
              No cross-pipeline lineage detected
            </span>
          )}
          {!loading && upstreamIds.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <div className="side-panel__kv-key" style={{ marginBottom: 4 }}>
                Upstream ({upstreamIds.length})
              </div>
              {upstreamIds.map((id) => (
                <div
                  key={id}
                  className="side-panel__lineage-item"
                  onClick={() => onNavigate?.(id)}
                  role={onNavigate ? 'button' : undefined}
                  tabIndex={onNavigate ? 0 : undefined}
                >
                  {names.get(id) ?? id}
                </div>
              ))}
            </div>
          )}
          {!loading && downstreamIds.length > 0 && (
            <div>
              <div className="side-panel__kv-key" style={{ marginBottom: 4 }}>
                Downstream ({downstreamIds.length})
              </div>
              {downstreamIds.map((id) => (
                <div
                  key={id}
                  className="side-panel__lineage-item"
                  onClick={() => onNavigate?.(id)}
                  role={onNavigate ? 'button' : undefined}
                  tabIndex={onNavigate ? 0 : undefined}
                >
                  {names.get(id) ?? id}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main SidePanel component
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Run history list sub-component
// ---------------------------------------------------------------------------

function RunHistoryList({
  runs,
  onViewRunDetail,
}: {
  runs: ApiPipelineRun[];
  onViewRunDetail?: (runId: string) => void;
}) {
  if (runs.length === 0) {
    return <span className="side-panel__empty">No runs yet</span>;
  }

  return (
    <div className="side-panel__run-history">
      {runs.slice(0, 5).map((run) => {
        const statusColor =
          run.status === 'success' ? '#16a34a'
          : run.status === 'failed' ? '#ef4444'
          : run.status === 'running' ? '#2563eb'
          : '#6b7280';
        const startStr = run.start_time
          ? new Date(run.start_time).toLocaleString([], {
              month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
            })
          : '—';

        return (
          <div
            key={run.id}
            className="side-panel__run-item"
            onClick={() => onViewRunDetail?.(run.id)}
            role={onViewRunDetail ? 'button' : undefined}
            tabIndex={onViewRunDetail ? 0 : undefined}
          >
            <span
              className="side-panel__run-status-dot"
              style={{ background: statusColor }}
              title={run.status}
            />
            <span className="side-panel__run-id">{run.id.slice(0, 8)}</span>
            <span className="side-panel__run-date">{startStr}</span>
            {run.triggered_by && (
              <span className="side-panel__run-trigger" title={run.triggered_by}>
                {run.triggered_by}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function SidePanel({ onNavigateToPipeline, onViewFailureReport, onViewRunDetail }: { onNavigateToPipeline?: (id: string) => void; onViewFailureReport?: (runId: string, nodeId: string) => void; onViewRunDetail?: (runId: string) => void } = {}) {
  const selectedNodeId = usePipelineStore((s) => s.selectedNodeId);
  const nodes = usePipelineStore((s) => s.nodes);
  const edges = usePipelineStore((s) => s.edges);
  const apiPipeline = usePipelineStore((s) => s.apiPipeline);
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId);
  const setEditingNodeId = usePipelineStore((s) => s.setEditingNodeId);
  const deleteNodes = usePipelineStore((s) => s.deleteNodes);
  const duplicateNode = usePipelineStore((s) => s.duplicateNode);
  const setNodes = usePipelineStore((s) => s.setNodes);
  const markDirty = usePipelineStore((s) => s.markDirty);
  const updateNodeConfig = usePipelineStore((s) => s.updateNodeConfig);

  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);

  const [preview, setPreview] = useState<Map<string, ApiPreviewNodeResponse>>(new Map());
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [sampleMethod, setSampleMethod] = useState<string | undefined>(undefined);
  const [runStats, setRunStats] = useState<Map<string, ApiNodeRunStats>>(new Map());
  const [testResults, setTestResults] = useState<Map<string, ApiTestResult>>(new Map());
  const [recentRuns, setRecentRuns] = useState<ApiPipelineRun[]>([]);
  const [runsTotalCount, setRunsTotalCount] = useState(0);
  const [loadingMoreRuns, setLoadingMoreRuns] = useState(false);
  const [receipts, setReceipts] = useState<Map<string, MaterializationReceipt>>(new Map());
  const [reExecute, setReExecute] = useState(false);
  const [columnLineageOpen, setColumnLineageOpen] = useState(false);
  const lastRunCompletedAt = usePipelineStore((s) => s.lastRunCompletedAt);

  // Cache key: pipeline version — invalidate when pipeline config changes
  const previewCacheRef = useRef<{
    version: number;
    data: Map<string, ApiPreviewNodeResponse>;
  } | null>(null);
  const pipelineVersion = apiPipeline?.version ?? 0;

  const selectedNode = nodes.find((n) => n.id === selectedNodeId) ?? null;
  const apiNode = apiPipeline?.nodes.find((n) => n.id === selectedNodeId);
  const isOpen = selectedNode !== null;

  // Compute upstream node names for transforms
  const upstreamNames: string[] = selectedNode
    ? edges
        .filter((e) => e.target === selectedNode.id)
        .map((e) => {
          const upstream = nodes.find((n) => n.id === e.source);
          return upstream?.data.label ?? e.source;
        })
    : [];

  // Check if this node feeds directly into a sink
  const feedsSink = selectedNode
    ? edges
        .filter((e) => e.source === selectedNode.id)
        .some((e) => {
          const downstream = nodes.find((n) => n.id === e.target);
          return downstream?.data.role === 'sink';
        })
    : false;

  // Reset re-execute toggle when selecting a different node
  useEffect(() => {
    setReExecute(false);
  }, [selectedNodeId]);

  // Fetch preview data when panel opens or selection changes
  useEffect(() => {
    if (!pipelineId || pipelineId === 'demo' || !selectedNodeId) return;

    // Use cached preview if pipeline version hasn't changed and not re-executing
    const cached = previewCacheRef.current;
    if (!reExecute && cached && cached.version === pipelineVersion && cached.data.has(selectedNodeId)) {
      setPreview(cached.data);
      setPreviewLoading(false);
      // Still load runs (cheap, important to be fresh)
    } else {
      // Cache miss — need to clear so we don't show stale data for this node
      setPreview(new Map());
    }

    const controller = new AbortController();

    async function loadPreview() {
      // Skip fetch if cache hit (but not when re-executing)
      if (!reExecute && cached && cached.version === pipelineVersion && cached.data.has(selectedNodeId!)) {
        return;
      }
      setPreviewLoading(true);
      setPreviewError(null);
      try {
        const res = await previewPipeline(
          pipelineId!,
          undefined, // Use pipeline's sample_config (backend falls back to default)
          controller.signal,
          reExecute ? selectedNodeId! : undefined,
        );
        if (controller.signal.aborted) return;
        const map = new Map<string, ApiPreviewNodeResponse>();
        for (const node of res.nodes) {
          map.set(node.node_id, node);
        }
        // Only cache non-re-execute results
        if (!reExecute) {
          previewCacheRef.current = { version: pipelineVersion, data: map };
        }
        setPreview(map);
        setSampleMethod(res.sample_method);
      } catch (err) {
        if ((err as Error).name === 'AbortError') return;
        setPreviewError((err as Error).message);
      } finally {
        if (!controller.signal.aborted) setPreviewLoading(false);
      }
    }

    async function loadRuns() {
      try {
        // Fetch up to 10 recent runs for test trend display
        const resp = await fetchPipelineRunsPaginated(pipelineId!, 10, 0);
        if (controller.signal.aborted) return;
        const runs = resp.data;
        setRecentRuns(runs);
        setRunsTotalCount(resp.total);
        if (runs.length > 0) {
          const run = runs[0];
          const map = new Map<string, ApiNodeRunStats>();
          for (const stat of run.node_stats) {
            map.set(stat.node_id, stat);
          }
          setRunStats(map);

          // Extract test results from the latest run
          const tmap = new Map<string, ApiTestResult>();
          if (run.test_results) {
            for (const tr of run.test_results) {
              tmap.set(tr.node_id, tr);
            }
          }
          setTestResults(tmap);

          // Best-effort: load receipts for this run so the side panel can show
          // per-sink incremental stats. Failures are non-fatal — old runs may
          // pre-date the receipt column.
          try {
            const stats = await fetchRunIncrementalStats(pipelineId!, run.id);
            if (controller.signal.aborted) return;
            const rmap = new Map<string, MaterializationReceipt>();
            for (const s of stats) {
              rmap.set(s.node_id, s.receipt);
            }
            setReceipts(rmap);
          } catch {
            // Receipts unavailable for this run.
          }
        }
      } catch {
        // Run history not available
      }
    }

    loadPreview();
    loadRuns();

    return () => {
      controller.abort();
    };
  }, [pipelineId, selectedNodeId, pipelineVersion, lastRunCompletedAt, reExecute]);

  // Rename handler — updates the node label in store and marks dirty
  const handleRename = useCallback(
    (newName: string) => {
      if (!selectedNodeId) return;
      setNodes((current) =>
        current.map((n) =>
          n.id === selectedNodeId
            ? { ...n, data: { ...n.data, label: newName } }
            : n,
        ),
      );
      markDirty();
    },
    [selectedNodeId, setNodes, markDirty],
  );

  const handleEdit = useCallback(() => {
    if (selectedNodeId) setEditingNodeId(selectedNodeId);
  }, [selectedNodeId, setEditingNodeId]);

  const handleDelete = useCallback(() => {
    if (selectedNodeId) {
      deleteNodes([selectedNodeId]);
      setSelectedNodeId(null);
    }
  }, [selectedNodeId, deleteNodes, setSelectedNodeId]);

  const handleDuplicate = useCallback(() => {
    if (selectedNodeId) duplicateNode(selectedNodeId);
  }, [selectedNodeId, duplicateNode]);

  const handleClose = useCallback(() => {
    setSelectedNodeId(null);
  }, [setSelectedNodeId]);

  // Update the pipeline's sample_config and invalidate preview cache.
  const handleSampleConfigChange = useCallback(
    async (config: ApiSampleConfig) => {
      if (!pipelineId || !apiPipeline) return;
      const updatedPipeline = { ...apiPipeline, sample_config: config };
      try {
        const full = buildApiPipeline(updatedPipeline, nodes, edges);
        await updatePipeline(pipelineId, full);
        // Invalidate cache so next render re-fetches preview with new config
        previewCacheRef.current = null;
        // Force a version bump to trigger the preview useEffect
        usePipelineStore.getState().loadPipeline(pipelineId);
      } catch (err) {
        console.error('Failed to save sample config:', err);
      }
    },
    [pipelineId, apiPipeline, nodes, edges],
  );

  // Compute schema diff for transform nodes by comparing upstream columns to output
  const schemaDiff: SchemaDiff | null = (() => {
    if (!selectedNode || selectedNode.data.role !== 'transform') return null;
    const nodePreview = preview.get(selectedNodeId!);
    if (!nodePreview) return null;

    // Gather all upstream columns (merge from all upstream nodes)
    const upstreamEdges = edges.filter((e) => e.target === selectedNode.id);
    const inputColumns: ApiColumnInfo[] = [];
    for (const edge of upstreamEdges) {
      const upstreamPreview = preview.get(edge.source);
      if (upstreamPreview) {
        inputColumns.push(...upstreamPreview.columns);
      }
    }

    if (inputColumns.length === 0) return null;
    return computeSchemaDiff(inputColumns, nodePreview.columns);
  })();

  const handleReExecuteChange = useCallback((value: boolean) => {
    setReExecute(value);
  }, []);

  const handleCacheRowLimitChange = useCallback(
    (value: number | undefined) => {
      if (!selectedNodeId) return;
      updateNodeConfig(selectedNodeId, { cache_row_limit: value });
    },
    [selectedNodeId, updateNodeConfig],
  );

  const handleMaterializedChange = useCallback(
    (value: boolean) => {
      if (!selectedNodeId) return;
      updateNodeConfig(selectedNodeId, { materialized: value });
    },
    [selectedNodeId, updateNodeConfig],
  );

  const handleLoadMoreRuns = useCallback(async () => {
    if (!pipelineId || loadingMoreRuns) return;
    setLoadingMoreRuns(true);
    try {
      const resp = await fetchPipelineRunsPaginated(pipelineId, 10, recentRuns.length);
      setRecentRuns((prev) => [...prev, ...resp.data]);
      setRunsTotalCount(resp.total);
    } catch {
      // Non-fatal — the existing runs remain visible.
    } finally {
      setLoadingMoreRuns(false);
    }
  }, [pipelineId, recentRuns.length, loadingMoreRuns]);

  const hasMoreRuns = recentRuns.length < runsTotalCount;

  // Build content props
  const contentProps: NodeContentProps | null = selectedNode
    ? {
        node: selectedNode,
        apiNode,
        preview: preview.get(selectedNodeId!) ?? null,
        previewLoading,
        sampleMethod,
        runStats: runStats.get(selectedNodeId!) ?? null,
        receipt: receipts.get(selectedNodeId!) ?? null,
        testResult: testResults.get(selectedNodeId!) ?? null,
        recentRuns,
        upstreamNames,
        schemaDiff,
        previewError,
        sampleConfig: apiPipeline?.sample_config,
        onSampleConfigChange: handleSampleConfigChange,
        reExecute,
        onReExecuteChange: handleReExecuteChange,
        onCacheRowLimitChange: handleCacheRowLimitChange,
        onMaterializedChange: handleMaterializedChange,
        feedsSink,
        latestRunId: recentRuns.length > 0 ? recentRuns[0].id : undefined,
        onViewFailureReport,
      }
    : null;

  return (
    <div
      className={`side-panel${isOpen ? ' side-panel--open' : ''}`}
      data-testid="side-panel"
    >
      {selectedNode && contentProps && (
        <>
          {/* Header */}
          <div className="side-panel__header">
            <span className="side-panel__role-icon">
              {roleIcon[selectedNode.data.role] ?? null}
            </span>
            <div className="side-panel__name">
              <InlineName name={selectedNode.data.label} onRename={handleRename} />
            </div>
            <span
              className={`side-panel__role-badge side-panel__role-badge--${selectedNode.data.role}`}
            >
              {selectedNode.data.role}
            </span>
            <button
              className="side-panel__close"
              onClick={handleClose}
              aria-label="Close panel"
              title="Close (Esc)"
            >
              &times;
            </button>
          </div>

          {/* Body */}
          <div className="side-panel__body">
            {selectedNode.data.role === 'source' && (
              <SourceContent {...contentProps} />
            )}
            {selectedNode.data.role === 'transform' && (
              <TransformContent {...contentProps} />
            )}
            {selectedNode.data.role === 'sink' && (
              <SinkContent {...contentProps} />
            )}
            {selectedNode.data.role === 'test' && (
              <TestContent {...contentProps} />
            )}
            {/* Run history */}
            {recentRuns.length > 0 && (
              <div className="side-panel__section">
                <div className="side-panel__section-title">Recent Runs</div>
                <RunHistoryList runs={recentRuns} onViewRunDetail={onViewRunDetail} />
                {hasMoreRuns && (
                  <button
                    className="side-panel__load-more"
                    onClick={handleLoadMoreRuns}
                    disabled={loadingMoreRuns}
                  >
                    {loadingMoreRuns ? 'Loading…' : `Load more (${recentRuns.length} of ${runsTotalCount})`}
                  </button>
                )}
              </div>
            )}
            {pipelineId && pipelineId !== 'demo' && (
              <LineageSection
                pipelineId={pipelineId}
                environment={activeEnvironment}
                onNavigate={onNavigateToPipeline}
              />
            )}
          </div>

          {/* Actions */}
          <div className="side-panel__actions">
            <button
              className="side-panel__action-btn side-panel__action-btn--primary"
              onClick={handleEdit}
            >
              Edit
            </button>
            {pipelineId && pipelineId !== 'demo' && (
              <button
                className="side-panel__action-btn"
                onClick={() => setColumnLineageOpen(true)}
              >
                Column Lineage
              </button>
            )}
            <button className="side-panel__action-btn" onClick={handleDuplicate}>
              Duplicate
            </button>
            <button
              className="side-panel__action-btn side-panel__action-btn--danger"
              onClick={handleDelete}
            >
              Delete
            </button>
          </div>
        </>
      )}

      {columnLineageOpen && pipelineId && selectedNodeId && (
        <ColumnLineageModal
          open={columnLineageOpen}
          pipelineId={pipelineId}
          nodeId={selectedNodeId}
          nodeName={selectedNode?.data.label ?? selectedNodeId}
          environment={activeEnvironment}
          onClose={() => setColumnLineageOpen(false)}
        />
      )}
    </div>
  );
}
