// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { PipelineNode } from '../../types/pipeline';
import type { ApiNode } from '../../api/pipelines';
import {
  previewPipeline,
  fetchPipelineRuns,
  type ApiPreviewNodeResponse,
  type ApiNodeRunStats,
} from '../../api/pipelines';
import './SidePanel.css';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const roleIcons: Record<string, string> = {
  source: '\u{1F4E5}',
  transform: '\u{2699}\u{FE0F}',
  sink: '\u{1F4E4}',
};

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
// Preview Table sub-component
// ---------------------------------------------------------------------------

interface PreviewTableProps {
  preview: ApiPreviewNodeResponse | null;
  loading: boolean;
}

function PreviewTable({ preview, loading }: PreviewTableProps) {
  if (loading) return <span className="side-panel__loading">Loading preview...</span>;
  if (!preview || preview.rows.length === 0) {
    return <span className="side-panel__empty">No preview data available</span>;
  }

  const columns = preview.columns.map((c) => c.name);
  const rows = preview.rows.slice(0, 10);

  return (
    <div className="side-panel__table-wrap">
      <table className="side-panel__table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {columns.map((col) => (
                <td key={col}>{String(row[col] ?? '')}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Schema List sub-component
// ---------------------------------------------------------------------------

interface SchemaListProps {
  preview: ApiPreviewNodeResponse | null;
}

function SchemaList({ preview }: SchemaListProps) {
  if (!preview || preview.columns.length === 0) {
    return <span className="side-panel__empty">No schema available</span>;
  }
  return (
    <ul className="side-panel__schema-list">
      {preview.columns.map((col) => (
        <li key={col.name} className="side-panel__schema-item">
          <span className="side-panel__schema-name">{col.name}</span>
          <span className="side-panel__schema-type">{col.data_type}</span>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// Run Stats sub-component
// ---------------------------------------------------------------------------

interface RunStatsProps {
  stats: ApiNodeRunStats | null;
  role: string;
}

function RunStats({ stats, role }: RunStatsProps) {
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
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Error</span>
          <span className="side-panel__kv-value" style={{ color: '#ef4444' }}>
            {stats.error}
          </span>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Source node content
// ---------------------------------------------------------------------------

interface NodeContentProps {
  node: PipelineNode;
  apiNode: ApiNode | undefined;
  preview: ApiPreviewNodeResponse | null;
  previewLoading: boolean;
  runStats: ApiNodeRunStats | null;
  upstreamNames: string[];
}

function SourceContent({ apiNode, preview, previewLoading, runStats }: NodeContentProps) {
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
        <RunStats stats={runStats} role="source" />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Preview (first 10 rows)</div>
        <PreviewTable preview={preview} loading={previewLoading} />
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
  apiNode,
  preview,
  previewLoading,
  runStats,
  upstreamNames,
}: NodeContentProps) {
  const mode = apiNode?.mode ?? 'sql';
  const code = apiNode?.code ?? '';

  return (
    <>
      <div className="side-panel__section">
        <div className="side-panel__section-title">Transform</div>
        <div className="side-panel__kv">
          <span className="side-panel__kv-key">Mode</span>
          <span className="side-panel__kv-value">{mode.toUpperCase()}</span>
        </div>
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
        <RunStats stats={runStats} role="transform" />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Preview (first 10 rows)</div>
        <PreviewTable preview={preview} loading={previewLoading} />
      </div>

      <div className="side-panel__section">
        <div className="side-panel__section-title">Output Schema</div>
        <SchemaList preview={preview} />
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Sink node content
// ---------------------------------------------------------------------------

function SinkContent({ node, apiNode, runStats }: NodeContentProps) {
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
        <RunStats stats={runStats} role="sink" />
      </div>
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

function InlineName({ name, onRename }: InlineNameProps) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setDraft(name);
    setEditing(false);
  }, [name]);

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

// ---------------------------------------------------------------------------
// Main SidePanel component
// ---------------------------------------------------------------------------

export function SidePanel() {
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

  const [preview, setPreview] = useState<Map<string, ApiPreviewNodeResponse>>(new Map());
  const [previewLoading, setPreviewLoading] = useState(false);
  const [runStats, setRunStats] = useState<Map<string, ApiNodeRunStats>>(new Map());

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

  // Fetch preview data when panel opens or selection changes
  useEffect(() => {
    if (!pipelineId || pipelineId === 'demo' || !selectedNodeId) return;

    let cancelled = false;

    async function loadPreview() {
      setPreviewLoading(true);
      try {
        const res = await previewPipeline(pipelineId!, { max_rows: 10 });
        if (cancelled) return;
        const map = new Map<string, ApiPreviewNodeResponse>();
        for (const node of res.nodes) {
          map.set(node.node_id, node);
        }
        setPreview(map);
      } catch {
        // Preview not available (backend may not be running)
      } finally {
        if (!cancelled) setPreviewLoading(false);
      }
    }

    async function loadRuns() {
      try {
        const res = await fetchPipelineRuns(pipelineId!, 1, 0);
        if (cancelled) return;
        if (res.data.length > 0) {
          const run = res.data[0];
          const map = new Map<string, ApiNodeRunStats>();
          for (const stat of run.node_stats) {
            map.set(stat.node_id, stat);
          }
          setRunStats(map);
        }
      } catch {
        // Run history not available
      }
    }

    loadPreview();
    loadRuns();

    return () => {
      cancelled = true;
    };
  }, [pipelineId, selectedNodeId]);

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

  // Build content props
  const contentProps: NodeContentProps | null = selectedNode
    ? {
        node: selectedNode,
        apiNode,
        preview: preview.get(selectedNodeId!) ?? null,
        previewLoading,
        runStats: runStats.get(selectedNodeId!) ?? null,
        upstreamNames,
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
              {roleIcons[selectedNode.data.role] ?? '?'}
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
          </div>

          {/* Actions */}
          <div className="side-panel__actions">
            <button
              className="side-panel__action-btn side-panel__action-btn--primary"
              onClick={handleEdit}
            >
              Edit
            </button>
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
    </div>
  );
}
