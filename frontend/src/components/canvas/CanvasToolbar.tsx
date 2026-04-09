// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useEnvironmentStore } from '../../stores/environmentStore';
import {
  listPipelines,
  createPipeline,
  importPipeline,
  exportPipeline,
  runPipeline,
  type ApiPipelineResponse,
} from '../../api/pipelines';
import { IconChevronDown } from '../icons';
import { EnvironmentSelector } from './EnvironmentSelector';
import './CanvasToolbar.css';

// ---------------------------------------------------------------------------
// Pipeline Selector (with New / Import / Export)
// ---------------------------------------------------------------------------

function PipelineSelector() {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const apiPipeline = usePipelineStore((s) => s.apiPipeline);
  const loadPipeline = usePipelineStore((s) => s.loadPipeline);
  const setActiveEnvironment = useEnvironmentStore(
    (s) => s.setActiveEnvironment,
  );

  const [open, setOpen] = useState(false);
  const [pipelines, setPipelines] = useState<ApiPipelineResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  // Load pipeline list when opened
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    listPipelines(100, 0)
      .then((res) => {
        if (!cancelled) setPipelines(res.data);
      })
      .catch(() => {
        if (!cancelled) setPipelines([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [open]);

  const switchToPipeline = useCallback(
    async (id: string) => {
      await loadPipeline(id);
      const pipeline = usePipelineStore.getState().apiPipeline;
      if (pipeline) {
        setActiveEnvironment(pipeline.default_environment);
      }
    },
    [loadPipeline, setActiveEnvironment],
  );

  const handleSelect = useCallback(
    async (id: string) => {
      setOpen(false);
      if (id === pipelineId) return;
      await switchToPipeline(id);
    },
    [pipelineId, switchToPipeline],
  );

  const handleNew = useCallback(async () => {
    setOpen(false);
    const name = window.prompt('Pipeline name:');
    if (!name?.trim()) return;
    try {
      const res = await createPipeline(name.trim());
      await switchToPipeline(res.id);
    } catch (err) {
      alert((err as Error).message);
    }
  }, [switchToPipeline]);

  const handleImport = useCallback(() => {
    setOpen(false);
    fileInputRef.current?.click();
  }, []);

  const handleFileSelected = useCallback(
    async (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (!file) return;
      // Reset so the same file can be re-imported
      e.target.value = '';
      try {
        const text = await file.text();
        const pipelineJson = JSON.parse(text);
        const res = await importPipeline(pipelineJson, 'rename');
        await switchToPipeline(res.id);
      } catch (err) {
        alert(`Import failed: ${(err as Error).message}`);
      }
    },
    [switchToPipeline],
  );

  const handleExport = useCallback(async () => {
    setOpen(false);
    if (!pipelineId || pipelineId === 'demo') return;
    try {
      await exportPipeline(pipelineId);
    } catch (err) {
      alert(`Export failed: ${(err as Error).message}`);
    }
  }, [pipelineId]);

  const name = apiPipeline?.name ?? 'No pipeline';

  return (
    <div className="pipeline-selector" ref={ref}>
      <button
        className="pipeline-selector__trigger"
        onClick={() => { setOpen((o) => !o); setLoading(true); }}
        aria-expanded={open}
        aria-haspopup="listbox"
      >
        <span className="pipeline-selector__name">{name}</span>
        <span className="pipeline-selector__chevron">
          <IconChevronDown size={12} />
        </span>
      </button>

      {/* Hidden file input for import */}
      <input
        ref={fileInputRef}
        type="file"
        accept=".json"
        style={{ display: 'none' }}
        onChange={handleFileSelected}
      />

      {open && (
        <div className="pipeline-selector__dropdown">
          {/* Actions */}
          <div className="pipeline-selector__actions">
            <button className="pipeline-selector__action" onClick={handleNew}>
              New Pipeline
            </button>
            <button className="pipeline-selector__action" onClick={handleImport}>
              Import JSON
            </button>
            {pipelineId && pipelineId !== 'demo' && (
              <button className="pipeline-selector__action" onClick={handleExport}>
                Export
              </button>
            )}
          </div>

          <div className="pipeline-selector__divider" />

          {/* Pipeline list */}
          {loading && (
            <div className="pipeline-selector__empty">Loading...</div>
          )}
          {!loading && pipelines.length === 0 && (
            <div className="pipeline-selector__empty">No pipelines</div>
          )}
          {!loading &&
            pipelines.map((p) => (
              <button
                key={p.id}
                className={`pipeline-selector__option${p.id === pipelineId ? ' pipeline-selector__option--active' : ''}`}
                role="option"
                aria-selected={p.id === pipelineId}
                onClick={() => handleSelect(p.id)}
              >
                <span className="pipeline-selector__option-name">
                  {p.pipeline.name}
                </span>
                <span className="pipeline-selector__option-meta">
                  {p.pipeline.nodes.length} nodes
                </span>
              </button>
            ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Run Button
// ---------------------------------------------------------------------------

type RunState = 'idle' | 'running' | 'success' | 'error';

function RunButton() {
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);
  const [state, setState] = useState<RunState>('idle');
  const [error, setError] = useState<string | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleRun = useCallback(async () => {
    if (!pipelineId || pipelineId === 'demo') return;
    setState('running');
    setError(null);
    try {
      await runPipeline(pipelineId, activeEnvironment);
      setState('success');
      usePipelineStore.getState().notifyRunCompleted();
      timerRef.current = setTimeout(() => setState('idle'), 3000);
    } catch (err) {
      setState('error');
      setError((err as Error).message);
      usePipelineStore.getState().notifyRunCompleted();
      timerRef.current = setTimeout(() => setState('idle'), 5000);
    }
  }, [pipelineId, activeEnvironment]);

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, []);

  const disabled =
    !pipelineId || pipelineId === 'demo' || state === 'running';

  const label = {
    idle: 'Run',
    running: 'Running...',
    success: 'Done',
    error: 'Failed',
  }[state];

  return (
    <button
      className={`toolbar-run toolbar-run--${state}`}
      onClick={handleRun}
      disabled={disabled}
      title={error ?? `Run pipeline in ${activeEnvironment} environment`}
    >
      {state === 'running' && <span className="toolbar-run__spinner" />}
      {label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Toolbar
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Secrets Button
// ---------------------------------------------------------------------------

function SecretsButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-secrets"
      onClick={onClick}
      title="Manage secrets"
    >
      Secrets
    </button>
  );
}

// ---------------------------------------------------------------------------
// History Button
// ---------------------------------------------------------------------------

function HistoryButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-history"
      onClick={onClick}
      title="Version history"
    >
      History
    </button>
  );
}

// ---------------------------------------------------------------------------
// System Info Button
// ---------------------------------------------------------------------------

function SystemInfoButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-system-info"
      onClick={onClick}
      title="System information"
    >
      System
    </button>
  );
}

function PluginsButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-system-info"
      onClick={onClick}
      title="Manage plugins"
    >
      Plugins
    </button>
  );
}

function LineageButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-system-info"
      onClick={onClick}
      title="Project lineage view"
    >
      Lineage
    </button>
  );
}

function TriggersButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-system-info"
      onClick={onClick}
      title="Manage triggers"
    >
      Triggers
    </button>
  );
}

function BackfillsButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-system-info"
      onClick={onClick}
      title="Manage backfills"
    >
      Backfills
    </button>
  );
}

function CatalogButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      className="toolbar-system-info"
      onClick={onClick}
      title="Resource catalog"
    >
      Catalog
    </button>
  );
}

// ---------------------------------------------------------------------------
// Toolbar
// ---------------------------------------------------------------------------

export function CanvasToolbar({
  onSecretsClick,
  onSystemInfoClick,
  onHistoryClick,
  onPluginsClick,
  onLineageClick,
  onTriggersClick,
  onBackfillsClick,
  onCatalogClick,
}: {
  onSecretsClick?: () => void;
  onSystemInfoClick?: () => void;
  onHistoryClick?: () => void;
  onPluginsClick?: () => void;
  onLineageClick?: () => void;
  onTriggersClick?: () => void;
  onBackfillsClick?: () => void;
  onCatalogClick?: () => void;
}) {
  return (
    <div className="canvas-toolbar">
      <PipelineSelector />
      <div className="canvas-toolbar__separator" />
      <EnvironmentSelector />
      <div className="canvas-toolbar__spacer" />
      {onLineageClick && <LineageButton onClick={onLineageClick} />}
      {onCatalogClick && <CatalogButton onClick={onCatalogClick} />}
      {onTriggersClick && <TriggersButton onClick={onTriggersClick} />}
      {onBackfillsClick && <BackfillsButton onClick={onBackfillsClick} />}
      {onHistoryClick && <HistoryButton onClick={onHistoryClick} />}
      {onPluginsClick && <PluginsButton onClick={onPluginsClick} />}
      {onSystemInfoClick && <SystemInfoButton onClick={onSystemInfoClick} />}
      {onSecretsClick && <SecretsButton onClick={onSecretsClick} />}
      <RunButton />
    </div>
  );
}
