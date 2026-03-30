// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useEnvironmentStore } from '../../stores/environmentStore';
import {
  listPipelines,
  runPipeline,
  type ApiPipelineResponse,
} from '../../api/pipelines';
import { IconChevronDown } from '../icons';
import { EnvironmentSelector } from './EnvironmentSelector';
import './CanvasToolbar.css';

// ---------------------------------------------------------------------------
// Pipeline Selector
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
    setLoading(true);
    listPipelines(100, 0)
      .then((res) => setPipelines(res.data))
      .catch(() => setPipelines([]))
      .finally(() => setLoading(false));
  }, [open]);

  const handleSelect = useCallback(
    async (id: string) => {
      setOpen(false);
      if (id === pipelineId) return;
      await loadPipeline(id);
      const pipeline = usePipelineStore.getState().apiPipeline;
      if (pipeline) {
        setActiveEnvironment(pipeline.default_environment);
      }
    },
    [pipelineId, loadPipeline, setActiveEnvironment],
  );

  const name = apiPipeline?.name ?? 'No pipeline';

  return (
    <div className="pipeline-selector" ref={ref}>
      <button
        className="pipeline-selector__trigger"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        aria-haspopup="listbox"
      >
        <span className="pipeline-selector__name">{name}</span>
        <span className="pipeline-selector__chevron">
          <IconChevronDown size={12} />
        </span>
      </button>

      {open && (
        <div className="pipeline-selector__dropdown" role="listbox">
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
      // Notify store so side panel refreshes run stats.
      usePipelineStore.getState().notifyRunCompleted();
      timerRef.current = setTimeout(() => setState('idle'), 3000);
    } catch (err) {
      setState('error');
      setError((err as Error).message);
      // Still notify — partial runs may have completed some nodes.
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

export function CanvasToolbar() {
  return (
    <div className="canvas-toolbar">
      <PipelineSelector />
      <div className="canvas-toolbar__separator" />
      <EnvironmentSelector />
      <div className="canvas-toolbar__spacer" />
      <RunButton />
    </div>
  );
}
