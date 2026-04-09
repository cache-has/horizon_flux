// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Project-level view showing all triggers across all pipelines.
 * Filterable by trigger kind and environment.
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTriggerStore } from '../../stores/triggerStore';
import type { TriggerResponse, TriggerKindName } from '../../api/triggers';
import { TRIGGER_KIND_LABELS } from '../../api/triggers';
import './ProjectTriggersView.css';

interface ProjectTriggersViewProps {
  onBack: () => void;
  onNavigateToPipeline?: (id: string) => void;
}

export function ProjectTriggersView({ onBack, onNavigateToPipeline }: ProjectTriggersViewProps) {
  const triggers = useTriggerStore((s) => s.triggers);
  const loading = useTriggerStore((s) => s.loading);
  const fetchTriggers = useTriggerStore((s) => s.fetchTriggers);
  const enableTrigger = useTriggerStore((s) => s.enableTrigger);
  const disableTrigger = useTriggerStore((s) => s.disableTrigger);
  const fireTrigger = useTriggerStore((s) => s.fireTrigger);

  const [kindFilter, setKindFilter] = useState<TriggerKindName | 'all'>('all');
  const [envFilter, setEnvFilter] = useState<string>('all');

  useEffect(() => {
    void fetchTriggers();
  }, [fetchTriggers]);

  // Derive unique environments from triggers
  const environments = useMemo(
    () => [...new Set(triggers.map((t) => t.environment))].sort(),
    [triggers],
  );

  const filtered = useMemo(() => {
    let result = triggers;
    if (kindFilter !== 'all') {
      result = result.filter((t) => t.kind.kind === kindFilter);
    }
    if (envFilter !== 'all') {
      result = result.filter((t) => t.environment === envFilter);
    }
    return result;
  }, [triggers, kindFilter, envFilter]);

  return (
    <div className="project-triggers">
      <div className="project-triggers__toolbar">
        <button className="project-triggers__back-btn" onClick={onBack}>
          Back
        </button>
        <span className="project-triggers__title">All Triggers</span>
      </div>

      <div className="project-triggers__filters">
        <span className="project-triggers__filter-label">Kind:</span>
        <select
          className="project-triggers__filter-select"
          value={kindFilter}
          onChange={(e) => setKindFilter(e.target.value as TriggerKindName | 'all')}
        >
          <option value="all">All</option>
          {(Object.keys(TRIGGER_KIND_LABELS) as TriggerKindName[]).map((k) => (
            <option key={k} value={k}>{TRIGGER_KIND_LABELS[k]}</option>
          ))}
        </select>

        <span className="project-triggers__filter-label">Env:</span>
        <select
          className="project-triggers__filter-select"
          value={envFilter}
          onChange={(e) => setEnvFilter(e.target.value)}
        >
          <option value="all">All</option>
          {environments.map((env) => (
            <option key={env} value={env}>{env}</option>
          ))}
        </select>
      </div>

      <div className="project-triggers__body">
        {loading && triggers.length === 0 && (
          <div className="project-triggers__loading">Loading triggers...</div>
        )}
        {!loading && filtered.length === 0 && (
          <div className="project-triggers__empty">
            {triggers.length === 0
              ? 'No triggers configured in this project.'
              : 'No triggers match the current filters.'}
          </div>
        )}

        <div className="project-triggers__grid">
          {filtered.map((t) => (
            <ProjectTriggerCard
              key={t.id}
              trigger={t}
              onNavigateToPipeline={onNavigateToPipeline}
              onToggle={async () => {
                if (t.enabled) await disableTrigger(t.id);
                else await enableTrigger(t.id);
              }}
              onFire={() => fireTrigger(t.id)}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Single trigger card for the project view
// ---------------------------------------------------------------------------

function ProjectTriggerCard({
  trigger,
  onNavigateToPipeline,
  onToggle,
  onFire,
}: {
  trigger: TriggerResponse;
  onNavigateToPipeline?: (id: string) => void;
  onToggle: () => void;
  onFire: () => void;
}) {
  const [firing, setFiring] = useState(false);
  const hasErrors = (trigger.state?.consecutive_errors ?? 0) >= 3;

  const handleFire = useCallback(async () => {
    setFiring(true);
    try { await onFire(); } finally { setFiring(false); }
  }, [onFire]);

  const kindDetail = useMemo(() => {
    switch (trigger.kind.kind) {
      case 'cron':
        return trigger.kind.expression;
      case 'interval':
        return trigger.kind.every;
      case 'file_arrival':
        return trigger.kind.path;
      case 'webhook':
        return trigger.kind.path;
      case 'pipeline_completion':
        return `${trigger.kind.upstream_pipeline} (${trigger.kind.on_status})`;
    }
  }, [trigger.kind]);

  const dotClass = hasErrors
    ? 'project-triggers__card-dot--error'
    : trigger.enabled
      ? 'project-triggers__card-dot--active'
      : 'project-triggers__card-dot--paused';

  const cardClass = [
    'project-triggers__card',
    !trigger.enabled && 'project-triggers__card--disabled',
    hasErrors && 'project-triggers__card--warning',
  ].filter(Boolean).join(' ');

  return (
    <div className={cardClass}>
      <div className="project-triggers__card-header">
        <span className={`project-triggers__card-dot ${dotClass}`} />
        <span className="project-triggers__card-name">{trigger.name}</span>
        <span className="project-triggers__card-kind">
          {TRIGGER_KIND_LABELS[trigger.kind.kind]}
        </span>
      </div>

      <div
        className="project-triggers__card-pipeline"
        onClick={() => onNavigateToPipeline?.(trigger.pipeline_id)}
      >
        Pipeline: {trigger.pipeline_id}
      </div>

      <div className="project-triggers__card-env">
        Env: {trigger.environment}
      </div>

      <div className="project-triggers__card-detail">{kindDetail}</div>

      {trigger.state?.next_fire_at && (
        <div className="project-triggers__card-next">
          Next: {new Date(trigger.state.next_fire_at).toLocaleString()}
        </div>
      )}

      {hasErrors && (
        <div className="project-triggers__card-warning">
          {trigger.state!.consecutive_errors} consecutive errors
        </div>
      )}

      <div className="project-triggers__card-actions">
        <button className="project-triggers__card-btn" onClick={onToggle}>
          {trigger.enabled ? 'Disable' : 'Enable'}
        </button>
        <button
          className="project-triggers__card-btn"
          onClick={handleFire}
          disabled={firing}
        >
          {firing ? 'Firing...' : 'Fire'}
        </button>
      </div>
    </div>
  );
}
