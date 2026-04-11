// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Cross-Pipeline Health Dashboard — project-wide at-a-glance view showing
 * run summary, top failing pipelines, slowest pipelines, trigger health,
 * SLA breaches, and a "things to look at" feed (planning doc 38).
 */

import { useCallback, useEffect } from 'react';
import { useHealthStore } from '../../stores/healthStore';
import type {
  TimeWindow,
  FailingPipeline,
  SlowestPipeline,
  NotableEvent,
  TriggerFailure,
  SlaBreach,
} from '../../api/health';
import './HealthDashboardView.css';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface HealthDashboardViewProps {
  onBack: () => void;
  onNavigateToPipeline?: (id: string) => void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const WINDOW_OPTIONS: { value: TimeWindow; label: string }[] = [
  { value: '24h', label: '24 hours' },
  { value: '7d', label: '7 days' },
  { value: '30d', label: '30 days' },
];

const EVENT_KIND_LABELS: Record<string, string> = {
  first_failure: 'New Failure',
  consecutive_trigger_failure: 'Trigger Issue',
  sla_breach: 'SLA Breach',
};

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const secs = ms / 1000;
  if (secs < 60) return `${secs.toFixed(1)}s`;
  const mins = secs / 60;
  if (mins < 60) return `${mins.toFixed(1)}m`;
  const hrs = mins / 60;
  return `${hrs.toFixed(1)}h`;
}

function formatTimestamp(iso?: string): string {
  if (!iso) return '\u2014';
  return new Date(iso).toLocaleString();
}

function formatIsoDuration(iso?: string): string {
  if (!iso) return '\u2014';
  const match = iso.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/);
  if (!match) return iso;
  const parts: string[] = [];
  if (match[1]) parts.push(`${match[1]}h`);
  if (match[2]) parts.push(`${match[2]}m`);
  if (match[3]) parts.push(`${match[3]}s`);
  return parts.join(' ') || '0s';
}

// ---------------------------------------------------------------------------
// Main View
// ---------------------------------------------------------------------------

export function HealthDashboardView({ onBack, onNavigateToPipeline }: HealthDashboardViewProps) {
  const overview = useHealthStore((s) => s.overview);
  const loading = useHealthStore((s) => s.loading);
  const error = useHealthStore((s) => s.error);
  const window = useHealthStore((s) => s.window);
  const fetchOverview = useHealthStore((s) => s.fetchOverview);
  const setWindow = useHealthStore((s) => s.setWindow);

  useEffect(() => {
    void fetchOverview();
  }, [fetchOverview]);

  // Re-fetch when window changes.
  useEffect(() => {
    void fetchOverview();
  }, [window, fetchOverview]);

  const handleWindowChange = useCallback(
    (w: TimeWindow) => {
      setWindow(w);
    },
    [setWindow],
  );

  const handlePipelineClick = useCallback(
    (name: string) => {
      onNavigateToPipeline?.(name);
    },
    [onNavigateToPipeline],
  );

  return (
    <div className="health-dash">
      <div className="health-dash__toolbar">
        <button className="health-dash__back-btn" onClick={onBack}>Back</button>
        <span className="health-dash__title">Health Dashboard</span>

        <div className="health-dash__window-selector">
          {WINDOW_OPTIONS.map((opt) => (
            <button
              key={opt.value}
              className={`health-dash__window-btn${window === opt.value ? ' health-dash__window-btn--active' : ''}`}
              onClick={() => handleWindowChange(opt.value)}
            >
              {opt.label}
            </button>
          ))}
        </div>

        {overview && (
          <span className="health-dash__generated">
            Generated {formatTimestamp(overview.generated_at)}
          </span>
        )}
      </div>

      <div className="health-dash__body">
        {error && <div className="health-dash__error">{error}</div>}
        {loading && !overview && (
          <div className="health-dash__empty">Loading health data...</div>
        )}

        {overview && (
          <>
            {/* Run Summary */}
            <RunSummarySection overview={overview} />

            {/* Notable Events ("Things to look at") */}
            <NotableEventsSection
              events={overview.notable_events}
              onPipelineClick={handlePipelineClick}
            />

            {/* Two-column grid: failing + slowest */}
            <div className="health-dash__grid">
              <TopFailingSection
                pipelines={overview.top_failing_pipelines}
                onPipelineClick={handlePipelineClick}
              />
              <SlowestSection
                pipelines={overview.slowest_pipelines}
                onPipelineClick={handlePipelineClick}
              />
            </div>

            {/* Two-column grid: triggers + SLA */}
            <div className="health-dash__grid">
              <TriggerHealthSection
                health={overview.trigger_health}
                onPipelineClick={handlePipelineClick}
              />
              <SlaBreachesSection
                summary={overview.sla_summary}
                onPipelineClick={handlePipelineClick}
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Run Summary
// ---------------------------------------------------------------------------

function RunSummarySection({ overview }: { overview: { run_summary: { total: number; success: number; failed: number; running: number; pending: number; cancelled: number } } }) {
  const s = overview.run_summary;
  return (
    <div className="health-dash__run-summary">
      <StatBadge label="Total" count={s.total} variant="total" />
      <StatBadge label="Success" count={s.success} variant="success" />
      <StatBadge label="Failed" count={s.failed} variant="failed" />
      <StatBadge label="Running" count={s.running} variant="running" />
      <StatBadge label="Pending" count={s.pending} variant="pending" />
      <StatBadge label="Cancelled" count={s.cancelled} variant="cancelled" />
    </div>
  );
}

function StatBadge({ label, count, variant }: { label: string; count: number; variant: string }) {
  return (
    <div className={`health-dash__stat-badge health-dash__stat-badge--${variant}`}>
      <span className="health-dash__stat-count">{count}</span>
      <span className="health-dash__stat-label">{label}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Notable Events ("Things to look at")
// ---------------------------------------------------------------------------

function NotableEventsSection({
  events,
  onPipelineClick,
}: {
  events: NotableEvent[];
  onPipelineClick: (name: string) => void;
}) {
  return (
    <div className="health-dash__section">
      <div className="health-dash__section-header">
        <span className="health-dash__section-title">Things to Look At</span>
        <span className="health-dash__section-meta">{events.length} items</span>
      </div>
      {events.length === 0 ? (
        <div className="health-dash__section-empty">Nothing notable in this time window.</div>
      ) : (
        <div className="health-dash__event-list">
          {events.map((event, i) => (
            <div key={`${event.kind}-${event.pipeline_name ?? ''}-${i}`} className="health-dash__event">
              <span className={`health-dash__event-kind health-dash__event-kind--${event.kind}`}>
                {EVENT_KIND_LABELS[event.kind] ?? event.kind}
              </span>
              <span className="health-dash__event-desc">
                {event.pipeline_name && (
                  <>
                    <span
                      className="health-dash__event-pipeline"
                      onClick={() => onPipelineClick(event.pipeline_name!)}
                    >
                      {event.pipeline_name}
                    </span>
                    {' \u2014 '}
                  </>
                )}
                {event.description}
              </span>
              {event.at && (
                <span className="health-dash__event-time">{formatTimestamp(event.at)}</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Top Failing Pipelines
// ---------------------------------------------------------------------------

function TopFailingSection({
  pipelines,
  onPipelineClick,
}: {
  pipelines: FailingPipeline[];
  onPipelineClick: (name: string) => void;
}) {
  return (
    <div className="health-dash__section">
      <div className="health-dash__section-header">
        <span className="health-dash__section-title">Top Failing Pipelines</span>
      </div>
      {pipelines.length === 0 ? (
        <div className="health-dash__section-empty">No failures in this window.</div>
      ) : (
        <table className="health-dash__table">
          <thead>
            <tr>
              <th className="health-dash__th">Pipeline</th>
              <th className="health-dash__th">Failures</th>
              <th className="health-dash__th">Last Failure</th>
              <th className="health-dash__th">Last Error</th>
            </tr>
          </thead>
          <tbody>
            {pipelines.map((p) => (
              <tr
                key={p.pipeline_name}
                className="health-dash__row health-dash__row--clickable"
                onClick={() => onPipelineClick(p.pipeline_name)}
              >
                <td className="health-dash__td health-dash__td--name">
                  <span className="health-dash__pipeline-link">{p.pipeline_name}</span>
                </td>
                <td className="health-dash__td health-dash__td--mono">{p.failure_count}</td>
                <td className="health-dash__td">{formatTimestamp(p.last_failure_at)}</td>
                <td className="health-dash__td health-dash__td--error" title={p.last_error ?? undefined}>
                  {p.last_error ?? '\u2014'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Slowest Pipelines
// ---------------------------------------------------------------------------

function SlowestSection({
  pipelines,
  onPipelineClick,
}: {
  pipelines: SlowestPipeline[];
  onPipelineClick: (name: string) => void;
}) {
  return (
    <div className="health-dash__section">
      <div className="health-dash__section-header">
        <span className="health-dash__section-title">Longest-Running Pipelines</span>
      </div>
      {pipelines.length === 0 ? (
        <div className="health-dash__section-empty">No successful runs in this window.</div>
      ) : (
        <table className="health-dash__table">
          <thead>
            <tr>
              <th className="health-dash__th">Pipeline</th>
              <th className="health-dash__th">Avg Duration</th>
              <th className="health-dash__th">Max Duration</th>
              <th className="health-dash__th">Runs</th>
            </tr>
          </thead>
          <tbody>
            {pipelines.map((p) => (
              <tr
                key={p.pipeline_name}
                className="health-dash__row health-dash__row--clickable"
                onClick={() => onPipelineClick(p.pipeline_name)}
              >
                <td className="health-dash__td health-dash__td--name">
                  <span className="health-dash__pipeline-link">{p.pipeline_name}</span>
                </td>
                <td className="health-dash__td">
                  <span className="health-dash__duration">{formatDuration(p.avg_duration_ms)}</span>
                </td>
                <td className="health-dash__td">
                  <span className="health-dash__duration">{formatDuration(p.max_duration_ms)}</span>
                </td>
                <td className="health-dash__td health-dash__td--mono">{p.run_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Trigger Health
// ---------------------------------------------------------------------------

function TriggerHealthSection({
  health,
  onPipelineClick,
}: {
  health: { total: number; healthy: number; consecutive_failures: TriggerFailure[] };
  onPipelineClick: (name: string) => void;
}) {
  const unhealthy = health.total - health.healthy;
  return (
    <div className="health-dash__section">
      <div className="health-dash__section-header">
        <span className="health-dash__section-title">Trigger Health</span>
      </div>
      <div className="health-dash__trigger-stats">
        <span className="health-dash__trigger-stat">
          Total: <span className="health-dash__trigger-stat-value">{health.total}</span>
        </span>
        <span className="health-dash__trigger-stat">
          Healthy: <span className="health-dash__trigger-stat-value health-dash__trigger-stat-value--healthy">{health.healthy}</span>
        </span>
        {unhealthy > 0 && (
          <span className="health-dash__trigger-stat">
            Failing: <span className="health-dash__trigger-stat-value health-dash__trigger-stat-value--unhealthy">{unhealthy}</span>
          </span>
        )}
      </div>
      {health.consecutive_failures.length === 0 ? (
        <div className="health-dash__section-empty">All triggers healthy.</div>
      ) : (
        <table className="health-dash__table">
          <thead>
            <tr>
              <th className="health-dash__th">Trigger</th>
              <th className="health-dash__th">Pipeline</th>
              <th className="health-dash__th">Consecutive Errors</th>
            </tr>
          </thead>
          <tbody>
            {health.consecutive_failures.map((tf) => (
              <tr
                key={tf.trigger_id}
                className="health-dash__row health-dash__row--clickable"
                onClick={() => onPipelineClick(tf.pipeline_id)}
              >
                <td className="health-dash__td health-dash__td--name">{tf.trigger_name}</td>
                <td className="health-dash__td">
                  <span className="health-dash__pipeline-link">{tf.pipeline_id}</span>
                </td>
                <td className="health-dash__td health-dash__td--mono">{tf.consecutive_errors}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// SLA Breaches
// ---------------------------------------------------------------------------

function SlaBreachesSection({
  summary,
  onPipelineClick,
}: {
  summary: { total: number; ok: number; warning: number; breach: number; unknown: number; breaches: SlaBreach[] };
  onPipelineClick: (name: string) => void;
}) {
  return (
    <div className="health-dash__section">
      <div className="health-dash__section-header">
        <span className="health-dash__section-title">SLA Status</span>
        <span className="health-dash__section-meta">{summary.total} resources</span>
      </div>
      <div className="health-dash__sla-stats">
        <span className="health-dash__sla-stat">
          <span className="health-dash__sla-dot health-dash__sla-dot--ok" />
          {summary.ok} OK
        </span>
        <span className="health-dash__sla-stat">
          <span className="health-dash__sla-dot health-dash__sla-dot--warning" />
          {summary.warning} Warning
        </span>
        <span className="health-dash__sla-stat">
          <span className="health-dash__sla-dot health-dash__sla-dot--breach" />
          {summary.breach} Breach
        </span>
        {summary.unknown > 0 && (
          <span className="health-dash__sla-stat">
            <span className="health-dash__sla-dot health-dash__sla-dot--unknown" />
            {summary.unknown} Unknown
          </span>
        )}
      </div>
      {summary.breaches.length === 0 ? (
        <div className="health-dash__section-empty">No SLA breaches.</div>
      ) : (
        <table className="health-dash__table">
          <thead>
            <tr>
              <th className="health-dash__th">Resource</th>
              <th className="health-dash__th">Age</th>
              <th className="health-dash__th">Max Age</th>
              <th className="health-dash__th">Producer</th>
            </tr>
          </thead>
          <tbody>
            {summary.breaches.map((b) => (
              <tr key={b.fingerprint} className="health-dash__row">
                <td className="health-dash__td health-dash__td--name">{b.fingerprint}</td>
                <td className="health-dash__td">
                  <span className="health-dash__duration">{formatIsoDuration(b.age)}</span>
                </td>
                <td className="health-dash__td">
                  <span className="health-dash__duration">{formatIsoDuration(b.max_age)}</span>
                </td>
                <td className="health-dash__td">
                  {b.producer_pipeline ? (
                    <span
                      className="health-dash__pipeline-link"
                      onClick={() => onPipelineClick(b.producer_pipeline!)}
                    >
                      {b.producer_pipeline}
                    </span>
                  ) : (
                    '\u2014'
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
