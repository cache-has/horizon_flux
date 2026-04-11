// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { HealthDashboardView } from './HealthDashboardView';
import type { HealthOverview } from '../../api/health';

const sampleOverview: HealthOverview = {
  window: '24h',
  generated_at: '2026-04-10T12:00:00Z',
  cached: false,
  run_summary: {
    total: 42,
    success: 35,
    failed: 5,
    running: 1,
    pending: 0,
    cancelled: 1,
    by_environment: {},
  },
  top_failing_pipelines: [
    {
      pipeline_name: 'daily_etl',
      failure_count: 3,
      last_failure_at: '2026-04-10T11:00:00Z',
      last_error: 'connection timeout',
    },
  ],
  slowest_pipelines: [
    {
      pipeline_name: 'heavy_transform',
      avg_duration_ms: 120000,
      max_duration_ms: 180000,
      run_count: 10,
    },
  ],
  trigger_health: {
    total: 5,
    healthy: 4,
    consecutive_failures: [
      {
        trigger_id: 't-1',
        trigger_name: 'hourly_ingest',
        pipeline_id: 'ingest_pipeline',
        consecutive_errors: 3,
      },
    ],
  },
  sla_summary: {
    total: 8,
    ok: 5,
    warning: 1,
    breach: 2,
    unknown: 0,
    breaches: [
      {
        fingerprint: 'table:public.orders',
        age: 'PT8H',
        max_age: 'PT6H',
        producer_pipeline: 'daily_etl',
      },
    ],
  },
  notable_events: [
    {
      kind: 'first_failure',
      pipeline_name: 'daily_etl',
      description: 'First failure of previously-healthy pipeline: connection timeout',
      at: '2026-04-10T11:00:00Z',
    },
    {
      kind: 'sla_breach',
      pipeline_name: 'daily_etl',
      description: 'Resource `table:public.orders` has breached its freshness SLA',
    },
  ],
};

vi.mock('../../api/health', () => ({
  fetchHealthOverview: vi.fn(),
}));

beforeEach(async () => {
  vi.clearAllMocks();
  const healthApi = await import('../../api/health');
  (healthApi.fetchHealthOverview as ReturnType<typeof vi.fn>).mockResolvedValue(sampleOverview);

  const { useHealthStore } = await import('../../stores/healthStore');
  useHealthStore.setState({
    overview: null,
    loading: false,
    error: null,
    window: '24h',
  });
});

describe('HealthDashboardView', () => {
  it('renders the dashboard title and fetches data', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    expect(screen.getByText('Health Dashboard')).toBeTruthy();

    await waitFor(() => {
      expect(screen.getByText('42')).toBeTruthy(); // total runs
    });
  });

  it('shows run summary badges', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('42')).toBeTruthy();
      expect(screen.getByText('35')).toBeTruthy();
      // '5' appears in multiple sections (failed count + trigger total + SLA ok),
      // so check at least one exists.
      expect(screen.getAllByText('5').length).toBeGreaterThan(0);
    });
  });

  it('shows top failing pipelines', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      // daily_etl appears in multiple sections (notable events, failing table, SLA)
      expect(screen.getAllByText('daily_etl').length).toBeGreaterThan(0);
      expect(screen.getByText('connection timeout')).toBeTruthy();
    });
  });

  it('shows slowest pipelines', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('heavy_transform')).toBeTruthy();
    });
  });

  it('shows notable events feed', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('Things to Look At')).toBeTruthy();
      expect(screen.getByText('2 items')).toBeTruthy();
    });
  });

  it('shows trigger health section', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('Trigger Health')).toBeTruthy();
      expect(screen.getByText('hourly_ingest')).toBeTruthy();
    });
  });

  it('shows SLA breaches section', async () => {
    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('SLA Status')).toBeTruthy();
      expect(screen.getByText('table:public.orders')).toBeTruthy();
    });
  });

  it('calls onBack when back button is clicked', () => {
    const onBack = vi.fn();
    render(<HealthDashboardView onBack={onBack} />);

    fireEvent.click(screen.getByText('Back'));
    expect(onBack).toHaveBeenCalled();
  });

  it('calls onNavigateToPipeline when a pipeline link is clicked', async () => {
    const onNavigate = vi.fn();
    render(<HealthDashboardView onBack={vi.fn()} onNavigateToPipeline={onNavigate} />);

    await waitFor(() => {
      expect(screen.getByText('Top Failing Pipelines')).toBeTruthy();
    });

    // Click on the first "daily_etl" link (in top failing table)
    const links = screen.getAllByText('daily_etl');
    fireEvent.click(links[0]);
    expect(onNavigate).toHaveBeenCalledWith('daily_etl');
  });

  it('switches time window when buttons are clicked', async () => {
    const healthApi = await import('../../api/health');
    const mockFetch = healthApi.fetchHealthOverview as ReturnType<typeof vi.fn>;

    render(<HealthDashboardView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('42')).toBeTruthy();
    });

    fireEvent.click(screen.getByText('7 days'));

    await waitFor(() => {
      // Should have been called with '7d' window
      const calls = mockFetch.mock.calls;
      expect(calls.some((c: string[]) => c[0] === '7d')).toBe(true);
    });
  });
});
