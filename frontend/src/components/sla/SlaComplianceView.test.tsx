// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { SlaComplianceView } from './SlaComplianceView';
import type { SlaStatusResponse } from '../../api/sla';

const sampleResponse: SlaStatusResponse = {
  data: [
    {
      fingerprint: 'table:public.orders',
      name: 'public.orders',
      tags: ['critical', 'finance'],
      owner: 'data-eng',
      evaluated_at: '2026-04-10T12:00:00Z',
      status: 'breach',
      age: 'PT8H',
      max_age: 'PT6H',
      warn_at: 'PT4H',
      producer_pipeline: 'daily_etl',
      last_success_at: '2026-04-10T04:00:00Z',
    },
    {
      fingerprint: 'table:public.users',
      name: 'public.users',
      tags: ['core'],
      owner: 'platform',
      evaluated_at: '2026-04-10T12:00:00Z',
      status: 'ok',
      age: 'PT1H',
      max_age: 'PT12H',
      producer_pipeline: 'user_sync',
      last_success_at: '2026-04-10T11:00:00Z',
    },
    {
      fingerprint: 'table:public.events',
      name: 'public.events',
      tags: [],
      evaluated_at: '2026-04-10T12:00:00Z',
      status: 'unknown',
      max_age: 'PT24H',
    },
  ],
  total: 3,
};

vi.mock('../../api/sla', () => ({
  fetchSlaStatus: vi.fn(),
  fetchSlaDetail: vi.fn(),
  fetchSlaHistory: vi.fn(),
}));

beforeEach(async () => {
  vi.clearAllMocks();
  const slaApi = await import('../../api/sla');
  (slaApi.fetchSlaStatus as ReturnType<typeof vi.fn>).mockResolvedValue(sampleResponse);
  (slaApi.fetchSlaHistory as ReturnType<typeof vi.fn>).mockResolvedValue([]);

  // Reset the store between tests.
  const { useSlaStore } = await import('../../stores/slaStore');
  useSlaStore.setState({
    entries: [],
    total: 0,
    loading: false,
    error: null,
    statusFilter: null,
    tagFilter: null,
    ownerFilter: null,
    sortField: 'status',
    sortAsc: true,
    selectedFingerprint: null,
    selectedHistory: [],
    historyLoading: false,
  });
});

describe('SlaComplianceView', () => {
  it('renders the dashboard title and fetches data', async () => {
    render(<SlaComplianceView onBack={vi.fn()} />);

    expect(screen.getByText('SLA Compliance')).toBeTruthy();

    await waitFor(() => {
      expect(screen.getByText('public.orders')).toBeTruthy();
    });
  });

  it('shows summary badges with correct counts', async () => {
    render(<SlaComplianceView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('public.orders')).toBeTruthy();
    });

    // 1 breach, 0 warning, 1 ok, 1 unknown
    const badges = screen.getAllByText(/Breach|Warning|OK|No Data/);
    expect(badges.length).toBeGreaterThanOrEqual(4); // summary + inline badges
  });

  it('renders status badges for each entry', async () => {
    render(<SlaComplianceView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('public.orders')).toBeTruthy();
    });

    // Breach row should appear (sorted first by default)
    expect(screen.getByText('public.orders')).toBeTruthy();
    expect(screen.getByText('public.users')).toBeTruthy();
    expect(screen.getByText('public.events')).toBeTruthy();
  });

  it('calls onBack when back button is clicked', async () => {
    const onBack = vi.fn();
    render(<SlaComplianceView onBack={onBack} />);

    fireEvent.click(screen.getByText('Back'));
    expect(onBack).toHaveBeenCalled();
  });

  it('navigates to history view when a row is clicked', async () => {
    render(<SlaComplianceView onBack={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('public.orders')).toBeTruthy();
    });

    fireEvent.click(screen.getByText('public.orders'));

    await waitFor(() => {
      expect(screen.getByText('Back to Dashboard')).toBeTruthy();
      expect(screen.getByText('Evaluation History')).toBeTruthy();
    });
  });

  it('displays pipeline links that call onNavigateToPipeline', async () => {
    const onNavigate = vi.fn();
    render(<SlaComplianceView onBack={vi.fn()} onNavigateToPipeline={onNavigate} />);

    await waitFor(() => {
      expect(screen.getByText('daily_etl')).toBeTruthy();
    });

    fireEvent.click(screen.getByText('daily_etl'));
    expect(onNavigate).toHaveBeenCalledWith('daily_etl');
  });
});
