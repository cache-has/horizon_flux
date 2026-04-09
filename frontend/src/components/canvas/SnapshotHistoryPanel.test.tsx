// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { SnapshotHistoryPanel } from './SnapshotHistoryPanel';
import type { MaterializationPolicy } from '../../api/pipelines';

const POLICY: MaterializationPolicy = {
  write_strategy: 'snapshot',
  unique_keys: ['customer_id'],
  snapshot: { change_detection: 'check', check_columns: ['email'] },
};

describe('SnapshotHistoryPanel', () => {
  let originalFetch: typeof fetch;
  beforeEach(() => {
    originalFetch = global.fetch;
  });
  afterEach(() => {
    global.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('prompts for unique_keys when none are configured', () => {
    render(
      <SnapshotHistoryPanel
        pipelineId="p1"
        nodeId="sink"
        policy={{ write_strategy: 'snapshot', snapshot: { change_detection: 'check' } }}
      />,
    );
    expect(screen.getByText(/Define/i)).toBeInTheDocument();
  });

  it('disables lookup until every unique_key has a value', () => {
    render(<SnapshotHistoryPanel pipelineId="p1" nodeId="sink" policy={POLICY} />);
    const button = screen.getByRole('button', { name: /Show history/i });
    expect(button).toBeDisabled();
    fireEvent.change(screen.getByPlaceholderText(/value for customer_id/i), {
      target: { value: '42' },
    });
    expect(button).not.toBeDisabled();
  });

  it('renders the timeline returned by the server', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        node_id: 'sink',
        table: 'customers',
        unique_keys: ['customer_id'],
        comparison_columns: ['email'],
        key: { customer_id: '42' },
        version_count: 2,
        versions: [
          {
            flux_scd_id: 'cur',
            flux_valid_from: '2026-04-08 12:00:00',
            flux_valid_to: null,
            flux_is_current: true,
            comparison: { email: 'new@example.com' },
          },
          {
            flux_scd_id: 'old',
            flux_valid_from: '2026-04-01 09:00:00',
            flux_valid_to: '2026-04-08 12:00:00',
            flux_is_current: false,
            comparison: { email: 'old@example.com' },
          },
        ],
      }),
    } as unknown as Response);

    render(<SnapshotHistoryPanel pipelineId="p1" nodeId="sink" policy={POLICY} />);
    fireEvent.change(screen.getByPlaceholderText(/value for customer_id/i), {
      target: { value: '42' },
    });
    fireEvent.click(screen.getByRole('button', { name: /Show history/i }));

    await waitFor(() => expect(screen.getByText(/2 versions/i)).toBeInTheDocument());
    expect(screen.getByText('new@example.com')).toBeInTheDocument();
    expect(screen.getByText('old@example.com')).toBeInTheDocument();
    expect(screen.getByText('current')).toBeInTheDocument();
    expect(screen.getByText('closed')).toBeInTheDocument();
    expect(global.fetch).toHaveBeenCalledWith(
      '/api/pipelines/p1/nodes/sink/snapshot/history',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('surfaces server error messages inline', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      json: async () => ({
        error: '`flux snapshot` v1 supports the `postgresql` sink only',
      }),
    } as unknown as Response);

    render(<SnapshotHistoryPanel pipelineId="p1" nodeId="sink" policy={POLICY} />);
    fireEvent.change(screen.getByPlaceholderText(/value for customer_id/i), {
      target: { value: '42' },
    });
    fireEvent.click(screen.getByRole('button', { name: /Show history/i }));
    await waitFor(() => expect(screen.getByRole('alert')).toBeInTheDocument());
    expect(screen.getByRole('alert').textContent).toMatch(/postgresql/);
  });

  it('shows empty state when no versions match', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        node_id: 'sink',
        table: 'customers',
        unique_keys: ['customer_id'],
        comparison_columns: ['email'],
        key: { customer_id: '99' },
        version_count: 0,
        versions: [],
      }),
    } as unknown as Response);

    render(<SnapshotHistoryPanel pipelineId="p1" nodeId="sink" policy={POLICY} />);
    fireEvent.change(screen.getByPlaceholderText(/value for customer_id/i), {
      target: { value: '99' },
    });
    fireEvent.click(screen.getByRole('button', { name: /Show history/i }));
    await waitFor(() => expect(screen.getByText(/No rows in/i)).toBeInTheDocument());
  });
});
