// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { RunDetailPanel } from './RunDetailPanel';
import type { ApiRunDetail } from '../../api/runs';

const sampleRun: ApiRunDetail = {
  id: '550e8400-e29b-41d4-a716-446655440000',
  pipeline_name: 'daily_etl',
  environment: 'prod',
  status: 'failed',
  start_time: { secs_since_epoch: 1700000000, nanos_since_epoch: 0 },
  end_time: { secs_since_epoch: 1700000005, nanos_since_epoch: 0 },
  node_stats: [
    {
      node_id: 'source-1',
      start_time: { secs_since_epoch: 1700000000, nanos_since_epoch: 0 },
      end_time: { secs_since_epoch: 1700000002, nanos_since_epoch: 0 },
      rows_in: 0,
      rows_out: 1000,
    },
    {
      node_id: 'transform-1',
      start_time: { secs_since_epoch: 1700000002, nanos_since_epoch: 0 },
      end_time: { secs_since_epoch: 1700000004, nanos_since_epoch: 0 },
      rows_in: 1000,
      rows_out: 500,
      error: 'column not found',
    },
  ],
  error: 'Pipeline failed at transform-1',
  triggered_by: 'cron:6h',
};

// Mock the API module
vi.mock('../../api/runs', async () => {
  const actual = await vi.importActual('../../api/runs');
  return {
    ...actual,
    fetchRun: vi.fn(),
  };
});

// Mock the pipeline store
vi.mock('../../stores/pipelineStore', () => ({
  usePipelineStore: vi.fn((selector: (s: unknown) => unknown) =>
    selector({
      apiPipeline: {
        name: 'daily_etl',
        nodes: [
          { id: 'source-1', name: 'CSV Import', type: 'source' },
          { id: 'transform-1', name: 'Filter Rows', type: 'transform' },
        ],
      },
    }),
  ),
}));

import { fetchRun } from '../../api/runs';

const mockFetchRun = fetchRun as ReturnType<typeof vi.fn>;

describe('RunDetailPanel', () => {
  const onClose = vi.fn();
  const onJumpToNode = vi.fn();
  const onViewFailureReport = vi.fn();
  const onCompare = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchRun.mockResolvedValue(sampleRun);
  });

  it('renders loading state while fetching', () => {
    mockFetchRun.mockReturnValue(new Promise(() => {}));
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    expect(screen.getByText('Loading run details...')).toBeTruthy();
  });

  it('renders run summary after loading', async () => {
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('failed')).toBeTruthy();
    });
    expect(screen.getByText('daily_etl')).toBeTruthy();
    expect(screen.getByText('prod')).toBeTruthy();
    expect(screen.getByText('cron:6h')).toBeTruthy();
  });

  it('renders Gantt timeline with node bars', async () => {
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('gantt-timeline')).toBeTruthy();
    });
    expect(screen.getByText('CSV Import')).toBeTruthy();
    // Filter Rows appears in both Gantt label and auto-selected node detail
    expect(screen.getAllByText('Filter Rows').length).toBeGreaterThanOrEqual(1);
  });

  it('auto-selects the failed node and shows its details', async () => {
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
        onViewFailureReport={onViewFailureReport}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('column not found')).toBeTruthy();
    });
    expect(screen.getByText('View failure report')).toBeTruthy();
  });

  it('shows Jump to failing node button for failed runs', async () => {
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('Jump to failing node')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('Jump to failing node'));
    expect(onJumpToNode).toHaveBeenCalledWith('transform-1');
  });

  it('shows Compare button when onCompare is provided', async () => {
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
        onCompare={onCompare}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('Compare with...')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('Compare with...'));
    expect(onCompare).toHaveBeenCalledWith('550e8400-e29b-41d4-a716-446655440000');
  });

  it('calls onClose when close button is clicked', () => {
    mockFetchRun.mockReturnValue(new Promise(() => {}));
    render(
      <RunDetailPanel
        runId="r1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    fireEvent.click(screen.getByLabelText('Close panel'));
    expect(onClose).toHaveBeenCalled();
  });

  it('shows error state when fetch fails', async () => {
    mockFetchRun.mockRejectedValue(new Error('Network error'));
    render(
      <RunDetailPanel
        runId="r1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('Network error')).toBeTruthy();
    });
  });

  it('clicking a node in Gantt shows its detail panel', async () => {
    render(
      <RunDetailPanel
        runId="550e8400-e29b-41d4-a716-446655440000"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('CSV Import')).toBeTruthy();
    });
    // Click on source node in Gantt
    fireEvent.click(screen.getByText('CSV Import'));
    // Should show node detail with Jump to node
    await waitFor(() => {
      expect(screen.getByText('Jump to node')).toBeTruthy();
    });
  });
});
