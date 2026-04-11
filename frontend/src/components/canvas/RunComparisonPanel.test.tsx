// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { RunComparisonPanel } from './RunComparisonPanel';
import type { ApiRunComparison } from '../../api/runs';
import type { ApiPipelineRun } from '../../api/pipelines';

const sampleComparison: ApiRunComparison = {
  run_id_a: 'aaa',
  run_id_b: 'bbb',
  pipeline_name_a: 'daily_etl',
  pipeline_name_b: 'daily_etl',
  status_a: 'success',
  status_b: 'failed',
  duration_ms_a: 5000,
  duration_ms_b: 8000,
  duration_delta_ms: 3000,
  total_rows_out_a: 1000,
  total_rows_out_b: 500,
  total_rows_out_delta: -500,
  node_comparisons: [
    {
      node_id: 'source-1',
      duration_ms_a: 2000,
      duration_ms_b: 3000,
      duration_delta_ms: 1000,
      rows_in_a: 0,
      rows_in_b: 0,
      rows_out_a: 1000,
      rows_out_b: 500,
      rows_out_delta: -500,
    },
  ],
  test_comparisons: [
    {
      node_id: 'test-1',
      passed_a: true,
      passed_b: false,
      changed: true,
    },
  ],
};

const sampleRuns: ApiPipelineRun[] = [
  {
    id: 'bbb',
    pipeline_name: 'daily_etl',
    environment: 'prod',
    status: 'failed',
    start_time: 1700000000000,
    node_stats: [],
  },
  {
    id: 'ccc',
    pipeline_name: 'daily_etl',
    environment: 'prod',
    status: 'success',
    start_time: 1699990000000,
    node_stats: [],
  },
];

// Mock the API modules
vi.mock('../../api/runs', async () => {
  const actual = await vi.importActual('../../api/runs');
  return {
    ...actual,
    compareRuns: vi.fn(),
  };
});

vi.mock('../../api/pipelines', async () => {
  const actual = await vi.importActual('../../api/pipelines');
  return {
    ...actual,
    fetchPipelineRuns: vi.fn(),
  };
});

// Mock the pipeline store
vi.mock('../../stores/pipelineStore', () => ({
  usePipelineStore: vi.fn((selector: (s: unknown) => unknown) =>
    selector({
      pipelineId: 'p1',
      apiPipeline: {
        name: 'daily_etl',
        nodes: [
          { id: 'source-1', name: 'CSV Import', type: 'source' },
          { id: 'test-1', name: 'Null Check', type: 'test' },
        ],
      },
    }),
  ),
}));

import { compareRuns } from '../../api/runs';
import { fetchPipelineRuns } from '../../api/pipelines';

const mockCompareRuns = compareRuns as ReturnType<typeof vi.fn>;
const mockFetchPipelineRuns = fetchPipelineRuns as ReturnType<typeof vi.fn>;

describe('RunComparisonPanel', () => {
  const onClose = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchPipelineRuns.mockResolvedValue(sampleRuns);
    mockCompareRuns.mockResolvedValue(sampleComparison);
  });

  it('shows run picker when opened', async () => {
    render(
      <RunComparisonPanel runId="aaa" open onClose={onClose} />,
    );
    await waitFor(() => {
      expect(screen.getByText('Select a run to compare with:')).toBeTruthy();
    });
  });

  it('lists available runs excluding the current one', async () => {
    render(
      <RunComparisonPanel runId="aaa" open onClose={onClose} />,
    );
    await waitFor(() => {
      expect(screen.getByText('bbb...')).toBeTruthy();
    });
    expect(screen.getByText('ccc...')).toBeTruthy();
  });

  it('shows comparison after selecting a run', async () => {
    render(
      <RunComparisonPanel runId="aaa" open onClose={onClose} />,
    );
    await waitFor(() => {
      expect(screen.getByText('bbb...')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('bbb...'));
    await waitFor(() => {
      expect(screen.getByTestId('node-comparison-table')).toBeTruthy();
    });
    expect(screen.getByText('CSV Import')).toBeTruthy();
  });

  it('shows test comparison when tests changed', async () => {
    render(
      <RunComparisonPanel runId="aaa" open onClose={onClose} />,
    );
    await waitFor(() => {
      expect(screen.getByText('bbb...')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('bbb...'));
    await waitFor(() => {
      expect(screen.getByTestId('test-comparison-table')).toBeTruthy();
    });
    expect(screen.getByText('Null Check')).toBeTruthy();
    expect(screen.getByText('Yes')).toBeTruthy(); // changed
  });

  it('calls onClose when close button is clicked', () => {
    mockFetchPipelineRuns.mockReturnValue(new Promise(() => {}));
    render(
      <RunComparisonPanel runId="aaa" open onClose={onClose} />,
    );
    fireEvent.click(screen.getByLabelText('Close panel'));
    expect(onClose).toHaveBeenCalled();
  });

  it('shows error state when comparison fails', async () => {
    mockCompareRuns.mockRejectedValue(new Error('Comparison failed'));
    render(
      <RunComparisonPanel runId="aaa" open onClose={onClose} />,
    );
    await waitFor(() => {
      expect(screen.getByText('bbb...')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('bbb...'));
    await waitFor(() => {
      expect(screen.getByText('Comparison failed')).toBeTruthy();
    });
  });
});
