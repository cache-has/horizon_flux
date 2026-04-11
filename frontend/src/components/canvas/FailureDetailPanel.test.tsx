// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { FailureDetailPanel } from './FailureDetailPanel';
import type { ApiFailureReport } from '../../api/runs';

const sampleReport: ApiFailureReport = {
  run_id: '550e8400-e29b-41d4-a716-446655440000',
  node_id: 'transform_1',
  pipeline_name: 'daily_etl',
  environment: 'prod',
  error_chain: [
    "column 'revenue' not found",
    'schema mismatch in SQL transform',
  ],
  node_config: { type: 'transform', mode: 'sql' },
  input_schemas: [
    {
      node_id: 'src',
      fields: [
        { name: 'id', data_type: 'Int32', nullable: false },
        { name: 'name', data_type: 'Utf8', nullable: true },
      ],
    },
  ],
  input_sample: [
    { id: 1, name: 'alice' },
    { id: 2, name: 'bob' },
  ],
  input_total_rows: 1000,
  executed_sql: 'SELECT revenue FROM input',
  captured_at_ms: 1_700_000_000_000,
};

// Mock the API module
vi.mock('../../api/runs', async () => {
  const actual = await vi.importActual('../../api/runs');
  return {
    ...actual,
    fetchFailureReport: vi.fn(),
    downloadReproduceBundle: vi.fn(),
  };
});

import { fetchFailureReport, downloadReproduceBundle } from '../../api/runs';

const mockFetchReport = fetchFailureReport as ReturnType<typeof vi.fn>;
const mockDownload = downloadReproduceBundle as ReturnType<typeof vi.fn>;

describe('FailureDetailPanel', () => {
  const onClose = vi.fn();
  const onJumpToNode = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchReport.mockResolvedValue(sampleReport);
    mockDownload.mockResolvedValue(undefined);
  });

  it('renders loading state while fetching', () => {
    mockFetchReport.mockReturnValue(new Promise(() => {})); // never resolves
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    expect(screen.getByText('Loading failure report...')).toBeTruthy();
  });

  it('renders error chain after loading', async () => {
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText("column 'revenue' not found")).toBeTruthy();
    });
    expect(screen.getByText(/schema mismatch/)).toBeTruthy();
  });

  it('renders context metadata', async () => {
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('daily_etl')).toBeTruthy();
    });
    expect(screen.getByText('prod')).toBeTruthy();
  });

  it('renders input schema fields', async () => {
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('from src')).toBeTruthy();
    });
    expect(screen.getByText('Int32')).toBeTruthy();
    expect(screen.getByText('Utf8')).toBeTruthy();
  });

  it('renders input sample table', async () => {
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('alice')).toBeTruthy();
    });
    expect(screen.getByText('bob')).toBeTruthy();
    expect(screen.getByText(/Showing 2 of 1,000/)).toBeTruthy();
  });

  it('calls onJumpToNode when Jump to Node is clicked', async () => {
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('Jump to Node')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('Jump to Node'));
    expect(onJumpToNode).toHaveBeenCalledWith('transform_1');
    expect(onClose).toHaveBeenCalled();
  });

  it('calls downloadReproduceBundle when Reproduce Locally is clicked', async () => {
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('Reproduce Locally')).toBeTruthy();
    });
    fireEvent.click(screen.getByText('Reproduce Locally'));
    await waitFor(() => {
      expect(mockDownload).toHaveBeenCalledWith('p1', 'r1', 'transform_1');
    });
  });

  it('calls onClose when close button is clicked', () => {
    mockFetchReport.mockReturnValue(new Promise(() => {}));
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    fireEvent.click(screen.getByLabelText('Close failure panel'));
    expect(onClose).toHaveBeenCalled();
  });

  it('shows error state when fetch fails', async () => {
    mockFetchReport.mockRejectedValue(new Error('Network error'));
    render(
      <FailureDetailPanel
        pipelineId="p1"
        runId="r1"
        nodeId="transform_1"
        open
        onClose={onClose}
        onJumpToNode={onJumpToNode}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText('Network error')).toBeTruthy();
    });
  });
});
