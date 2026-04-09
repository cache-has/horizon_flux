// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { SnapshotDiffPanel } from './SnapshotDiffPanel';

const HAPPY_PAYLOAD = {
  node_id: 'sink',
  table: 'customers',
  environment: 'dev',
  unique_keys: ['customer_id'],
  comparison_columns: ['email'],
  stats: { unchanged: 7, changed: 2, new_versions: 1, gone: 3 },
  sample: [
    { classification: 'changed', unique_key: ['42'] },
    { classification: 'new', unique_key: ['43'] },
    { classification: 'gone', unique_key: ['7'] },
  ],
  staged_row_count: 10,
  sample_truncated: false,
  staged_row_cap: 10000,
  cached: false,
};

describe('SnapshotDiffPanel', () => {
  let originalFetch: typeof fetch;
  beforeEach(() => {
    originalFetch = global.fetch;
  });
  afterEach(() => {
    global.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('prompts to save the pipeline when no pipelineId is set', () => {
    render(<SnapshotDiffPanel />);
    expect(screen.getByText(/Save the pipeline first/i)).toBeInTheDocument();
  });

  it('disables the preview button while the pipeline is dirty', () => {
    render(
      <SnapshotDiffPanel pipelineId="p1" nodeId="sink" environment="dev" dirty />,
    );
    const button = screen.getByRole('button', { name: /Preview diff against dev/i });
    expect(button).toBeDisabled();
    expect(screen.getByText(/Save the pipeline before previewing/i)).toBeInTheDocument();
  });

  it('renders the four counts and sample after a successful preview', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => HAPPY_PAYLOAD,
    } as unknown as Response);

    render(<SnapshotDiffPanel pipelineId="p1" nodeId="sink" environment="dev" />);
    fireEvent.click(screen.getByRole('button', { name: /Preview diff against dev/i }));

    await waitFor(() => expect(screen.getByText('unchanged')).toBeInTheDocument());
    // Counts render as `.snapshot-diff-count-value` divs.
    const values = document.querySelectorAll('.snapshot-diff-count-value');
    const texts = Array.from(values).map((v) => v.textContent);
    expect(texts).toEqual(['7', '2', '1', '3']);
    // Sample table renders the badges
    expect(screen.getAllByText('changed').length).toBeGreaterThan(0);
    expect(screen.getAllByText('new').length).toBeGreaterThan(0);
    expect(screen.getAllByText('gone').length).toBeGreaterThan(0);
    expect(global.fetch).toHaveBeenCalledWith(
      '/api/pipelines/p1/nodes/sink/snapshot/diff',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('shows the truncated banner when sample_truncated is true', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        ...HAPPY_PAYLOAD,
        sample_truncated: true,
        staged_row_count: 10000,
      }),
    } as unknown as Response);

    render(<SnapshotDiffPanel pipelineId="p1" nodeId="sink" environment="dev" />);
    fireEvent.click(screen.getByRole('button', { name: /Preview diff against dev/i }));
    await waitFor(() => expect(screen.getByRole('note')).toBeInTheDocument());
    expect(screen.getByRole('note').textContent).toMatch(/flux snapshot diff/);
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

    render(<SnapshotDiffPanel pipelineId="p1" nodeId="sink" environment="dev" />);
    fireEvent.click(screen.getByRole('button', { name: /Preview diff against dev/i }));
    await waitFor(() => expect(screen.getByRole('alert')).toBeInTheDocument());
    expect(screen.getByRole('alert').textContent).toMatch(/postgresql/);
  });

  it('renders the empty-diff message when no rows would change', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        ...HAPPY_PAYLOAD,
        stats: { unchanged: 100, changed: 0, new_versions: 0, gone: 0 },
        sample: [],
      }),
    } as unknown as Response);

    render(<SnapshotDiffPanel pipelineId="p1" nodeId="sink" environment="dev" />);
    fireEvent.click(screen.getByRole('button', { name: /Preview diff against dev/i }));
    await waitFor(() =>
      expect(screen.getByText(/No rows would change/i)).toBeInTheDocument(),
    );
  });
});
