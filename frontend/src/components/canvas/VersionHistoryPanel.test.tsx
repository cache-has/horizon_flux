// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { VersionHistoryPanel } from './VersionHistoryPanel';
import { usePipelineStore } from '../../stores/pipelineStore';

// Mock Monaco DiffEditor
vi.mock('@monaco-editor/react', () => ({
  DiffEditor: () => <div data-testid="mock-diff-editor" />,
}));

// Mock API functions
const mockFetchVersions = vi.fn();
const mockFetchVersion = vi.fn();
const mockRestoreVersion = vi.fn();

vi.mock('../../api/pipelines', async () => {
  const actual = await vi.importActual('../../api/pipelines');
  return {
    ...actual,
    fetchVersions: (...args: unknown[]) => mockFetchVersions(...args),
    fetchVersion: (...args: unknown[]) => mockFetchVersion(...args),
    restoreVersion: (...args: unknown[]) => mockRestoreVersion(...args),
  };
});

// Mock matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// Mock HTMLDialogElement methods
HTMLDialogElement.prototype.showModal = vi.fn();
HTMLDialogElement.prototype.close = vi.fn();

const SAMPLE_VERSIONS = [
  { version: 3, saved_at: 1711900000000 },
  { version: 2, saved_at: 1711800000000 },
  { version: 1, saved_at: 1711700000000 },
];

const SAMPLE_PIPELINE = {
  name: 'Test Pipeline',
  version: 3,
  default_environment: 'dev',
  code_dir: 'transforms/',
  variables: {},
  environment_overrides: {},
  sample_config: { mode: 'first_n' as const, count: 100 },
  nodes: [],
  edges: [],
};

describe('VersionHistoryPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchVersions.mockResolvedValue({
      data: SAMPLE_VERSIONS,
      total: 3,
      limit: 100,
      offset: 0,
    });
    usePipelineStore.setState({
      pipelineId: 'test-id',
      apiPipeline: SAMPLE_PIPELINE,
    });
  });

  it('renders closed by default', () => {
    render(<VersionHistoryPanel open={false} onClose={vi.fn()} />);
    const panel = document.querySelector('.version-panel');
    expect(panel).not.toBeNull();
    expect(panel?.classList.contains('version-panel--open')).toBe(false);
  });

  it('loads and displays versions when opened', async () => {
    render(<VersionHistoryPanel open={true} onClose={vi.fn()} />);

    await waitFor(() => {
      expect(mockFetchVersions).toHaveBeenCalledWith('test-id', 100, 0);
    });

    await waitFor(() => {
      expect(screen.getByText('v3')).toBeDefined();
      expect(screen.getByText('v2')).toBeDefined();
      expect(screen.getByText('v1')).toBeDefined();
    });
  });

  it('shows current badge on the latest version', async () => {
    render(<VersionHistoryPanel open={true} onClose={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('current')).toBeDefined();
    });
  });

  it('enables compare button when a version is selected', async () => {
    render(<VersionHistoryPanel open={true} onClose={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('v2')).toBeDefined();
    });

    const checkboxes = screen.getAllByRole('checkbox');
    // Select version 2 (second checkbox)
    fireEvent.click(checkboxes[1]);

    const compareBtn = screen.getByText('Compare with Current');
    expect(compareBtn.closest('button')?.disabled).toBe(false);
  });

  it('opens diff modal on compare', async () => {
    mockFetchVersion.mockResolvedValue({
      version: 1,
      saved_at: 1711700000000,
      snapshot: { ...SAMPLE_PIPELINE, version: 1, name: 'Old Name' },
    });

    render(<VersionHistoryPanel open={true} onClose={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('v1')).toBeDefined();
    });

    // Select version 1 (third checkbox)
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[2]);

    const compareBtn = screen.getByText('Compare with Current');
    fireEvent.click(compareBtn);

    await waitFor(() => {
      expect(mockFetchVersion).toHaveBeenCalledWith('test-id', 1);
    });

    await waitFor(() => {
      expect(screen.getByTestId('mock-diff-editor')).toBeDefined();
    });
  });

  it('does not show restore button for current version', async () => {
    render(<VersionHistoryPanel open={true} onClose={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText('v3')).toBeDefined();
    });

    // Should have restore buttons for v2 and v1, but not v3
    const restoreButtons = screen.getAllByText('Restore');
    expect(restoreButtons).toHaveLength(2);
  });
});
