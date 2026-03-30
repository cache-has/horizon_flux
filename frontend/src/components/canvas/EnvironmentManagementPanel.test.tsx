// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { EnvironmentManagementPanel } from './EnvironmentManagementPanel';
import { useEnvironmentStore } from '../../stores/environmentStore';

// Mock pipeline store to provide node labels
vi.mock('../../stores/pipelineStore', () => ({
  usePipelineStore: vi.fn((selector) => {
    const state = {
      nodes: [
        { id: '1', data: { label: 'users' } },
        { id: '2', data: { label: 'orders' } },
      ],
    };
    return selector(state);
  }),
}));

const mockListTableOverrides = vi.fn().mockResolvedValue([]);

// Mock API calls
vi.mock('../../api/environments', async () => {
  const actual = await vi.importActual('../../api/environments');
  return {
    ...actual,
    listEnvironments: vi.fn().mockResolvedValue([
      { name: 'prod', fallback: null },
      { name: 'dev', fallback: 'prod' },
    ]),
    listTableOverrides: (...args: unknown[]) => mockListTableOverrides(...args),
    createEnvironment: vi.fn().mockResolvedValue({ name: 'staging', fallback: 'prod' }),
    deleteEnvironment: vi.fn().mockResolvedValue(undefined),
    updateEnvironment: vi.fn().mockResolvedValue({ name: 'dev', fallback: 'staging' }),
    createTableOverride: vi.fn().mockResolvedValue(undefined),
    deleteTableOverride: vi.fn().mockResolvedValue(undefined),
  };
});

beforeEach(() => {
  useEnvironmentStore.setState({
    environments: [
      { name: 'prod', fallback: null },
      { name: 'dev', fallback: 'prod' },
    ],
    activeEnvironment: 'dev',
    tableOverrides: [],
    loading: false,
    error: null,
    managementPanelOpen: false,
  });
});

describe('EnvironmentManagementPanel', () => {
  it('renders nothing visible when closed', () => {
    render(<EnvironmentManagementPanel />);
    const panel = document.querySelector('.env-panel');
    expect(panel).toBeTruthy();
    expect(panel?.classList.contains('env-panel--open')).toBe(false);
  });

  it('shows environment list when open', () => {
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    expect(screen.getByText('Environments')).toBeTruthy();
    // Use getAllByText since 'prod' appears in name and fallback chain
    expect(screen.getAllByText('prod').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('dev').length).toBeGreaterThanOrEqual(1);
  });

  it('shows fallback chain display', () => {
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    // dev -> prod chain should be visible
    expect(screen.getByText('dev \u2192 prod')).toBeTruthy();
  });

  it('does not show Delete button for prod', () => {
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    const deleteButtons = screen.getAllByText('Delete');
    // Only one Delete button (for dev), not for prod
    expect(deleteButtons.length).toBe(1);
  });

  it('has create environment form', () => {
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    expect(screen.getByText('Create Environment')).toBeTruthy();
    expect(screen.getByLabelText('Name')).toBeTruthy();
    expect(screen.getByLabelText('Fallback')).toBeTruthy();
    expect(screen.getByText('Create')).toBeTruthy();
  });

  it('closes on close button click', () => {
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    fireEvent.click(screen.getByLabelText('Close'));
    expect(useEnvironmentStore.getState().managementPanelOpen).toBe(false);
  });

  it('shows override toggle buttons with counts', () => {
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    // Both environments should have clickable override count buttons
    const toggleButtons = screen.getAllByRole('button', { name: /table override/i });
    expect(toggleButtons.length).toBe(2);
  });

  it('expands override list when toggle is clicked', async () => {
    mockListTableOverrides.mockResolvedValue([
      { environment: 'dev', schema_name: 'public', table_name: 'users' },
    ]);
    useEnvironmentStore.setState({ managementPanelOpen: true });
    render(<EnvironmentManagementPanel />);

    // Click the dev override toggle
    const toggleButtons = screen.getAllByRole('button', { name: /table override/i });
    await act(async () => {
      fireEvent.click(toggleButtons[1]); // dev is second
    });

    await waitFor(() => {
      expect(screen.getByText('users')).toBeTruthy();
    });
  });
});
