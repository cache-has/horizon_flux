// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { SidePanel } from './SidePanel';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { PipelineNode, PipelineEdge } from '../../types/pipeline';
import type { ApiPipeline } from '../../api/pipelines';

// Mock the API functions so they don't make real network calls
vi.mock('../../api/pipelines', async () => {
  const actual = await vi.importActual('../../api/pipelines');
  return {
    ...actual,
    previewPipeline: vi.fn().mockRejectedValue(new Error('not available')),
    fetchPipelineRuns: vi.fn().mockRejectedValue(new Error('not available')),
  };
});

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const sourceNode: PipelineNode = {
  id: 'source-1',
  type: 'pipeline',
  position: { x: 0, y: 0 },
  data: {
    label: 'CSV Import',
    role: 'source',
    status: 'idle',
    pinnedPosition: false,
    envOverridden: false,
  },
};

const transformNode: PipelineNode = {
  id: 'transform-1',
  type: 'pipeline',
  position: { x: 200, y: 0 },
  data: {
    label: 'Filter Rows',
    role: 'transform',
    status: 'success',
    pinnedPosition: false,
    envOverridden: false,
  },
};

const sinkNode: PipelineNode = {
  id: 'sink-1',
  type: 'pipeline',
  position: { x: 400, y: 0 },
  data: {
    label: 'PostgreSQL Output',
    role: 'sink',
    status: 'idle',
    pinnedPosition: false,
    envOverridden: true,
  },
};

const testEdges: PipelineEdge[] = [
  { id: 'e-source-1-transform-1', source: 'source-1', target: 'transform-1', type: 'pipeline' },
  { id: 'e-transform-1-sink-1', source: 'transform-1', target: 'sink-1', type: 'pipeline' },
];

const testApiPipeline: ApiPipeline = {
  name: 'test-pipeline',
  version: 1,
  default_environment: 'dev',
  variables: {},
  environment_overrides: {},
  nodes: [
    {
      id: 'source-1',
      name: 'CSV Import',
      type: 'source',
      connector: 'csv',
      config: { path: '/data/input.csv' },
      position: { x: 0, y: 0 },
      pinned_position: false,
    },
    {
      id: 'transform-1',
      name: 'Filter Rows',
      type: 'transform',
      mode: 'sql',
      code: 'SELECT * FROM input\nWHERE status = \'active\'\nORDER BY id',
      materialized: false,
      position: { x: 200, y: 0 },
      pinned_position: false,
    },
    {
      id: 'sink-1',
      name: 'PostgreSQL Output',
      type: 'sink',
      connector: 'postgresql',
      config: { table: 'output_table', write_mode: 'upsert' },
      position: { x: 400, y: 0 },
      pinned_position: false,
    },
  ],
  edges: [
    { from: 'source-1', to: 'transform-1' },
    { from: 'transform-1', to: 'sink-1' },
  ],
};

function setupStore(selectedNodeId: string | null = null) {
  usePipelineStore.setState({
    pipelineId: 'demo',
    apiPipeline: testApiPipeline,
    nodes: [sourceNode, transformNode, sinkNode],
    edges: testEdges,
    selectedNodeId,
    editingNodeId: null,
    dirty: false,
    loading: false,
    error: null,
    simulationHasRun: true,
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('SidePanel', () => {
  beforeEach(() => {
    setupStore(null);
  });

  it('is hidden when no node is selected', () => {
    render(<SidePanel />);
    const panel = screen.getByTestId('side-panel');
    expect(panel.classList.contains('side-panel--open')).toBe(false);
  });

  it('opens when a node is selected', () => {
    setupStore('source-1');
    render(<SidePanel />);
    const panel = screen.getByTestId('side-panel');
    expect(panel.classList.contains('side-panel--open')).toBe(true);
  });

  it('shows source node content', () => {
    setupStore('source-1');
    render(<SidePanel />);
    expect(screen.getByText('CSV Import')).toBeInTheDocument();
    expect(screen.getByText('csv')).toBeInTheDocument();
    expect(screen.getByText('/data/input.csv')).toBeInTheDocument();
  });

  it('shows transform node content with mode and code preview', () => {
    setupStore('transform-1');
    render(<SidePanel />);
    expect(screen.getByText('Filter Rows')).toBeInTheDocument();
    expect(screen.getByText('SQL')).toBeInTheDocument();
    // Code preview should be visible
    expect(screen.getByText(/SELECT \* FROM input/)).toBeInTheDocument();
  });

  it('shows transform upstream inputs', () => {
    setupStore('transform-1');
    render(<SidePanel />);
    expect(screen.getByText('CSV Import')).toBeInTheDocument();
  });

  it('shows sink node content with connector config', () => {
    setupStore('sink-1');
    render(<SidePanel />);
    expect(screen.getByText('PostgreSQL Output')).toBeInTheDocument();
    expect(screen.getByText('postgresql')).toBeInTheDocument();
    expect(screen.getByText('output_table')).toBeInTheDocument();
    expect(screen.getByText('upsert')).toBeInTheDocument();
  });

  it('shows environment override badge for sink', () => {
    setupStore('sink-1');
    render(<SidePanel />);
    expect(screen.getByText('Override active')).toBeInTheDocument();
  });

  it('closes when close button is clicked', () => {
    setupStore('source-1');
    render(<SidePanel />);
    fireEvent.click(screen.getByLabelText('Close panel'));
    expect(usePipelineStore.getState().selectedNodeId).toBeNull();
  });

  it('updates content when selection changes', () => {
    setupStore('source-1');
    const { rerender } = render(<SidePanel />);
    expect(screen.getByText('CSV Import')).toBeInTheDocument();

    act(() => {
      usePipelineStore.getState().setSelectedNodeId('transform-1');
    });
    rerender(<SidePanel />);
    expect(screen.getByText('Filter Rows')).toBeInTheDocument();
  });

  it('renders action buttons', () => {
    setupStore('source-1');
    render(<SidePanel />);
    expect(screen.getByText('Edit')).toBeInTheDocument();
    expect(screen.getByText('Duplicate')).toBeInTheDocument();
    expect(screen.getByText('Delete')).toBeInTheDocument();
  });

  it('Edit button sets editingNodeId', () => {
    setupStore('source-1');
    render(<SidePanel />);
    fireEvent.click(screen.getByText('Edit'));
    expect(usePipelineStore.getState().editingNodeId).toBe('source-1');
  });

  it('allows inline name editing', () => {
    setupStore('source-1');
    render(<SidePanel />);

    // Click on the name to enter edit mode
    fireEvent.click(screen.getByText('CSV Import'));

    // Should now show an input
    const input = screen.getByDisplayValue('CSV Import');
    expect(input).toBeInTheDocument();

    // Change the name and blur to commit
    fireEvent.change(input, { target: { value: 'Renamed Source' } });
    fireEvent.blur(input);

    // Store should have the updated name
    const node = usePipelineStore.getState().nodes.find((n) => n.id === 'source-1');
    expect(node?.data.label).toBe('Renamed Source');
  });

  it('shows role badge with correct class', () => {
    setupStore('transform-1');
    const { container } = render(<SidePanel />);
    expect(
      container.querySelector('.side-panel__role-badge--transform'),
    ).toBeInTheDocument();
  });
});
