// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { NodeEditorModal } from './NodeEditorModal';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { PipelineNode, PipelineEdge } from '../../types/pipeline';
import type { ApiPipeline } from '../../api/pipelines';

// Mock Monaco editor — it requires a browser environment
vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value: string; onChange?: (v: string) => void }) => (
    <textarea
      data-testid="mock-monaco"
      value={value}
      onChange={(e) => onChange?.(e.target.value)}
    />
  ),
}));

// Mock API functions
vi.mock('../../api/pipelines', async () => {
  const actual = await vi.importActual('../../api/pipelines');
  return {
    ...actual,
    previewPipeline: vi.fn().mockRejectedValue(new Error('not available')),
    previewNode: vi.fn().mockRejectedValue(new Error('not available')),
    updatePipeline: vi.fn().mockResolvedValue({
      id: 'test-pipeline',
      pipeline: {} as ApiPipeline,
      created_at: 0,
      updated_at: 0,
    }),
  };
});

// Mock matchMedia (not available in jsdom)
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

// HTMLDialogElement.showModal/close are not implemented in jsdom
beforeEach(() => {
  HTMLDialogElement.prototype.showModal = vi.fn(function (this: HTMLDialogElement) {
    this.setAttribute('open', '');
  });
  HTMLDialogElement.prototype.close = vi.fn(function (this: HTMLDialogElement) {
    this.removeAttribute('open');
  });
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const transformNode: PipelineNode = {
  id: 'transform-1',
  type: 'pipeline',
  position: { x: 200, y: 0 },
  data: {
    label: 'Filter Rows',
    role: 'transform',
    status: 'idle',
    pinnedPosition: false,
    envOverridden: false,
  },
};

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

const sinkNode: PipelineNode = {
  id: 'sink-1',
  type: 'pipeline',
  position: { x: 400, y: 0 },
  data: {
    label: 'PG Output',
    role: 'sink',
    status: 'idle',
    pinnedPosition: false,
    envOverridden: false,
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
      code: 'SELECT * FROM input WHERE active = true',
      position: { x: 200, y: 0 },
      pinned_position: false,
    },
    {
      id: 'sink-1',
      name: 'PG Output',
      type: 'sink',
      connector: 'postgres',
      config: { table: 'output', write_mode: 'insert' },
      position: { x: 400, y: 0 },
      pinned_position: false,
    },
  ],
  edges: [
    { from: 'source-1', to: 'transform-1' },
    { from: 'transform-1', to: 'sink-1' },
  ],
};

function setupStore(editingNodeId: string | null = null) {
  usePipelineStore.setState({
    pipelineId: 'demo',
    apiPipeline: testApiPipeline,
    nodes: [sourceNode, transformNode, sinkNode],
    edges: testEdges,
    selectedNodeId: null,
    editingNodeId,
    dirty: false,
    loading: false,
    error: null,
    simulationHasRun: true,
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('NodeEditorModal', () => {
  beforeEach(() => {
    setupStore(null);
  });

  it('renders nothing when no node is being edited', () => {
    const { container } = render(<NodeEditorModal />);
    expect(container.querySelector('.node-editor')).toBeNull();
  });

  it('opens dialog when editingNodeId is set', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    expect(HTMLDialogElement.prototype.showModal).toHaveBeenCalled();
  });

  it('shows node name in header for transform', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    const nameInput = screen.getByLabelText('Node name');
    expect(nameInput).toHaveValue('Filter Rows');
  });

  it('shows role badge', () => {
    setupStore('transform-1');
    const { container } = render(<NodeEditorModal />);
    expect(container.querySelector('.node-editor__role-badge--transform')).toBeInTheDocument();
  });

  it('shows mode tabs for transform nodes', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    expect(screen.getByText('SQL')).toBeInTheDocument();
    expect(screen.getByText('Python')).toBeInTheDocument();
    expect(screen.getByText('Expression')).toBeInTheDocument();
  });

  it('Expression tab is disabled', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    const exprTab = screen.getByText('Expression');
    expect(exprTab).toBeDisabled();
  });

  it('does not show mode tabs for source nodes', () => {
    setupStore('source-1');
    render(<NodeEditorModal />);
    expect(screen.queryByText('SQL')).toBeNull();
  });

  it('shows footer with keyboard shortcut hints', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    const footerHint = document.querySelector('.node-editor__footer-hint');
    expect(footerHint).toBeInTheDocument();
    expect(footerHint!.textContent).toContain('save');
    expect(footerHint!.textContent).toContain('close');
  });

  it('shows Save and Cancel buttons', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    expect(screen.getByText('Save')).toBeInTheDocument();
    expect(screen.getByText('Cancel')).toBeInTheDocument();
  });

  it('shows Run Preview button for transforms', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    expect(screen.getByText('Run Preview')).toBeInTheDocument();
  });

  it('marks as dirty when name changes', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    const nameInput = screen.getByLabelText('Node name');
    fireEvent.change(nameInput, { target: { value: 'Renamed' } });
    expect(screen.getByText('unsaved')).toBeInTheDocument();
  });

  it('close button triggers close when not dirty', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    fireEvent.click(screen.getByLabelText('Close editor'));
    expect(usePipelineStore.getState().editingNodeId).toBeNull();
  });

  it('shows discard prompt when closing with unsaved changes', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    // Make it dirty
    const nameInput = screen.getByLabelText('Node name');
    fireEvent.change(nameInput, { target: { value: 'Changed' } });
    // Try to close
    fireEvent.click(screen.getByLabelText('Close editor'));
    // Should show discard prompt
    expect(screen.getByText('Unsaved Changes')).toBeInTheDocument();
  });

  it('discard prompt cancels back to editing', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    const nameInput = screen.getByLabelText('Node name');
    fireEvent.change(nameInput, { target: { value: 'Changed' } });
    fireEvent.click(screen.getByLabelText('Close editor'));
    fireEvent.click(screen.getByText('Keep Editing'));
    // Should still be editing
    expect(usePipelineStore.getState().editingNodeId).toBe('transform-1');
  });

  it('discard prompt confirms and closes', () => {
    setupStore('transform-1');
    render(<NodeEditorModal />);
    const nameInput = screen.getByLabelText('Node name');
    fireEvent.change(nameInput, { target: { value: 'Changed' } });
    fireEvent.click(screen.getByLabelText('Close editor'));
    fireEvent.click(screen.getByText('Discard'));
    expect(usePipelineStore.getState().editingNodeId).toBeNull();
  });

  it('switches between source/transform/sink editors', () => {
    // Transform shows Monaco
    setupStore('transform-1');
    const { rerender } = render(<NodeEditorModal />);
    expect(screen.getByTestId('mock-monaco')).toBeInTheDocument();

    // Source shows connector form
    act(() => {
      usePipelineStore.getState().setEditingNodeId('source-1');
    });
    rerender(<NodeEditorModal />);
    expect(screen.getByText('Connector Type')).toBeInTheDocument();

    // Sink shows connector form
    act(() => {
      usePipelineStore.getState().setEditingNodeId('sink-1');
    });
    rerender(<NodeEditorModal />);
    expect(screen.getByText('Connector Type')).toBeInTheDocument();
  });
});
