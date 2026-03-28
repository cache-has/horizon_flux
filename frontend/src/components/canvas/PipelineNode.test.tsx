// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeAll } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ReactFlow } from '@xyflow/react';
import type { NodeTypes } from '@xyflow/react';
import { PipelineNodeComponent } from './PipelineNode';
import type { PipelineNode } from '../../types/pipeline';
import type { PipelineNodeData } from '../../types/pipeline';

beforeAll(() => {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});

const nodeTypes: NodeTypes = { pipeline: PipelineNodeComponent };

function renderNode(data: PipelineNodeData) {
  const nodes: PipelineNode[] = [
    { id: 'test', type: 'pipeline', position: { x: 0, y: 0 }, data },
  ];
  return render(<ReactFlow nodes={nodes} nodeTypes={nodeTypes} />);
}

const baseData: PipelineNodeData = {
  label: 'Test Node',
  role: 'source',
  status: 'idle',
  pinnedPosition: false,
  envOverridden: false,
};

describe('PipelineNode', () => {
  it('renders label text', () => {
    renderNode({ ...baseData, label: 'My Source' });
    expect(screen.getByText('My Source')).toBeInTheDocument();
  });

  it('applies role-specific CSS class', () => {
    const { container } = renderNode({ ...baseData, role: 'transform' });
    expect(
      container.querySelector('.pipeline-node--transform'),
    ).toBeInTheDocument();
  });

  it('shows environment badge when overridden', () => {
    const { container } = renderNode({
      ...baseData,
      role: 'sink',
      envOverridden: true,
    });
    expect(
      container.querySelector('.pipeline-node__env-badge'),
    ).toBeInTheDocument();
  });

  it('hides environment badge when not overridden', () => {
    const { container } = renderNode({ ...baseData, role: 'sink' });
    expect(
      container.querySelector('.pipeline-node__env-badge'),
    ).not.toBeInTheDocument();
  });

  it('renders status indicator with correct class', () => {
    const { container } = renderNode({ ...baseData, status: 'running' });
    expect(container.querySelector('.status-running')).toBeInTheDocument();
  });

  it('shows check icon for success status', () => {
    const { container } = renderNode({ ...baseData, status: 'success' });
    const statusEl = container.querySelector('.status-success');
    expect(statusEl).toBeInTheDocument();
    expect(statusEl?.textContent).toBe('\u2713');
  });

  it('shows x icon for error status', () => {
    const { container } = renderNode({ ...baseData, status: 'error' });
    const statusEl = container.querySelector('.status-error');
    expect(statusEl).toBeInTheDocument();
    expect(statusEl?.textContent).toBe('\u2717');
  });

  it('shows tooltip with stats on hover', () => {
    const { container } = renderNode({
      ...baseData,
      rowCount: 1500,
      lastRunDurationMs: 230,
      schemaSummary: [
        { name: 'id', dataType: 'Int32' },
        { name: 'name', dataType: 'Utf8' },
      ],
    });

    const node = container.querySelector('.pipeline-node')!;
    expect(container.querySelector('.pipeline-node__tooltip')).not.toBeInTheDocument();

    fireEvent.mouseEnter(node);
    expect(container.querySelector('.pipeline-node__tooltip')).toBeInTheDocument();
    expect(screen.getByText('1,500')).toBeInTheDocument();
    expect(screen.getByText('230ms')).toBeInTheDocument();
    expect(screen.getByText('id')).toBeInTheDocument();

    fireEvent.mouseLeave(node);
    expect(container.querySelector('.pipeline-node__tooltip')).not.toBeInTheDocument();
  });

  it('does not show tooltip when no stats available', () => {
    const { container } = renderNode(baseData);
    const node = container.querySelector('.pipeline-node')!;
    fireEvent.mouseEnter(node);
    expect(container.querySelector('.pipeline-node__tooltip')).not.toBeInTheDocument();
  });
});
