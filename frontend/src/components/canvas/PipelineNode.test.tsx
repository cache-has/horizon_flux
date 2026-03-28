// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeAll } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ReactFlow } from '@xyflow/react';
import type { NodeTypes } from '@xyflow/react';
import { PipelineNodeComponent } from './PipelineNode';
import type { PipelineNode } from '../../types/pipeline';

beforeAll(() => {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});

const nodeTypes: NodeTypes = { pipeline: PipelineNodeComponent };

function renderNode(data: PipelineNode['data']) {
  const nodes: PipelineNode[] = [
    { id: 'test', type: 'pipeline', position: { x: 0, y: 0 }, data },
  ];
  return render(<ReactFlow nodes={nodes} nodeTypes={nodeTypes} />);
}

describe('PipelineNode', () => {
  it('renders label text', () => {
    renderNode({
      label: 'My Source',
      role: 'source',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: false,
    });
    expect(screen.getByText('My Source')).toBeInTheDocument();
  });

  it('applies role-specific CSS class', () => {
    const { container } = renderNode({
      label: 'Transform',
      role: 'transform',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: false,
    });
    expect(
      container.querySelector('.pipeline-node--transform'),
    ).toBeInTheDocument();
  });

  it('shows environment badge when overridden', () => {
    const { container } = renderNode({
      label: 'Sink',
      role: 'sink',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: true,
    });
    expect(
      container.querySelector('.pipeline-node__env-badge'),
    ).toBeInTheDocument();
  });

  it('hides environment badge when not overridden', () => {
    const { container } = renderNode({
      label: 'Sink',
      role: 'sink',
      status: 'idle',
      pinnedPosition: false,
      envOverridden: false,
    });
    expect(
      container.querySelector('.pipeline-node__env-badge'),
    ).not.toBeInTheDocument();
  });

  it('renders status indicator with correct class', () => {
    const { container } = renderNode({
      label: 'Running',
      role: 'source',
      status: 'running',
      pinnedPosition: false,
      envOverridden: false,
    });
    expect(container.querySelector('.status-running')).toBeInTheDocument();
  });
});
