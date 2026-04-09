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

  it('shows override badge when environment overridden', () => {
    const { container } = renderNode({
      ...baseData,
      role: 'sink',
      envOverridden: true,
    });
    expect(
      container.querySelector('.pipeline-node__env-badge--override'),
    ).toBeInTheDocument();
  });

  it('shows fallthrough badge when not overridden', () => {
    const { container } = renderNode({ ...baseData, role: 'sink' });
    expect(
      container.querySelector('.pipeline-node__env-badge--fallthrough'),
    ).toBeInTheDocument();
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

  it('renders incremental badge for incremental sinks and shows watermark in tooltip', () => {
    const { container } = renderNode({
      ...baseData,
      role: 'sink',
      materializationPolicy: {
        read_mode: 'incremental',
        write_strategy: 'merge',
        unique_keys: ['id'],
        watermark: { column: 'updated_at', type: 'timestamp' },
      },
      materializationReceipt: {
        write_strategy: 'merge',
        read_mode: 'incremental',
        rows_scanned: 42,
        rows_filtered_by_watermark: 100,
        rows_written: 42,
        rows_inserted: 40,
        rows_updated: 2,
        rows_deleted: 0,
        watermark_after: {
          value: '2026-04-08T12:00:00.000000000Z',
          type: 'timestamp',
        },
      },
    });
    const badge = container.querySelector('.pipeline-node__inc-badge')!;
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toBe('INC');

    const node = container.querySelector('.pipeline-node')!;
    fireEvent.mouseEnter(node);
    expect(screen.getByText('Watermark')).toBeInTheDocument();
    expect(screen.getByText('Filtered')).toBeInTheDocument();
    expect(screen.getByText('100')).toBeInTheDocument();
  });

  it('does not render incremental badge for full-read sinks', () => {
    const { container } = renderNode({
      ...baseData,
      role: 'sink',
      materializationPolicy: {
        read_mode: 'full',
        write_strategy: 'append',
      },
    });
    expect(
      container.querySelector('.pipeline-node__inc-badge'),
    ).not.toBeInTheDocument();
  });

  it('does not show tooltip when no stats available', () => {
    const { container } = renderNode(baseData);
    const node = container.querySelector('.pipeline-node')!;
    fireEvent.mouseEnter(node);
    expect(container.querySelector('.pipeline-node__tooltip')).not.toBeInTheDocument();
  });
});
