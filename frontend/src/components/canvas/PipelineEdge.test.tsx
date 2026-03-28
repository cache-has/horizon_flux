// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeAll } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ReactFlowProvider, Position, type EdgeProps } from '@xyflow/react';
import { PipelineEdgeComponent, EdgeMarkerDefs } from './PipelineEdge';
import type { PipelineEdge } from '../../types/pipeline';

beforeAll(() => {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});

/** Build minimal EdgeProps for testing the component directly. */
function makeEdgeProps(
  overrides: Partial<EdgeProps<PipelineEdge>> = {},
): EdgeProps<PipelineEdge> {
  return {
    id: 'e-test',
    source: 'src',
    target: 'sink',
    sourceX: 0,
    sourceY: 50,
    targetX: 300,
    targetY: 50,
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    data: {},
    selected: false,
    animated: false,
    sourceHandleId: null,
    targetHandleId: null,
    ...overrides,
  } as EdgeProps<PipelineEdge>;
}

function renderEdge(overrides: Partial<EdgeProps<PipelineEdge>> = {}) {
  const props = makeEdgeProps(overrides);
  return render(
    <ReactFlowProvider>
      <svg>
        <PipelineEdgeComponent {...props} />
      </svg>
    </ReactFlowProvider>,
  );
}

describe('EdgeMarkerDefs', () => {
  it('renders arrow marker definitions', () => {
    const { container } = render(<EdgeMarkerDefs />);
    expect(container.querySelector('#pipeline-edge-arrow')).toBeInTheDocument();
    expect(
      container.querySelector('#pipeline-edge-arrow-selected'),
    ).toBeInTheDocument();
  });

  it('markers contain path elements', () => {
    const { container } = render(<EdgeMarkerDefs />);
    const marker = container.querySelector('#pipeline-edge-arrow');
    expect(marker?.querySelector('path')).toBeInTheDocument();
  });
});

describe('PipelineEdgeComponent', () => {
  it('renders the invisible hit area path', () => {
    const { container } = renderEdge();
    expect(
      container.querySelector('.pipeline-edge__hitarea'),
    ).toBeInTheDocument();
  });

  it('renders a BaseEdge path element', () => {
    const { container } = renderEdge();
    expect(
      container.querySelector('.pipeline-edge__path'),
    ).toBeInTheDocument();
  });

  it('applies selected class when selected', () => {
    const { container } = renderEdge({ selected: true });
    expect(
      container.querySelector('.pipeline-edge__path--selected'),
    ).toBeInTheDocument();
  });

  it('renders animated dot when data.animated is true', () => {
    const { container } = renderEdge({ data: { animated: true } });
    expect(
      container.querySelector('.pipeline-edge__dot'),
    ).toBeInTheDocument();
  });

  it('does not render animated dot by default', () => {
    const { container } = renderEdge();
    expect(
      container.querySelector('.pipeline-edge__dot'),
    ).not.toBeInTheDocument();
  });

  it('shows tooltip with metadata on hover', () => {
    const { container } = renderEdge({
      data: {
        rowCount: 5000,
        dataVolumeBytes: 1024 * 512,
        elapsedMs: 1500,
      },
    });

    const hitarea = container.querySelector('.pipeline-edge__hitarea')!;
    fireEvent.mouseEnter(hitarea);

    expect(screen.getByText('5,000')).toBeInTheDocument();
    expect(screen.getByText('512.0 KB')).toBeInTheDocument();
    expect(screen.getByText('1.5s')).toBeInTheDocument();
  });

  it('does not show tooltip when no metadata present', () => {
    const { container } = renderEdge();
    const hitarea = container.querySelector('.pipeline-edge__hitarea')!;
    fireEvent.mouseEnter(hitarea);
    expect(
      container.querySelector('.pipeline-edge-tooltip'),
    ).not.toBeInTheDocument();
  });

  it('shows schema columns in tooltip', () => {
    const { container } = renderEdge({
      data: {
        schemaSummary: [
          { name: 'id', dataType: 'Int32' },
          { name: 'name', dataType: 'Utf8' },
        ],
      },
    });

    const hitarea = container.querySelector('.pipeline-edge__hitarea')!;
    fireEvent.mouseEnter(hitarea);

    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('Utf8')).toBeInTheDocument();
  });

  it('pins tooltip on click', () => {
    const { container } = renderEdge({
      data: { rowCount: 42 },
    });

    const hitarea = container.querySelector('.pipeline-edge__hitarea')!;

    // Click to pin
    fireEvent.click(hitarea);
    expect(
      container.querySelector('.pipeline-edge-tooltip'),
    ).toBeInTheDocument();

    // Mouse leave — tooltip stays because it's pinned
    fireEvent.mouseLeave(hitarea);
    expect(
      container.querySelector('.pipeline-edge-tooltip'),
    ).toBeInTheDocument();

    // Click again to unpin, then leave
    fireEvent.click(hitarea);
    fireEvent.mouseLeave(hitarea);
    expect(
      container.querySelector('.pipeline-edge-tooltip'),
    ).not.toBeInTheDocument();
  });
});
