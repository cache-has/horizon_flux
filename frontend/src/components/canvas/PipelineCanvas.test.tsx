// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { PipelineCanvas } from './PipelineCanvas';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { ApiPipelineResponse } from '../../api/pipelines';

// React Flow requires ResizeObserver, which jsdom doesn't provide.
beforeAll(() => {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});

const DEMO_RESPONSE: ApiPipelineResponse = {
  id: 'test-1',
  pipeline: {
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
        config: {},
        position: { x: 100, y: 200 },
        pinned_position: false,
      },
      {
        id: 'transform-1',
        name: 'Filter Rows',
        type: 'transform',
        mode: 'sql',
        code: '',
        materialized: false,
        position: { x: 400, y: 200 },
        pinned_position: false,
      },
      {
        id: 'sink-1',
        name: 'PostgreSQL',
        type: 'sink',
        connector: 'postgresql',
        config: {},
        position: { x: 700, y: 200 },
        pinned_position: false,
      },
    ],
    edges: [
      { from: 'source-1', to: 'transform-1' },
      { from: 'transform-1', to: 'sink-1' },
    ],
  },
  created_at: Date.now(),
  updated_at: Date.now(),
};

/** Pre-populate the store before each test. */
beforeEach(() => {
  usePipelineStore.getState().loadFromResponse(DEMO_RESPONSE);
});

describe('PipelineCanvas', () => {
  it('renders without crashing', () => {
    const { container } = render(<PipelineCanvas />);
    expect(container.querySelector('.pipeline-canvas')).toBeInTheDocument();
  });

  it('renders the React Flow viewport', () => {
    const { container } = render(<PipelineCanvas />);
    expect(
      container.querySelector('.react-flow'),
    ).toBeInTheDocument();
  });

  it('renders nodes from the store', () => {
    render(<PipelineCanvas />);
    expect(screen.getByText('CSV Import')).toBeInTheDocument();
    expect(screen.getByText('Filter Rows')).toBeInTheDocument();
    expect(screen.getByText('PostgreSQL')).toBeInTheDocument();
  });

  it('renders background dots', () => {
    const { container } = render(<PipelineCanvas />);
    expect(
      container.querySelector('.react-flow__background'),
    ).toBeInTheDocument();
  });

  it('renders controls', () => {
    const { container } = render(<PipelineCanvas />);
    expect(
      container.querySelector('.react-flow__controls'),
    ).toBeInTheDocument();
  });

  it('renders minimap', () => {
    const { container } = render(<PipelineCanvas />);
    expect(
      container.querySelector('.react-flow__minimap'),
    ).toBeInTheDocument();
  });

  it('renders re-layout button', () => {
    render(<PipelineCanvas />);
    expect(screen.getByText('Re-layout')).toBeInTheDocument();
  });

  it('renders unpin all checkbox', () => {
    render(<PipelineCanvas />);
    expect(screen.getByText('Unpin all')).toBeInTheDocument();
  });
});
