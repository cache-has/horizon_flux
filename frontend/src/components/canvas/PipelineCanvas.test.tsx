// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, beforeAll } from 'vitest';
import { render, screen } from '@testing-library/react';
import { PipelineCanvas } from './PipelineCanvas';

// React Flow requires ResizeObserver, which jsdom doesn't provide.
beforeAll(() => {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
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

  it('renders initial demo nodes', () => {
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
});
