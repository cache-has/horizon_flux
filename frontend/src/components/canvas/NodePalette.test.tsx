// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { NodePalette, PALETTE_DRAG_TYPE } from './NodePalette';

describe('NodePalette', () => {
  it('renders section headers when expanded', () => {
    render(<NodePalette collapsed={false} onToggle={vi.fn()} />);
    expect(screen.getByText('Sources')).toBeInTheDocument();
    expect(screen.getByText('Transforms')).toBeInTheDocument();
    expect(screen.getByText('Sinks')).toBeInTheDocument();
  });

  it('hides content when collapsed', () => {
    render(<NodePalette collapsed={true} onToggle={vi.fn()} />);
    expect(screen.queryByText('Sources')).not.toBeInTheDocument();
    expect(screen.queryByText('Filter nodes...')).not.toBeInTheDocument();
  });

  it('calls onToggle when toggle button is clicked', () => {
    const onToggle = vi.fn();
    render(<NodePalette collapsed={false} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText('Close node palette'));
    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  it('filters items by search input', () => {
    render(<NodePalette collapsed={false} onToggle={vi.fn()} />);
    const search = screen.getByPlaceholderText('Filter nodes...');
    fireEvent.change(search, { target: { value: 'sql' } });
    // SQL transform should be visible
    expect(screen.getByText('SQL')).toBeInTheDocument();
    // CSV items should be hidden
    expect(screen.queryAllByText('CSV')).toHaveLength(0);
  });

  it('sets drag data on drag start', () => {
    render(<NodePalette collapsed={false} onToggle={vi.fn()} />);
    const sqlItem = screen.getByText('SQL').closest('[draggable]')!;
    const setData = vi.fn();
    fireEvent.dragStart(sqlItem, {
      dataTransfer: { setData, effectAllowed: '' },
    });
    expect(setData).toHaveBeenCalledWith(
      PALETTE_DRAG_TYPE,
      expect.stringContaining('"role":"transform"'),
    );
  });

  it('renders all expected palette items', () => {
    render(<NodePalette collapsed={false} onToggle={vi.fn()} />);
    // Sources: CSV, PostgreSQL, REST API
    expect(screen.getByText('REST API')).toBeInTheDocument();
    // Transforms: SQL, Python
    expect(screen.getByText('Python')).toBeInTheDocument();
    // Sinks: stdout
    expect(screen.getByText('stdout')).toBeInTheDocument();
  });
});
