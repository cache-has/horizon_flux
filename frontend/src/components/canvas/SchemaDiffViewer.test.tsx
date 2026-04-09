// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { SchemaDiffViewer } from './SchemaDiffViewer';

describe('SchemaDiffViewer', () => {
  it('renders nothing for null diff', () => {
    const { container } = render(<SchemaDiffViewer diff={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders empty placeholder when all arrays are empty', () => {
    render(<SchemaDiffViewer diff={{ added: [], removed: [], type_changed: [] }} />);
    expect(screen.getByText(/No schema changes/i)).toBeInTheDocument();
  });

  it('renders added, removed, and type-changed columns', () => {
    render(
      <SchemaDiffViewer
        diff={{
          added: [{ name: 'new_col', data_type: 'Utf8' }],
          removed: [{ name: 'old_col', data_type: 'Int32' }],
          type_changed: [{ name: 'amount', before: 'Int32', after: 'Int64' }],
        }}
      />,
    );
    expect(screen.getByText('new_col')).toBeInTheDocument();
    expect(screen.getByText(/added/i)).toBeInTheDocument();
    expect(screen.getByText('old_col')).toBeInTheDocument();
    expect(screen.getByText(/removed/i)).toBeInTheDocument();
    expect(screen.getByText('amount')).toBeInTheDocument();
    expect(screen.getByText(/Int32 → Int64/)).toBeInTheDocument();
  });
});
