// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { JsonSchemaForm } from './JsonSchemaForm';

describe('JsonSchemaForm', () => {
  const schema = {
    type: 'object',
    required: ['host'],
    properties: {
      host: { type: 'string', description: 'DB host' },
      port: { type: 'integer', default: 5432 },
      enabled: { type: 'boolean' },
      mode: { type: 'string', enum: ['append', 'overwrite'] },
    },
  };

  it('renders fields, edits, and emits typed values', () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <JsonSchemaForm schema={schema} value={{}} onChange={onChange} />,
    );

    // host string
    fireEvent.change(screen.getByPlaceholderText('DB host'), {
      target: { value: 'db.local' },
    });
    expect(onChange).toHaveBeenLastCalledWith({ host: 'db.local' });

    // integer
    rerender(<JsonSchemaForm schema={schema} value={{ host: 'db.local' }} onChange={onChange} />);
    const portInput = screen.getAllByRole('spinbutton')[0] as HTMLInputElement;
    fireEvent.change(portInput, { target: { value: '6543' } });
    expect(onChange).toHaveBeenLastCalledWith({ host: 'db.local', port: 6543 });

    // boolean
    fireEvent.click(screen.getByLabelText(/enabled/i));
    expect(onChange).toHaveBeenLastCalledWith({ host: 'db.local', enabled: true });

    // enum
    fireEvent.change(screen.getByDisplayValue('(none)'), { target: { value: 'overwrite' } });
    expect(onChange).toHaveBeenLastCalledWith({ host: 'db.local', mode: 'overwrite' });
  });

  it('falls back to JSON textarea for empty schemas', () => {
    const onChange = vi.fn();
    render(<JsonSchemaForm schema={null} value={{ a: 1 }} onChange={onChange} />);
    expect(screen.getByRole('textbox')).toHaveValue('{\n  "a": 1\n}');
  });
});
