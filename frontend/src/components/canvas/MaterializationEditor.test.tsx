// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MaterializationEditor } from './MaterializationEditor';
import { validatePolicy } from './materializationPolicy';
import type { MaterializationPolicy } from '../../api/pipelines';

describe('validatePolicy', () => {
  it('accepts default full+append', () => {
    expect(validatePolicy({ read_mode: 'full', write_strategy: 'append' })).toEqual([]);
  });

  it('flagship orthogonality: full + merge with unique_keys is valid', () => {
    expect(
      validatePolicy({
        read_mode: 'full',
        write_strategy: 'merge',
        unique_keys: ['id'],
      }),
    ).toEqual([]);
  });

  it('merge requires unique_keys', () => {
    const errs = validatePolicy({ read_mode: 'full', write_strategy: 'merge' });
    expect(errs.some((e) => e.includes('unique_keys is required'))).toBe(true);
  });

  it('append rejects unique_keys', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'append',
      unique_keys: ['id'],
    });
    expect(errs.some((e) => e.includes('unique_keys must not be set'))).toBe(true);
  });

  it('incremental requires watermark', () => {
    const errs = validatePolicy({ read_mode: 'incremental', write_strategy: 'append' });
    expect(errs.some((e) => e.includes('requires a watermark'))).toBe(true);
  });

  it('full mode rejects watermark', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'append',
      watermark: { column: 'updated_at', type: 'timestamp' },
    });
    expect(errs.some((e) => e.includes('must not be set when read_mode is full'))).toBe(true);
  });

  it('insert_overwrite requires partition_column', () => {
    const errs = validatePolicy({ read_mode: 'full', write_strategy: 'insert_overwrite' });
    expect(errs.some((e) => e.includes('partition_column is required'))).toBe(true);
  });

  it('lookback rejected for int64 watermark', () => {
    const errs = validatePolicy({
      read_mode: 'incremental',
      write_strategy: 'append',
      watermark: { column: 'id', type: 'int64' },
      lookback: 'PT1H',
    });
    expect(errs.some((e) => e.includes('lookback only applies'))).toBe(true);
  });

  it('snapshot requires a snapshot sub-block', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
    });
    expect(errs.some((e) => e.includes('requires a snapshot sub-block'))).toBe(true);
  });

  it('snapshot requires unique_keys', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'snapshot',
      snapshot: { change_detection: 'check', check_columns: ['email'] },
    });
    expect(errs.some((e) => e.includes('unique_keys is required'))).toBe(true);
  });

  it('snapshot check requires non-empty check_columns', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
      snapshot: { change_detection: 'check', check_columns: [] },
    });
    expect(errs.some((e) => e.includes('check_columns is required'))).toBe(true);
  });

  it('snapshot check + incremental is incoherent', () => {
    const errs = validatePolicy({
      read_mode: 'incremental',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
      watermark: { column: 'updated_at', type: 'timestamp' },
      snapshot: { change_detection: 'check', check_columns: ['email'] },
    });
    expect(errs.some((e) => e.includes('incoherent with read_mode'))).toBe(true);
  });

  it('snapshot timestamp requires updated_at_column', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
      snapshot: { change_detection: 'timestamp' },
    });
    expect(errs.some((e) => e.includes('updated_at_column is required'))).toBe(true);
  });

  it('snapshot incremental+timestamp requires watermark to match updated_at_column', () => {
    const errs = validatePolicy({
      read_mode: 'incremental',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
      watermark: { column: 'modified_at', type: 'timestamp' },
      snapshot: { change_detection: 'timestamp', updated_at_column: 'updated_at' },
    });
    expect(errs.some((e) => e.includes('to match snapshot.updated_at_column'))).toBe(true);
  });

  it('snapshot incremental+timestamp accepts matching watermark', () => {
    expect(
      validatePolicy({
        read_mode: 'incremental',
        write_strategy: 'snapshot',
        unique_keys: ['id'],
        watermark: { column: 'updated_at', type: 'timestamp' },
        snapshot: { change_detection: 'timestamp', updated_at_column: 'updated_at' },
      }),
    ).toEqual([]);
  });

  it('snapshot sub-block rejected on non-snapshot strategy', () => {
    const errs = validatePolicy({
      read_mode: 'full',
      write_strategy: 'append',
      snapshot: { change_detection: 'check', check_columns: ['email'] },
    });
    expect(errs.some((e) => e.includes('snapshot sub-block must not be set'))).toBe(true);
  });

  it('lookback invalid format rejected', () => {
    const errs = validatePolicy({
      read_mode: 'incremental',
      write_strategy: 'append',
      watermark: { column: 'updated_at', type: 'timestamp' },
      lookback: '1 hour',
    });
    expect(errs.some((e) => e.includes('not a valid ISO 8601 duration'))).toBe(true);
  });
});

describe('MaterializationEditor', () => {
  it('disabled by default; enabling emits a default policy', () => {
    const onChange = vi.fn();
    render(<MaterializationEditor policy={undefined} onChange={onChange} />);
    fireEvent.click(screen.getByLabelText(/Enable materialization policy/));
    expect(onChange).toHaveBeenCalledWith({ read_mode: 'full', write_strategy: 'append' });
  });

  it('selecting merge surfaces unique_keys input', () => {
    const onChange = vi.fn();
    const policy: MaterializationPolicy = { read_mode: 'full', write_strategy: 'merge' };
    render(<MaterializationEditor policy={policy} onChange={onChange} />);
    expect(screen.getByPlaceholderText('id, tenant_id')).toBeInTheDocument();
    // Validation error visible until keys are provided
    expect(screen.getByText(/unique_keys is required/)).toBeInTheDocument();
  });

  it('switching to incremental shows watermark fields', () => {
    const onChange = vi.fn();
    const policy: MaterializationPolicy = { read_mode: 'incremental', write_strategy: 'append', watermark: { column: 'updated_at', type: 'timestamp' } };
    render(<MaterializationEditor policy={policy} onChange={onChange} />);
    expect(screen.getByPlaceholderText('updated_at')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('PT1H')).toBeInTheDocument();
  });

  it('selecting snapshot surfaces the snapshot sub-block and adds default policy', () => {
    const onChange = vi.fn();
    const policy: MaterializationPolicy = { read_mode: 'full', write_strategy: 'append' };
    render(<MaterializationEditor policy={policy} onChange={onChange} />);
    fireEvent.change(screen.getByDisplayValue('Append'), { target: { value: 'snapshot' } });
    expect(onChange).toHaveBeenCalled();
    const next = onChange.mock.calls[0][0] as MaterializationPolicy;
    expect(next.write_strategy).toBe('snapshot');
    expect(next.snapshot).toEqual({ change_detection: 'check', check_columns: [], hard_deletes: 'ignore' });
  });

  it('snapshot sub-block renders check_columns input under check detection', () => {
    const policy: MaterializationPolicy = {
      read_mode: 'full',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
      snapshot: { change_detection: 'check', check_columns: ['email'] },
    };
    render(<MaterializationEditor policy={policy} onChange={() => {}} />);
    expect(screen.getByTestId('snapshot-subblock')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('email, plan, status')).toBeInTheDocument();
  });

  it('snapshot timestamp detection swaps to updated_at_column input', () => {
    const policy: MaterializationPolicy = {
      read_mode: 'full',
      write_strategy: 'snapshot',
      unique_keys: ['id'],
      snapshot: { change_detection: 'timestamp', updated_at_column: 'updated_at' },
    };
    render(<MaterializationEditor policy={policy} onChange={() => {}} />);
    expect(screen.getByPlaceholderText('updated_at')).toBeInTheDocument();
    expect(screen.queryByPlaceholderText('email, plan, status')).toBeNull();
  });

  it('reset button only appears for incremental sinks with pipelineId+nodeId', () => {
    const policy: MaterializationPolicy = { read_mode: 'full', write_strategy: 'append' };
    const { rerender } = render(
      <MaterializationEditor
        policy={policy}
        onChange={() => {}}
        pipelineId="pipe-1"
        nodeId="sink-1"
      />,
    );
    expect(screen.queryByTestId('reset-incremental-state-btn')).toBeNull();

    rerender(
      <MaterializationEditor
        policy={{ ...policy, read_mode: 'incremental', watermark: { column: 'updated_at', type: 'timestamp' } }}
        onChange={() => {}}
        pipelineId="pipe-1"
        nodeId="sink-1"
      />,
    );
    expect(screen.getByTestId('reset-incremental-state-btn')).toBeInTheDocument();
  });

  it('reset button calls API after confirmation', async () => {
    // jsdom doesn't implement <dialog>.showModal/close
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (HTMLDialogElement.prototype as any).showModal = function () { this.open = true; };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (HTMLDialogElement.prototype as any).close = function () { this.open = false; };
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', fetchMock);
    const policy: MaterializationPolicy = {
      read_mode: 'incremental',
      write_strategy: 'append',
      watermark: { column: 'updated_at', type: 'timestamp' },
    };
    render(
      <MaterializationEditor
        policy={policy}
        onChange={() => {}}
        pipelineId="pipe-1"
        nodeId="sink-1"
        environment="dev"
      />,
    );
    fireEvent.click(screen.getByTestId('reset-incremental-state-btn'));
    // Confirm dialog open — click the destructive confirm
    fireEvent.click(screen.getByRole('button', { name: 'Reset' }));
    // Wait a microtask for the async handler
    await Promise.resolve();
    await Promise.resolve();
    expect(fetchMock).toHaveBeenCalled();
    const url = fetchMock.mock.calls[0][0] as string;
    expect(url).toContain('/api/pipelines/pipe-1/nodes/sink-1/incremental/reset');
    expect(url).toContain('env=dev');
    vi.unstubAllGlobals();
  });
});
