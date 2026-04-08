// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { describe, it, expect } from 'vitest';
import { generateJoinSql, type JoinInput, type JoinMapping } from './joinSql';

const orders: JoinInput = {
  nodeName: 'orders',
  columns: [
    { name: 'id', data_type: 'Int64', nullable: false },
    { name: 'customer_id', data_type: 'Int64', nullable: false },
    { name: 'amount', data_type: 'Float64', nullable: true },
  ],
};

const customers: JoinInput = {
  nodeName: 'customers',
  columns: [
    { name: 'id', data_type: 'Int64', nullable: false },
    { name: 'name', data_type: 'Utf8', nullable: true },
    { name: 'email', data_type: 'Utf8', nullable: true },
  ],
};

describe('generateJoinSql', () => {
  it('generates INNER JOIN with one mapping', () => {
    const mappings: JoinMapping[] = [
      { leftCol: 'customer_id', rightCol: 'id' },
    ];
    const sql = generateJoinSql(orders, customers, 'INNER', mappings);
    expect(sql).toContain('INNER JOIN customers');
    expect(sql).toContain('orders.customer_id = customers.id');
  });

  it('generates LEFT JOIN', () => {
    const mappings: JoinMapping[] = [
      { leftCol: 'customer_id', rightCol: 'id' },
    ];
    const sql = generateJoinSql(orders, customers, 'LEFT', mappings);
    expect(sql).toContain('LEFT JOIN customers');
  });

  it('generates CROSS JOIN without ON clause', () => {
    const sql = generateJoinSql(orders, customers, 'CROSS', []);
    expect(sql).toContain('CROSS JOIN customers');
    expect(sql).not.toContain('ON');
  });

  it('generates placeholder comment when no mappings', () => {
    const sql = generateJoinSql(orders, customers, 'INNER', []);
    expect(sql).toContain('/* select join columns */');
  });

  it('auto-aliases colliding column names', () => {
    // Both tables have "id"
    const mappings: JoinMapping[] = [
      { leftCol: 'customer_id', rightCol: 'id' },
    ];
    const sql = generateJoinSql(orders, customers, 'INNER', mappings);
    expect(sql).toContain('orders.id AS orders_id');
    expect(sql).toContain('customers.id AS customers_id');
    // Non-colliding columns should NOT be aliased
    expect(sql).toContain('orders.amount');
    expect(sql).not.toContain('orders.amount AS');
    expect(sql).toContain('customers.name');
    expect(sql).not.toContain('customers.name AS');
  });

  it('handles multiple join conditions with AND', () => {
    const mappings: JoinMapping[] = [
      { leftCol: 'id', rightCol: 'id' },
      { leftCol: 'customer_id', rightCol: 'name' },
    ];
    const sql = generateJoinSql(orders, customers, 'FULL', mappings);
    expect(sql).toContain('FULL JOIN customers');
    expect(sql).toContain('AND');
    expect(sql).toContain('orders.id = customers.id');
    expect(sql).toContain('orders.customer_id = customers.name');
  });

  it('handles tables with no colliding columns', () => {
    const left: JoinInput = {
      nodeName: 'a',
      columns: [{ name: 'x', data_type: 'Int64', nullable: false }],
    };
    const right: JoinInput = {
      nodeName: 'b',
      columns: [{ name: 'y', data_type: 'Int64', nullable: false }],
    };
    const sql = generateJoinSql(left, right, 'INNER', [
      { leftCol: 'x', rightCol: 'y' },
    ]);
    // No aliasing needed
    expect(sql).toContain('a.x');
    expect(sql).not.toContain('AS');
    expect(sql).toContain('b.y');
  });
});
