// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { ApiColumnInfo } from '../../api/pipelines';

export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';

export interface JoinMapping {
  leftCol: string;
  rightCol: string;
}

export interface JoinInput {
  nodeName: string;
  columns: ApiColumnInfo[];
}

export function generateJoinSql(
  left: JoinInput,
  right: JoinInput,
  joinType: JoinType,
  mappings: JoinMapping[],
): string {
  const leftName = left.nodeName;
  const rightName = right.nodeName;

  // Determine column name collisions for aliasing
  const leftColNames = new Set(left.columns.map((c) => c.name));
  const rightColNames = new Set(right.columns.map((c) => c.name));
  const collisions = new Set(
    [...leftColNames].filter((n) => rightColNames.has(n)),
  );

  // Build SELECT columns — prefix colliding columns with table name
  const selectCols: string[] = [];
  for (const col of left.columns) {
    if (collisions.has(col.name)) {
      selectCols.push(`${leftName}.${col.name} AS ${leftName}_${col.name}`);
    } else {
      selectCols.push(`${leftName}.${col.name}`);
    }
  }
  for (const col of right.columns) {
    if (collisions.has(col.name)) {
      selectCols.push(`${rightName}.${col.name} AS ${rightName}_${col.name}`);
    } else {
      selectCols.push(`${rightName}.${col.name}`);
    }
  }

  const selectClause = selectCols.length > 0
    ? selectCols.join(',\n  ')
    : '*';

  // CROSS JOIN has no ON clause
  if (joinType === 'CROSS') {
    return `SELECT\n  ${selectClause}\nFROM ${leftName}\nCROSS JOIN ${rightName}`;
  }

  // Build ON clause
  if (mappings.length === 0) {
    return `SELECT\n  ${selectClause}\nFROM ${leftName}\n${joinType} JOIN ${rightName}\n  ON /* select join columns */`;
  }

  const onConditions = mappings
    .map((m) => `${leftName}.${m.leftCol} = ${rightName}.${m.rightCol}`)
    .join('\n  AND ');

  return `SELECT\n  ${selectClause}\nFROM ${leftName}\n${joinType} JOIN ${rightName}\n  ON ${onConditions}`;
}
