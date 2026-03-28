// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import type { ApiColumnInfo } from '../../api/pipelines';
import './join-configurator.css';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';

export interface JoinMapping {
  leftCol: string;
  rightCol: string;
}

export interface JoinInput {
  nodeName: string;
  columns: ApiColumnInfo[];
}

export interface JoinConfiguratorProps {
  left: JoinInput;
  right: JoinInput;
  onSqlGenerated: (sql: string) => void;
  onSwitchToCode: () => void;
}

// ---------------------------------------------------------------------------
// SQL generation (exported for testing)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Column list sub-component
// ---------------------------------------------------------------------------

function ColumnPanel({
  input,
  side,
  mappedCols,
  selectedCol,
  onColClick,
  rowRefs,
}: {
  input: JoinInput;
  side: 'left' | 'right';
  mappedCols: Set<string>;
  selectedCol: string | null;
  onColClick: (col: string) => void;
  rowRefs: React.MutableRefObject<Map<string, HTMLDivElement | null>>;
}) {
  return (
    <div className="join-cfg__panel">
      <div className="join-cfg__panel-header">
        {input.nodeName}
        <span className="join-cfg__panel-badge">
          {input.columns.length} cols
        </span>
      </div>
      {input.columns.map((col) => {
        const isMapped = mappedCols.has(col.name);
        const isSelected = selectedCol === col.name;
        const cls = [
          'join-cfg__col-row',
          isMapped ? 'join-cfg__col-row--mapped' : '',
          isSelected ? 'join-cfg__col-row--selected' : '',
        ]
          .filter(Boolean)
          .join(' ');

        return (
          <div
            key={col.name}
            className={cls}
            onClick={() => onColClick(col.name)}
            ref={(el) => rowRefs.current.set(`${side}:${col.name}`, el)}
          >
            <span className="join-cfg__col-dot" />
            <span className="join-cfg__col-label">{col.name}</span>
            <span className="join-cfg__col-dtype">{col.data_type}</span>
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main JoinConfigurator
// ---------------------------------------------------------------------------

export function JoinConfigurator({
  left,
  right,
  onSqlGenerated,
  onSwitchToCode,
}: JoinConfiguratorProps) {
  const [joinType, setJoinType] = useState<JoinType>('INNER');
  const [mappings, setMappings] = useState<JoinMapping[]>([]);
  const [pendingLeft, setPendingLeft] = useState<string | null>(null);

  const rowRefs = useRef<Map<string, HTMLDivElement | null>>(new Map());
  const linesRef = useRef<HTMLDivElement>(null);
  const [lineCoords, setLineCoords] = useState<
    { x1: number; y1: number; x2: number; y2: number; key: string }[]
  >([]);

  // Track which columns are already mapped
  const leftMapped = new Set(mappings.map((m) => m.leftCol));
  const rightMapped = new Set(mappings.map((m) => m.rightCol));

  // Generate SQL whenever mappings or join type change
  useEffect(() => {
    const sql = generateJoinSql(left, right, joinType, mappings);
    onSqlGenerated(sql);
  }, [left, right, joinType, mappings, onSqlGenerated]);

  // Compute SVG line positions
  const updateLines = useCallback(() => {
    const container = linesRef.current;
    if (!container) return;
    const containerRect = container.getBoundingClientRect();

    const coords = mappings.map((m) => {
      const leftEl = rowRefs.current.get(`left:${m.leftCol}`);
      const rightEl = rowRefs.current.get(`right:${m.rightCol}`);
      if (!leftEl || !rightEl) return null;

      const lr = leftEl.getBoundingClientRect();
      const rr = rightEl.getBoundingClientRect();

      return {
        x1: 0,
        y1: lr.top + lr.height / 2 - containerRect.top,
        x2: containerRect.width,
        y2: rr.top + rr.height / 2 - containerRect.top,
        key: `${m.leftCol}:${m.rightCol}`,
      };
    }).filter(Boolean) as typeof lineCoords;

    setLineCoords(coords);
  }, [mappings]);

  useEffect(() => {
    updateLines();
    // Also update on window resize
    window.addEventListener('resize', updateLines);
    return () => window.removeEventListener('resize', updateLines);
  }, [updateLines]);

  // Re-compute lines after render settles (refs get assigned)
  useEffect(() => {
    const id = requestAnimationFrame(updateLines);
    return () => cancelAnimationFrame(id);
  }, [mappings, updateLines]);

  // Click handlers for creating mappings
  const handleLeftColClick = useCallback(
    (col: string) => {
      // If already mapped, remove the mapping
      if (leftMapped.has(col)) {
        setMappings((prev) => prev.filter((m) => m.leftCol !== col));
        setPendingLeft(null);
        return;
      }
      setPendingLeft(col);
    },
    [leftMapped],
  );

  const handleRightColClick = useCallback(
    (col: string) => {
      // If already mapped, remove the mapping
      if (rightMapped.has(col)) {
        setMappings((prev) => prev.filter((m) => m.rightCol !== col));
        setPendingLeft(null);
        return;
      }
      // If we have a pending left column, create the mapping
      if (pendingLeft) {
        setMappings((prev) => [
          ...prev,
          { leftCol: pendingLeft, rightCol: col },
        ]);
        setPendingLeft(null);
        return;
      }
    },
    [pendingLeft, rightMapped],
  );

  const removeMapping = useCallback((key: string) => {
    const [leftCol, rightCol] = key.split(':');
    setMappings((prev) =>
      prev.filter((m) => !(m.leftCol === leftCol && m.rightCol === rightCol)),
    );
  }, []);

  return (
    <div className="join-cfg">
      {/* Header */}
      <div className="join-cfg__header">
        <label htmlFor="join-type-select">Join Type</label>
        <select
          id="join-type-select"
          className="join-cfg__type-select"
          value={joinType}
          onChange={(e) => setJoinType(e.target.value as JoinType)}
        >
          <option value="INNER">INNER JOIN</option>
          <option value="LEFT">LEFT JOIN</option>
          <option value="RIGHT">RIGHT JOIN</option>
          <option value="FULL">FULL JOIN</option>
          <option value="CROSS">CROSS JOIN</option>
        </select>
        <div className="join-cfg__spacer" />
        <button
          className="join-cfg__switch-btn"
          onClick={onSwitchToCode}
          title="Dismiss visual configurator and edit SQL directly"
        >
          Switch to code &rarr;
        </button>
      </div>

      {/* Body: left panel, SVG lines, right panel */}
      <div className="join-cfg__body">
        <ColumnPanel
          input={left}
          side="left"
          mappedCols={leftMapped}
          selectedCol={pendingLeft}
          onColClick={handleLeftColClick}
          rowRefs={rowRefs}
        />
        <div className="join-cfg__lines" ref={linesRef}>
          <svg>
            {lineCoords.map((c) => (
              <line
                key={c.key}
                className="join-cfg__line"
                x1={c.x1}
                y1={c.y1}
                x2={c.x2}
                y2={c.y2}
                onClick={() => removeMapping(c.key)}
                style={{ pointerEvents: 'stroke' }}
              />
            ))}
          </svg>
        </div>
        <ColumnPanel
          input={right}
          side="right"
          mappedCols={rightMapped}
          selectedCol={null}
          onColClick={handleRightColClick}
          rowRefs={rowRefs}
        />
      </div>

      {/* Mappings summary */}
      <div className="join-cfg__mappings">
        {mappings.length === 0 && joinType !== 'CROSS' && (
          <span className="join-cfg__empty">
            Click a column on the left, then one on the right to create a join condition
          </span>
        )}
        {joinType === 'CROSS' && mappings.length === 0 && (
          <span className="join-cfg__empty">
            Cross join — no join conditions needed
          </span>
        )}
        {mappings.map((m) => (
          <span
            key={`${m.leftCol}:${m.rightCol}`}
            className="join-cfg__mapping-chip"
            onClick={() => removeMapping(`${m.leftCol}:${m.rightCol}`)}
            title="Click to remove"
          >
            {left.nodeName}.{m.leftCol} = {right.nodeName}.{m.rightCol}
          </span>
        ))}
      </div>
    </div>
  );
}
