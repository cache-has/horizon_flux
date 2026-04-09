// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Renders the `schema_diff` field from a `MaterializationReceipt`.
//!
//! This is the receipt-side counterpart to the `SchemaList` component's inline
//! diff rendering. Used by the side panel to show what changed for a sink on
//! its last run, and by failed-run displays to surface why an `OnSchemaChange::Fail`
//! policy aborted.

import type { SchemaDiff } from '../../api/pipelines';
import './SidePanel.css';

interface SchemaDiffViewerProps {
  diff: SchemaDiff | null | undefined;
}

export function SchemaDiffViewer({ diff }: SchemaDiffViewerProps) {
  if (!diff) return null;
  const added = diff.added ?? [];
  const removed = diff.removed ?? [];
  const typeChanged = diff.type_changed ?? [];
  const total = added.length + removed.length + typeChanged.length;
  if (total === 0) {
    return <span className="side-panel__empty">No schema changes</span>;
  }

  return (
    <ul className="side-panel__schema-list" data-testid="schema-diff-viewer">
      {added.map((f) => (
        <li
          key={`added-${f.name}`}
          className="side-panel__schema-item side-panel__schema-item--diff-added"
        >
          <span className="side-panel__schema-name">
            {f.name}
            <span className="side-panel__schema-diff-label side-panel__schema-diff-label--added">
              {' '}(added)
            </span>
          </span>
          <span className="side-panel__schema-meta">
            <span className="side-panel__schema-type">{f.data_type}</span>
          </span>
        </li>
      ))}
      {removed.map((f) => (
        <li
          key={`removed-${f.name}`}
          className="side-panel__schema-item side-panel__schema-item--diff-removed"
        >
          <span className="side-panel__schema-name">
            <s>{f.name}</s>
            <span className="side-panel__schema-diff-label side-panel__schema-diff-label--removed">
              {' '}(removed)
            </span>
          </span>
          <span className="side-panel__schema-meta">
            <span className="side-panel__schema-type">{f.data_type}</span>
          </span>
        </li>
      ))}
      {typeChanged.map((f) => (
        <li
          key={`type-${f.name}`}
          className="side-panel__schema-item side-panel__schema-item--diff-type_changed"
        >
          <span className="side-panel__schema-name">
            {f.name}
            <span className="side-panel__schema-diff-label side-panel__schema-diff-label--type_changed">
              {' '}({f.before} → {f.after})
            </span>
          </span>
        </li>
      ))}
    </ul>
  );
}
