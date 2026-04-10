// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import type { ConfidenceLevel } from '../../api/columnLineage';

const LABELS: Record<ConfidenceLevel, string> = {
  exact: 'Exact',
  lazyframe: 'LazyFrame',
  annotation: 'Annotated',
  opaque: 'Opaque',
};

const TOOLTIPS: Record<ConfidenceLevel, string> = {
  exact: 'Derived from DataFusion logical plan — ground truth',
  lazyframe: 'Derived from Polars LazyFrame plan',
  annotation: 'User-provided lineage annotation',
  opaque: 'Conservative fallback — all inputs connected to all outputs',
};

export function ConfidenceBadge({ level }: { level: ConfidenceLevel }) {
  return (
    <span
      className={`confidence-badge confidence-badge--${level}`}
      title={TOOLTIPS[level]}
    >
      {LABELS[level]}
    </span>
  );
}
