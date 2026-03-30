// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Simple monochrome SVG icons used throughout the UI.
 * All icons render at 1em by default and inherit currentColor.
 */

import type { CSSProperties } from 'react';

interface IconProps {
  size?: number | string;
  className?: string;
  style?: CSSProperties;
}

const defaults = (props: IconProps) => ({
  width: props.size ?? '1em',
  height: props.size ?? '1em',
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 2,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  className: props.className,
  style: props.style,
});

// -- Role icons (source / transform / sink) --------------------------------

/** Download arrow — represents data coming in (source). */
export function IconSource(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <path d="M12 3v12m0 0l-4-4m4 4l4-4" />
      <path d="M4 17v2a2 2 0 002 2h12a2 2 0 002-2v-2" />
    </svg>
  );
}

/** Sliders — represents data transformation. */
export function IconTransform(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <line x1="4" y1="21" x2="4" y2="14" />
      <line x1="4" y1="10" x2="4" y2="3" />
      <line x1="12" y1="21" x2="12" y2="12" />
      <line x1="12" y1="8" x2="12" y2="3" />
      <line x1="20" y1="21" x2="20" y2="16" />
      <line x1="20" y1="12" x2="20" y2="3" />
      <line x1="1" y1="14" x2="7" y2="14" />
      <line x1="9" y1="8" x2="15" y2="8" />
      <line x1="17" y1="16" x2="23" y2="16" />
    </svg>
  );
}

/** Upload arrow — represents data going out (sink). */
export function IconSink(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <path d="M12 15V3m0 0l-4 4m4-4l4 4" />
      <path d="M4 17v2a2 2 0 002 2h12a2 2 0 002-2v-2" />
    </svg>
  );
}

// -- Connector / subtype icons ---------------------------------------------

/** File/document icon — CSV files. */
export function IconFile(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
      <polyline points="14 2 14 8 20 8" />
    </svg>
  );
}

/** Database cylinder — PostgreSQL. */
export function IconDatabase(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4.03 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
    </svg>
  );
}

/** Globe — REST API. */
export function IconGlobe(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <circle cx="12" cy="12" r="10" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z" />
    </svg>
  );
}

/** Code brackets — SQL. */
export function IconCode(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <polyline points="16 18 22 12 16 6" />
      <polyline points="8 6 2 12 8 18" />
    </svg>
  );
}

/** Terminal/command prompt — Python / stdout. */
export function IconTerminal(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <polyline points="4 17 10 11 4 5" />
      <line x1="12" y1="19" x2="20" y2="19" />
    </svg>
  );
}

// -- Chevrons (for toggle / dropdown) --------------------------------------

export function IconChevronLeft(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <polyline points="15 18 9 12 15 6" />
    </svg>
  );
}

export function IconChevronRight(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <polyline points="9 6 15 12 9 18" />
    </svg>
  );
}

export function IconChevronDown(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

export function IconChevronUp(props: IconProps = {}) {
  return (
    <svg {...defaults(props)}>
      <polyline points="18 15 12 9 6 15" />
    </svg>
  );
}

// -- Lookup maps for dynamic use -------------------------------------------

import type { ReactNode } from 'react';

export const roleIcon: Record<string, ReactNode> = {
  source: <IconSource />,
  transform: <IconTransform />,
  sink: <IconSink />,
};

export const paletteIcon: Record<string, ReactNode> = {
  csv: <IconFile />,
  postgresql: <IconDatabase />,
  rest: <IconGlobe />,
  sql: <IconCode />,
  python: <IconTerminal />,
  stdout: <IconTerminal />,
};
