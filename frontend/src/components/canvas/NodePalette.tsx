// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useState, useCallback, useEffect, useMemo, type ReactNode } from 'react';
import type { NodeRole } from '../../types/pipeline';
import { IconChevronLeft, IconChevronRight, IconSink } from '../icons';
import { roleIcon, paletteIcon } from '../iconMaps';
import { usePluginStore } from '../../stores/pluginStore';
import './NodePalette.css';

// ---------------------------------------------------------------------------
// Palette item definitions
// ---------------------------------------------------------------------------

export interface PaletteItem {
  /** Unique key for this palette entry. */
  id: string;
  /** Display label. */
  label: string;
  /** Icon shown alongside the label. */
  icon: ReactNode;
  /** Node role created when dropped. */
  role: NodeRole;
  /** Connector type for source/sink, or mode for transform. */
  subtype: string;
  /** Which field to set: 'connector' for source/sink, 'mode' for transform. */
  subtypeField: 'connector' | 'mode';
  /** Marks this entry as plugin-provided so it can be tagged in the UI. */
  plugin?: boolean;
}

const PALETTE_ITEMS: PaletteItem[] = [
  // Sources
  { id: 'source-csv', label: 'CSV', icon: paletteIcon.csv, role: 'source', subtype: 'csv', subtypeField: 'connector' },
  { id: 'source-parquet', label: 'Parquet', icon: paletteIcon.parquet, role: 'source', subtype: 'parquet', subtypeField: 'connector' },
  { id: 'source-postgresql', label: 'PostgreSQL', icon: paletteIcon.postgresql, role: 'source', subtype: 'postgresql', subtypeField: 'connector' },
  { id: 'source-rest', label: 'REST API', icon: paletteIcon.rest, role: 'source', subtype: 'rest', subtypeField: 'connector' },
  // Transforms
  { id: 'transform-sql', label: 'SQL', icon: paletteIcon.sql, role: 'transform', subtype: 'sql', subtypeField: 'mode' },
  { id: 'transform-python', label: 'Python', icon: paletteIcon.python, role: 'transform', subtype: 'python', subtypeField: 'mode' },
  // Sinks
  { id: 'sink-csv', label: 'CSV', icon: paletteIcon.csv, role: 'sink', subtype: 'csv', subtypeField: 'connector' },
  { id: 'sink-parquet', label: 'Parquet', icon: paletteIcon.parquet, role: 'sink', subtype: 'parquet', subtypeField: 'connector' },
  { id: 'sink-postgresql', label: 'PostgreSQL', icon: paletteIcon.postgresql, role: 'sink', subtype: 'postgresql', subtypeField: 'connector' },
  { id: 'sink-stdout', label: 'stdout', icon: paletteIcon.stdout, role: 'sink', subtype: 'stdout', subtypeField: 'connector' },
  // Tests
  { id: 'test-assertion', label: 'Assertion', icon: paletteIcon.test, role: 'test', subtype: 'test', subtypeField: 'connector' },
];

const SECTIONS: { role: NodeRole; label: string; icon: ReactNode }[] = [
  { role: 'source', label: 'Sources', icon: roleIcon.source },
  { role: 'transform', label: 'Transforms', icon: roleIcon.transform },
  { role: 'sink', label: 'Sinks', icon: roleIcon.sink },
  { role: 'test', label: 'Tests', icon: roleIcon.test },
];

// ---------------------------------------------------------------------------
// Drag data key — used by PipelineCanvas drop handler
// ---------------------------------------------------------------------------

/** The MIME type key used in dataTransfer for palette drag-and-drop. */
export const PALETTE_DRAG_TYPE = 'application/armillary-palette-item';

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface NodePaletteProps {
  collapsed: boolean;
  onToggle: () => void;
}

export function NodePalette({ collapsed, onToggle }: NodePaletteProps) {
  const [filter, setFilter] = useState('');

  // Hydrate plugin sinks into the palette.
  const pluginsLoaded = usePluginStore((s) => s.loaded);
  const fetchPlugins = usePluginStore((s) => s.fetchPlugins);
  const sinkOptions = usePluginStore((s) => s.sinkOptions());
  useEffect(() => {
    if (!pluginsLoaded) void fetchPlugins();
  }, [pluginsLoaded, fetchPlugins]);

  const allItems: PaletteItem[] = useMemo(() => {
    const builtinSinkTypes = new Set(
      PALETTE_ITEMS.filter((i) => i.role === 'sink').map((i) => i.subtype),
    );
    const pluginItems: PaletteItem[] = sinkOptions
      .filter((o) => !builtinSinkTypes.has(o.sink.type))
      .map((o) => ({
        id: `sink-plugin-${o.pluginName}-${o.sink.type}`,
        label: o.sink.display_name,
        icon: <IconSink />,
        role: 'sink',
        subtype: o.sink.type,
        subtypeField: 'connector',
        plugin: true,
      }));
    return [...PALETTE_ITEMS, ...pluginItems];
  }, [sinkOptions]);

  const handleDragStart = useCallback(
    (e: React.DragEvent, item: PaletteItem) => {
      e.dataTransfer.setData(PALETTE_DRAG_TYPE, JSON.stringify(item));
      e.dataTransfer.effectAllowed = 'move';
    },
    [],
  );

  const normalizedFilter = filter.toLowerCase().trim();
  const filteredItems = normalizedFilter
    ? allItems.filter(
        (item) =>
          item.label.toLowerCase().includes(normalizedFilter) ||
          item.role.toLowerCase().includes(normalizedFilter),
      )
    : allItems;

  return (
    <div className={`node-palette ${collapsed ? 'node-palette--collapsed' : ''}`}>
      <button
        className="node-palette__toggle"
        onClick={onToggle}
        title={collapsed ? 'Open node palette' : 'Close node palette'}
        aria-label={collapsed ? 'Open node palette' : 'Close node palette'}
      >
        {collapsed ? <IconChevronRight size={12} /> : <IconChevronLeft size={12} />}
      </button>

      {!collapsed && (
        <div className="node-palette__content">
          <div className="node-palette__header">Nodes</div>

          <input
            className="node-palette__search"
            type="text"
            placeholder="Filter nodes..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />

          {SECTIONS.map((section) => {
            const items = filteredItems.filter((i) => i.role === section.role);
            if (items.length === 0) return null;
            return (
              <div key={section.role} className="node-palette__section">
                <div className="node-palette__section-header">
                  <span className="node-palette__section-icon">
                    {section.icon}
                  </span>
                  {section.label}
                </div>
                {items.map((item) => (
                  <div
                    key={item.id}
                    className={`node-palette__item node-palette__item--${item.role}${item.plugin ? ' node-palette__item--plugin' : ''}`}
                    draggable
                    onDragStart={(e) => handleDragStart(e, item)}
                    title={
                      item.plugin
                        ? `Plugin sink — drag to add ${item.label}`
                        : `Drag to add ${item.label} ${section.label.toLowerCase().slice(0, -1)}`
                    }
                  >
                    <span className="node-palette__item-icon">{item.icon}</span>
                    <span className="node-palette__item-label">{item.label}</span>
                    {item.plugin && (
                      <span className="node-palette__item-tag">plugin</span>
                    )}
                  </div>
                ))}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
