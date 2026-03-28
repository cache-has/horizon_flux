// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect, useRef, useCallback } from 'react';
import type { NodeRole } from '../../types/pipeline';
import './CanvasContextMenu.css';

// ---------------------------------------------------------------------------
// Menu item types
// ---------------------------------------------------------------------------

export interface MenuDivider {
  type: 'divider';
}

export interface MenuItem {
  type: 'item';
  label: string;
  icon?: string;
  disabled?: boolean;
  destructive?: boolean;
  onClick: () => void;
}

export interface MenuSubmenu {
  type: 'submenu';
  label: string;
  icon?: string;
  items: MenuItem[];
}

export type MenuEntry = MenuItem | MenuDivider | MenuSubmenu;

// ---------------------------------------------------------------------------
// Context menu state (what was right-clicked)
// ---------------------------------------------------------------------------

export interface ContextMenuStateNode {
  kind: 'node';
  nodeId: string;
  nodeRole: NodeRole;
  nodeLabel: string;
  isPinned: boolean;
  x: number;
  y: number;
}

export interface ContextMenuStateEdge {
  kind: 'edge';
  edgeId: string;
  x: number;
  y: number;
}

export interface ContextMenuStateCanvas {
  kind: 'canvas';
  /** Canvas coordinates of the right-click. */
  canvasX: number;
  canvasY: number;
  x: number;
  y: number;
}

export interface ContextMenuStateMulti {
  kind: 'multi';
  nodeIds: string[];
  x: number;
  y: number;
}

export type ContextMenuState =
  | ContextMenuStateNode
  | ContextMenuStateEdge
  | ContextMenuStateCanvas
  | ContextMenuStateMulti
  | null;

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface CanvasContextMenuProps {
  state: ContextMenuState;
  onClose: () => void;
  onAction: (action: string, payload?: Record<string, unknown>) => void;
}

function buildMenuEntries(
  state: NonNullable<ContextMenuState>,
  onAction: CanvasContextMenuProps['onAction'],
): MenuEntry[] {
  switch (state.kind) {
    case 'node':
      return [
        {
          type: 'item',
          label: 'Edit',
          onClick: () => onAction('edit-node', { nodeId: state.nodeId }),
        },
        {
          type: 'item',
          label: 'Rename',
          onClick: () => onAction('rename-node', { nodeId: state.nodeId }),
        },
        {
          type: 'item',
          label: 'Duplicate',
          onClick: () => onAction('duplicate-node', { nodeId: state.nodeId }),
        },
        { type: 'divider' },
        {
          type: 'item',
          label: state.isPinned ? 'Unpin Position' : 'Pin Position',
          onClick: () =>
            onAction(state.isPinned ? 'unpin-node' : 'pin-node', {
              nodeId: state.nodeId,
            }),
        },
        {
          type: 'item',
          label: 'Create Dev Override',
          onClick: () =>
            onAction('create-dev-override', { nodeId: state.nodeId }),
        },
        {
          type: 'item',
          label: 'View Preview',
          onClick: () => onAction('view-preview', { nodeId: state.nodeId }),
        },
        { type: 'divider' },
        {
          type: 'item',
          label: 'Delete',
          destructive: true,
          onClick: () => onAction('delete-node', { nodeId: state.nodeId }),
        },
      ];

    case 'edge':
      return [
        {
          type: 'item',
          label: 'View Metadata',
          onClick: () => onAction('view-edge-metadata', { edgeId: state.edgeId }),
        },
        { type: 'divider' },
        {
          type: 'item',
          label: 'Delete Connection',
          destructive: true,
          onClick: () => onAction('delete-edge', { edgeId: state.edgeId }),
        },
      ];

    case 'canvas':
      return [
        {
          type: 'submenu',
          label: 'Add Source',
          items: [
            {
              type: 'item',
              label: 'CSV',
              onClick: () =>
                onAction('add-node', {
                  role: 'source',
                  connector: 'csv',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
            {
              type: 'item',
              label: 'PostgreSQL',
              onClick: () =>
                onAction('add-node', {
                  role: 'source',
                  connector: 'postgresql',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
            {
              type: 'item',
              label: 'REST API',
              onClick: () =>
                onAction('add-node', {
                  role: 'source',
                  connector: 'rest',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
          ],
        },
        {
          type: 'submenu',
          label: 'Add Transform',
          items: [
            {
              type: 'item',
              label: 'SQL',
              onClick: () =>
                onAction('add-node', {
                  role: 'transform',
                  mode: 'sql',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
            {
              type: 'item',
              label: 'Python',
              onClick: () =>
                onAction('add-node', {
                  role: 'transform',
                  mode: 'python',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
          ],
        },
        {
          type: 'submenu',
          label: 'Add Sink',
          items: [
            {
              type: 'item',
              label: 'CSV',
              onClick: () =>
                onAction('add-node', {
                  role: 'sink',
                  connector: 'csv',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
            {
              type: 'item',
              label: 'PostgreSQL',
              onClick: () =>
                onAction('add-node', {
                  role: 'sink',
                  connector: 'postgresql',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
            {
              type: 'item',
              label: 'stdout',
              onClick: () =>
                onAction('add-node', {
                  role: 'sink',
                  connector: 'stdout',
                  x: state.canvasX,
                  y: state.canvasY,
                }),
            },
          ],
        },
      ];

    case 'multi':
      return [
        {
          type: 'item',
          label: 'Create Transform from Selected',
          onClick: () =>
            onAction('create-transform-from-selected', {
              nodeIds: state.nodeIds,
            }),
        },
        { type: 'divider' },
        {
          type: 'item',
          label: `Delete ${state.nodeIds.length} Nodes`,
          destructive: true,
          onClick: () =>
            onAction('delete-nodes', { nodeIds: state.nodeIds }),
        },
      ];
  }
}

export function CanvasContextMenu({
  state,
  onClose,
  onAction,
}: CanvasContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    if (!state) return;
    const handle = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    // Delay listener to avoid the right-click event itself closing the menu
    const id = requestAnimationFrame(() => {
      document.addEventListener('mousedown', handle);
    });
    return () => {
      cancelAnimationFrame(id);
      document.removeEventListener('mousedown', handle);
    };
  }, [state, onClose]);

  // Close on Escape
  useEffect(() => {
    if (!state) return;
    const handle = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handle);
    return () => document.removeEventListener('keydown', handle);
  }, [state, onClose]);

  if (!state) return null;

  const entries = buildMenuEntries(state, (action, payload) => {
    onAction(action, payload);
    onClose();
  });

  return (
    <div
      ref={menuRef}
      className="canvas-context-menu"
      style={{ left: state.x, top: state.y }}
    >
      {entries.map((entry, i) => (
        <ContextMenuEntry key={i} entry={entry} />
      ))}
    </div>
  );
}

function ContextMenuEntry({ entry }: { entry: MenuEntry }) {
  if (entry.type === 'divider') {
    return <div className="canvas-context-menu__divider" />;
  }

  if (entry.type === 'submenu') {
    return <SubmenuItem entry={entry} />;
  }

  return (
    <button
      className={`canvas-context-menu__item ${entry.destructive ? 'canvas-context-menu__item--destructive' : ''} ${entry.disabled ? 'canvas-context-menu__item--disabled' : ''}`}
      onClick={entry.onClick}
      disabled={entry.disabled}
    >
      {entry.icon && (
        <span className="canvas-context-menu__icon">{entry.icon}</span>
      )}
      {entry.label}
    </button>
  );
}

function SubmenuItem({ entry }: { entry: MenuSubmenu }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const handleMouseEnter = useCallback(() => {
    containerRef.current?.classList.add('canvas-context-menu__submenu--open');
  }, []);
  const handleMouseLeave = useCallback(() => {
    containerRef.current?.classList.remove('canvas-context-menu__submenu--open');
  }, []);

  return (
    <div
      ref={containerRef}
      className="canvas-context-menu__submenu"
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <button className="canvas-context-menu__item">
        {entry.icon && (
          <span className="canvas-context-menu__icon">{entry.icon}</span>
        )}
        {entry.label}
        <span className="canvas-context-menu__arrow">&#9656;</span>
      </button>
      <div className="canvas-context-menu__submenu-items">
        {entry.items.map((item, i) => (
          <button
            key={i}
            className="canvas-context-menu__item"
            onClick={item.onClick}
            disabled={item.disabled}
          >
            {item.label}
          </button>
        ))}
      </div>
    </div>
  );
}
