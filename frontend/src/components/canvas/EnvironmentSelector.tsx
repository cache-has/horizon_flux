// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { useEnvironmentStore } from '../../stores/environmentStore';
import { IconChevronDown, IconChevronUp } from '../icons';
import './EnvironmentSelector.css';

/** Color mapping for well-known environments. */
const ENV_COLORS: Record<string, string> = {
  prod: '#ef4444',
  production: '#ef4444',
  staging: '#f59e0b',
  dev: '#3b82f6',
  development: '#3b82f6',
};

function envColor(name: string): string {
  return ENV_COLORS[name.toLowerCase()] ?? '#8b5cf6';
}

export function EnvironmentSelector() {
  const environments = useEnvironmentStore((s) => s.environments);
  const activeEnvironment = useEnvironmentStore((s) => s.activeEnvironment);
  const loading = useEnvironmentStore((s) => s.loading);
  const fetchEnvironments = useEnvironmentStore((s) => s.fetchEnvironments);
  const setActiveEnvironment = useEnvironmentStore((s) => s.setActiveEnvironment);
  const setManagementPanelOpen = useEnvironmentStore((s) => s.setManagementPanelOpen);

  const [open, setOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetchEnvironments();
  }, [fetchEnvironments]);

  // Close dropdown on outside click
  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  const handleSelect = useCallback(
    (name: string) => {
      setActiveEnvironment(name);
      setOpen(false);
    },
    [setActiveEnvironment],
  );

  const color = envColor(activeEnvironment);

  return (
    <div className="env-selector" ref={dropdownRef}>
      <button
        className="env-selector__trigger"
        onClick={() => setOpen((o) => !o)}
        disabled={loading}
        aria-expanded={open}
        aria-haspopup="listbox"
      >
        <span
          className="env-selector__dot"
          style={{ background: color }}
        />
        <span className="env-selector__name">{activeEnvironment}</span>
        <span className="env-selector__chevron">{open ? <IconChevronUp size={12} /> : <IconChevronDown size={12} />}</span>
      </button>

      {open && (
        <div className="env-selector__dropdown" role="listbox">
          {environments.map((env) => {
            const isActive = env.name === activeEnvironment;
            return (
              <button
                key={env.name}
                className={`env-selector__option${isActive ? ' env-selector__option--active' : ''}`}
                role="option"
                aria-selected={isActive}
                onClick={() => handleSelect(env.name)}
              >
                <span
                  className="env-selector__dot"
                  style={{ background: envColor(env.name) }}
                />
                <span className="env-selector__option-name">{env.name}</span>
                {env.fallback && (
                  <span className="env-selector__fallback">
                    &rarr; {env.fallback}
                  </span>
                )}
              </button>
            );
          })}
          {environments.length === 0 && !loading && (
            <div className="env-selector__empty">No environments</div>
          )}
          <div className="env-selector__divider" />
          <button
            className="env-selector__manage"
            onClick={() => {
              setOpen(false);
              setManagementPanelOpen(true);
            }}
          >
            Manage Environments
          </button>
        </div>
      )}
    </div>
  );
}
