// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useState } from 'react';

/**
 * Collapsible key-value editor for cloud storage options (credentials,
 * endpoint overrides, etc.). Used by both FileSourceForm and FileSinkForm.
 */
export function StorageOptionsEditor({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  const [expanded, setExpanded] = useState(false);

  const storageOptions = (config.storage_options ?? {}) as Record<string, string>;
  const entries = Object.entries(storageOptions);

  const updateOption = (key: string, value: string) => {
    onChange({
      ...config,
      storage_options: { ...storageOptions, [key]: value },
    });
  };

  const removeOption = (key: string) => {
    const next = { ...storageOptions };
    delete next[key];
    // Remove the field entirely when empty so it doesn't pollute saved config.
    if (Object.keys(next).length === 0) {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { storage_options: _unused, ...rest } = config;
      onChange(rest);
    } else {
      onChange({ ...config, storage_options: next });
    }
  };

  const addOption = () => {
    onChange({
      ...config,
      storage_options: { ...storageOptions, '': '' },
    });
    if (!expanded) setExpanded(true);
  };

  return (
    <div className="connector-editor__section" style={{ marginTop: 4 }}>
      <button
        type="button"
        className="connector-editor__collapse-btn"
        onClick={() => setExpanded(!expanded)}
      >
        <span style={{ display: 'inline-block', transform: expanded ? 'rotate(90deg)' : 'none', transition: 'transform 0.15s' }}>
          &#9654;
        </span>{' '}
        Storage Options{entries.length > 0 ? ` (${entries.length})` : ''}
      </button>
      {expanded && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 6 }}>
          {entries.map(([key, value], i) => (
            <div key={i} style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
              <input
                className="connector-editor__input"
                type="text"
                value={key}
                onChange={(e) => {
                  const newKey = e.target.value;
                  const next = { ...storageOptions };
                  delete next[key];
                  next[newKey] = value;
                  onChange({ ...config, storage_options: next });
                }}
                placeholder="key"
                style={{ flex: 1 }}
              />
              <input
                className="connector-editor__input"
                type="text"
                value={value}
                onChange={(e) => updateOption(key, e.target.value)}
                placeholder="value"
                style={{ flex: 1 }}
              />
              <button
                type="button"
                className="connector-editor__remove-btn"
                onClick={() => removeOption(key)}
                title="Remove"
              >
                &times;
              </button>
            </div>
          ))}
          <button
            type="button"
            className="connector-editor__add-btn"
            onClick={addOption}
          >
            + Add option
          </button>
        </div>
      )}
    </div>
  );
}
