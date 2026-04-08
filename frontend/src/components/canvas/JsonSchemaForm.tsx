// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Minimal JSON Schema → form renderer used by plugin sink editors.
//!
//! Supports the subset of JSON Schema needed by v1 plugin sinks: an object
//! with primitive properties (string / number / integer / boolean), optional
//! `enum` lists, `default` values, `description`, and `required`. Anything
//! more exotic falls through to a JSON textarea so the user is never locked
//! out of editing.

import { useMemo } from 'react';
import './connector-editor.css';

export interface JsonSchemaFormProps {
  schema: Record<string, unknown> | null;
  value: Record<string, unknown>;
  onChange: (next: Record<string, unknown>) => void;
}

interface PropSchema {
  type?: string | string[];
  description?: string;
  default?: unknown;
  enum?: unknown[];
  title?: string;
  format?: string;
}

function getProperties(schema: Record<string, unknown>): Record<string, PropSchema> {
  const props = schema.properties;
  if (props && typeof props === 'object') {
    return props as Record<string, PropSchema>;
  }
  return {};
}

function getRequired(schema: Record<string, unknown>): Set<string> {
  const req = schema.required;
  if (Array.isArray(req)) return new Set(req.filter((r): r is string => typeof r === 'string'));
  return new Set();
}

function inferType(prop: PropSchema): string {
  if (typeof prop.type === 'string') return prop.type;
  if (Array.isArray(prop.type) && prop.type.length > 0) {
    // pick the first non-null type
    return (prop.type.find((t) => t !== 'null') as string | undefined) ?? 'string';
  }
  if (prop.enum) return 'string';
  return 'string';
}

export function JsonSchemaForm({ schema, value, onChange }: JsonSchemaFormProps) {
  const properties = useMemo(() => (schema ? getProperties(schema) : {}), [schema]);
  const required = useMemo(() => (schema ? getRequired(schema) : new Set<string>()), [schema]);

  if (!schema || Object.keys(properties).length === 0) {
    // Fallback: raw JSON editor so the user is never blocked.
    return (
      <div className="connector-editor__field">
        <label className="connector-editor__label">Config (JSON)</label>
        <textarea
          className="connector-editor__input"
          rows={6}
          value={JSON.stringify(value ?? {}, null, 2)}
          onChange={(e) => {
            try {
              onChange(JSON.parse(e.target.value));
            } catch {
              /* ignore parse errors mid-edit */
            }
          }}
        />
      </div>
    );
  }

  const set = (key: string, v: unknown) => {
    if (v === undefined || v === '') {
      const { [key]: _omit, ...rest } = value;
      onChange(rest);
    } else {
      onChange({ ...value, [key]: v });
    }
  };

  return (
    <>
      {Object.entries(properties).map(([key, prop]) => {
        const type = inferType(prop);
        const current = value[key] ?? prop.default ?? '';
        const label = prop.title ?? key;
        const isReq = required.has(key);
        const placeholder = prop.description ?? '';

        if (Array.isArray(prop.enum) && prop.enum.length > 0) {
          return (
            <div key={key} className="connector-editor__field">
              <label className="connector-editor__label">
                {label}
                {isReq && ' *'}
              </label>
              <select
                className="connector-editor__select"
                value={String(current ?? '')}
                onChange={(e) => set(key, e.target.value)}
              >
                {!isReq && <option value="">(none)</option>}
                {prop.enum.map((opt) => (
                  <option key={String(opt)} value={String(opt)}>
                    {String(opt)}
                  </option>
                ))}
              </select>
              {prop.description && <small className="connector-editor__hint">{prop.description}</small>}
            </div>
          );
        }

        if (type === 'boolean') {
          return (
            <div key={key} className="connector-editor__field">
              <label className="connector-editor__label">
                <input
                  type="checkbox"
                  checked={Boolean(current)}
                  onChange={(e) => set(key, e.target.checked)}
                />
                {' '}
                {label}
              </label>
              {prop.description && <small className="connector-editor__hint">{prop.description}</small>}
            </div>
          );
        }

        if (type === 'integer' || type === 'number') {
          return (
            <div key={key} className="connector-editor__field">
              <label className="connector-editor__label">
                {label}
                {isReq && ' *'}
              </label>
              <input
                className="connector-editor__input"
                type="number"
                value={current === '' ? '' : Number(current)}
                onChange={(e) => {
                  const raw = e.target.value;
                  if (raw === '') {
                    set(key, undefined);
                  } else {
                    const n = type === 'integer' ? parseInt(raw, 10) : Number(raw);
                    set(key, Number.isFinite(n) ? n : undefined);
                  }
                }}
                placeholder={placeholder}
              />
              {prop.description && <small className="connector-editor__hint">{prop.description}</small>}
            </div>
          );
        }

        // Default: string
        const isSecret = prop.format === 'password' || /password|secret|token/i.test(key);
        return (
          <div key={key} className="connector-editor__field">
            <label className="connector-editor__label">
              {label}
              {isReq && ' *'}
            </label>
            <input
              className="connector-editor__input"
              type={isSecret ? 'password' : 'text'}
              value={String(current ?? '')}
              onChange={(e) => set(key, e.target.value)}
              placeholder={placeholder}
            />
            {prop.description && <small className="connector-editor__hint">{prop.description}</small>}
          </div>
        );
      })}
    </>
  );
}
