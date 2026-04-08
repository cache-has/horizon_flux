// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import type { ApiNode } from '../../api/pipelines';
import { previewNode } from '../../api/pipelines';
import { StorageOptionsEditor } from './StorageOptionsEditor';
import { SecretPicker } from './SecretPicker';
import { JsonSchemaForm } from './JsonSchemaForm';
import { usePluginStore } from '../../stores/pluginStore';
import { getPluginSinkSchema } from '../../api/plugins';
import './connector-editor.css';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SinkEditorProps {
  apiNode: ApiNode;
  config: Record<string, unknown>;
  connector: string;
  onConfigChange: (config: Record<string, unknown>) => void;
  onConnectorChange: (connector: string) => void;
  /** Resolved pipeline variable defaults (name → value). */
  pipelineVariables?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// PostgreSQL Sink Form
// ---------------------------------------------------------------------------

function PostgresSinkForm({
  config,
  onChange,
  variables,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
  variables?: Record<string, unknown>;
}) {
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [showSecretPicker, setShowSecretPicker] = useState(false);

  const handleTestConnection = useCallback(async () => {
    setTestResult(null);
    try {
      // Test by trying to preview a simple query on the target
      await previewNode({
        node: {
          type: 'source',
          connector: 'postgres',
          config: { ...config, query: 'SELECT 1' },
        },
        sample: { mode: 'first_n', count: 1 },
        variables,
      });
      setTestResult({ ok: true, message: 'Connection successful' });
    } catch (err) {
      setTestResult({ ok: false, message: (err as Error).message });
    }
  }, [config, variables]);

  return (
    <>
      <div className="connector-editor__section">
        <div className="connector-editor__section-title">Connection</div>
        <div className="connector-editor__field connector-editor__field-with-secret">
          <div className="connector-editor__label-row">
            <label className="connector-editor__label">Connection String</label>
            <button
              className="connector-editor__secret-btn"
              type="button"
              onClick={() => setShowSecretPicker((v) => !v)}
            >
              Use Secret
            </button>
          </div>
          <input
            className="connector-editor__input"
            type="password"
            value={String(config.connection_string ?? '')}
            onChange={(e) => onChange({ ...config, connection_string: e.target.value })}
            placeholder="postgres://user:pass@host:5432/db"
          />
          {showSecretPicker && (
            <SecretPicker
              onSelect={(tpl) => {
                onChange({ ...config, connection_string: tpl });
                setShowSecretPicker(false);
              }}
              onClose={() => setShowSecretPicker(false)}
            />
          )}
        </div>
        <div className="connector-editor__field">
          <label className="connector-editor__label">Table Name</label>
          <input
            className="connector-editor__input"
            type="text"
            value={String(config.table ?? '')}
            onChange={(e) => onChange({ ...config, table: e.target.value })}
            placeholder="public.my_table"
          />
        </div>
        <div className="connector-editor__field">
          <label className="connector-editor__label">Write Mode</label>
          <select
            className="connector-editor__select"
            value={String(config.write_mode ?? 'insert')}
            onChange={(e) => onChange({ ...config, write_mode: e.target.value })}
          >
            <option value="insert">Insert</option>
            <option value="upsert">Upsert</option>
            <option value="truncate">Truncate &amp; Insert</option>
          </select>
        </div>
        <button className="connector-editor__test-btn" onClick={handleTestConnection}>
          Test Connection
        </button>
        {testResult && (
          <span
            className={`connector-editor__test-result ${testResult.ok ? 'connector-editor__test-result--ok' : 'connector-editor__test-result--err'}`}
          >
            {testResult.message}
          </span>
        )}
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// CSV / Parquet Sink Form
// ---------------------------------------------------------------------------

function FileSinkForm({
  config,
  connector,
  onChange,
}: {
  config: Record<string, unknown>;
  connector: string;
  onChange: (config: Record<string, unknown>) => void;
}) {
  return (
    <div className="connector-editor__section">
      <div className="connector-editor__section-title">File Configuration</div>
      <div className="connector-editor__field">
        <label className="connector-editor__label">Output Path</label>
        <input
          className="connector-editor__input"
          type="text"
          value={String(config.path ?? '')}
          onChange={(e) => onChange({ ...config, path: e.target.value })}
          placeholder="Local path or cloud URL (s3://, gs://, az://)"
        />
      </div>
      {connector === 'parquet' && (
        <div className="connector-editor__field">
          <label className="connector-editor__label">Compression</label>
          <select
            className="connector-editor__select"
            value={String(config.compression ?? 'snappy')}
            onChange={(e) => onChange({ ...config, compression: e.target.value })}
          >
            <option value="none">None</option>
            <option value="snappy">Snappy</option>
            <option value="gzip">Gzip</option>
            <option value="zstd">Zstd</option>
          </select>
        </div>
      )}
      {connector === 'csv' && (
        <div className="connector-editor__row">
          <div className="connector-editor__field">
            <label className="connector-editor__label">Delimiter</label>
            <input
              className="connector-editor__input"
              type="text"
              value={String(config.delimiter ?? ',')}
              onChange={(e) => onChange({ ...config, delimiter: e.target.value })}
              maxLength={1}
              style={{ width: 48 }}
            />
          </div>
          <div className="connector-editor__field">
            <label className="connector-editor__label">Include Header</label>
            <select
              className="connector-editor__select"
              value={config.include_header === false ? 'no' : 'yes'}
              onChange={(e) =>
                onChange({ ...config, include_header: e.target.value === 'yes' })
              }
            >
              <option value="yes">Yes</option>
              <option value="no">No</option>
            </select>
          </div>
        </div>
      )}
      <StorageOptionsEditor config={config} onChange={onChange} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Stdout Sink Form
// ---------------------------------------------------------------------------

function StdoutSinkForm({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  return (
    <div className="connector-editor__section">
      <div className="connector-editor__section-title">Stdout Options</div>
      <div className="connector-editor__row">
        <div className="connector-editor__field">
          <label className="connector-editor__label">Format</label>
          <select
            className="connector-editor__select"
            value={String(config.format ?? 'table')}
            onChange={(e) => onChange({ ...config, format: e.target.value })}
          >
            <option value="table">Table</option>
            <option value="csv">CSV</option>
            <option value="json">JSON</option>
          </select>
        </div>
        <div className="connector-editor__field">
          <label className="connector-editor__label">Max Rows</label>
          <input
            className="connector-editor__input"
            type="number"
            value={Number(config.max_rows ?? 100)}
            onChange={(e) => onChange({ ...config, max_rows: Number(e.target.value) })}
            min={1}
            style={{ width: 80 }}
          />
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Environment Overrides
// ---------------------------------------------------------------------------

function EnvironmentOverrides({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  const overrides = (config.environment_overrides ?? {}) as Record<string, Record<string, unknown>>;
  const envKeys = Object.keys(overrides);

  return (
    <div className="connector-editor__section">
      <div className="connector-editor__section-title">Environment Overrides</div>
      {envKeys.length === 0 ? (
        <span className="connector-editor__empty">
          No environment overrides configured
        </span>
      ) : (
        envKeys.map((env) => (
          <div key={env} className="connector-editor__env-section">
            <div className="connector-editor__env-title">{env}</div>
            {Object.entries(overrides[env]).map(([k, v]) => (
              <div key={k} className="connector-editor__field">
                <label className="connector-editor__label">{k}</label>
                <input
                  className="connector-editor__input"
                  type="text"
                  value={String(v ?? '')}
                  onChange={(e) => {
                    const updatedOverrides = {
                      ...overrides,
                      [env]: { ...overrides[env], [k]: e.target.value },
                    };
                    onChange({ ...config, environment_overrides: updatedOverrides });
                  }}
                />
              </div>
            ))}
          </div>
        ))
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main SinkEditor
// ---------------------------------------------------------------------------

const BUILTIN_CONNECTOR_OPTIONS = ['postgresql', 'csv', 'parquet', 'stdout'];
const CONNECTOR_LABELS: Record<string, string> = {
  postgresql: 'PostgreSQL',
  csv: 'CSV',
  parquet: 'Parquet',
  stdout: 'Stdout',
};

// ---------------------------------------------------------------------------
// Plugin sink form (JSON-Schema driven)
// ---------------------------------------------------------------------------

function PluginSinkForm({
  pluginName,
  sinkType,
  config,
  onChange,
}: {
  pluginName: string;
  sinkType: string;
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  const [schema, setSchema] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const ctrl = new AbortController();
    setLoading(true);
    setError(null);
    setSchema(null);
    getPluginSinkSchema(pluginName, sinkType, ctrl.signal)
      .then((s) => setSchema(s))
      .catch((e) => {
        if ((e as Error).name !== 'AbortError') setError((e as Error).message);
      })
      .finally(() => setLoading(false));
    return () => ctrl.abort();
  }, [pluginName, sinkType]);

  return (
    <div className="connector-editor__section">
      <div className="connector-editor__section-title">
        Configuration
        <span className="connector-editor__plugin-badge">plugin</span>
      </div>
      {loading && <div className="connector-editor__empty">Loading schema…</div>}
      {error && <div className="connector-editor__error">Failed to load schema: {error}</div>}
      {!loading && !error && (
        <JsonSchemaForm schema={schema} value={config} onChange={onChange} />
      )}
    </div>
  );
}

/** Normalize connector aliases to canonical names. */
function normalizeConnector(c: string): string {
  const lc = c.toLowerCase();
  if (lc === 'postgres' || lc === 'postgresql') return 'postgresql';
  if (lc === 'file') return 'csv';
  if (lc === 'csv' || lc === 'parquet' || lc === 'stdout') return lc;
  return c;
}

function isPostgres(c: string): boolean {
  const lc = c.toLowerCase();
  return lc === 'postgresql' || lc === 'postgres';
}

function isFile(c: string): boolean {
  const lc = c.toLowerCase();
  return lc === 'csv' || lc === 'parquet' || lc === 'file';
}

export function SinkEditor({
  config,
  connector,
  onConfigChange,
  onConnectorChange,
  pipelineVariables,
}: SinkEditorProps) {
  const norm = normalizeConnector(connector);
  const format = config.format as string | undefined;
  const effectiveFileType = norm === 'parquet' || format === 'parquet' ? 'parquet' : 'csv';

  // Plugin discovery — lazy fetch on mount.
  const pluginsLoaded = usePluginStore((s) => s.loaded);
  const fetchPlugins = usePluginStore((s) => s.fetchPlugins);
  const sinkOptions = usePluginStore((s) => s.sinkOptions());
  useEffect(() => {
    if (!pluginsLoaded) void fetchPlugins();
  }, [pluginsLoaded, fetchPlugins]);

  // Determine if the current connector is plugin-provided. Built-ins win on
  // collisions so removing a plugin can never silently change a built-in node.
  const isBuiltin = BUILTIN_CONNECTOR_OPTIONS.includes(norm);
  const pluginOwner = !isBuiltin
    ? sinkOptions.find((o) => o.sink.type === norm || o.sink.type === connector)
    : undefined;

  const allOptions = [
    ...BUILTIN_CONNECTOR_OPTIONS.map((opt) => ({
      value: opt,
      label: CONNECTOR_LABELS[opt] ?? opt,
      plugin: false,
    })),
    ...sinkOptions
      .filter((o) => !BUILTIN_CONNECTOR_OPTIONS.includes(o.sink.type))
      .map((o) => ({
        value: o.sink.type,
        label: `${o.sink.display_name} (${o.pluginName})`,
        plugin: true,
      })),
  ];

  // If the saved connector references a plugin that hasn't loaded yet, surface
  // it as an option so the dropdown reflects current state instead of snapping.
  if (
    !isBuiltin &&
    !pluginOwner &&
    !allOptions.some((o) => o.value === norm || o.value === connector)
  ) {
    allOptions.push({ value: connector, label: `${connector} (unavailable)`, plugin: true });
  }

  return (
    <div className="connector-editor">
      <div className="connector-editor__section">
        <div className="connector-editor__section-title">Connector Type</div>
        <select
          className="connector-editor__select"
          value={pluginOwner ? pluginOwner.sink.type : norm}
          onChange={(e) => onConnectorChange(e.target.value)}
        >
          {allOptions.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}{opt.plugin ? ' [plugin]' : ''}
            </option>
          ))}
        </select>
      </div>

      {isPostgres(connector) && isBuiltin && (
        <PostgresSinkForm config={config} onChange={onConfigChange} variables={pipelineVariables} />
      )}
      {isFile(connector) && isBuiltin && (
        <FileSinkForm config={config} connector={effectiveFileType} onChange={onConfigChange} />
      )}
      {connector.toLowerCase() === 'stdout' && isBuiltin && (
        <StdoutSinkForm config={config} onChange={onConfigChange} />
      )}
      {pluginOwner && (
        <PluginSinkForm
          pluginName={pluginOwner.pluginName}
          sinkType={pluginOwner.sink.type}
          config={config}
          onChange={onConfigChange}
        />
      )}

      <EnvironmentOverrides config={config} onChange={onConfigChange} />
    </div>
  );
}
