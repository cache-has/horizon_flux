// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useState } from 'react';
import type { ApiNode } from '../../api/pipelines';
import { previewNode } from '../../api/pipelines';
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
}

// ---------------------------------------------------------------------------
// PostgreSQL Sink Form
// ---------------------------------------------------------------------------

function PostgresSinkForm({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string } | null>(null);

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
        sample: { max_rows: 1 },
      });
      setTestResult({ ok: true, message: 'Connection successful' });
    } catch (err) {
      setTestResult({ ok: false, message: (err as Error).message });
    }
  }, [config]);

  return (
    <>
      <div className="connector-editor__section">
        <div className="connector-editor__section-title">Connection</div>
        <div className="connector-editor__field">
          <label className="connector-editor__label">Connection String</label>
          <input
            className="connector-editor__input"
            type="password"
            value={String(config.connection_string ?? '')}
            onChange={(e) => onChange({ ...config, connection_string: e.target.value })}
            placeholder="postgres://user:pass@host:5432/db"
          />
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
          placeholder={connector === 'csv' ? '/path/to/output.csv' : '/path/to/output.parquet'}
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

const CONNECTOR_OPTIONS = ['postgres', 'csv', 'parquet', 'stdout'];

export function SinkEditor({
  config,
  connector,
  onConfigChange,
  onConnectorChange,
}: SinkEditorProps) {
  return (
    <div className="connector-editor">
      <div className="connector-editor__section">
        <div className="connector-editor__section-title">Connector Type</div>
        <select
          className="connector-editor__select"
          value={connector}
          onChange={(e) => onConnectorChange(e.target.value)}
        >
          {CONNECTOR_OPTIONS.map((opt) => (
            <option key={opt} value={opt}>
              {opt.charAt(0).toUpperCase() + opt.slice(1)}
            </option>
          ))}
        </select>
      </div>

      {connector === 'postgres' && (
        <PostgresSinkForm config={config} onChange={onConfigChange} />
      )}
      {(connector === 'csv' || connector === 'parquet') && (
        <FileSinkForm config={config} connector={connector} onChange={onConfigChange} />
      )}
      {connector === 'stdout' && <StdoutSinkForm config={config} onChange={onConfigChange} />}

      <EnvironmentOverrides config={config} onChange={onConfigChange} />
    </div>
  );
}
