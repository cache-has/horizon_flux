// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useState } from 'react';
import Editor from '@monaco-editor/react';
import type { ApiNode, ApiPreviewNodeResponse } from '../../api/pipelines';
import { previewNode } from '../../api/pipelines';
import './connector-editor.css';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SourceEditorProps {
  apiNode: ApiNode;
  config: Record<string, unknown>;
  connector: string;
  onConfigChange: (config: Record<string, unknown>) => void;
  onConnectorChange: (connector: string) => void;
}

// ---------------------------------------------------------------------------
// PostgreSQL Source Form
// ---------------------------------------------------------------------------

function PostgresSourceForm({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string } | null>(null);
  const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

  const handleTestConnection = useCallback(async () => {
    setTestResult(null);
    try {
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

      <div className="connector-editor__section">
        <div className="connector-editor__section-title">Query</div>
        <div className="connector-editor__query-wrap">
          <Editor
            language="sql"
            theme={isDark ? 'vs-dark' : 'vs'}
            value={String(config.query ?? '')}
            onChange={(v: string | undefined) => onChange({ ...config, query: v ?? '' })}
            options={{
              fontSize: 13,
              minimap: { enabled: false },
              lineNumbers: 'on',
              scrollBeyondLastLine: false,
              automaticLayout: true,
              padding: { top: 8 },
            }}
          />
        </div>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// CSV / Parquet Source Form
// ---------------------------------------------------------------------------

function FileSourceForm({
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
        <label className="connector-editor__label">File Path</label>
        <input
          className="connector-editor__input"
          type="text"
          value={String(config.path ?? '')}
          onChange={(e) => onChange({ ...config, path: e.target.value })}
          placeholder={connector === 'csv' ? '/path/to/data.csv' : '/path/to/data.parquet'}
        />
      </div>
      {connector === 'csv' && (
        <>
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
              <label className="connector-editor__label">Has Header</label>
              <select
                className="connector-editor__select"
                value={config.has_header === false ? 'no' : 'yes'}
                onChange={(e) =>
                  onChange({ ...config, has_header: e.target.value === 'yes' })
                }
              >
                <option value="yes">Yes</option>
                <option value="no">No</option>
              </select>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// REST API Source Form
// ---------------------------------------------------------------------------

function RestSourceForm({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}) {
  return (
    <div className="connector-editor__section">
      <div className="connector-editor__section-title">REST API</div>
      <div className="connector-editor__field">
        <label className="connector-editor__label">URL</label>
        <input
          className="connector-editor__input"
          type="text"
          value={String(config.url ?? '')}
          onChange={(e) => onChange({ ...config, url: e.target.value })}
          placeholder="https://api.example.com/data"
        />
      </div>
      <div className="connector-editor__row">
        <div className="connector-editor__field">
          <label className="connector-editor__label">Method</label>
          <select
            className="connector-editor__select"
            value={String(config.method ?? 'GET')}
            onChange={(e) => onChange({ ...config, method: e.target.value })}
          >
            <option value="GET">GET</option>
            <option value="POST">POST</option>
          </select>
        </div>
        <div className="connector-editor__field">
          <label className="connector-editor__label">Response Format</label>
          <select
            className="connector-editor__select"
            value={String(config.response_format ?? 'json')}
            onChange={(e) => onChange({ ...config, response_format: e.target.value })}
          >
            <option value="json">JSON</option>
            <option value="csv">CSV</option>
          </select>
        </div>
      </div>
      <div className="connector-editor__field">
        <label className="connector-editor__label">Headers (JSON)</label>
        <input
          className="connector-editor__input"
          type="text"
          value={String(config.headers ?? '{}')}
          onChange={(e) => onChange({ ...config, headers: e.target.value })}
          placeholder='{"Authorization": "Bearer ..."}'
        />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Preview section
// ---------------------------------------------------------------------------

function SourcePreview({ config, connector }: { config: Record<string, unknown>; connector: string }) {
  const [preview, setPreview] = useState<ApiPreviewNodeResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const runPreview = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await previewNode({
        node: { type: 'source', connector, config },
        sample: { max_rows: 10 },
      });
      setPreview(result);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }, [config, connector]);

  return (
    <div className="connector-editor__section connector-editor__preview">
      <div className="connector-editor__section-title">
        Preview
        <button
          className="connector-editor__test-btn"
          onClick={runPreview}
          style={{ marginLeft: 12 }}
        >
          {loading ? 'Loading...' : 'Fetch Preview'}
        </button>
      </div>
      {error && (
        <span className="connector-editor__test-result connector-editor__test-result--err">
          {error}
        </span>
      )}
      {preview && preview.rows.length > 0 && (
        <>
          <div className="connector-editor__preview-table-wrap">
            <table className="connector-editor__preview-table">
              <thead>
                <tr>
                  {preview.columns.map((c) => (
                    <th key={c.name}>{c.name}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.rows.map((row, i) => (
                  <tr key={i}>
                    {preview.columns.map((c) => (
                      <td key={c.name}>{String(row[c.name] ?? '')}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="connector-editor__section" style={{ padding: '8px 0 0' }}>
            <div className="connector-editor__section-title">Discovered Schema</div>
            <ul className="connector-editor__schema-list">
              {preview.columns.map((c) => (
                <li key={c.name} className="connector-editor__schema-item">
                  <span className="connector-editor__schema-name">{c.name}</span>
                  <span className="connector-editor__schema-type">{c.data_type}</span>
                </li>
              ))}
            </ul>
          </div>
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main SourceEditor
// ---------------------------------------------------------------------------

const CONNECTOR_OPTIONS = ['postgres', 'csv', 'parquet', 'rest'];

export function SourceEditor({
  config,
  connector,
  onConfigChange,
  onConnectorChange,
}: SourceEditorProps) {
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
        <PostgresSourceForm config={config} onChange={onConfigChange} />
      )}
      {(connector === 'csv' || connector === 'parquet') && (
        <FileSourceForm config={config} connector={connector} onChange={onConfigChange} />
      )}
      {connector === 'rest' && <RestSourceForm config={config} onChange={onConfigChange} />}

      <SourcePreview config={config} connector={connector} />
    </div>
  );
}
