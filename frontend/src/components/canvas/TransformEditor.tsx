// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { ApiNode, ApiColumnInfo, ApiPreviewNodeResponse } from '../../api/pipelines';
import { previewNode } from '../../api/pipelines';
import './transform-editor.css';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface InputSchema {
  nodeName: string;
  columns: ApiColumnInfo[];
}

export interface TransformEditorProps {
  apiNode: ApiNode;
  inputSchemas: InputSchema[];
  mode: 'sql' | 'python';
  code: string;
  onModeChange: (mode: 'sql' | 'python') => void;
  onCodeChange: (code: string) => void;
  /** Ref callback so parent can trigger preview imperatively. */
  onPreviewRef?: (run: () => void) => void;
}

// ---------------------------------------------------------------------------
// Schema Sidebar
// ---------------------------------------------------------------------------

function SchemaSidebar({
  inputs,
  onColumnClick,
}: {
  inputs: InputSchema[];
  onColumnClick: (col: string) => void;
}) {
  const [openSections, setOpenSections] = useState<Set<string>>(
    () => new Set(inputs.map((i) => i.nodeName)),
  );

  const toggle = (name: string) => {
    setOpenSections((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  return (
    <div className="transform-editor__schema">
      <div className="transform-editor__schema-header">Input Schemas</div>
      {inputs.length === 0 && (
        <div style={{ padding: '8px 12px', fontSize: 12, color: 'var(--text)' }}>
          No upstream nodes
        </div>
      )}
      {inputs.map((input) => {
        const isOpen = openSections.has(input.nodeName);
        return (
          <div key={input.nodeName} className="transform-editor__input-group">
            <div
              className="transform-editor__input-name"
              onClick={() => toggle(input.nodeName)}
            >
              <span
                className={`transform-editor__input-toggle${isOpen ? ' transform-editor__input-toggle--open' : ''}`}
              >
                &#9654;
              </span>
              {input.nodeName}
            </div>
            {isOpen && (
              <ul className="transform-editor__col-list">
                {input.columns.map((col) => (
                  <li
                    key={col.name}
                    className="transform-editor__col-item"
                    onClick={() => onColumnClick(col.name)}
                    title={`Click to insert "${col.name}" at cursor`}
                  >
                    <span className="transform-editor__col-name">{col.name}</span>
                    <span className="transform-editor__col-type">{col.data_type}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Preview Table
// ---------------------------------------------------------------------------

function PreviewPanel({
  preview,
  loading,
  error,
}: {
  preview: ApiPreviewNodeResponse | null;
  loading: boolean;
  error: string | null;
}) {
  if (loading) {
    return (
      <div className="transform-editor__preview">
        <div className="transform-editor__preview-header">
          <strong>Preview</strong>
        </div>
        <span className="transform-editor__preview-loading">Running preview...</span>
      </div>
    );
  }

  return (
    <div className="transform-editor__preview">
      <div className="transform-editor__preview-header">
        <strong>Preview</strong>
        {preview && (
          <span className="transform-editor__preview-info">
            {preview.row_count} rows &middot; {preview.columns.length} cols &middot;{' '}
            {preview.duration_ms}ms
          </span>
        )}
      </div>
      {error && <div className="transform-editor__preview-error">{error}</div>}
      {!error && !preview && (
        <span className="transform-editor__preview-empty">
          Press Run Preview or {navigator.platform.includes('Mac') ? '\u2318' : 'Ctrl'}+Enter
        </span>
      )}
      {!error && preview && preview.rows.length > 0 && (
        <div className="transform-editor__preview-table-wrap">
          <table className="transform-editor__preview-table">
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
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main TransformEditor
// ---------------------------------------------------------------------------

// Monaco types extracted from OnMount callback
type MonacoEditor = Parameters<OnMount>[0];
type Monaco = Parameters<OnMount>[1];

export function TransformEditor({
  inputSchemas,
  mode,
  code,
  onCodeChange,
  onPreviewRef,
}: TransformEditorProps) {
  const editorRef = useRef<MonacoEditor | null>(null);
  const [preview, setPreview] = useState<ApiPreviewNodeResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  // Debounced preview on code change
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const codeRef = useRef(code);
  codeRef.current = code;
  const modeRef = useRef(mode);
  modeRef.current = mode;

  const runPreview = useCallback(async () => {
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const result = await previewNode({
        node: {
          type: 'transform',
          mode: modeRef.current,
          code: codeRef.current,
        },
        sample: { max_rows: 50 },
      });
      setPreview(result);
    } catch (err) {
      setPreviewError((err as Error).message);
    } finally {
      setPreviewLoading(false);
    }
  }, []);

  // Expose runPreview to parent
  useEffect(() => {
    onPreviewRef?.(runPreview);
  }, [runPreview, onPreviewRef]);

  // Debounced preview on code changes
  const handleCodeChange = useCallback(
    (value: string | undefined) => {
      const newCode = value ?? '';
      onCodeChange(newCode);

      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        runPreview();
      }, 500);
    },
    [onCodeChange, runPreview],
  );

  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  const handleEditorMount: OnMount = useCallback((editor: MonacoEditor, monaco: Monaco) => {
    editorRef.current = editor;
    editor.focus();

    // Register custom SQL autocomplete for table/column names
    monaco.languages.registerCompletionItemProvider('sql', {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      provideCompletionItems: (model: any, position: any) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const suggestions: any[] = [];

        // Add input table names
        for (const input of inputSchemas) {
          suggestions.push({
            label: input.nodeName,
            kind: monaco.languages.CompletionItemKind.Struct,
            insertText: input.nodeName,
            range,
            detail: 'input table',
          });

          // Add column names
          for (const col of input.columns) {
            suggestions.push({
              label: col.name,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col.name,
              range,
              detail: `${col.data_type} (${input.nodeName})`,
            });
          }
        }

        // DataFusion SQL functions
        const dfFunctions = [
          'count', 'sum', 'avg', 'min', 'max', 'coalesce', 'nullif',
          'cast', 'trim', 'lower', 'upper', 'length', 'substr',
          'concat', 'now', 'date_part', 'date_trunc', 'extract',
          'row_number', 'rank', 'dense_rank', 'lag', 'lead',
          'array_agg', 'string_agg',
        ];
        for (const fn of dfFunctions) {
          suggestions.push({
            label: fn,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: fn + '($0)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range,
            detail: 'DataFusion function',
          });
        }

        return { suggestions };
      },
    });
  }, [inputSchemas]);

  const handleColumnClick = useCallback((colName: string) => {
    const editor = editorRef.current;
    if (!editor) return;
    const selection = editor.getSelection();
    if (!selection) return;
    editor.executeEdits('schema-sidebar', [
      { range: selection, text: colName },
    ]);
    editor.focus();
  }, []);

  const editorLanguage = mode === 'sql' ? 'sql' : 'python';
  const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

  return (
    <div className="transform-editor">
      <div className="transform-editor__main">
        <div className="transform-editor__code">
          <div className="monaco-container">
            <Editor
              language={editorLanguage}
              theme={isDark ? 'vs-dark' : 'vs'}
              value={code}
              onChange={handleCodeChange}
              onMount={handleEditorMount}
              options={{
                fontSize: 13,
                fontFamily: 'var(--mono)',
                minimap: { enabled: code.split('\n').length > 30 },
                lineNumbers: 'on',
                scrollBeyondLastLine: false,
                wordWrap: 'off',
                tabSize: mode === 'python' ? 4 : 2,
                automaticLayout: true,
                padding: { top: 8 },
              }}
            />
          </div>
        </div>
        <SchemaSidebar inputs={inputSchemas} onColumnClick={handleColumnClick} />
      </div>
      <PreviewPanel preview={preview} loading={previewLoading} error={previewError} />
    </div>
  );
}
