// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { usePipelineStore } from '../../stores/pipelineStore';
import type { ApiNode, ApiColumnInfo } from '../../api/pipelines';
import { previewPipeline } from '../../api/pipelines';
import { ConfirmDialog } from './ConfirmDialog';
import { TransformEditor } from './TransformEditor';
import { SourceEditor } from './SourceEditor';
import { SinkEditor } from './SinkEditor';
import './node-editor-modal.css';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const isMac = typeof navigator !== 'undefined' && navigator.platform.includes('Mac');
const modKey = isMac ? '\u2318' : 'Ctrl';

interface InputSchema {
  nodeName: string;
  columns: ApiColumnInfo[];
}

// ---------------------------------------------------------------------------
// NodeEditorModal
// ---------------------------------------------------------------------------

export function NodeEditorModal() {
  const editingNodeId = usePipelineStore((s) => s.editingNodeId);
  const setEditingNodeId = usePipelineStore((s) => s.setEditingNodeId);
  const apiPipeline = usePipelineStore((s) => s.apiPipeline);
  const pipelineId = usePipelineStore((s) => s.pipelineId);
  const edges = usePipelineStore((s) => s.edges);
  const nodes = usePipelineStore((s) => s.nodes);
  const updateNodeConfig = usePipelineStore((s) => s.updateNodeConfig);

  const dialogRef = useRef<HTMLDialogElement>(null);
  const [dirty, setDirty] = useState(false);
  const [showDiscardPrompt, setShowDiscardPrompt] = useState(false);
  const [saving, setSaving] = useState(false);

  // Local editor state — tracks uncommitted changes
  const [localName, setLocalName] = useState('');
  const [localMode, setLocalMode] = useState<'sql' | 'python'>('sql');
  const [localCode, setLocalCode] = useState('');
  const [localConnector, setLocalConnector] = useState('');
  const [localConfig, setLocalConfig] = useState<Record<string, unknown>>({});

  // Input schemas and upstream data for transform editor
  const [inputSchemas, setInputSchemas] = useState<InputSchema[]>([]);
  const [upstreamData, setUpstreamData] = useState<
    Record<string, Record<string, unknown>[]>
  >({});

  // Preview runner ref (set by TransformEditor)
  const previewRunnerRef = useRef<(() => void) | null>(null);

  // The API node being edited
  const apiNode: ApiNode | undefined = apiPipeline?.nodes.find(
    (n) => n.id === editingNodeId,
  );
  const rfNode = nodes.find((n) => n.id === editingNodeId);
  const role = rfNode?.data.role;

  // -----------------------------------------------------------------------
  // Open / close dialog
  // -----------------------------------------------------------------------

  useEffect(() => {
    const el = dialogRef.current;
    if (!el) return;
    if (editingNodeId && !el.open) {
      el.showModal();
    } else if (!editingNodeId && el.open) {
      el.close();
    }
  }, [editingNodeId]);

  // Populate local state when opening
  useEffect(() => {
    if (!apiNode) return;
    setLocalName(apiNode.name);
    setLocalMode(apiNode.mode ?? 'sql');
    setLocalCode(apiNode.code ?? '');
    setLocalConnector(apiNode.connector ?? '');
    setLocalConfig((apiNode.config as Record<string, unknown>) ?? {});
    setDirty(false);
    setShowDiscardPrompt(false);
  }, [apiNode?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load input schemas and upstream data for transforms
  useEffect(() => {
    if (!editingNodeId || role !== 'transform' || !pipelineId || pipelineId === 'demo')
      return;

    const controller = new AbortController();

    async function loadUpstream() {
      try {
        // Fetch with rows so we can pass upstream data to single-node preview
        const result = await previewPipeline(
          pipelineId!,
          { max_rows: 100 },
          controller.signal,
        );
        if (controller.signal.aborted) return;

        // Find upstream node IDs
        const upstreamIds = edges
          .filter((e) => e.target === editingNodeId)
          .map((e) => e.source);
        const schemas: InputSchema[] = [];
        const data: Record<string, Record<string, unknown>[]> = {};
        for (const uid of upstreamIds) {
          const upstreamNode = result.nodes.find((n) => n.node_id === uid);
          const rfUpstream = nodes.find((n) => n.id === uid);
          const name = rfUpstream?.data.label ?? uid;
          if (upstreamNode) {
            schemas.push({
              nodeName: name,
              columns: upstreamNode.columns,
            });
            data[name] = upstreamNode.rows;
          }
        }
        setInputSchemas(schemas);
        setUpstreamData(data);
      } catch (err) {
        if ((err as Error).name === 'AbortError') return;
        // Backend not available — empty schemas
        setInputSchemas([]);
        setUpstreamData({});
      }
    }

    loadUpstream();
    return () => { controller.abort(); };
  }, [editingNodeId, role, pipelineId, edges, nodes]);

  // -----------------------------------------------------------------------
  // Close handling
  // -----------------------------------------------------------------------

  const requestClose = useCallback(() => {
    if (dirty) {
      setShowDiscardPrompt(true);
    } else {
      setEditingNodeId(null);
    }
  }, [dirty, setEditingNodeId]);

  const confirmDiscard = useCallback(() => {
    setShowDiscardPrompt(false);
    setDirty(false);
    setEditingNodeId(null);
  }, [setEditingNodeId]);

  const cancelDiscard = useCallback(() => {
    setShowDiscardPrompt(false);
  }, []);

  // -----------------------------------------------------------------------
  // Save
  // -----------------------------------------------------------------------

  const handleSave = useCallback(async () => {
    if (!editingNodeId) return;
    setSaving(true);
    try {
      const patch: Partial<Pick<ApiNode, 'name' | 'mode' | 'code' | 'connector' | 'config'>> = {
        name: localName,
      };
      if (role === 'transform') {
        patch.mode = localMode;
        patch.code = localCode;
      } else {
        patch.connector = localConnector;
        patch.config = localConfig;
      }
      await updateNodeConfig(editingNodeId, patch);
      setDirty(false);
    } catch (err) {
      console.error('Save failed:', err);
    } finally {
      setSaving(false);
    }
  }, [editingNodeId, localName, localMode, localCode, localConnector, localConfig, role, updateNodeConfig]);

  // -----------------------------------------------------------------------
  // Keyboard shortcuts
  // -----------------------------------------------------------------------

  useEffect(() => {
    if (!editingNodeId) return;

    function handleKeyDown(e: KeyboardEvent) {
      const mod = isMac ? e.metaKey : e.ctrlKey;

      // Cmd/Ctrl+S: save
      if (mod && e.key === 's') {
        e.preventDefault();
        handleSave();
        return;
      }

      // Cmd/Ctrl+Enter: run preview
      if (mod && e.key === 'Enter') {
        e.preventDefault();
        previewRunnerRef.current?.();
        return;
      }

      // Escape: close
      if (e.key === 'Escape') {
        e.preventDefault();
        requestClose();
      }
    }

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [editingNodeId, handleSave, requestClose]);

  // -----------------------------------------------------------------------
  // Change handlers that mark dirty
  // -----------------------------------------------------------------------

  const handleNameChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setLocalName(e.target.value);
    setDirty(true);
  }, []);

  const handleModeChange = useCallback((mode: 'sql' | 'python') => {
    setLocalMode(mode);
    setDirty(true);
  }, []);

  const handleCodeChange = useCallback((code: string) => {
    setLocalCode(code);
    setDirty(true);
  }, []);

  const handleConnectorChange = useCallback((connector: string) => {
    setLocalConnector(connector);
    setDirty(true);
  }, []);

  const handleConfigChange = useCallback((config: Record<string, unknown>) => {
    setLocalConfig(config);
    setDirty(true);
  }, []);

  const setPreviewRunner = useCallback((runner: () => void) => {
    previewRunnerRef.current = runner;
  }, []);

  // -----------------------------------------------------------------------
  // Render
  // -----------------------------------------------------------------------

  if (!editingNodeId) return null;

  return (
    <>
      <dialog
        ref={dialogRef}
        className="node-editor"
        onClick={(e) => {
          if (e.target === dialogRef.current) requestClose();
        }}
        onKeyDown={(e) => {
          // Prevent native dialog Escape (we handle it ourselves)
          if (e.key === 'Escape') e.preventDefault();
        }}
      >
        {apiNode && rfNode && (
          <>
            {/* Header */}
            <div className="node-editor__header">
              <input
                className="node-editor__name-input"
                value={localName}
                onChange={handleNameChange}
                aria-label="Node name"
              />
              <span className={`node-editor__role-badge node-editor__role-badge--${role}`}>
                {role}
              </span>

              {role === 'transform' && (
                <div className="node-editor__mode-tabs">
                  <button
                    className={`node-editor__mode-tab${localMode === 'sql' ? ' node-editor__mode-tab--active' : ''}`}
                    onClick={() => handleModeChange('sql')}
                  >
                    SQL
                  </button>
                  <button
                    className={`node-editor__mode-tab${localMode === 'python' ? ' node-editor__mode-tab--active' : ''}`}
                    onClick={() => handleModeChange('python')}
                  >
                    Python
                  </button>
                  <button
                    className="node-editor__mode-tab node-editor__mode-tab--disabled"
                    disabled
                    title="Expression mode coming in v1.1"
                  >
                    Expression
                  </button>
                </div>
              )}

              <div className="node-editor__spacer" />
              {dirty && (
                <span style={{ fontSize: 11, color: 'var(--text)', fontStyle: 'italic' }}>
                  unsaved
                </span>
              )}
              <button
                className="node-editor__close"
                onClick={requestClose}
                aria-label="Close editor"
                title="Close (Esc)"
              >
                &times;
              </button>
            </div>

            {/* Body — type-specific editor */}
            <div className="node-editor__body">
              {role === 'transform' && (
                <TransformEditor
                  apiNode={apiNode}
                  inputSchemas={inputSchemas}
                  upstreamData={upstreamData}
                  mode={localMode}
                  code={localCode}
                  onModeChange={handleModeChange}
                  onCodeChange={handleCodeChange}
                  onPreviewRef={setPreviewRunner}
                />
              )}
              {role === 'source' && (
                <SourceEditor
                  apiNode={apiNode}
                  config={localConfig}
                  connector={localConnector}
                  onConfigChange={handleConfigChange}
                  onConnectorChange={handleConnectorChange}
                />
              )}
              {role === 'sink' && (
                <SinkEditor
                  apiNode={apiNode}
                  config={localConfig}
                  connector={localConnector}
                  onConfigChange={handleConfigChange}
                  onConnectorChange={handleConnectorChange}
                />
              )}
            </div>

            {/* Footer */}
            <div className="node-editor__footer">
              <span className="node-editor__footer-hint">
                <kbd>{modKey}+S</kbd> save &middot; <kbd>{modKey}+Enter</kbd> preview &middot;{' '}
                <kbd>Esc</kbd> close
              </span>
              {role === 'transform' && (
                <button
                  className="node-editor__btn"
                  onClick={() => previewRunnerRef.current?.()}
                >
                  Run Preview
                </button>
              )}
              <button className="node-editor__btn" onClick={requestClose}>
                Cancel
              </button>
              <button
                className="node-editor__btn node-editor__btn--primary"
                onClick={handleSave}
                disabled={saving}
              >
                {saving ? 'Saving...' : 'Save'}
              </button>
            </div>
          </>
        )}
      </dialog>

      <ConfirmDialog
        open={showDiscardPrompt}
        title="Unsaved Changes"
        message="You have unsaved changes. Discard them?"
        confirmLabel="Discard"
        cancelLabel="Keep Editing"
        destructive
        onConfirm={confirmDiscard}
        onCancel={cancelDiscard}
      />
    </>
  );
}
