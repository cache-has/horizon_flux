// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef } from 'react';
import Editor from '@monaco-editor/react';
import './VersionViewModal.css';

interface VersionViewModalProps {
  versionLabel: string;
  json: string;
  onClose: () => void;
}

export function VersionViewModal({ versionLabel, json, onClose }: VersionViewModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const el = dialogRef.current;
    if (el && !el.open) el.showModal();
    return () => {
      if (el?.open) el.close();
    };
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        onClose();
      }
    },
    [onClose],
  );

  return (
    <dialog
      ref={dialogRef}
      className="version-view-modal"
      onKeyDown={handleKeyDown}
      onClick={(e) => {
        if (e.target === dialogRef.current) onClose();
      }}
    >
      <div className="version-view-modal__content">
        <div className="version-view-modal__header">
          <h3 className="version-view-modal__title">
            Pipeline Snapshot &mdash; {versionLabel}
          </h3>
          <button
            className="version-view-modal__close"
            onClick={onClose}
            title="Close"
          >
            &times;
          </button>
        </div>
        <div className="version-view-modal__editor">
          <Editor
            value={json}
            language="json"
            theme="vs-dark"
            options={{
              readOnly: true,
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
              fontSize: 13,
              lineNumbers: 'on',
              folding: true,
              wordWrap: 'on',
            }}
          />
        </div>
      </div>
    </dialog>
  );
}
