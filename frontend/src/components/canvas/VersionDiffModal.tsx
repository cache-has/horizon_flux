// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef } from 'react';
import { DiffEditor } from '@monaco-editor/react';
import './VersionDiffModal.css';

interface VersionDiffModalProps {
  leftLabel: string;
  rightLabel: string;
  leftJson: string;
  rightJson: string;
  onClose: () => void;
}

export function VersionDiffModal({
  leftLabel,
  rightLabel,
  leftJson,
  rightJson,
  onClose,
}: VersionDiffModalProps) {
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
      className="version-diff-modal"
      onKeyDown={handleKeyDown}
      onClick={(e) => {
        if (e.target === dialogRef.current) onClose();
      }}
    >
      <div className="version-diff-modal__content">
        <div className="version-diff-modal__header">
          <h3 className="version-diff-modal__title">
            Comparing {leftLabel} &harr; {rightLabel}
          </h3>
          <button
            className="version-diff-modal__close"
            onClick={onClose}
            title="Close"
          >
            &times;
          </button>
        </div>
        <div className="version-diff-modal__labels">
          <span className="version-diff-modal__label">{leftLabel}</span>
          <span className="version-diff-modal__label">{rightLabel}</span>
        </div>
        <div className="version-diff-modal__editor">
          <DiffEditor
            original={leftJson}
            modified={rightJson}
            language="json"
            theme="vs-dark"
            options={{
              readOnly: true,
              renderSideBySide: true,
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
