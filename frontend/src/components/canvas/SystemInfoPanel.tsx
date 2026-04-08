// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect, useReducer } from 'react';
import { getSystemInfo, type SystemInfo } from '../../api/system';
import './SystemInfoPanel.css';

interface SystemInfoPanelProps {
  open: boolean;
  onClose: () => void;
}

type FetchState = { info: SystemInfo | null; error: string | null; loading: boolean };
type FetchAction =
  | { type: 'start' }
  | { type: 'success'; info: SystemInfo }
  | { type: 'error'; message: string };

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'start': return { info: null, error: null, loading: true };
    case 'success': return { info: action.info, error: null, loading: false };
    case 'error': return { info: null, error: action.message, loading: false };
  }
}

export function SystemInfoPanel({ open, onClose }: SystemInfoPanelProps) {
  const [{ info, error, loading }, dispatch] = useReducer(fetchReducer, {
    info: null,
    error: null,
    loading: false,
  });

  useEffect(() => {
    if (!open) return;
    dispatch({ type: 'start' });
    let cancelled = false;
    getSystemInfo()
      .then((data) => { if (!cancelled) dispatch({ type: 'success', info: data }); })
      .catch((err) => { if (!cancelled) dispatch({ type: 'error', message: (err as Error).message }); });
    return () => { cancelled = true; };
  }, [open]);

  if (!open) return null;

  return (
    <div className="system-info-panel system-info-panel--open">
      <div className="system-info-panel__header">
        <h3 className="system-info-panel__title">System Info</h3>
        <button className="system-info-panel__close" onClick={onClose} title="Close">
          &times;
        </button>
      </div>

      <div className="system-info-panel__body">
        {loading && <div className="system-info-panel__loading">Loading...</div>}
        {error && <p className="system-info-panel__error">{error}</p>}
        {info && !loading && (
          <dl className="system-info-panel__list">
            <div className="system-info-panel__row">
              <dt className="system-info-panel__label">Server Version</dt>
              <dd className="system-info-panel__value">{info.version}</dd>
            </div>
            <div className="system-info-panel__row">
              <dt className="system-info-panel__label">Metadata Backend</dt>
              <dd className="system-info-panel__value system-info-panel__value--mono">
                {info.metadata_backend}
              </dd>
            </div>
            <div className="system-info-panel__row">
              <dt className="system-info-panel__label">Data Directory</dt>
              <dd className="system-info-panel__value system-info-panel__value--mono">
                {info.data_dir}
              </dd>
            </div>
            <div className="system-info-panel__row">
              <dt className="system-info-panel__label">Config Source</dt>
              <dd className="system-info-panel__value">{info.config_source}</dd>
            </div>
            {info.connection_string && (
              <div className="system-info-panel__row">
                <dt className="system-info-panel__label">Connection String</dt>
                <dd className="system-info-panel__value system-info-panel__value--mono">
                  {info.connection_string}
                </dd>
              </div>
            )}
          </dl>
        )}
      </div>
    </div>
  );
}
