// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect, useState } from 'react';
import { getSystemInfo, type SystemInfo } from '../../api/system';
import './SystemInfoPanel.css';

interface SystemInfoPanelProps {
  open: boolean;
  onClose: () => void;
}

export function SystemInfoPanel({ open, onClose }: SystemInfoPanelProps) {
  const [info, setInfo] = useState<SystemInfo | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    setError(null);
    getSystemInfo()
      .then(setInfo)
      .catch((err) => setError((err as Error).message))
      .finally(() => setLoading(false));
  }, [open]);

  return (
    <div className={`system-info-panel${open ? ' system-info-panel--open' : ''}`}>
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
