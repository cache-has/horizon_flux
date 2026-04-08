// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect } from 'react';
import { usePluginStore } from '../../stores/pluginStore';
import { isPluginOk, pluginStatusError } from '../../api/plugins';
import './SystemInfoPanel.css';
import './PluginsPanel.css';

interface PluginsPanelProps {
  open: boolean;
  onClose: () => void;
}

export function PluginsPanel({ open, onClose }: PluginsPanelProps) {
  const plugins = usePluginStore((s) => s.plugins);
  const loading = usePluginStore((s) => s.loading);
  const error = usePluginStore((s) => s.error);
  const fetchPlugins = usePluginStore((s) => s.fetchPlugins);
  const reload = usePluginStore((s) => s.reload);

  useEffect(() => {
    if (open) void fetchPlugins();
  }, [open, fetchPlugins]);

  if (!open) return null;

  return (
    <div className="system-info-panel system-info-panel--open">
      <div className="system-info-panel__header">
        <h3 className="system-info-panel__title">Plugins</h3>
        <button
          className="plugins-panel__reload"
          onClick={() => void reload()}
          disabled={loading}
          title="Rescan plugin directories"
        >
          {loading ? 'Reloading…' : 'Reload'}
        </button>
        <button className="system-info-panel__close" onClick={onClose} title="Close">
          &times;
        </button>
      </div>

      <div className="system-info-panel__body">
        {loading && plugins.length === 0 && (
          <div className="system-info-panel__loading">Loading plugins…</div>
        )}
        {error && <p className="system-info-panel__error">{error}</p>}
        {!loading && plugins.length === 0 && !error && (
          <p className="plugins-panel__empty">
            No plugins discovered. Drop a plugin directory into the plugin scan path
            and press Reload.
          </p>
        )}
        <ul className="plugins-panel__list">
          {plugins.map((p) => {
            const ok = isPluginOk(p.status);
            const errMsg = pluginStatusError(p.status);
            const sinks = p.manifest?.sinks ?? [];
            return (
              <li
                key={p.directory}
                className={`plugins-panel__item plugins-panel__item--${ok ? 'ok' : 'error'}`}
              >
                <div className="plugins-panel__item-header">
                  <span className="plugins-panel__name">{p.name}</span>
                  {p.manifest?.version && (
                    <span className="plugins-panel__version">v{p.manifest.version}</span>
                  )}
                  <span
                    className={`plugins-panel__status plugins-panel__status--${ok ? 'ok' : 'error'}`}
                  >
                    {ok ? 'ok' : 'error'}
                  </span>
                </div>
                <div className="plugins-panel__directory">{p.directory}</div>
                {p.manifest?.description && (
                  <div className="plugins-panel__description">{p.manifest.description}</div>
                )}
                {errMsg && <div className="plugins-panel__error-msg">{errMsg}</div>}
                {sinks.length > 0 && (
                  <ul className="plugins-panel__sinks">
                    {sinks.map((s) => (
                      <li key={s.type} className="plugins-panel__sink">
                        <span className="plugins-panel__sink-name">{s.display_name}</span>
                        <span className="plugins-panel__sink-type">{s.type}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}
