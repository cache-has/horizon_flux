// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import {
  listPlugins,
  reloadPlugins,
  isPluginOk,
  type DiscoveredPlugin,
  type PluginSinkDeclaration,
} from '../api/plugins';

export interface PluginSinkOption {
  pluginName: string;
  sink: PluginSinkDeclaration;
}

export interface PluginStoreState {
  plugins: DiscoveredPlugin[];
  loading: boolean;
  error: string | null;
  loaded: boolean;
}

export interface PluginStoreActions {
  /** Fetch the plugin list (idempotent — calling again refreshes). */
  fetchPlugins: () => Promise<void>;
  /** Trigger a server-side rescan and refresh local state. */
  reload: () => Promise<void>;
  /** Flat list of usable sink declarations across healthy plugins. */
  sinkOptions: () => PluginSinkOption[];
  /** Look up which plugin owns a given sink type, if any. */
  findSinkOwner: (sinkType: string) => PluginSinkOption | undefined;
}

export type PluginStore = PluginStoreState & PluginStoreActions;

export const usePluginStore = create<PluginStore>((set, get) => ({
  plugins: [],
  loading: false,
  error: null,
  loaded: false,

  async fetchPlugins() {
    set({ loading: true, error: null });
    try {
      const res = await listPlugins();
      set({ plugins: res.plugins, loading: false, loaded: true });
    } catch (err) {
      set({ error: (err as Error).message, loading: false, loaded: true });
    }
  },

  async reload() {
    set({ loading: true, error: null });
    try {
      await reloadPlugins();
      const res = await listPlugins();
      set({ plugins: res.plugins, loading: false, loaded: true });
    } catch (err) {
      set({ error: (err as Error).message, loading: false });
    }
  },

  sinkOptions() {
    const out: PluginSinkOption[] = [];
    for (const p of get().plugins) {
      if (!isPluginOk(p.status)) continue;
      const sinks = p.manifest?.sinks ?? [];
      for (const sink of sinks) {
        out.push({ pluginName: p.name, sink });
      }
    }
    return out;
  },

  findSinkOwner(sinkType: string) {
    return get().sinkOptions().find((o) => o.sink.type === sinkType);
  },
}));
