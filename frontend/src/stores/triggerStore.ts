// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import type { TriggerResponse, TriggerHistoryEntry } from '../api/triggers';
import * as api from '../api/triggers';

interface TriggerStoreState {
  /** All triggers for the current filter context. */
  triggers: TriggerResponse[];
  /** Loading state for list fetches. */
  loading: boolean;
  /** Last error message. */
  error: string | null;
  /** Trigger history keyed by trigger ID. */
  history: Map<string, TriggerHistoryEntry[]>;
}

interface TriggerStoreActions {
  /** Fetch triggers, optionally filtered by pipeline. */
  fetchTriggers(pipelineId?: string, environment?: string): Promise<void>;
  /** Fetch a single trigger and update it in the list. */
  refreshTrigger(id: string): Promise<void>;
  /** Create a new trigger. */
  createTrigger(req: api.CreateTriggerRequest): Promise<TriggerResponse>;
  /** Update a trigger. */
  updateTrigger(id: string, req: api.UpdateTriggerRequest): Promise<TriggerResponse>;
  /** Delete a trigger. */
  deleteTrigger(id: string): Promise<void>;
  /** Enable a trigger. */
  enableTrigger(id: string): Promise<void>;
  /** Disable a trigger. */
  disableTrigger(id: string): Promise<void>;
  /** Manually fire a trigger. */
  fireTrigger(id: string): Promise<api.FireResponse>;
  /** Fetch history for a trigger. */
  fetchHistory(id: string, limit?: number): Promise<void>;
  /** Handle a WebSocket TriggerChanged event. */
  handleTriggerChanged(triggerId: string, action: string): void;
}

export const useTriggerStore = create<TriggerStoreState & TriggerStoreActions>((set, get) => ({
  triggers: [],
  loading: false,
  error: null,
  history: new Map(),

  async fetchTriggers(pipelineId?, environment?) {
    set({ loading: true, error: null });
    try {
      const triggers = await api.listTriggers(pipelineId, environment);
      // Fetch state for each trigger to build TriggerResponse[]
      const responses: TriggerResponse[] = [];
      for (const t of triggers) {
        try {
          const full = await api.getTrigger(t.id);
          responses.push(full);
        } catch {
          responses.push({ ...t, state: undefined });
        }
      }
      set({ triggers: responses, loading: false });
    } catch (e) {
      set({ error: (e as Error).message, loading: false });
    }
  },

  async refreshTrigger(id) {
    try {
      const updated = await api.getTrigger(id);
      set((s) => ({
        triggers: s.triggers.map((t) => (t.id === id ? updated : t)),
      }));
    } catch {
      // Trigger may have been deleted
    }
  },

  async createTrigger(req) {
    const resp = await api.createTrigger(req);
    set((s) => ({ triggers: [...s.triggers, resp] }));
    return resp;
  },

  async updateTrigger(id, req) {
    const resp = await api.updateTrigger(id, req);
    set((s) => ({
      triggers: s.triggers.map((t) => (t.id === id ? resp : t)),
    }));
    return resp;
  },

  async deleteTrigger(id) {
    await api.deleteTrigger(id);
    set((s) => ({
      triggers: s.triggers.filter((t) => t.id !== id),
    }));
  },

  async enableTrigger(id) {
    const resp = await api.enableTrigger(id);
    set((s) => ({
      triggers: s.triggers.map((t) => (t.id === id ? resp : t)),
    }));
  },

  async disableTrigger(id) {
    const resp = await api.disableTrigger(id);
    set((s) => ({
      triggers: s.triggers.map((t) => (t.id === id ? resp : t)),
    }));
  },

  async fireTrigger(id) {
    const resp = await api.fireTrigger(id);
    // Refresh trigger state after firing.
    void get().refreshTrigger(id);
    return resp;
  },

  async fetchHistory(id, limit = 50) {
    try {
      const entries = await api.getTriggerHistory(id, limit);
      set((s) => {
        const next = new Map(s.history);
        next.set(id, entries);
        return { history: next };
      });
    } catch {
      // Ignore history fetch errors silently.
    }
  },

  handleTriggerChanged(triggerId, action) {
    if (action === 'deleted') {
      set((s) => ({
        triggers: s.triggers.filter((t) => t.id !== triggerId),
      }));
    } else {
      // Refresh the changed trigger.
      void get().refreshTrigger(triggerId);
    }
  },
}));
