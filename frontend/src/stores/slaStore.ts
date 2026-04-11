// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import type { SlaStatusEntry, SlaEvaluation, SlaStatusKind } from '../api/sla';
import * as api from '../api/sla';

export type SlaSortField = 'name' | 'status' | 'age' | 'max_age';

interface SlaStoreState {
  entries: SlaStatusEntry[];
  total: number;
  loading: boolean;
  error: string | null;

  /** Filters. */
  statusFilter: SlaStatusKind | null;
  tagFilter: string | null;
  ownerFilter: string | null;

  /** Sorting. */
  sortField: SlaSortField;
  sortAsc: boolean;

  /** Selected resource history. */
  selectedFingerprint: string | null;
  selectedHistory: SlaEvaluation[];
  historyLoading: boolean;
}

interface SlaStoreActions {
  fetchStatus(): Promise<void>;
  setStatusFilter(status: SlaStatusKind | null): void;
  setTagFilter(tag: string | null): void;
  setOwnerFilter(owner: string | null): void;
  setSortField(field: SlaSortField): void;
  selectResource(fingerprint: string): Promise<void>;
  clearSelection(): void;
}

/** Order for status-based sorting: breach first, then warning, unknown, ok. */
const STATUS_ORDER: Record<SlaStatusKind, number> = {
  breach: 0,
  warning: 1,
  unknown: 2,
  ok: 3,
};

export const useSlaStore = create<SlaStoreState & SlaStoreActions>((set, get) => ({
  entries: [],
  total: 0,
  loading: false,
  error: null,
  statusFilter: null,
  tagFilter: null,
  ownerFilter: null,
  sortField: 'status',
  sortAsc: true,
  selectedFingerprint: null,
  selectedHistory: [],
  historyLoading: false,

  async fetchStatus() {
    const { statusFilter, tagFilter, ownerFilter } = get();
    set({ loading: true, error: null });
    try {
      const result = await api.fetchSlaStatus({
        status: statusFilter ?? undefined,
        tag: tagFilter ?? undefined,
        owner: ownerFilter ?? undefined,
      });
      set({ entries: result.data, total: result.total, loading: false });
    } catch (e) {
      set({ error: (e as Error).message, loading: false });
    }
  },

  setStatusFilter(status) {
    set({ statusFilter: status });
  },

  setTagFilter(tag) {
    set({ tagFilter: tag });
  },

  setOwnerFilter(owner) {
    set({ ownerFilter: owner });
  },

  setSortField(field) {
    const { sortField, sortAsc } = get();
    if (sortField === field) {
      set({ sortAsc: !sortAsc });
    } else {
      set({ sortField: field, sortAsc: true });
    }
  },

  async selectResource(fingerprint) {
    set({ selectedFingerprint: fingerprint, historyLoading: true, selectedHistory: [] });
    try {
      const history = await api.fetchSlaHistory(fingerprint, 50);
      set({ selectedHistory: history, historyLoading: false });
    } catch {
      set({ historyLoading: false });
    }
  },

  clearSelection() {
    set({ selectedFingerprint: null, selectedHistory: [] });
  },
}));

/** Sort entries according to current store state. Pure helper for the view. */
export function sortSlaEntries(
  entries: SlaStatusEntry[],
  field: SlaSortField,
  asc: boolean,
): SlaStatusEntry[] {
  const arr = [...entries];
  arr.sort((a, b) => {
    let cmp = 0;
    switch (field) {
      case 'name':
        cmp = a.name.localeCompare(b.name);
        break;
      case 'status':
        cmp = STATUS_ORDER[a.status] - STATUS_ORDER[b.status];
        break;
      case 'age':
        cmp = (a.age ?? '').localeCompare(b.age ?? '');
        break;
      case 'max_age':
        cmp = a.max_age.localeCompare(b.max_age);
        break;
    }
    return asc ? cmp : -cmp;
  });
  return arr;
}
