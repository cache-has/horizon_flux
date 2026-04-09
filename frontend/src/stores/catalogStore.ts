// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import type { CatalogEntry, MetadataUpdateRequest } from '../api/catalog';
import * as api from '../api/catalog';

interface CatalogStoreState {
  /** All catalog entries for the current filter context. */
  entries: CatalogEntry[];
  /** Total count from the last list request. */
  total: number;
  /** Currently selected resource for detail view. */
  selectedEntry: CatalogEntry | null;
  /** Loading state for list fetches. */
  loading: boolean;
  /** Last error message. */
  error: string | null;
  /** Available tags for filter dropdown. */
  tags: string[];
  /** Available owner teams for filter dropdown. */
  owners: string[];
  /** Current search query. */
  searchQuery: string;
  /** Current tag filter. */
  tagFilter: string | null;
  /** Current owner filter. */
  ownerFilter: string | null;
}

interface CatalogStoreActions {
  /** Fetch catalog entries with current filters. */
  fetchEntries(env?: string): Promise<void>;
  /** Fetch filter options (tags, owners). */
  fetchFilterOptions(env?: string): Promise<void>;
  /** Select a resource for detail view. */
  selectEntry(fingerprint: string, env?: string): Promise<void>;
  /** Clear the selected entry (back to list). */
  clearSelection(): void;
  /** Update metadata for a resource. */
  updateMetadata(fingerprint: string, body: MetadataUpdateRequest): Promise<CatalogEntry>;
  /** Scaffold metadata files. */
  scaffoldMetadata(fingerprint?: string, env?: string): Promise<string[]>;
  /** Set search query and re-fetch. */
  setSearchQuery(q: string): void;
  /** Set tag filter. */
  setTagFilter(tag: string | null): void;
  /** Set owner filter. */
  setOwnerFilter(owner: string | null): void;
  /** Handle a WebSocket catalog metadata_updated event. */
  handleMetadataUpdated(fingerprint: string): void;
}

export const useCatalogStore = create<CatalogStoreState & CatalogStoreActions>((set, get) => ({
  entries: [],
  total: 0,
  selectedEntry: null,
  loading: false,
  error: null,
  tags: [],
  owners: [],
  searchQuery: '',
  tagFilter: null,
  ownerFilter: null,

  async fetchEntries(env?) {
    const { searchQuery, tagFilter, ownerFilter } = get();
    set({ loading: true, error: null });
    try {
      const result = await api.listResources({
        q: searchQuery || undefined,
        tag: tagFilter ?? undefined,
        owner: ownerFilter ?? undefined,
        env,
      });
      set({ entries: result.data, total: result.total, loading: false });
    } catch (e) {
      set({ error: (e as Error).message, loading: false });
    }
  },

  async fetchFilterOptions(env?) {
    try {
      const [tags, owners] = await Promise.all([
        api.listTags(env),
        api.listOwners(env),
      ]);
      set({ tags, owners });
    } catch {
      // Silently ignore filter option fetch errors.
    }
  },

  async selectEntry(fingerprint, env?) {
    set({ loading: true, error: null });
    try {
      const entry = await api.getResource(fingerprint, env);
      set({ selectedEntry: entry, loading: false });
    } catch (e) {
      set({ error: (e as Error).message, loading: false });
    }
  },

  clearSelection() {
    set({ selectedEntry: null });
  },

  async updateMetadata(fingerprint, body) {
    const entry = await api.updateMetadata(fingerprint, body);
    // Update in list if present.
    set((s) => ({
      selectedEntry: s.selectedEntry?.fingerprint === fingerprint ? entry : s.selectedEntry,
      entries: s.entries.map((e) => (e.fingerprint === fingerprint ? entry : e)),
    }));
    return entry;
  },

  async scaffoldMetadata(fingerprint?, env?) {
    const result = await api.describeResources({
      fingerprint,
      all: !fingerprint,
      environment: env,
    });
    // Re-fetch entries to pick up new annotations.
    void get().fetchEntries(env);
    return result.created;
  },

  setSearchQuery(q) {
    set({ searchQuery: q });
  },

  setTagFilter(tag) {
    set({ tagFilter: tag });
  },

  setOwnerFilter(owner) {
    set({ ownerFilter: owner });
  },

  handleMetadataUpdated(fingerprint) {
    // Re-fetch the updated entry if it's currently selected.
    const { selectedEntry } = get();
    if (selectedEntry?.fingerprint === fingerprint) {
      void get().selectEntry(fingerprint);
    }
    // Re-fetch the list.
    void get().fetchEntries();
  },
}));
