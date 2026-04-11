// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import type { HealthOverview, TimeWindow } from '../api/health';
import * as api from '../api/health';

interface HealthStoreState {
  overview: HealthOverview | null;
  loading: boolean;
  error: string | null;
  window: TimeWindow;
}

interface HealthStoreActions {
  fetchOverview(): Promise<void>;
  setWindow(window: TimeWindow): void;
}

export const useHealthStore = create<HealthStoreState & HealthStoreActions>((set, get) => ({
  overview: null,
  loading: false,
  error: null,
  window: '24h',

  async fetchOverview() {
    const { window } = get();
    set({ loading: true, error: null });
    try {
      const overview = await api.fetchHealthOverview(window);
      set({ overview, loading: false });
    } catch (e) {
      set({ error: (e as Error).message, loading: false });
    }
  },

  setWindow(window) {
    set({ window });
  },
}));
