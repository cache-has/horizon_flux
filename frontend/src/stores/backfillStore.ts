// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { create } from 'zustand';
import type {
  BackfillResponse,
  BackfillDetailResponse,
  BackfillProgress,
} from '../api/backfills';
import * as api from '../api/backfills';

interface BackfillStoreState {
  backfills: BackfillResponse[];
  loading: boolean;
  error: string | null;
  /** Currently viewed backfill detail (with iterations). */
  detail: BackfillDetailResponse | null;
  detailLoading: boolean;
}

interface BackfillStoreActions {
  fetchBackfills(pipelineId?: string): Promise<void>;
  fetchDetail(id: string): Promise<void>;
  clearDetail(): void;
  createBackfill(req: api.CreateBackfillRequest): Promise<BackfillResponse>;
  resumeBackfill(id: string): Promise<void>;
  cancelBackfill(id: string): Promise<void>;
  deleteBackfill(id: string): Promise<void>;
  /** Update progress for a running backfill from a WebSocket event. */
  updateProgress(backfillId: string, progress: BackfillProgress): void;
  /** Update iteration status from a WebSocket event. */
  updateIterationStatus(
    backfillId: string,
    iterationIndex: number,
    status: api.IterationStatus,
    extra?: { run_id?: string; error?: string },
  ): void;
  /** Mark a backfill as completed/cancelled from a WebSocket event. */
  updateBackfillStatus(backfillId: string, status: api.BackfillStatus, progress?: BackfillProgress): void;
}

export const useBackfillStore = create<BackfillStoreState & BackfillStoreActions>((set, get) => ({
  backfills: [],
  loading: false,
  error: null,
  detail: null,
  detailLoading: false,

  async fetchBackfills(pipelineId?) {
    set({ loading: true, error: null });
    try {
      const backfills = await api.listBackfills(pipelineId);
      set({ backfills, loading: false });
    } catch (e) {
      set({ error: (e as Error).message, loading: false });
    }
  },

  async fetchDetail(id) {
    set({ detailLoading: true });
    try {
      const detail = await api.getBackfill(id);
      set({ detail, detailLoading: false });
    } catch (e) {
      set({ error: (e as Error).message, detailLoading: false });
    }
  },

  clearDetail() {
    set({ detail: null });
  },

  async createBackfill(req) {
    const resp = await api.createBackfill(req);
    set((s) => ({ backfills: [resp, ...s.backfills] }));
    return resp;
  },

  async resumeBackfill(id) {
    const resp = await api.resumeBackfill(id);
    set((s) => ({
      backfills: s.backfills.map((b) => (b.id === id ? resp : b)),
    }));
  },

  async cancelBackfill(id) {
    const resp = await api.cancelBackfill(id);
    set((s) => ({
      backfills: s.backfills.map((b) => (b.id === id ? resp : b)),
    }));
  },

  async deleteBackfill(id) {
    await api.deleteBackfill(id);
    set((s) => ({
      backfills: s.backfills.filter((b) => b.id !== id),
      detail: s.detail?.id === id ? null : s.detail,
    }));
  },

  updateProgress(backfillId, progress) {
    set((s) => ({
      backfills: s.backfills.map((b) =>
        b.id === backfillId ? { ...b, progress, status: 'running' as const } : b,
      ),
      detail:
        s.detail?.id === backfillId ? { ...s.detail, progress } : s.detail,
    }));
  },

  updateIterationStatus(backfillId, iterationIndex, status, extra) {
    const { detail } = get();
    if (detail?.id !== backfillId) return;
    set({
      detail: {
        ...detail,
        iterations: detail.iterations.map((it) =>
          it.iteration_index === iterationIndex
            ? { ...it, status, run_id: extra?.run_id ?? it.run_id, error: extra?.error ?? it.error }
            : it,
        ),
      },
    });
  },

  updateBackfillStatus(backfillId, status, progress) {
    set((s) => ({
      backfills: s.backfills.map((b) =>
        b.id === backfillId
          ? { ...b, status, progress: progress ?? b.progress }
          : b,
      ),
      detail:
        s.detail?.id === backfillId
          ? { ...s.detail, status, progress: progress ?? s.detail.progress }
          : s.detail,
    }));
  },
}));
