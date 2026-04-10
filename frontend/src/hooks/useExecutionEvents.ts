// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * WebSocket hook that listens for pipeline execution events from the backend
 * and updates node statuses in the pipeline store in real-time.
 */

import { useEffect, useRef } from 'react';
import type { MaterializationReceipt } from '../api/pipelines';
import type { BackfillProgress } from '../api/backfills';
import { usePipelineStore } from '../stores/pipelineStore';
import { usePluginStore } from '../stores/pluginStore';
import { useTriggerStore } from '../stores/triggerStore';
import { useBackfillStore } from '../stores/backfillStore';
import { useCatalogStore } from '../stores/catalogStore';
import { useColumnLineageStore } from '../stores/columnLineageStore';

/** Matches the backend's ExecutionEvent variants (serialized as tagged JSON). */
interface NodeStartedEvent {
  type: 'node_started';
  run_id: string;
  node_id: string;
}

interface NodeCompletedEvent {
  type: 'node_completed';
  run_id: string;
  node_id: string;
  rows_out: number;
  duration_ms: number;
  /** Doc 27: present for sink nodes that performed a materialized write. */
  materialization_receipt?: MaterializationReceipt;
}

interface NodeFailedEvent {
  type: 'node_failed';
  run_id: string;
  node_id: string;
  error: string;
}

interface TestNodePassedEvent {
  type: 'test_node_passed';
  run_id: string;
  node_id: string;
  assertions_count: number;
}

interface TestNodeFailedEvent {
  type: 'test_node_failed';
  run_id: string;
  node_id: string;
  severity: 'error' | 'warn';
  failures: string[];
}

interface RunCompletedEvent {
  type: 'run_completed';
  run_id: string;
  status: string;
  duration_ms: number;
}

interface RunStartedEvent {
  type: 'run_started';
  run_id: string;
  pipeline_name: string;
}

interface PluginRegistryReloadedEvent {
  type: 'plugin_registry_reloaded';
  count: number;
  ok_count: number;
  invalid_count: number;
}

interface TriggerChangedEvent {
  type: 'trigger_changed';
  trigger_id: string;
  action: string;
}

interface BackfillStartedEvent {
  type: 'backfill_started';
  backfill_id: string;
  total_iterations: number;
}

interface IterationStartedEvent {
  type: 'iteration_started';
  backfill_id: string;
  iteration_index: number;
  iteration_key: string;
}

interface IterationCompletedEvent {
  type: 'iteration_completed';
  backfill_id: string;
  iteration_index: number;
  iteration_key: string;
  run_id: string;
}

interface IterationFailedEvent {
  type: 'iteration_failed';
  backfill_id: string;
  iteration_index: number;
  iteration_key: string;
  error: string;
}

interface IterationSkippedEvent {
  type: 'iteration_skipped';
  backfill_id: string;
  iteration_index: number;
  iteration_key: string;
}

interface BackfillCompletedEvent {
  type: 'backfill_completed';
  backfill_id: string;
  progress: BackfillProgress;
}

interface BackfillCancelledEvent {
  type: 'backfill_cancelled';
  backfill_id: string;
}

interface MetadataUpdatedEvent {
  type: 'metadata_updated';
  fingerprint: string;
}

interface ColumnLineageUpdatedEvent {
  type: 'column_lineage_updated';
  pipeline_id: string;
  environment: string;
  edge_count: number;
}

type ExecutionEvent =
  | RunStartedEvent
  | NodeStartedEvent
  | NodeCompletedEvent
  | NodeFailedEvent
  | TestNodePassedEvent
  | TestNodeFailedEvent
  | RunCompletedEvent
  | PluginRegistryReloadedEvent
  | TriggerChangedEvent
  | BackfillStartedEvent
  | IterationStartedEvent
  | IterationCompletedEvent
  | IterationFailedEvent
  | IterationSkippedEvent
  | BackfillCompletedEvent
  | BackfillCancelledEvent
  | MetadataUpdatedEvent
  | ColumnLineageUpdatedEvent;

export function useExecutionEvents() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    function connect() {
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${protocol}//${location.host}/ws`);
      wsRef.current = ws;

      ws.onopen = () => {
        console.debug('[WS] Connected to execution events');
      };

      ws.onmessage = (msg) => {
        try {
          const event: ExecutionEvent = JSON.parse(msg.data);
          const store = usePipelineStore.getState();

          switch (event.type) {
            case 'run_started':
              // Reset all to idle, then mark as they start
              store.resetNodeStatuses();
              break;
            case 'node_started':
              store.setNodeStatus(event.node_id, 'running');
              break;
            case 'node_completed':
              store.setNodeStatus(event.node_id, 'success');
              if (event.materialization_receipt) {
                store.setNodeReceipt(event.node_id, event.materialization_receipt);
              }
              break;
            case 'node_failed':
              store.setNodeStatus(event.node_id, 'error', event.error);
              break;
            case 'test_node_passed':
              store.setNodeStatus(event.node_id, 'success');
              break;
            case 'test_node_failed':
              store.setNodeStatus(
                event.node_id,
                event.severity === 'warn' ? 'success' : 'error',
                event.failures.join('; '),
              );
              break;
            case 'run_completed':
              // Leave node statuses as-is (success/error) so user can see results.
              // Signal that run data is fresh so side panel re-fetches stats.
              store.notifyRunCompleted();
              break;
            case 'plugin_registry_reloaded':
              // Push notification: refresh the plugin list so the canvas
              // (NodePalette / PluginsPanel) reflects the new registry without
              // requiring the user to reopen the panel.
              void usePluginStore.getState().fetchPlugins();
              break;
            case 'trigger_changed':
              useTriggerStore.getState().handleTriggerChanged(
                event.trigger_id,
                event.action,
              );
              break;
            case 'iteration_started':
              useBackfillStore.getState().updateIterationStatus(
                event.backfill_id,
                event.iteration_index,
                'running',
              );
              break;
            case 'iteration_completed':
              useBackfillStore.getState().updateIterationStatus(
                event.backfill_id,
                event.iteration_index,
                'succeeded',
                { run_id: event.run_id },
              );
              break;
            case 'iteration_failed':
              useBackfillStore.getState().updateIterationStatus(
                event.backfill_id,
                event.iteration_index,
                'failed',
                { error: event.error },
              );
              break;
            case 'iteration_skipped':
              useBackfillStore.getState().updateIterationStatus(
                event.backfill_id,
                event.iteration_index,
                'skipped',
              );
              break;
            case 'backfill_completed':
              useBackfillStore.getState().updateBackfillStatus(
                event.backfill_id,
                'completed',
                event.progress,
              );
              break;
            case 'backfill_cancelled':
              useBackfillStore.getState().updateBackfillStatus(
                event.backfill_id,
                'cancelled',
              );
              break;
            case 'backfill_started':
              // Backfill list will auto-refresh via polling
              break;
            case 'metadata_updated':
              useCatalogStore.getState().handleMetadataUpdated(event.fingerprint);
              break;
            case 'column_lineage_updated':
              useColumnLineageStore.getState().handleLineageUpdated(event.pipeline_id);
              break;
          }
        } catch {
          // Ignore non-JSON messages
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        // Reconnect after a delay
        reconnectRef.current = setTimeout(connect, 3000);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      if (reconnectRef.current) clearTimeout(reconnectRef.current);
      wsRef.current?.close();
    };
  }, []);
}
