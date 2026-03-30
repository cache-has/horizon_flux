// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * WebSocket hook that listens for pipeline execution events from the backend
 * and updates node statuses in the pipeline store in real-time.
 */

import { useEffect, useRef } from 'react';
import { usePipelineStore } from '../stores/pipelineStore';

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
}

interface NodeFailedEvent {
  type: 'node_failed';
  run_id: string;
  node_id: string;
  error: string;
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

type ExecutionEvent =
  | RunStartedEvent
  | NodeStartedEvent
  | NodeCompletedEvent
  | NodeFailedEvent
  | RunCompletedEvent;

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
              break;
            case 'node_failed':
              store.setNodeStatus(event.node_id, 'error', event.error);
              break;
            case 'run_completed':
              // Leave node statuses as-is (success/error) so user can see results.
              // Signal that run data is fresh so side panel re-fetches stats.
              store.notifyRunCompleted();
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
