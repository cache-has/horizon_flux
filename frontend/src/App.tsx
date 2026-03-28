// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useEffect, useRef } from 'react';
import { PipelineCanvas } from './components/canvas';
import { usePipelineStore } from './stores/pipelineStore';
import { listPipelines } from './api/pipelines';
import type { ApiPipelineResponse } from './api/pipelines';

/** Demo pipeline used when no backend pipelines exist. */
const DEMO_RESPONSE: ApiPipelineResponse = {
  id: 'demo',
  pipeline: {
    name: 'demo-pipeline',
    version: 1,
    default_environment: 'dev',
    variables: {},
    environment_overrides: {},
    nodes: [
      {
        id: 'source-1',
        name: 'CSV Import',
        type: 'source',
        connector: 'csv',
        config: {},
        position: { x: 0, y: 0 },
        pinned_position: false,
      },
      {
        id: 'transform-1',
        name: 'Filter Rows',
        type: 'transform',
        mode: 'sql',
        code: '',
        materialized: false,
        position: { x: 0, y: 0 },
        pinned_position: false,
      },
      {
        id: 'sink-1',
        name: 'PostgreSQL',
        type: 'sink',
        connector: 'postgresql',
        config: {},
        position: { x: 0, y: 0 },
        pinned_position: false,
      },
    ],
    edges: [
      { from: 'source-1', to: 'transform-1' },
      { from: 'transform-1', to: 'sink-1' },
    ],
  },
  created_at: Date.now(),
  updated_at: Date.now(),
};

function App() {
  const loadFromResponse = usePipelineStore((s) => s.loadFromResponse);
  const loadPipeline = usePipelineStore((s) => s.loadPipeline);
  const initRef = useRef(false);

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    // Try to load the first pipeline from the backend; fall back to demo data
    listPipelines(1, 0)
      .then((res) => {
        if (res.data.length > 0) {
          loadPipeline(res.data[0].id);
        } else {
          loadFromResponse(DEMO_RESPONSE);
        }
      })
      .catch(() => {
        loadFromResponse(DEMO_RESPONSE);
      });
  }, [loadFromResponse, loadPipeline]);

  return <PipelineCanvas />;
}

export default App;
