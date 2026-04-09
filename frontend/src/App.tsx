// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react';
import { PipelineCanvas } from './components/canvas';
import { ProjectLineageView } from './components/lineage/ProjectLineageView';
import { ProjectTriggersView } from './components/triggers/ProjectTriggersView';
import { CatalogView } from './components/catalog/CatalogView';
import { usePipelineStore } from './stores/pipelineStore';
import { useCatalogStore } from './stores/catalogStore';
import { useEnvironmentStore } from './stores/environmentStore';
import { useExecutionEvents } from './hooks/useExecutionEvents';
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

type ViewMode = 'pipeline' | 'project' | 'triggers' | 'catalog';

function App() {
  const loadFromResponse = usePipelineStore((s) => s.loadFromResponse);
  const loadPipeline = usePipelineStore((s) => s.loadPipeline);
  const setActiveEnvironment = useEnvironmentStore((s) => s.setActiveEnvironment);
  const initRef = useRef(false);
  const [viewMode, setViewMode] = useState<ViewMode>('pipeline');

  // Connect to backend WebSocket for real-time execution status updates.
  useExecutionEvents();

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    // Try to load the first pipeline from the backend; fall back to demo data
    listPipelines(1, 0)
      .then((res) => {
        if (res.data.length > 0) {
          loadPipeline(res.data[0].id).then(() => {
            const pipeline = usePipelineStore.getState().apiPipeline;
            if (pipeline) {
              setActiveEnvironment(pipeline.default_environment);
            }
          });
        } else {
          loadFromResponse(DEMO_RESPONSE);
          setActiveEnvironment(DEMO_RESPONSE.pipeline.default_environment);
        }
      })
      .catch(() => {
        loadFromResponse(DEMO_RESPONSE);
        setActiveEnvironment(DEMO_RESPONSE.pipeline.default_environment);
      });
  }, [loadFromResponse, loadPipeline, setActiveEnvironment]);

  const handleNavigateToPipeline = useCallback(
    async (id: string) => {
      await loadPipeline(id);
      const pipeline = usePipelineStore.getState().apiPipeline;
      if (pipeline) {
        setActiveEnvironment(pipeline.default_environment);
      }
      setViewMode('pipeline');
    },
    [loadPipeline, setActiveEnvironment],
  );

  const handleLineageClick = useCallback(() => {
    setViewMode('project');
  }, []);

  const handleCatalogClick = useCallback((fingerprint?: string) => {
    if (fingerprint) {
      useCatalogStore.getState().selectEntry(fingerprint);
    }
    setViewMode('catalog');
  }, []);

  const handleBackToPipeline = useCallback(() => {
    setViewMode('pipeline');
  }, []);

  if (viewMode === 'catalog') {
    return (
      <CatalogView
        onBack={handleBackToPipeline}
        onNavigateToPipeline={handleNavigateToPipeline}
      />
    );
  }

  if (viewMode === 'project') {
    return (
      <ProjectLineageView
        onBack={handleBackToPipeline}
        onNavigateToPipeline={handleNavigateToPipeline}
      />
    );
  }

  if (viewMode === 'triggers') {
    return (
      <ProjectTriggersView
        onBack={handleBackToPipeline}
        onNavigateToPipeline={handleNavigateToPipeline}
      />
    );
  }

  return (
    <PipelineCanvas
      onLineageClick={handleLineageClick}
      onCatalogClick={handleCatalogClick}
      onNavigateToPipeline={handleNavigateToPipeline}
    />
  );
}

export default App;
