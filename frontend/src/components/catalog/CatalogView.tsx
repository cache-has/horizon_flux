// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

/**
 * Project-level view showing the resource catalog — a searchable, browseable
 * view of tables, files, and other resources that armillary pipelines produce and
 * consume (planning doc 34).
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useCatalogStore } from '../../stores/catalogStore';
import { useEnvironmentStore } from '../../stores/environmentStore';
import type { CatalogEntry, MergedColumn, MetadataUpdateRequest } from '../../api/catalog';
import { ImpactAnalysisModal } from '../lineage/ImpactAnalysisModal';
import { ColumnLineageGraph } from '../lineage/ColumnLineageGraph';
import './CatalogView.css';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface CatalogViewProps {
  onBack: () => void;
  onNavigateToPipeline?: (id: string) => void;
}

// ---------------------------------------------------------------------------
// Main view — switches between list and detail
// ---------------------------------------------------------------------------

export function CatalogView({ onBack, onNavigateToPipeline }: CatalogViewProps) {
  const selectedEntry = useCatalogStore((s) => s.selectedEntry);
  const clearSelection = useCatalogStore((s) => s.clearSelection);

  if (selectedEntry) {
    return (
      <ResourceDetail
        entry={selectedEntry}
        onBack={clearSelection}
        onNavigateToPipeline={onNavigateToPipeline}
      />
    );
  }

  return <ResourceList onBack={onBack} onNavigateToPipeline={onNavigateToPipeline} />;
}

// ---------------------------------------------------------------------------
// Resource List
// ---------------------------------------------------------------------------

function ResourceList({ onBack, onNavigateToPipeline }: CatalogViewProps) {
  const entries = useCatalogStore((s) => s.entries);
  const loading = useCatalogStore((s) => s.loading);
  const tags = useCatalogStore((s) => s.tags);
  const owners = useCatalogStore((s) => s.owners);
  const searchQuery = useCatalogStore((s) => s.searchQuery);
  const tagFilter = useCatalogStore((s) => s.tagFilter);
  const ownerFilter = useCatalogStore((s) => s.ownerFilter);
  const fetchEntries = useCatalogStore((s) => s.fetchEntries);
  const fetchFilterOptions = useCatalogStore((s) => s.fetchFilterOptions);
  const setSearchQuery = useCatalogStore((s) => s.setSearchQuery);
  const setTagFilter = useCatalogStore((s) => s.setTagFilter);
  const setOwnerFilter = useCatalogStore((s) => s.setOwnerFilter);
  const selectEntry = useCatalogStore((s) => s.selectEntry);
  const scaffoldMetadata = useCatalogStore((s) => s.scaffoldMetadata);
  const activeEnv = useEnvironmentStore((s) => s.activeEnvironment);

  const [scaffolding, setScaffolding] = useState(false);
  const [scaffoldResult, setScaffoldResult] = useState<string | null>(null);

  useEffect(() => {
    void fetchEntries(activeEnv);
    void fetchFilterOptions(activeEnv);
  }, [fetchEntries, fetchFilterOptions, activeEnv]);

  // Re-fetch when filters change.
  useEffect(() => {
    void fetchEntries(activeEnv);
  }, [searchQuery, tagFilter, ownerFilter, fetchEntries, activeEnv]);

  const [sortField, setSortField] = useState<'name' | 'type' | 'updated'>('name');
  const [sortAsc, setSortAsc] = useState(true);

  const sorted = useMemo(() => {
    const arr = [...entries];
    arr.sort((a, b) => {
      let cmp = 0;
      if (sortField === 'name') {
        cmp = a.name.localeCompare(b.name);
      } else if (sortField === 'type') {
        cmp = (a.derived.resource_type ?? '').localeCompare(b.derived.resource_type ?? '');
      } else if (sortField === 'updated') {
        cmp = (a.derived.last_updated ?? '').localeCompare(b.derived.last_updated ?? '');
      }
      return sortAsc ? cmp : -cmp;
    });
    return arr;
  }, [entries, sortField, sortAsc]);

  const handleSort = useCallback(
    (field: 'name' | 'type' | 'updated') => {
      if (sortField === field) {
        setSortAsc((a) => !a);
      } else {
        setSortField(field);
        setSortAsc(true);
      }
    },
    [sortField],
  );

  const handleScaffoldAll = useCallback(async () => {
    setScaffolding(true);
    setScaffoldResult(null);
    try {
      const created = await scaffoldMetadata(undefined, activeEnv);
      setScaffoldResult(
        created.length > 0
          ? `Scaffolded ${created.length} metadata file(s).`
          : 'All resources already have metadata files.',
      );
    } catch (e) {
      setScaffoldResult(`Error: ${(e as Error).message}`);
    } finally {
      setScaffolding(false);
    }
  }, [scaffoldMetadata]);

  const sortIndicator = (field: string) => {
    if (sortField !== field) return '';
    return sortAsc ? ' \u25B2' : ' \u25BC';
  };

  return (
    <div className="catalog-view">
      <div className="catalog-view__toolbar">
        <button className="catalog-view__back-btn" onClick={onBack}>
          Back
        </button>
        <span className="catalog-view__title">Resource Catalog</span>
        <span className="catalog-view__count">{entries.length} resources</span>
        <button
          className="catalog-view__scaffold-btn"
          onClick={handleScaffoldAll}
          disabled={scaffolding}
          title="Scaffold metadata files for all undocumented resources"
        >
          {scaffolding ? 'Scaffolding...' : 'Scaffold All'}
        </button>
      </div>

      <div className="catalog-view__filters">
        <input
          className="catalog-view__search"
          type="text"
          placeholder="Search resources..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
        />

        <span className="catalog-view__filter-label">Tag:</span>
        <select
          className="catalog-view__filter-select"
          value={tagFilter ?? ''}
          onChange={(e) => setTagFilter(e.target.value || null)}
        >
          <option value="">All</option>
          {tags.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>

        <span className="catalog-view__filter-label">Owner:</span>
        <select
          className="catalog-view__filter-select"
          value={ownerFilter ?? ''}
          onChange={(e) => setOwnerFilter(e.target.value || null)}
        >
          <option value="">All</option>
          {owners.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </div>

      {scaffoldResult && (
        <div className="catalog-view__scaffold-result">{scaffoldResult}</div>
      )}

      <div className="catalog-view__body">
        {loading && entries.length === 0 && (
          <div className="catalog-view__loading">Loading catalog...</div>
        )}
        {!loading && sorted.length === 0 && (
          <div className="catalog-view__empty">
            {entries.length === 0
              ? 'No resources discovered. Run a pipeline to populate the catalog.'
              : 'No resources match the current filters.'}
          </div>
        )}

        {sorted.length > 0 && (
          <table className="catalog-view__table">
            <thead>
              <tr>
                <th className="catalog-view__th" onClick={() => handleSort('name')}>
                  Name{sortIndicator('name')}
                </th>
                <th className="catalog-view__th" onClick={() => handleSort('type')}>
                  Type{sortIndicator('type')}
                </th>
                <th className="catalog-view__th">Tags</th>
                <th className="catalog-view__th">Owner</th>
                <th className="catalog-view__th">Producers</th>
                <th className="catalog-view__th">Consumers</th>
                <th className="catalog-view__th" onClick={() => handleSort('updated')}>
                  Last Updated{sortIndicator('updated')}
                </th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((entry) => (
                <tr
                  key={entry.fingerprint}
                  className="catalog-view__row"
                  onClick={() => selectEntry(entry.fingerprint, activeEnv)}
                >
                  <td className="catalog-view__td catalog-view__td--name">
                    <div className="catalog-view__entry-name">{entry.name}</div>
                    <div className="catalog-view__entry-fp">{entry.fingerprint}</div>
                  </td>
                  <td className="catalog-view__td">
                    <span className="catalog-view__type-badge">
                      {entry.derived.resource_type ?? 'unknown'}
                    </span>
                  </td>
                  <td className="catalog-view__td">
                    {entry.tags.map((t) => (
                      <span key={t} className="catalog-view__tag">{t}</span>
                    ))}
                  </td>
                  <td className="catalog-view__td">
                    {entry.owner?.team ?? '\u2014'}
                  </td>
                  <td className="catalog-view__td">
                    {entry.derived.producers.map((p) => (
                      <span
                        key={`${p.pipeline_id}:${p.node_id}`}
                        className="catalog-view__pipeline-link"
                        onClick={(e) => {
                          e.stopPropagation();
                          onNavigateToPipeline?.(p.pipeline_id);
                        }}
                      >
                        {p.pipeline_id}
                      </span>
                    ))}
                    {entry.derived.producers.length === 0 && '\u2014'}
                  </td>
                  <td className="catalog-view__td">
                    {entry.derived.consumers.map((c) => (
                      <span
                        key={`${c.pipeline_id}:${c.node_id}`}
                        className="catalog-view__pipeline-link"
                        onClick={(e) => {
                          e.stopPropagation();
                          onNavigateToPipeline?.(c.pipeline_id);
                        }}
                      >
                        {c.pipeline_id}
                      </span>
                    ))}
                    {entry.derived.consumers.length === 0 && '\u2014'}
                  </td>
                  <td className="catalog-view__td">
                    {entry.derived.last_updated
                      ? new Date(entry.derived.last_updated).toLocaleString()
                      : '\u2014'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Resource Detail
// ---------------------------------------------------------------------------

function ResourceDetail({
  entry,
  onBack,
  onNavigateToPipeline,
}: {
  entry: CatalogEntry;
  onBack: () => void;
  onNavigateToPipeline?: (id: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [impactColumn, setImpactColumn] = useState<string | null>(null);
  const [lineageColumn, setLineageColumn] = useState<string | null>(null);
  const activeEnv = useEnvironmentStore((s) => s.activeEnvironment);

  return (
    <div className="catalog-view">
      <div className="catalog-view__toolbar">
        <button className="catalog-view__back-btn" onClick={onBack}>
          Back to List
        </button>
        <span className="catalog-view__title">{entry.name}</span>
        <span className="catalog-view__type-badge">
          {entry.derived.resource_type ?? 'unknown'}
        </span>
        <div className="catalog-view__spacer" />
        <button
          className="catalog-view__edit-btn"
          onClick={() => setEditing((e) => !e)}
        >
          {editing ? 'Cancel Edit' : 'Edit Metadata'}
        </button>
      </div>

      <div className="catalog-view__detail-body">
        {editing ? (
          <AnnotationEditor
            entry={entry}
            onSaved={() => setEditing(false)}
          />
        ) : (
          <>
            <DetailSummary entry={entry} onNavigateToPipeline={onNavigateToPipeline} />
            <SchemaTable
              columns={entry.columns}
              fingerprint={entry.fingerprint}
              onShowImpact={setImpactColumn}
              onShowLineage={setLineageColumn}
            />
            {lineageColumn && (
              <div className="catalog-view__section">
                <ColumnLineageGraph
                  fingerprint={entry.fingerprint}
                  column={lineageColumn}
                  environment={activeEnv}
                  onClose={() => setLineageColumn(null)}
                  onNavigateToPipeline={onNavigateToPipeline}
                />
              </div>
            )}

            {impactColumn && (
              <ImpactAnalysisModal
                fingerprint={entry.fingerprint}
                column={impactColumn}
                onClose={() => setImpactColumn(null)}
                onNavigateToPipeline={onNavigateToPipeline}
              />
            )}
            {Object.keys(entry.custom).length > 0 && (
              <div className="catalog-view__section">
                <h3 className="catalog-view__section-title">Custom Metadata</h3>
                <div className="catalog-view__custom-grid">
                  {Object.entries(entry.custom).map(([k, v]) => (
                    <div key={k} className="catalog-view__custom-item">
                      <span className="catalog-view__custom-key">{k}</span>
                      <span className="catalog-view__custom-value">{String(v)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {entry.annotation_file && (
        <div className="catalog-view__file-banner">
          Backed by <code>{entry.annotation_file}</code> — changes are written to this
          file and should go through git review.
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Detail Summary
// ---------------------------------------------------------------------------

function DetailSummary({
  entry,
  onNavigateToPipeline,
}: {
  entry: CatalogEntry;
  onNavigateToPipeline?: (id: string) => void;
}) {
  return (
    <div className="catalog-view__summary">
      <div className="catalog-view__summary-grid">
        <div className="catalog-view__summary-item">
          <span className="catalog-view__summary-label">Fingerprint</span>
          <code className="catalog-view__summary-value">{entry.fingerprint}</code>
        </div>

        {entry.description && (
          <div className="catalog-view__summary-item catalog-view__summary-item--wide">
            <span className="catalog-view__summary-label">Description</span>
            <span className="catalog-view__summary-value">{entry.description}</span>
          </div>
        )}

        {entry.owner && (
          <div className="catalog-view__summary-item">
            <span className="catalog-view__summary-label">Owner</span>
            <span className="catalog-view__summary-value">
              {entry.owner.team ?? ''}
              {entry.owner.contact && ` (${entry.owner.contact})`}
            </span>
          </div>
        )}

        {entry.tags.length > 0 && (
          <div className="catalog-view__summary-item">
            <span className="catalog-view__summary-label">Tags</span>
            <span className="catalog-view__summary-value">
              {entry.tags.map((t) => (
                <span key={t} className="catalog-view__tag">{t}</span>
              ))}
            </span>
          </div>
        )}

        {entry.environment && (
          <div className="catalog-view__summary-item">
            <span className="catalog-view__summary-label">Environment</span>
            <span className="catalog-view__summary-value">{entry.environment}</span>
          </div>
        )}

        {entry.derived.last_updated && (
          <div className="catalog-view__summary-item">
            <span className="catalog-view__summary-label">Last Updated</span>
            <span className="catalog-view__summary-value">
              {new Date(entry.derived.last_updated).toLocaleString()}
            </span>
          </div>
        )}

        {entry.derived.row_count != null && (
          <div className="catalog-view__summary-item">
            <span className="catalog-view__summary-label">Row Count</span>
            <span className="catalog-view__summary-value">
              {entry.derived.row_count.toLocaleString()}
            </span>
          </div>
        )}

        {entry.derived.size_bytes != null && (
          <div className="catalog-view__summary-item">
            <span className="catalog-view__summary-label">Size</span>
            <span className="catalog-view__summary-value">
              {formatBytes(entry.derived.size_bytes)}
            </span>
          </div>
        )}
      </div>

      {(entry.derived.producers.length > 0 || entry.derived.consumers.length > 0) && (
        <div className="catalog-view__section">
          <h3 className="catalog-view__section-title">Pipelines</h3>
          <div className="catalog-view__pipelines-grid">
            {entry.derived.producers.length > 0 && (
              <div>
                <span className="catalog-view__pipeline-direction">Producers</span>
                {entry.derived.producers.map((p) => (
                  <span
                    key={`${p.pipeline_id}:${p.node_id}`}
                    className="catalog-view__pipeline-link"
                    onClick={() => onNavigateToPipeline?.(p.pipeline_id)}
                  >
                    {p.pipeline_id} ({p.node_id})
                  </span>
                ))}
              </div>
            )}
            {entry.derived.consumers.length > 0 && (
              <div>
                <span className="catalog-view__pipeline-direction">Consumers</span>
                {entry.derived.consumers.map((c) => (
                  <span
                    key={`${c.pipeline_id}:${c.node_id}`}
                    className="catalog-view__pipeline-link"
                    onClick={() => onNavigateToPipeline?.(c.pipeline_id)}
                  >
                    {c.pipeline_id} ({c.node_id})
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Schema Table (merged auto-detected + annotated columns)
// ---------------------------------------------------------------------------

function SchemaTable({
  columns,
  fingerprint,
  onShowImpact,
  onShowLineage,
}: {
  columns: MergedColumn[];
  fingerprint?: string;
  onShowImpact?: (column: string) => void;
  onShowLineage?: (column: string) => void;
}) {
  if (columns.length === 0) return null;

  return (
    <div className="catalog-view__section">
      <h3 className="catalog-view__section-title">Schema ({columns.length} columns)</h3>
      <table className="catalog-view__schema-table">
        <thead>
          <tr>
            <th>Column</th>
            <th>Type</th>
            <th>Nullable</th>
            <th>Description</th>
            <th>Accepted Values</th>
            <th>Lineage</th>
          </tr>
        </thead>
        <tbody>
          {columns.map((col) => (
            <tr key={col.name}>
              <td className="catalog-view__col-name">{col.name}</td>
              <td className="catalog-view__col-type">{col.data_type ?? '\u2014'}</td>
              <td>{col.nullable == null ? '\u2014' : col.nullable ? 'Yes' : 'No'}</td>
              <td>{col.description ?? ''}</td>
              <td>
                {col.accepted_values?.map((v) => (
                  <code key={v} className="catalog-view__accepted-value">{v}</code>
                ))}
              </td>
              <td className="catalog-view__col-actions">
                {fingerprint && (
                  <>
                    <button
                      className="catalog-view__col-action-btn"
                      title="Show upstream/downstream lineage"
                      onClick={() => onShowLineage?.(col.name)}
                    >
                      &#8644;
                    </button>
                    <button
                      className="catalog-view__col-action-btn"
                      title="Impact analysis: what breaks?"
                      onClick={() => onShowImpact?.(col.name)}
                    >
                      &#9888;
                    </button>
                  </>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Inline Annotation Editor
// ---------------------------------------------------------------------------

function AnnotationEditor({
  entry,
  onSaved,
}: {
  entry: CatalogEntry;
  onSaved: () => void;
}) {
  const updateMetadata = useCatalogStore((s) => s.updateMetadata);

  const [name, setName] = useState(entry.name);
  const [description, setDescription] = useState(entry.description ?? '');
  const [ownerTeam, setOwnerTeam] = useState(entry.owner?.team ?? '');
  const [ownerContact, setOwnerContact] = useState(entry.owner?.contact ?? '');
  const [tagsStr, setTagsStr] = useState(entry.tags.join(', '));
  const [columnDescs, setColumnDescs] = useState<Record<string, string>>(() => {
    const descs: Record<string, string> = {};
    for (const col of entry.columns) {
      if (col.description) descs[col.name] = col.description;
    }
    return descs;
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSave = useCallback(async () => {
    setSaving(true);
    setError(null);
    try {
      const body: MetadataUpdateRequest = {
        name: name || undefined,
        description: description || undefined,
        owner:
          ownerTeam || ownerContact
            ? { team: ownerTeam || undefined, contact: ownerContact || undefined }
            : undefined,
        tags: tagsStr
          .split(',')
          .map((t) => t.trim())
          .filter(Boolean),
        columns: Object.fromEntries(
          Object.entries(columnDescs)
            .filter(([, desc]) => desc)
            .map(([col, desc]) => [col, { description: desc }]),
        ),
      };
      await updateMetadata(entry.fingerprint, body);
      onSaved();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [name, description, ownerTeam, ownerContact, tagsStr, columnDescs, entry.fingerprint, updateMetadata, onSaved]);

  return (
    <div className="catalog-view__editor">
      <div className="catalog-view__editor-field">
        <label>Name</label>
        <input type="text" value={name} onChange={(e) => setName(e.target.value)} />
      </div>

      <div className="catalog-view__editor-field">
        <label>Description</label>
        <textarea
          rows={3}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
      </div>

      <div className="catalog-view__editor-row">
        <div className="catalog-view__editor-field">
          <label>Owner Team</label>
          <input type="text" value={ownerTeam} onChange={(e) => setOwnerTeam(e.target.value)} />
        </div>
        <div className="catalog-view__editor-field">
          <label>Owner Contact</label>
          <input type="text" value={ownerContact} onChange={(e) => setOwnerContact(e.target.value)} />
        </div>
      </div>

      <div className="catalog-view__editor-field">
        <label>Tags (comma-separated)</label>
        <input type="text" value={tagsStr} onChange={(e) => setTagsStr(e.target.value)} />
      </div>

      {entry.columns.length > 0 && (
        <div className="catalog-view__editor-field">
          <label>Column Descriptions</label>
          <div className="catalog-view__editor-columns">
            {entry.columns.map((col) => (
              <div key={col.name} className="catalog-view__editor-col-row">
                <code className="catalog-view__editor-col-name">{col.name}</code>
                <input
                  type="text"
                  placeholder="Description..."
                  value={columnDescs[col.name] ?? ''}
                  onChange={(e) =>
                    setColumnDescs((prev) => ({ ...prev, [col.name]: e.target.value }))
                  }
                />
              </div>
            ))}
          </div>
        </div>
      )}

      {error && <div className="catalog-view__editor-error">{error}</div>}

      <div className="catalog-view__editor-actions">
        <button
          className="catalog-view__save-btn"
          onClick={handleSave}
          disabled={saving}
        >
          {saving ? 'Saving...' : 'Save Metadata'}
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
