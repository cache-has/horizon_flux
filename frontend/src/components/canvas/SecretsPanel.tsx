// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { useCallback, useEffect, useState } from 'react';
import {
  getSecretStatus,
  initSecretStore,
  unlockSecrets,
  lockSecrets,
  listSecrets,
  setSecret,
  deleteSecret,
  UnlockedRequiredError,
  type SecretMetadata,
} from '../../api/secrets';
import { ConfirmDialog } from './ConfirmDialog';
import './SecretsPanel.css';

interface SecretsPanelProps {
  open: boolean;
  onClose: () => void;
}

type StoreState = 'loading' | 'not_initialized' | 'locked' | 'unlocked';

export function SecretsPanel({ open, onClose }: SecretsPanelProps) {
  const [storeState, setStoreState] = useState<StoreState>('loading');
  const [secrets, setSecrets] = useState<SecretMetadata[]>([]);
  const [error, setError] = useState<string | null>(null);

  // Auth form state
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [submitting, setSubmitting] = useState(false);

  // Add secret form state
  const [newName, setNewName] = useState('');
  const [newValue, setNewValue] = useState('');
  const [newEnv, setNewEnv] = useState('');
  const [saving, setSaving] = useState(false);

  // Delete confirmation
  const [deleteTarget, setDeleteTarget] = useState<SecretMetadata | null>(null);

  // Fetch status on open
  useEffect(() => {
    if (!open) return;
    setError(null);
    getSecretStatus()
      .then((s) => {
        if (!s.initialized) setStoreState('not_initialized');
        else if (!s.unlocked) setStoreState('locked');
        else setStoreState('unlocked');
      })
      .catch((e) => {
        setError((e as Error).message);
        setStoreState('locked');
      });
  }, [open]);

  const refreshSecrets = useCallback(async () => {
    try {
      const list = await listSecrets();
      setSecrets(list);
      setError(null);
    } catch (e) {
      if (e instanceof UnlockedRequiredError) {
        setStoreState('locked');
      } else {
        setError((e as Error).message);
      }
    }
  }, []);

  // Fetch secrets when unlocked
  useEffect(() => {
    if (storeState !== 'unlocked') return;
    refreshSecrets();
  }, [storeState, refreshSecrets]);

  // Init handler
  const handleInit = useCallback(async () => {
    setError(null);
    if (password.length < 1) {
      setError('Password is required');
      return;
    }
    if (password !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }
    setSubmitting(true);
    try {
      await initSecretStore(password, confirmPassword);
      setStoreState('unlocked');
      setPassword('');
      setConfirmPassword('');
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  }, [password, confirmPassword]);

  // Unlock handler
  const handleUnlock = useCallback(async () => {
    setError(null);
    if (!password) {
      setError('Password is required');
      return;
    }
    setSubmitting(true);
    try {
      await unlockSecrets(password);
      setStoreState('unlocked');
      setPassword('');
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  }, [password]);

  // Lock handler
  const handleLock = useCallback(async () => {
    try {
      await lockSecrets();
      setStoreState('locked');
      setSecrets([]);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  // Add secret handler
  const handleAddSecret = useCallback(async () => {
    setError(null);
    if (!newName.trim()) {
      setError('Secret name is required');
      return;
    }
    if (!newValue) {
      setError('Secret value is required');
      return;
    }
    setSaving(true);
    try {
      await setSecret(newName.trim(), newValue, newEnv || null);
      setNewName('');
      setNewValue('');
      setNewEnv('');
      await refreshSecrets();
    } catch (e) {
      if (e instanceof UnlockedRequiredError) {
        setStoreState('locked');
      } else {
        setError((e as Error).message);
      }
    } finally {
      setSaving(false);
    }
  }, [newName, newValue, newEnv, refreshSecrets]);

  // Delete handler
  const handleConfirmDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await deleteSecret(deleteTarget.name, deleteTarget.environment);
      setDeleteTarget(null);
      await refreshSecrets();
    } catch (e) {
      if (e instanceof UnlockedRequiredError) {
        setStoreState('locked');
      } else {
        setError((e as Error).message);
      }
      setDeleteTarget(null);
    }
  }, [deleteTarget, refreshSecrets]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        if (storeState === 'not_initialized') handleInit();
        else if (storeState === 'locked') handleUnlock();
      }
    },
    [storeState, handleInit, handleUnlock],
  );

  // Deduplicate environment names from existing secrets for the dropdown
  const existingEnvs = Array.from(
    new Set(
      secrets
        .map((s) => s.environment)
        .filter((e): e is string => e !== null && e !== ''),
    ),
  );

  if (!open) return null;

  return (
    <>
      <div className="secrets-panel secrets-panel--open">
        <div className="secrets-panel__header">
          <h3 className="secrets-panel__title">Secrets</h3>
          {storeState === 'unlocked' && (
            <button className="secrets-panel__lock-btn" onClick={handleLock}>
              Lock
            </button>
          )}
          <button className="secrets-panel__close" onClick={onClose}>
            &times;
          </button>
        </div>

        <div className="secrets-panel__body">
          {storeState === 'loading' && (
            <div className="secrets-panel__empty">Loading...</div>
          )}

          {/* Init form */}
          {storeState === 'not_initialized' && (
            <div className="secrets-panel__auth" onKeyDown={handleKeyDown}>
              <h4 className="secrets-panel__auth-title">
                Initialize Secret Store
              </h4>
              <p className="secrets-panel__auth-desc">
                Choose a password to encrypt your secrets. This cannot be
                recovered if lost.
              </p>
              <input
                type="password"
                className="secrets-panel__input"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoFocus
              />
              <input
                type="password"
                className="secrets-panel__input"
                placeholder="Confirm password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
              />
              {error && <p className="secrets-panel__error">{error}</p>}
              <button
                className="secrets-panel__submit"
                onClick={handleInit}
                disabled={submitting}
              >
                {submitting ? 'Initializing...' : 'Initialize'}
              </button>
            </div>
          )}

          {/* Unlock form */}
          {storeState === 'locked' && (
            <div className="secrets-panel__auth" onKeyDown={handleKeyDown}>
              <h4 className="secrets-panel__auth-title">Unlock Secret Store</h4>
              <p className="secrets-panel__auth-desc">
                Enter your password to access secrets.
              </p>
              <input
                type="password"
                className="secrets-panel__input"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoFocus
              />
              {error && <p className="secrets-panel__error">{error}</p>}
              <button
                className="secrets-panel__submit"
                onClick={handleUnlock}
                disabled={submitting}
              >
                {submitting ? 'Unlocking...' : 'Unlock'}
              </button>
            </div>
          )}

          {/* Unlocked: secret list + add form */}
          {storeState === 'unlocked' && (
            <>
              {error && <p className="secrets-panel__error">{error}</p>}

              <div className="secrets-panel__list">
                {secrets.length === 0 && (
                  <div className="secrets-panel__empty">
                    No secrets stored yet.
                  </div>
                )}
                {secrets.map((s) => {
                  const key = `${s.name}::${s.environment ?? ''}`;
                  return (
                    <div className="secrets-panel__item" key={key}>
                      <div className="secrets-panel__item-info">
                        <div className="secrets-panel__item-name">{s.name}</div>
                        <div className="secrets-panel__item-meta">
                          {s.environment ? (
                            <span className="secrets-panel__item-env">
                              {s.environment}
                            </span>
                          ) : (
                            <span className="secrets-panel__item-env">
                              default
                            </span>
                          )}{' '}
                          &middot; {new Date(s.updated_at).toLocaleDateString()}
                        </div>
                      </div>
                      <button
                        className="secrets-panel__item-delete"
                        title="Delete secret"
                        onClick={() => setDeleteTarget(s)}
                      >
                        &times;
                      </button>
                    </div>
                  );
                })}
              </div>

              <div className="secrets-panel__add-section">
                <h4 className="secrets-panel__add-title">Add / Update Secret</h4>
                <div className="secrets-panel__add-form">
                  <div className="secrets-panel__field">
                    <label className="secrets-panel__label">Name</label>
                    <input
                      type="text"
                      className="secrets-panel__input"
                      placeholder="e.g. db_password"
                      value={newName}
                      onChange={(e) => setNewName(e.target.value)}
                    />
                  </div>
                  <div className="secrets-panel__field">
                    <label className="secrets-panel__label">Value</label>
                    <input
                      type="password"
                      className="secrets-panel__input"
                      placeholder="Secret value"
                      value={newValue}
                      onChange={(e) => setNewValue(e.target.value)}
                    />
                  </div>
                  <div className="secrets-panel__field">
                    <label className="secrets-panel__label">
                      Environment (optional)
                    </label>
                    <select
                      className="secrets-panel__select"
                      value={newEnv}
                      onChange={(e) => setNewEnv(e.target.value)}
                    >
                      <option value="">Default (all environments)</option>
                      {existingEnvs.map((env) => (
                        <option key={env} value={env}>
                          {env}
                        </option>
                      ))}
                    </select>
                  </div>
                  <button
                    className="secrets-panel__submit"
                    onClick={handleAddSecret}
                    disabled={saving}
                  >
                    {saving ? 'Saving...' : 'Save Secret'}
                  </button>
                </div>
              </div>
            </>
          )}
        </div>
      </div>

      <ConfirmDialog
        open={deleteTarget !== null}
        title="Delete Secret"
        message={`Delete secret "${deleteTarget?.name ?? ''}"${deleteTarget?.environment ? ` (${deleteTarget.environment})` : ''}? This cannot be undone.`}
        confirmLabel="Delete"
        onConfirm={handleConfirmDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </>
  );
}
