// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

import { createContext, useContext } from 'react';

/**
 * Callback to navigate to the catalog detail view for a given resource
 * fingerprint. Provided by PipelineCanvas, consumed by PipelineNode's
 * resource badge.
 */
export type CatalogNavigationFn = (fingerprint: string) => void;

export const CatalogNavigationContext = createContext<CatalogNavigationFn | null>(null);

export function useCatalogNavigation(): CatalogNavigationFn | null {
  return useContext(CatalogNavigationContext);
}
