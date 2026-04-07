import { getRecommendations, searchPrices } from './db';

const fallbackMetrics = {
  totalSearches: 0,
  fallbackUsed: 0,
};

export async function searchPricesImproved(
  query: string,
  state: string = '',
  zip: string = ''
): Promise<any[]> {
  const results = await searchPrices(query, state, zip);
  fallbackMetrics.totalSearches += 1;
  if (results.length > 0 && results[0].isFallback) {
    fallbackMetrics.fallbackUsed += 1;
  }
  return results;
}

export function getFallbackMetrics() {
  const fallbackRate = fallbackMetrics.totalSearches > 0
    ? (fallbackMetrics.fallbackUsed / fallbackMetrics.totalSearches) * 100
    : 0;
  return { ...fallbackMetrics, fallbackRate };
}

export function clearSearchCache() {
  // Canonical cache is owned by db.ts; no-op for compatibility.
}

export async function getSearchSuggestions(query: string, limit: number = 8): Promise<any[]> {
  return getRecommendations(query).slice(0, limit).map((r) => ({
    query: r.query,
    code: r.code,
  }));
}
