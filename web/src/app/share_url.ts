/** Build a shareable search URL (no imports from search_controller — avoids circular deps). */
export function buildSearchShareUrl(query: string, state: string): string {
  const url = new URL(window.location.origin + window.location.pathname);
  url.searchParams.set('q', query);
  if (state) url.searchParams.set('state', state);
  url.hash = '#search';
  return url.toString();
}
