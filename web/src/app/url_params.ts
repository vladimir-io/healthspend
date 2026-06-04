import { performSearch } from './search_controller';
import { navigateTo, setHospitalsTab } from './nav';

/** Apply ?q= &state= &tab= from the URL on first load. */
export function applyUrlParamsOnLoad(): void {
  const params = new URLSearchParams(window.location.search);
  const q = params.get('q')?.trim();
  const state = params.get('state')?.trim()?.toUpperCase() ?? '';
  const tab = params.get('tab');
  const hash = window.location.hash || '#search';

  if (tab === 'failing') {
    setHospitalsTab('failing');
  }

  if (hash === '#hospitals' || hash === '#incidents' || hash === '#scorecard') {
    return;
  }

  if (!q) return;

  const input = document.getElementById('search-input') as HTMLInputElement | null;
  const stateSelect = document.getElementById('search-state') as HTMLSelectElement | null;
  if (input) input.value = q;
  if (state && stateSelect) {
    const opt = [...stateSelect.options].find((o) => o.value === state);
    if (opt) stateSelect.value = state;
  }

  document.getElementById('search-welcome')?.classList.add('hidden');
  window.location.hash = '#search';
  void performSearch(q, stateSelect?.value ?? '');
}

export function buildSearchShareUrl(query: string, state: string): string {
  const url = new URL(window.location.origin + window.location.pathname);
  url.searchParams.set('q', query);
  if (state) url.searchParams.set('state', state);
  url.hash = '#search';
  return url.toString();
}

export function navigateToHospitalIssues(): void {
  navigateTo('hospitals', { hospitalsTab: 'failing' });
}
