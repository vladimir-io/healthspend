import { recordPageView } from '../rum';
import { ensureDeferredView } from './deferred_views';
import { setupNavigation } from './nav';
import { performSearch } from './search_controller';

export function setupRouting(): void {
  setupNavigation((route) => {
    recordPageView(route);
    void ensureDeferredView(route);
  });

  const input = document.getElementById('search-input') as HTMLInputElement | null;
  const stateSelect = document.getElementById('search-state') as HTMLSelectElement | null;
  const recommendationEl = document.getElementById('search-recommendations') as HTMLDivElement | null;

  const welcome = document.getElementById('search-welcome');
  const summary = document.getElementById('results-summary');
  if (summary && !sessionStorage.getItem('hs_hero_intent')) {
    summary.textContent = 'Search a procedure to compare published cash rates near you.';
  }
  welcome?.classList.remove('hidden');

  document.querySelectorAll('.shortcut-tag').forEach((tag) => {
    tag.addEventListener('click', () => {
      const query = (tag as HTMLElement).dataset.query;
      if (!query || !input) return;
      input.value = query;
      recommendationEl?.classList.add('hidden');
      void performSearch(query, stateSelect?.value ?? '');
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  });
}
