import { setupNavigation } from './nav';
import { performSearch } from './search_controller';

export function setupRouting(): void {
  setupNavigation();

  const input = document.getElementById('search-input') as HTMLInputElement;
  const stateSelect = document.getElementById('search-state') as HTMLSelectElement;
  const recommendationEl = document.getElementById('search-recommendations') as HTMLDivElement;

  const welcome = document.getElementById('search-welcome');
  const summary = document.getElementById('results-summary');
  if (summary && !sessionStorage.getItem('hs_hero_intent')) {
    summary.textContent = 'Search a procedure to compare published cash rates near you.';
  }
  welcome?.classList.remove('hidden');

  document.querySelectorAll('.search-tag').forEach((tag) => {
    tag.addEventListener('click', () => {
      const q = tag.textContent || '';
      if (input) input.value = q;
      recommendationEl?.classList.add('hidden');
      void performSearch(q, stateSelect?.value ?? '');
    });
  });
}
