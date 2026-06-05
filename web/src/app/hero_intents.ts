import { performSearch } from './search_controller';
import { navigateToHospitalIssues } from './url_params';

export function setupHeroIntents(): void {
  document.querySelectorAll('[data-hero-intent]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const intent = (btn as HTMLElement).dataset.heroIntent;
      if (intent) sessionStorage.setItem('hs_hero_intent', intent);

      if (intent === 'cms') {
        navigateToHospitalIssues();
        return;
      }

      window.location.hash = '#search';
      document.getElementById('search-welcome')?.classList.add('hidden');

      const searchInput = document.getElementById('search-input') as HTMLInputElement | null;
      const stateSelect = document.getElementById('search-state') as HTMLSelectElement | null;
      const state = stateSelect?.value ?? '';

      if (intent === 'bill' && searchInput) {
        const billed = document.getElementById('dispute-intent-billed') as HTMLInputElement | null;
        if (billed) billed.checked = true;
        searchInput.value = 'MRI';
        void performSearch('MRI', state);
      } else if (intent === 'shop' && searchInput) {
        searchInput.value = 'Metabolic Panel';
        searchInput.focus();
        void performSearch('Metabolic Panel', state);
      }
    });
  });
}
