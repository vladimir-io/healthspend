import { prefetchDatabase } from '../worker';
import { markColdStart } from '../rum';
import { renderMethodology } from '../views/methodology';
import { setupCanvasSearch } from './canvas';
import { setupCptPanel } from './cpt_panel';
import { setupOverlays } from './overlays';
import { setupMicroInteractions } from './micro';
import { setupOnboarding } from './onboarding';
import { setupDatabaseStatusBanner } from './db_status';
import { setupRouting } from './routing';
import { applyUrlParamsOnLoad } from './url_params';
import { performSearch } from './search_controller';
import { navigateToHospitalIssues } from './url_params';
import { setupDynamicYear, setupThemeToggle } from './theme';

export async function bootstrap(): Promise<void> {
  markColdStart();

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => undefined);
  }

  setupThemeToggle();
  setupDynamicYear();
  setupMicroInteractions();
  setupOverlays();
  setupCanvasSearch();
  setupCptPanel();
  setupRouting();
  setupOnboarding();
  setupDatabaseStatusBanner();
  applyUrlParamsOnLoad();

  const input = document.getElementById('search-input');
  input?.addEventListener('focus', () => prefetchDatabase(), { once: false });

  document.querySelectorAll('.shortcut-tag').forEach((tag) => {
    tag.addEventListener('click', () => {
      const query = (tag as HTMLElement).dataset.query;
      if (query && input) {
        (input as HTMLInputElement).value = query;
        void performSearch(query, (document.getElementById('search-state') as HTMLSelectElement)?.value ?? '');
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    });
  });

  document.querySelectorAll('[data-hero-intent]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const intent = (btn as HTMLElement).dataset.heroIntent;
      if (intent) sessionStorage.setItem('hs_hero_intent', intent);
      if (intent === 'cms') {
        navigateToHospitalIssues();
        return;
      }
      window.location.hash = '#search';
      const searchInput = document.getElementById('search-input') as HTMLInputElement | null;
      const welcome = document.getElementById('search-welcome');
      welcome?.classList.add('hidden');
      if (intent === 'bill' && searchInput) {
        const billed = document.getElementById('dispute-intent-billed') as HTMLInputElement | null;
        if (billed) billed.checked = true;
        searchInput.value = 'MRI';
        void performSearch('MRI', (document.getElementById('search-state') as HTMLSelectElement)?.value ?? '');
      } else if (intent === 'shop' && searchInput) {
        searchInput.value = 'Metabolic Panel';
        searchInput.focus();
        void performSearch('Metabolic Panel', (document.getElementById('search-state') as HTMLSelectElement)?.value ?? '');
      }
    });
  });

  const { renderHospitals } = await import('../views/hospitals');
  await renderHospitals('view-hospitals');

  await renderMethodology('view-methodology');
}
