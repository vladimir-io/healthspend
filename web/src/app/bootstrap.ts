import { prefetchDatabase } from '../worker';
import { markColdStart } from '../rum';
import { renderMethodology } from '../views/methodology';
import { setupCanvasSearch } from './canvas';
import { setupCptPanel } from './cpt_panel';
import { setupOverlays } from './overlays';
import { setupMicroInteractions } from './micro';
import { setupHelp } from './help';
import { setupHeroIntents } from './hero_intents';
import { setupDatabaseStatusBanner } from './db_status';
import { setupRouting } from './routing';
import { applyUrlParamsOnLoad } from './url_params';
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
  setupHelp();
  setupHeroIntents();
  setupDatabaseStatusBanner();
  prefetchDatabase();
  applyUrlParamsOnLoad();

  const input = document.getElementById('search-input');
  input?.addEventListener('focus', () => prefetchDatabase(), { once: false });

  const { renderHospitals } = await import('../views/hospitals');
  await renderHospitals('view-hospitals');

  await renderMethodology('view-methodology');
}
