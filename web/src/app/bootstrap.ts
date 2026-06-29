import { markColdStart } from '../rum';
import { setupCanvasSearch } from './canvas';
import { setupCptPanel } from './cpt_panel';
import { setupOverlays } from './overlays';
import { setupMicroInteractions } from './micro';
import { setupHelp } from './help';
import { setupHeroIntents } from './hero_intents';
import { setupDataFreshness } from './data_freshness';
import { setupDatabaseStatusBanner } from './db_status';
import { warmDatabaseNow } from './db_warm';
import { setupRouting } from './routing';
import { applyUrlParamsOnLoad } from './url_params';
import { setupDynamicYear, setupThemeToggle } from './theme';

export function bootstrap(): void {
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
  setupDataFreshness();

  warmDatabaseNow();
  applyUrlParamsOnLoad();

  const input = document.getElementById('search-input');
  input?.addEventListener('focus', () => warmDatabaseNow(), { once: false });
}
