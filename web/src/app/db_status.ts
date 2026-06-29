import { getSharedWorker } from '../worker';

let warmStarted = false;

export function setupDatabaseStatusBanner(): void {
  const banner = document.getElementById('db-status-banner');
  const label = document.getElementById('db-status-label');
  if (!banner || !label) return;

  const show = (text: string) => {
    label.textContent = text;
    banner.classList.remove('hidden');
  };
  const hide = () => {
    banner.classList.add('hidden');
  };

  const warm = () => {
    if (warmStarted) return;
    warmStarted = true;
    show('Loading price database… first search is faster once this finishes.');
    const t0 = performance.now();
    void getSharedWorker()
      .then(() => {
        const ms = performance.now() - t0;
        if (ms > 600) {
          label.textContent = 'Database ready — search anytime.';
          window.setTimeout(hide, 2500);
        } else {
          hide();
        }
      })
      .catch(() => {
        label.textContent = 'Database unavailable — reload or try again later.';
      });
  };

  warm();

  document.getElementById('search-input')?.addEventListener('focus', warm, { once: false });
}
