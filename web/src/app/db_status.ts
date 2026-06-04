import { getSharedWorker } from '../worker';

let bannerShown = false;

export function setupDatabaseStatusBanner(): void {
  const banner = document.getElementById('db-status-banner');
  const label = document.getElementById('db-status-label');
  if (!banner || !label) return;

  const show = (text: string) => {
    label.textContent = text;
    banner.classList.remove('hidden');
    bannerShown = true;
  };
  const hide = () => {
    banner.classList.add('hidden');
  };

  document.getElementById('search-input')?.addEventListener(
    'focus',
    () => {
      if (bannerShown) return;
      show('Loading price database… first search may take a few seconds.');
      const t0 = performance.now();
      void getSharedWorker()
        .then(() => {
          if (performance.now() - t0 > 800) {
            label.textContent = 'Database ready — search anytime.';
            window.setTimeout(hide, 2200);
          } else {
            hide();
          }
        })
        .catch(() => {
          label.textContent = 'Database unavailable — reload or try again later.';
        });
    },
    { once: false }
  );
}
