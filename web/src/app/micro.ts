/** Shared micro-interactions and motion helpers. */

export function prefersReducedMotion(): boolean {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

export function openSheet(sheet: HTMLElement): void {
  sheet.classList.remove('hidden', 'is-closing');
  document.body.classList.add('sheet-open');
  requestAnimationFrame(() => sheet.classList.add('is-open'));
  const content = sheet.querySelector('.sheet-content') as HTMLElement | null;
  content?.focus({ preventScroll: true });
}

export function closeSheet(sheet: HTMLElement): void {
  sheet.classList.remove('is-open');
  if (prefersReducedMotion()) {
    sheet.classList.add('hidden');
    if (!document.querySelector('.bottom-sheet.is-open')) {
      document.body.classList.remove('sheet-open');
    }
    return;
  }
  sheet.classList.add('is-closing');
  window.setTimeout(() => {
    sheet.classList.add('hidden');
    sheet.classList.remove('is-closing');
    if (!document.querySelector('.bottom-sheet.is-open')) {
      document.body.classList.remove('sheet-open');
    }
  }, 320);
}

export function closeAllSheets(): void {
  document.querySelectorAll('.bottom-sheet').forEach((el) => {
    if (!el.classList.contains('hidden')) closeSheet(el as HTMLElement);
  });
}

export function setupMicroInteractions(): void {
  setupHeroJourneyCards();
  setupButtonPress();
  setupSheetEscape();
}

function setupHeroJourneyCards(): void {
  const cards = document.querySelectorAll<HTMLButtonElement>('[data-hero-intent]');
  const active = sessionStorage.getItem('hs_hero_intent');
  cards.forEach((card) => {
    if (card.dataset.heroIntent === active) card.classList.add('is-selected');
    card.addEventListener('click', () => {
      cards.forEach((c) => c.classList.remove('is-selected'));
      card.classList.add('is-selected');
    });
  });
}

function setupButtonPress(): void {
  document.addEventListener(
    'click',
    (e) => {
      const btn = (e.target as HTMLElement).closest('.btn, .brutalist-action, .load-more-button, .hero-journey-card');
      if (!btn || prefersReducedMotion()) return;
      btn.classList.add('is-pressed');
      window.setTimeout(() => btn.classList.remove('is-pressed'), 140);
    },
    true
  );
}

function setupSheetEscape(): void {
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    closeAllSheets();
  });
}
