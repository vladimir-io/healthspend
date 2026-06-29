const THEME_META: Record<string, string> = {
  dark: '#050507',
  light: '#f4f4f7',
};

export function applyTheme(theme: 'dark' | 'light'): void {
  document.documentElement.setAttribute('data-theme', theme);
  document.documentElement.style.colorScheme = theme;
  localStorage.setItem('theme', theme);

  const meta = document.getElementById('meta-theme-color');
  if (meta) meta.setAttribute('content', THEME_META[theme]);
}

export function setupThemeToggle(): void {
  const toggle = document.getElementById('theme-toggle') as HTMLButtonElement;
  const sun = document.querySelector('.sun-icon') as SVGElement;
  const moon = document.querySelector('.moon-icon') as SVGElement;

  const saved = (localStorage.getItem('theme') as 'dark' | 'light' | null) || 'dark';
  const initial = saved === 'light' ? 'light' : 'dark';
  applyTheme(initial);
  updateIcons(initial, sun, moon);

  toggle?.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
    const next: 'dark' | 'light' = current === 'dark' ? 'light' : 'dark';

    const run = () => {
      applyTheme(next);
      updateIcons(next, sun, moon);
    };

    const doc = document as Document & { startViewTransition?: (cb: () => void) => void };
    if (doc.startViewTransition) {
      doc.startViewTransition(run);
    } else {
      run();
    }

    const icon = next === 'light' ? sun : moon;
    icon?.animate(
      [
        { transform: 'rotate(-90deg) scale(0.4)', opacity: '0' },
        { transform: 'rotate(20deg) scale(1.15)', opacity: '1', offset: 0.7 },
        { transform: 'rotate(0deg) scale(1)', opacity: '1' },
      ],
      { duration: 500, easing: 'cubic-bezier(0.34, 1.56, 0.64, 1)', fill: 'forwards' }
    );
  });
}

function updateIcons(theme: string, sun: SVGElement | null, moon: SVGElement | null): void {
  if (theme === 'dark') {
    sun?.classList.add('hidden');
    moon?.classList.remove('hidden');
  } else {
    sun?.classList.remove('hidden');
    moon?.classList.add('hidden');
  }
}

export function setupDynamicYear(): void {
  const year = new Date().getFullYear();
  document.querySelectorAll('.current-year').forEach((el) => {
    el.textContent = year.toString();
  });

  const metaDesc = document.getElementById('meta-desc');
  if (metaDesc) {
    metaDesc.setAttribute(
      'content',
      metaDesc.getAttribute('content')?.replace(/2026/g, year.toString()) || ''
    );
  }
  const ogDesc = document.getElementById('og-desc');
  if (ogDesc) {
    ogDesc.setAttribute('content', ogDesc.getAttribute('content')?.replace(/2026/g, year.toString()) || '');
  }
  const ldJson = document.getElementById('ld-json');
  if (ldJson) {
    try {
      const parsed = JSON.parse(ldJson.textContent || '{}');
      const items = Array.isArray(parsed) ? parsed : [parsed];
      let changed = false;
      for (const data of items) {
        if (typeof data.description === 'string' && data.description.includes('2026')) {
          data.description = data.description.replace(/2026/g, year.toString());
          changed = true;
        }
      }
      if (changed) {
        ldJson.textContent = JSON.stringify(Array.isArray(parsed) ? items : items[0], null, 2);
      }
    } catch {
      /* ignore */
    }
  }
}
