/** Desktop tabs + mobile bottom bar + “More” menu. */

export type AppRoute = 'search' | 'scorecard' | 'incidents' | 'methodology' | 'mission';

const ROUTE_HASH: Record<AppRoute, string> = {
  search: '#search',
  scorecard: '#scorecard',
  incidents: '#incidents',
  methodology: '#methodology',
  mission: '#mission',
};

const HASH_TO_ROUTE: Record<string, AppRoute> = {
  '#search': 'search',
  '#scorecard': 'scorecard',
  '#incidents': 'incidents',
  '#methodology': 'methodology',
  '#mission': 'mission',
};

const AUDIT_ROUTES: AppRoute[] = ['scorecard', 'incidents'];

export function routeFromHash(hash: string): AppRoute {
  return HASH_TO_ROUTE[hash] ?? 'search';
}

export function setupNavigation(onRouteChange?: (route: AppRoute) => void): void {
  const indicator = document.getElementById('nav-indicator');
  const moreMenu = document.getElementById('nav-more-menu');
  const moreBtn = document.getElementById('nav-more-btn');
  let lastLeft = 0;

  const updateIndicator = (activeTab: HTMLElement) => {
    if (!indicator || window.matchMedia('(max-width: 768px)').matches) return;
    const { offsetWidth, offsetLeft } = activeTab;
    if (offsetWidth === 0) return;
    const delta = Math.abs(offsetLeft - lastLeft);
    const stretch = 1 + Math.min(delta / 200, 0.4);
    indicator.style.width = `${offsetWidth}px`;
    indicator.style.transform = `translateX(${offsetLeft}px) scaleX(${stretch})`;
    indicator.style.opacity = '1';
    lastLeft = offsetLeft;
    window.setTimeout(() => {
      indicator.style.transform = `translateX(${offsetLeft}px) scaleX(1)`;
    }, 400);
  };

  const setActiveUi = (route: AppRoute) => {
    const hash = ROUTE_HASH[route];
    document.querySelectorAll<HTMLElement>('[data-route]').forEach((el) => {
      const r = el.dataset.route as AppRoute | 'audit' | undefined;
      const active =
        r === route || (r === 'audit' && AUDIT_ROUTES.includes(route));
      el.classList.toggle('is-active', !!active);
      if (el.classList.contains('nav-tab')) {
        el.classList.toggle('active', !!active);
      }
    });

    const desktopTab = document.querySelector<HTMLElement>(`.nav-tab[href="${hash}"]`);
    if (desktopTab) requestAnimationFrame(() => updateIndicator(desktopTab));

    document.querySelectorAll('.tab-view').forEach((v) => {
      const show = v.id === `view-${route}`;
      v.classList.toggle('hidden', !show);
      v.classList.toggle('active', show);
    });

    if (moreMenu && !['methodology', 'mission'].includes(route)) {
      moreMenu.classList.add('hidden');
    }

    onRouteChange?.(route);
  };

  const onHashChange = () => {
    const hash = window.location.hash || '#search';
    if (!HASH_TO_ROUTE[hash]) {
      window.location.hash = '#search';
      return;
    }
    setActiveUi(routeFromHash(hash));
  };

  window.addEventListener('hashchange', onHashChange);

  moreBtn?.addEventListener('click', () => {
    moreMenu?.classList.toggle('hidden');
  });

  document.addEventListener('click', (e) => {
    const t = e.target as HTMLElement;
    if (!moreMenu || moreMenu.classList.contains('hidden')) return;
    if (moreMenu.contains(t) || moreBtn?.contains(t)) return;
    moreMenu.classList.add('hidden');
  });

  moreMenu?.querySelectorAll('[data-route]').forEach((link) => {
    link.addEventListener('click', () => moreMenu.classList.add('hidden'));
  });

  window.addEventListener('resize', () => {
    const active = document.querySelector<HTMLElement>('.nav-tab.active');
    if (active) updateIndicator(active);
  });

  onHashChange();
}

export function navigateTo(route: AppRoute): void {
  window.location.hash = ROUTE_HASH[route];
}
