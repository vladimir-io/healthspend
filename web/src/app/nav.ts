/** Desktop tabs + mobile bottom bar + legacy hash redirects. */

export type AppRoute = 'search' | 'hospitals' | 'methodology';

const ROUTE_HASH: Record<AppRoute, string> = {
  search: '#search',
  hospitals: '#hospitals',
  methodology: '#methodology',
};

const HASH_TO_ROUTE: Record<string, AppRoute> = {
  '#search': 'search',
  '#hospitals': 'hospitals',
  '#methodology': 'methodology',
  '#scorecard': 'hospitals',
  '#incidents': 'hospitals',
  '#mission': 'methodology',
};

export type HospitalsTab = 'all' | 'failing';

let hospitalsTab: HospitalsTab = 'all';

export function getHospitalsTab(): HospitalsTab {
  return hospitalsTab;
}

export function setHospitalsTab(tab: HospitalsTab): void {
  hospitalsTab = tab;
  window.dispatchEvent(new CustomEvent('hs:hospitals-tab', { detail: tab }));
}

export function routeFromHash(hash: string): AppRoute {
  return HASH_TO_ROUTE[hash] ?? 'search';
}

export function parseLegacyRoute(): { route: AppRoute; hospitalsTab?: HospitalsTab } {
  const hash = window.location.hash || '#search';
  const params = new URLSearchParams(window.location.search);
  const tab = params.get('tab');
  if (hash === '#incidents' || tab === 'failing') {
    return { route: 'hospitals', hospitalsTab: 'failing' };
  }
  if (hash === '#scorecard') {
    return { route: 'hospitals', hospitalsTab: 'all' };
  }
  if (hash === '#mission') {
    return { route: 'methodology' };
  }
  const route = routeFromHash(hash);
  if (route === 'hospitals' && tab === 'failing') {
    return { route, hospitalsTab: 'failing' };
  }
  return { route };
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
      const r = el.dataset.route as AppRoute | undefined;
      const active = r === route;
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

    if (moreMenu && route !== 'methodology') {
      moreMenu.classList.add('hidden');
    }

    onRouteChange?.(route);
  };

  const onHashChange = () => {
    const legacy = parseLegacyRoute();
    if (legacy.hospitalsTab) setHospitalsTab(legacy.hospitalsTab);
    const hash = window.location.hash || '#search';
    if (!HASH_TO_ROUTE[hash] && !['#scorecard', '#incidents', '#mission'].includes(hash)) {
      window.location.hash = '#search';
      return;
    }
    if (['#scorecard', '#incidents', '#mission'].includes(hash)) {
      window.location.replace(`${ROUTE_HASH[legacy.route]}${window.location.search}`);
      return;
    }
    setActiveUi(legacy.route);
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

  const legacy = parseLegacyRoute();
  if (legacy.hospitalsTab) setHospitalsTab(legacy.hospitalsTab);
  if (['#scorecard', '#incidents', '#mission'].includes(window.location.hash)) {
    window.location.replace(`${ROUTE_HASH[legacy.route]}${window.location.search}`);
  } else {
    onHashChange();
  }
}

export function navigateTo(route: AppRoute, opts?: { hospitalsTab?: HospitalsTab }): void {
  if (opts?.hospitalsTab) {
    setHospitalsTab(opts.hospitalsTab);
    const url = new URL(window.location.href);
    if (opts.hospitalsTab === 'failing') {
      url.searchParams.set('tab', 'failing');
    } else {
      url.searchParams.delete('tab');
    }
    window.history.replaceState(null, '', `${url.pathname}${url.search}${ROUTE_HASH[route]}`);
  }
  window.location.hash = ROUTE_HASH[route];
}
