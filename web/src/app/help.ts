import { prefersReducedMotion } from './micro';
import { performSearch } from './search_controller';
import { navigateToHospitalIssues } from './url_params';

const SEEN_KEY = 'hs_help_seen_v1';

type HelpTip = {
  step: string;
  title: string;
  body: string;
  action?: { label: string; run: () => void };
};

const TIPS: HelpTip[] = [
  {
    step: '1',
    title: 'Compare before care',
    body: 'Search a procedure or CPT code. Add a state filter for local results. Tap a quick-search tag to try common procedures.',
    action: {
      label: 'Try MRI search',
      run: () => {
        window.location.hash = '#search';
        document.getElementById('search-welcome')?.classList.add('hidden');
        const input = document.getElementById('search-input') as HTMLInputElement | null;
        const state = (document.getElementById('search-state') as HTMLSelectElement | null)?.value ?? '';
        if (input) {
          input.value = 'MRI';
          input.focus();
          void performSearch('MRI', state);
        }
      },
    },
  },
  {
    step: '2',
    title: 'Bill above the posted rate?',
    body: 'Open Use this rate on any result for an email template to request an itemization or adjustment against the published cash price.',
  },
  {
    step: '3',
    title: 'Broken transparency filing?',
    body: 'Use Hospitals → Filing issues to report broken machine-readable price files. That path is separate from billing emails.',
    action: {
      label: 'Open filing issues',
      run: () => navigateToHospitalIssues(),
    },
  },
];

export function setupHelp(): void {
  const root = document.getElementById('help-root');
  const fab = document.getElementById('help-fab');
  const panel = document.getElementById('help-panel');
  const closeBtn = document.getElementById('help-panel-close');
  const body = document.getElementById('help-panel-body');
  if (!root || !fab || !panel || !body) return;

  if (!localStorage.getItem(SEEN_KEY)) {
    fab.classList.add('help-fab--hint');
  }

  body.innerHTML = TIPS.map(
    (tip) => `
    <article class="help-tip">
      <span class="help-tip__step">${tip.step}</span>
      <div class="help-tip__content">
        <h3 class="help-tip__title">${tip.title}</h3>
        <p class="help-tip__body">${tip.body}</p>
        ${tip.action ? `<button type="button" class="help-tip__action" data-help-action="${tip.step}">${tip.action.label}</button>` : ''}
      </div>
    </article>`
  ).join('');

  TIPS.forEach((tip) => {
    if (!tip.action) return;
    body.querySelector(`[data-help-action="${tip.step}"]`)?.addEventListener('click', () => {
      close();
      tip.action!.run();
    });
  });

  const open = () => {
    localStorage.setItem(SEEN_KEY, '1');
    fab.classList.remove('help-fab--hint');
    panel.classList.remove('hidden');
    fab.setAttribute('aria-expanded', 'true');
    panel.setAttribute('aria-hidden', 'false');
    if (!prefersReducedMotion()) {
      requestAnimationFrame(() => panel.classList.add('is-open'));
    } else {
      panel.classList.add('is-open');
    }
    closeBtn?.focus({ preventScroll: true });
  };

  const close = () => {
    panel.classList.remove('is-open');
    fab.setAttribute('aria-expanded', 'false');
    panel.setAttribute('aria-hidden', 'true');
    const delay = prefersReducedMotion() ? 0 : 220;
    window.setTimeout(() => {
      if (!panel.classList.contains('is-open')) panel.classList.add('hidden');
    }, delay);
    fab.focus({ preventScroll: true });
  };

  fab.addEventListener('click', () => {
    if (panel.classList.contains('is-open')) close();
    else open();
  });

  closeBtn?.addEventListener('click', close);

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && panel.classList.contains('is-open')) close();
  });

  document.addEventListener('click', (e) => {
    if (!panel.classList.contains('is-open')) return;
    const target = e.target as Node;
    if (!root.contains(target)) close();
  });
}
