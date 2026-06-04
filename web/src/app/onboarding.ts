const STORAGE_KEY = 'hs_onboarding_v1';

type Step = {
  target: string;
  title: string;
  body: string;
};

const STEPS: Step[] = [
  {
    target: '.hero-journey',
    title: 'Pick your situation',
    body: 'Compare prices before care, challenge a bill above the posted cash rate, or explore transparency violations.',
  },
  {
    target: '#search-input',
    title: 'Search published cash rates',
    body: 'Type a procedure (e.g. MRI, colonoscopy) or CPT code. Add a state filter if you want local results.',
  },
];

export function setupOnboarding(): void {
  if (localStorage.getItem(STORAGE_KEY)) return;
  if (window.location.hash && window.location.hash !== '#search') return;

  const root = document.getElementById('onboarding-root');
  const bubble = document.getElementById('onboarding-bubble');
  const titleEl = document.getElementById('onboarding-title');
  const bodyEl = document.getElementById('onboarding-body');
  const nextBtn = document.getElementById('onboarding-next');
  const skipBtn = document.getElementById('onboarding-skip');
  if (!root || !bubble || !titleEl || !bodyEl || !nextBtn || !skipBtn) return;

  let step = 0;

  const finish = () => {
    localStorage.setItem(STORAGE_KEY, '1');
    root.classList.add('hidden');
    root.setAttribute('aria-hidden', 'true');
    document.querySelectorAll('.onboarding-highlight').forEach((el) => {
      el.classList.remove('onboarding-highlight');
    });
  };

  const positionBubble = (target: Element) => {
    target.classList.add('onboarding-highlight');
    const rect = target.getBoundingClientRect();
    const bubbleRect = bubble.getBoundingClientRect();
    let top = rect.bottom + 12;
    let left = rect.left + rect.width / 2 - bubbleRect.width / 2;
    left = Math.max(12, Math.min(left, window.innerWidth - bubbleRect.width - 12));
    if (top + bubbleRect.height > window.innerHeight - 12) {
      top = rect.top - bubbleRect.height - 12;
    }
    bubble.style.top = `${top}px`;
    bubble.style.left = `${left}px`;
  };

  const showStep = (index: number) => {
    document.querySelectorAll('.onboarding-highlight').forEach((el) => {
      el.classList.remove('onboarding-highlight');
    });
    const cfg = STEPS[index];
    const target = document.querySelector(cfg.target);
    if (!target) {
      finish();
      return;
    }
    titleEl.textContent = cfg.title;
    bodyEl.textContent = cfg.body;
    nextBtn.textContent = index === STEPS.length - 1 ? 'Got it' : 'Next';
    root.classList.remove('hidden');
    root.setAttribute('aria-hidden', 'false');
    requestAnimationFrame(() => positionBubble(target));
  };

  skipBtn.addEventListener('click', finish);
  nextBtn.addEventListener('click', () => {
    step += 1;
    if (step >= STEPS.length) finish();
    else showStep(step);
  });

  window.addEventListener('resize', () => {
    if (root.classList.contains('hidden')) return;
    const cfg = STEPS[step];
    const target = document.querySelector(cfg.target);
    if (target) positionBubble(target);
  });

  window.setTimeout(() => showStep(0), 600);
}
