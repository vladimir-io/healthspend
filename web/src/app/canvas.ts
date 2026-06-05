export function setupCanvasSearch(): void {
  const canvas = document.getElementById('search-canvas') as HTMLCanvasElement | null;
  const wrapper = document.getElementById('search-canvas-wrapper');
  const input = document.getElementById('search-input') as HTMLInputElement | null;
  if (!canvas || !wrapper || !input) return;

  const ctx = canvas.getContext('2d')!;
  let focused = false;
  let hasQuery = false;
  let t = 0;

  const sizeCanvas = () => {
    const dpr = window.devicePixelRatio || 1;
    const rect = wrapper.getBoundingClientRect();
    if (!canvas) return;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);
    canvas.style.width = `${rect.width}px`;
    canvas.style.height = `${rect.height}px`;
  };
  sizeCanvas();
  new ResizeObserver(sizeCanvas).observe(wrapper);

  const isDark = () => document.documentElement.getAttribute('data-theme') !== 'light';

  let animating = false;

  function draw() {
    if (!animating) return;
    requestAnimationFrame(draw);
    if (!canvas) return;
    t += 0.016;
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width / dpr;
    const H = canvas.height / dpr;
    ctx.clearRect(0, 0, W, H);

    const dotBase = isDark() ? 'rgba(255,255,255,' : 'rgba(0,0,0,';

    if (!focused && !hasQuery) {
      const cols = 20;
      const rows = 6;
      const gx = W / cols;
      const gy = H / rows;
      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          const phase = (r * cols + c) * 0.15;
          const alpha = 0.02 + 0.02 * Math.sin(t * 0.5 + phase);
          ctx.beginPath();
          ctx.arc(gx * c + gx / 2, gy * r + gy / 2, 0.8, 0, Math.PI * 2);
          ctx.fillStyle = dotBase + alpha + ')';
          ctx.fill();
        }
      }
    } else {
      const gradX = Math.sin(t * 0.5) * (W * 0.1) + W / 2;
      const grad = ctx.createRadialGradient(gradX, H / 2, 0, gradX, H / 2, W * 0.3);
      grad.addColorStop(0, isDark() ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.02)');
      grad.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.fillStyle = grad;
      ctx.fillRect(0, 0, W, H);
    }
  }

  input.addEventListener('focus', () => {
    focused = true;
  });
  input.addEventListener('blur', () => {
    focused = false;
  });
  input.addEventListener('input', () => {
    hasQuery = input.value.length > 0;
  });

  const startAnimation = () => {
    if (animating) return;
    animating = true;
    draw();
  };

  const scheduleAnimation = () => {
    if (typeof requestIdleCallback === 'function') {
      requestIdleCallback(startAnimation, { timeout: 3000 });
    } else {
      setTimeout(startAnimation, 1500);
    }
  };

  const searchView = document.getElementById('view-search');
  if (searchView && !searchView.classList.contains('hidden')) {
    scheduleAnimation();
  }

  window.addEventListener('hashchange', () => {
    const visible = !searchView?.classList.contains('hidden');
    if (visible) scheduleAnimation();
    else animating = false;
  });
}
