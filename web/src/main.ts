import { searchPrices, getRecommendations } from './db';

// --- Theme toggle with circular ripple reveal ---
function setupThemeToggle() {
  const toggle = document.getElementById('theme-toggle') as HTMLButtonElement;
  const sun  = document.querySelector('.sun-icon')  as SVGElement;
  const moon = document.querySelector('.moon-icon') as SVGElement;

  const savedTheme = localStorage.getItem('theme') || 'dark';
  document.documentElement.setAttribute('data-theme', savedTheme);
  updateIcons(savedTheme);

  toggle?.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme') || 'dark';
    const next = current === 'dark' ? 'light' : 'dark';

    // Decide whether to use the View Transitions API (Modern Browsers) or just CSS
    if ((document as any).startViewTransition) {
      (document as any).startViewTransition(() => {
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('theme', next);
        updateIcons(next);
      });
    } else {
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('theme', next);
      updateIcons(next);
    }

    // High-fidelity icon spring micro-interaction
    const icon = next === 'light' ? sun : moon;
    icon?.animate([
      { transform: 'rotate(-90deg) scale(0.4)', opacity: '0' },
      { transform: 'rotate(20deg)  scale(1.15)', opacity: '1', offset: 0.7 },
      { transform: 'rotate(0deg)   scale(1)',    opacity: '1' },
    ], { duration: 500, easing: 'cubic-bezier(0.34, 1.56, 0.64, 1)', fill: 'forwards' });
  });

  function updateIcons(theme: string) {
    if (theme === 'dark') {
      sun?.classList.add('hidden');
      moon?.classList.remove('hidden');
    } else {
      sun?.classList.remove('hidden');
      moon?.classList.add('hidden');
    }
  }
}

let searchCounter = 0;

const input = document.getElementById('search-input') as HTMLInputElement;
const stateSelect = document.getElementById('search-state') as HTMLSelectElement;
const resultsContainer = document.getElementById('results-container') as HTMLDivElement;
const contextBanner = document.getElementById('results-context-banner') as HTMLDivElement;
const sortSelect = document.getElementById('sort-select') as HTMLSelectElement;
const recommendationEl = document.getElementById('search-recommendations') as HTMLDivElement;

let currentResults: any[] = [];

const overlay = document.getElementById('letter-overlay') as HTMLDivElement;
const sheetBackdrop = document.querySelector('.sheet-backdrop') as HTMLDivElement;
const btnCloseLetter = document.getElementById('btn-close-letter') as HTMLButtonElement;
const letterDraft = document.getElementById('letter-draft') as HTMLTextAreaElement;
const btnCopy = document.getElementById('btn-copy') as HTMLButtonElement;
const btnGmail = document.getElementById('btn-gmail') as HTMLAnchorElement;
const btnOutlook = document.getElementById('btn-outlook') as HTMLAnchorElement;

let debounceTimer: any;

input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    const query = input.value;
    const state = stateSelect.value;
    
    // Autocomplete
    const recs = getRecommendations(query);
    if (recs.length > 0) {
        renderRecommendations(recs);
        recommendationEl.classList.remove('hidden');
    } else {
        recommendationEl.classList.add('hidden');
    }

    if (query.length > 2) {
        debounceTimer = setTimeout(() => performSearch(query, state), 300);
    } else {
        resultsContainer.classList.add('hidden');
        contextBanner.classList.add('hidden');
    }
});

function renderRecommendations(recs: {query: string, code: string}[]) {
    recommendationEl.innerHTML = recs.map(r => `
        <div class="recommendation-item" data-query="${r.query}">
            <span class="rec-query">${r.query}</span>
            <span class="rec-code">CPT ${r.code}</span>
        </div>
    `).join('');

    recommendationEl.querySelectorAll('.recommendation-item').forEach(item => {
        item.addEventListener('click', () => {
            const q = (item as HTMLElement).dataset.query!;
            input.value = q;
            recommendationEl.classList.add('hidden');
            performSearch(q, stateSelect.value);
        });
    });
}

// Hide recommendations on click outside
document.addEventListener('click', (e) => {
    if (!recommendationEl.contains(e.target as Node) && e.target !== input) {
        recommendationEl.classList.add('hidden');
    }
});

sortSelect.addEventListener('change', () => {
    if (currentResults.length > 0) {
        applySortAndRender();
    }
});

function applySortAndRender() {
    const val = sortSelect.value;
    const sorted = [...currentResults];
    
    if (val === 'price-asc') {
        sorted.sort((a, b) => a.cash_price - b.cash_price);
    } else if (val === 'price-desc') {
        sorted.sort((a, b) => b.cash_price - a.cash_price);
    } else if (val === 'score-desc') {
        sorted.sort((a, b) => b.score - a.score);
    }
    
    renderResults(sorted);
}

stateSelect.addEventListener('change', () => {
    const query = input.value;
    const state = stateSelect.value;
    if (query.length > 2) performSearch(query, state);
});

// --- SOTA UI Routing ---
function setupRouting() {
  const tabs = document.querySelectorAll('.nav-tab');
  const indicator = document.getElementById('nav-indicator');

  if (indicator) indicator.style.opacity = '0';

  let lastLeft = 0;

  const updateIndicator = (activeTab: HTMLElement) => {
    if (!indicator) return;
    const { offsetWidth, offsetLeft } = activeTab;
    if (offsetWidth === 0) return;

    const delta = Math.abs(offsetLeft - lastLeft);
    const stretch = 1 + Math.min(delta / 200, 0.4);
    
    indicator.style.width = `${offsetWidth}px`;
    indicator.style.transform = `translateX(${offsetLeft}px) scaleX(${stretch})`;
    indicator.style.opacity = '1';
    
    lastLeft = offsetLeft;
    
    // Reset stretch after animation
    setTimeout(() => {
        indicator.style.transform = `translateX(${offsetLeft}px) scaleX(1)`;
    }, 400);
  };

  const onHashChange = () => {
    const hash = window.location.hash || '#search';
    const viewId = `view-${hash.substring(1)}`;
    const activeTab = document.querySelector(`.nav-tab[href="${hash}"]`) as HTMLElement;
    
    // Toggle View visibility
    const views = document.querySelectorAll('.tab-view');
    let viewFound = false;
    views.forEach(v => {
      const isMatch = v.id === viewId;
      v.classList.toggle('hidden', !isMatch);
      if (isMatch) viewFound = true;
    });

    // Fallback if view doesn't exist
    if (!viewFound) {
        window.location.hash = '#search';
        return;
    }

    if (activeTab) {
        tabs.forEach(t => t.classList.toggle('active', t === activeTab));
        requestAnimationFrame(() => updateIndicator(activeTab));
    }
  };

  window.addEventListener('hashchange', onHashChange);
  window.addEventListener('resize', () => {
    const active = document.querySelector('.nav-tab.active') as HTMLElement;
    if (active) updateIndicator(active);
  });

  // --- Initial Platform State ---
  onHashChange();
  
  const bootQuery = "Metabolic Panel";
  input.value = bootQuery;
  performSearch(bootQuery, "");

  // Wire up Quick Search Ecosystem
  document.querySelectorAll('.search-tag').forEach(tag => {
    tag.addEventListener('click', () => {
      const q = tag.textContent || "";
      input.value = q;
      recommendationEl.classList.add('hidden');
      performSearch(q, stateSelect.value);
      setTimeout(() => tag.classList.add('active'), 0);
      setTimeout(() => tag.classList.remove('active'), 200);
    });
  });
}

async function init() {
  setupThemeToggle();
  setupRouting();
  setupCanvasSearch();
  setupCptPanel();
  setupDynamicYear();
  
  // Dynamic Views
  const { renderScorecard } = await import('./views/scorecard');
  await renderScorecard('view-scorecard');
  
  const { renderIncidents } = await import('./views/incidents');
  await renderIncidents('view-incidents');

  // Pinned Audits: Shortcut Tags
  document.querySelectorAll('.shortcut-tag').forEach(tag => {
      tag.addEventListener('click', () => {
          const query = (tag as HTMLElement).dataset.query;
          if (query) {
              input.value = query;
              performSearch(query, stateSelect.value);
              window.scrollTo({ top: 0, behavior: 'smooth' });
          }
      });
  });
}

// --- Canvas Search Animation ---
function setupCptPanel() {
  const overlay     = document.getElementById('cpt-overlay')!;
  const backdrop    = document.getElementById('cpt-backdrop')!;
  const btnOpen     = document.getElementById('btn-open-cpt')!;
  const btnClose    = document.getElementById('btn-close-cpt')!;
  const filterInput = document.getElementById('cpt-filter') as HTMLInputElement;
  const pillsEl     = document.getElementById('cpt-category-pills')!;
  const tableEl     = document.getElementById('cpt-table-container')!;

  let activeCategory = '';

  const open  = () => overlay.classList.remove('hidden');
  const close = () => overlay.classList.add('hidden');
  btnOpen.addEventListener('click', () => { renderTable('', ''); open(); });
  btnClose.addEventListener('click', close);
  backdrop.addEventListener('click', close);

  // Lazy-import catalog only when opened
  async function renderTable(filter: string, category: string) {
    const { CPT_CATALOG, CPT_CATEGORIES } = await import('./cpt_catalog.js');

    // Render category pills once
    if (!pillsEl.children.length) {
      pillsEl.innerHTML = ['All', ...CPT_CATEGORIES].map(cat => `
        <button class="cpt-pill ${cat === 'All' ? 'active' : ''}" data-cat="${cat === 'All' ? '' : cat}">${cat}</button>
      `).join('');
      pillsEl.querySelectorAll('.cpt-pill').forEach(pill => {
        pill.addEventListener('click', () => {
          activeCategory = (pill as HTMLElement).dataset.cat!;
          pillsEl.querySelectorAll('.cpt-pill').forEach(p => p.classList.remove('active'));
          pill.classList.add('active');
          renderTable(filterInput.value, activeCategory);
        });
      });
    }

    const norm = filter.toLowerCase();
    const filtered = CPT_CATALOG.filter(e =>
      (!category || e.category === category) &&
      (!norm || e.plain.toLowerCase().includes(norm) || e.code.includes(norm) || e.technical.toLowerCase().includes(norm))
    );

    if (filtered.length === 0) {
      tableEl.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-tertiary);font-size:0.85rem;">No codes match this filter.</div>`;
      return;
    }

    tableEl.innerHTML = `
      <div style="overflow-x: auto; -webkit-overflow-scrolling: touch; width: 100%;">
        <table style="width:100%;border-collapse:collapse;font-size:0.82rem;min-width:600px;">
          <thead>
            <tr style="border-bottom:2px solid var(--border-medium);">
              <th style="text-align:left;padding:10px 14px;font-family:var(--font-mono);font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;color:var(--text-tertiary);font-weight:700;white-space:nowrap;">CPT Code</th>
              <th style="text-align:left;padding:10px 14px;font-family:var(--font-mono);font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;color:var(--text-tertiary);font-weight:700;">Plain English</th>
              <th style="text-align:left;padding:10px 14px;font-family:var(--font-mono);font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;color:var(--text-tertiary);font-weight:700;display:none;" class="tech-col">Technical Term</th>
              <th style="text-align:left;padding:10px 14px;font-family:var(--font-mono);font-size:0.62rem;text-transform:uppercase;letter-spacing:0.08em;color:var(--text-tertiary);font-weight:700;white-space:nowrap;width:120px;">Category</th>
              <th style="padding:10px 14px;width:110px;"></th>
            </tr>
          </thead>
          <tbody>
            ${filtered.map(e => `
              <tr class="cpt-row" data-code="${e.code}" data-plain="${e.plain}" style="border-bottom:1px solid var(--border-subtle);cursor:pointer;transition:background 0.12s;">
                <td style="padding:12px 14px;font-family:var(--font-mono);font-weight:700;color:var(--yc-orange);font-size:0.8rem;white-space:nowrap;">${e.code}</td>
                <td style="padding:12px 14px;font-weight:600;color:var(--text-primary);min-width:180px;">${e.plain}<div style="font-size:0.7rem;color:var(--text-tertiary);font-weight:400;margin-top:2px;">${e.technical}</div></td>
                <td style="padding:12px 14px;color:var(--text-tertiary);font-size:0.75rem;display:none;" class="tech-col">${e.technical}</td>
                <td style="padding:12px 14px;white-space:nowrap;"><span style="font-family:var(--font-mono);font-size:0.62rem;background:var(--bg-card-2);border:1px solid var(--border-subtle);padding:3px 8px;border-radius:4px;color:var(--text-secondary);">${e.category}</span></td>
                <td style="padding:12px 14px;text-align:right;white-space:nowrap;"><button class="brutalist-action cpt-search-btn" style="font-size:0.65rem;padding:6px 12px;white-space:nowrap;">Search Prices</button></td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;

    tableEl.querySelectorAll('.cpt-row').forEach(row => {
      const el = row as HTMLElement;
      el.addEventListener('mouseenter', () => { el.style.background = 'var(--bg-card-hover)'; });
      el.addEventListener('mouseleave', () => { el.style.background = ''; });
      el.querySelector('.cpt-search-btn')?.addEventListener('click', (e) => {
        e.stopPropagation();
        const q = el.dataset.code!;
        close();
        input.value = el.dataset.plain!;
        performSearch(q, stateSelect.value);
        window.scrollTo({ top: 0, behavior: 'smooth' });
      });
    });
  }

  filterInput.addEventListener('input', () => {
    renderTable(filterInput.value, activeCategory);
  });
}

function setupCanvasSearch() {
  const canvas  = document.getElementById('search-canvas') as HTMLCanvasElement | null;
  const wrapper = document.getElementById('search-canvas-wrapper');
  const input   = document.getElementById('search-input') as HTMLInputElement | null;
  if (!canvas || !wrapper || !input) return;

  const ctx = canvas.getContext('2d')!;
  let focused  = false;
  let hasQuery = false;
  let t        = 0;

  // Size canvas to wrapper
  const sizeCanvas = () => {
    const dpr = window.devicePixelRatio || 1;
    const rect = wrapper.getBoundingClientRect();
    canvas!.width  = rect.width  * dpr;
    canvas!.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    canvas!.style.width  = rect.width  + 'px';
    canvas!.style.height = rect.height + 'px';
  };
  sizeCanvas();
  new ResizeObserver(sizeCanvas).observe(wrapper);

  const isDark = () => document.documentElement.getAttribute('data-theme') !== 'light';

  function draw() {
    requestAnimationFrame(draw);
    t += 0.016;
    const dpr = window.devicePixelRatio || 1;
    const W = canvas!.width  / dpr;
    const H = canvas!.height / dpr;
    ctx.clearRect(0, 0, W, H);

    const dark = isDark();
    const dotBase   = dark ? 'rgba(255,255,255,' : 'rgba(0,0,0,';

    if (!focused && !hasQuery) {
      // ---- Minimalist: very soft breathing dots ----
      const cols = 20, rows = 6;
      const gx = W / cols, gy = H / rows;
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
      // ---- Focused: subtler, cleaner pulse ----
      const gradX = Math.sin(t * 0.5) * (W * 0.1) + (W / 2);
      const grad = ctx.createRadialGradient(gradX, H / 2, 0, gradX, H / 2, W * 0.3);
      const accentColor = dark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.02)';
      grad.addColorStop(0, accentColor);
      grad.addColorStop(1, 'rgba(0,0,0,0)');
      
      ctx.fillStyle = grad;
      ctx.fillRect(0, 0, W, H);

      // Fine grid for technical precision feel
      const cols = 24, rows = 8;
      const gx = W / cols, gy = H / rows;
      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          const alpha = 0.03 + 0.02 * Math.sin(t * 0.8 + (c + r) * 0.1);
          ctx.beginPath();
          ctx.arc(gx * c + gx / 2, gy * r + gy / 2, 0.6, 0, Math.PI * 2);
          ctx.fillStyle = dotBase + alpha + ')';
          ctx.fill();
        }
      }
    }
  }

  input.addEventListener('focus', () => { focused = true; });
  input.addEventListener('blur',  () => { focused = false; });
  input.addEventListener('input', () => { hasQuery = input.value.length > 0; });
  draw();
}

init();

async function performSearch(query: string, state: string = '') {
    const searchId = ++searchCounter;
    const marketPanel = document.getElementById('market-rates-panel');
    
    contextBanner.classList.add('hidden');
    marketPanel?.classList.add('hidden');
    resultsContainer.innerHTML = `<div class="skeleton-card animate-pulse"></div>`;
    resultsContainer.classList.remove('hidden');

    // ZIP Extraction Logic
    const zipMatch = query.match(/\b\d{5}\b/);
    const searchZip = zipMatch ? zipMatch[0] : '';
    const cleanQuery = query.replace(/\b\d{5}\b/, '').trim();

    const auditLabel = document.getElementById('results-audit-label');
    if (auditLabel) {
        let label = state ? `${state} Price Audit` : `National Price Audit`;
        if (searchZip) label = `Audit: ${searchZip} Region`;
        auditLabel.innerText = label;
    }

    try {
        const results = await searchPrices(cleanQuery || query, state, searchZip);
        if (searchId === searchCounter) {
            currentResults = results;
            
            // Check for fallback awareness
            const isFallback = results.length > 0 && results[0].isFallback;
          const fallbackReason = results.length > 0 ? results[0].fallbackReason : '';
          const fallbackMessageByReason: Record<string, string> = {
            national_scope: 'NO LOCAL DATA : SHOWING NATIONAL RESULTS',
            zip_relaxed_national: 'NO ZIP-REGION DATA : RELAXED TO NATIONAL SCOPE',
            national_text_match: 'NO CPT MATCH : SHOWING NATIONAL TEXT MATCHES',
            category_fallback: 'NO DIRECT MATCH : SHOWING CLOSE CATEGORY RESULTS',
          };
            const statsEl = document.getElementById('omitted-stats');
            if (statsEl) {
            statsEl.innerText = isFallback
              ? (fallbackMessageByReason[fallbackReason] || 'NO LOCAL DATA : SHOWING NATIONAL RESULTS')
              : '';
                statsEl.style.color = isFallback ? "var(--yc-orange)" : "var(--text-tertiary)";
            }

            // Market Aggregation Logic (10x In-Situ Parity)
            if (results.length > 0 && marketPanel) {
                const foundPrices = results.map((r: any) => r.cash_price).sort((a: number, b: number) => a - b);
                const minVal = foundPrices[0];
                const maxVal = foundPrices[foundPrices.length - 1];
                const midIdx = Math.floor(foundPrices.length / 2);
                const medVal = foundPrices.length % 2 !== 0 ? foundPrices[midIdx] : (foundPrices[midIdx - 1] + foundPrices[midIdx]) / 2;

                const minEl = document.getElementById('market-min');
                const medEl = document.getElementById('market-median');
                const maxEl = document.getElementById('market-max');
                
                if (minEl) minEl.innerText = minVal.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                if (medEl) medEl.innerText = medVal.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                if (maxEl) maxEl.innerText = maxVal.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                
                marketPanel.classList.remove('hidden');
            }

            contextBanner.classList.remove('hidden');
            applySortAndRender();
        }
    } catch (e) {
        console.error("VFS Search Error:", e);
    }
}

function renderResults(results: any[]) {
    const validResults = results; 

    resultsContainer.innerHTML = '';
    if (validResults.length === 0) {
        resultsContainer.innerHTML = `
            <div style="padding:60px;text-align:center;color:var(--text-tertiary);font-size:0.9rem;">
                No pricing data found for this search.
                <br /><small style="opacity:0.6; margin-top:8px; display:block;">Try a broader procedure or different state.</small>
            </div>`;
        return;
    }

    validResults.forEach(row => {
        const el = document.createElement('div');
        const city     = row.city  || '';
        const state    = row.state || '';
        const score    = row.score ?? 0;
      const hasAttestedAudit = score > 0;
        const location = (city && state) ? `${city}, ${state}` : (city || state || 'Location not available');
        const price    = (row.cash_price ?? 0).toLocaleString('en-US', { style: 'currency', currency: 'USD' });
        const scoreCol = score > 80 ? 'var(--rh-green)' : score > 50 ? 'var(--amber)' : 'var(--yc-orange)';
      const auditText = hasAttestedAudit
        ? '§ 180.50 : Federally Attested Cash Standard'
        : 'Reported cash rate only · transparency attestation unavailable';

        const fallbackBadgeText = row.fallbackLabel || 'National Result';
        const fallbackBadge = row.isFallback ? `<span style="font-size:0.55rem; background:rgba(255,165,0,0.1); color:orange; padding:2px 6px; border-radius:4px; margin-left:8px; border:1px solid rgba(255,165,0,0.2); font-family:var(--font-mono); vertical-align:middle; letter-spacing:0.05em; text-transform:uppercase;">${fallbackBadgeText}</span>` : '';

        const perfectScoreClass = score === 100 ? 'perfect-audit-card' : '';
        const perfectScoreBadge = score === 100 ? `<span style="font-size:0.55rem; background:rgba(255,215,0,0.15); color:#FFD700; padding:2px 6px; border-radius:4px; margin-left:8px; border:1px solid rgba(255,215,0,0.3); font-family:var(--font-mono); vertical-align:middle; letter-spacing:0.05em; text-transform:uppercase; box-shadow: 0 0 5px rgba(255,215,0,0.2);">★ Perfect Audit</span>` : '';

        el.innerHTML = `
            <div class="search-result-card animate-up ${perfectScoreClass}">
                <div>
                    <p class="result-label">Procedure ${fallbackBadge}</p>
                    <p class="result-procedure">${row.description}</p>
                    <p class="result-hospital">${row.hospital_name} &nbsp;·&nbsp; <span class="result-location">${location}</span> ${perfectScoreBadge}</p>
                    <p class="result-attest">${auditText}</p>
                </div>

                <div class="result-price-block" style="text-align:right;">
                    <p class="result-label">Audit Index</p>
                    <div class="result-score" style="color:${score === 100 ? '#FFD700' : scoreCol}; font-family:var(--font-mono); font-size:1.1rem; font-weight:700;">${score}<span style="font-size:0.6rem; color:var(--text-tertiary); font-weight:400;">/100</span></div>
                    <p class="result-price-label btn-audit-detail" style="cursor:pointer; text-decoration:underline; text-underline-offset:3px; font-weight:700; color:var(--text-secondary); opacity:0.8;">Full Audit →</p>
                </div>

                <div class="result-price-block">
                    <p class="result-label">Cash Rate</p>
                    <div class="result-price">${price}</div>
                    <p class="result-price-label">Negotiated Net</p>
                </div>

                <div class="result-actions-col">
                    <button class="btn primary btn-dispute" style="width:100%;">Claim Rate</button>
                    ${score === 100 
                        ? `<button class="btn secondary" disabled style="width:100%; border-color:var(--rh-green); color:var(--rh-green); font-family:var(--font-mono); font-size:0.6rem; letter-spacing:0.05em;">★ PERFECT AUDIT</button>`
                        : `<button class="btn secondary btn-draft" style="width:100%; border-color:var(--yc-orange); color:var(--yc-orange);">Report</button>`
                    }
                </div>
            </div>
        `;

        el.querySelector('.btn-audit-detail')?.addEventListener('click', async () => {
            const { showAuditDetail } = await import('./views/detail.js');
            // We need name/state/city for the Header in detail
            showAuditDetail({ ...row, name: row.hospital_name });
        });
        el.querySelector('.btn-dispute')?.addEventListener('click', () => handleDispute(row));
        el.querySelector('.btn-draft')?.addEventListener('click', () => handleDraft(row, el.querySelector('.btn-draft') as HTMLButtonElement));
        resultsContainer.appendChild(el);
    });

    const apiFooter = document.createElement('div');
    apiFooter.className = 'api-cta-footer animate-up';
    apiFooter.innerHTML = `Need this data in your own stack? &nbsp;&middot;&nbsp; <strong>Use the Healthspend API</strong>`;
    apiFooter.onclick = () => window.location.hash = 'api';
    resultsContainer.appendChild(apiFooter);
}

const handleDraft = (row: any, btn: HTMLButtonElement) => {
    const oldText = btn.innerText;
    btn.innerText = "DRAFTING...";
    
    setTimeout(() => {
        const subjectLine = `Formal Complaint of Noncompliance: ${row.hospital_name}`;
        const toEmail = "HPTCompliance@cms.hhs.gov";

        const letter = `To the CMS Price Transparency Enforcement Division:

I am submitting a formal complaint regarding suspected noncompliance by ${row.hospital_name} with the Hospital Price Transparency Rule (45 CFR § 180.50).

An independent review of this facility's data catalog revealed the use of algorithm-derived models and estimated pricing, specifically regarding CPT Code ${row.cpt_code}. Under the CY ${new Date().getFullYear()} CMS Final Rule, the use of estimates is strictly prohibited; hospitals are legally mandated to publish actual dollar amounts for all standard charges in a machine-readable format.

I respectfully request that CMS open an immediate investigation into this facility's disclosures and assess Civil Monetary Penalties (CMPs) if this obstruction of public data is confirmed.

Sincerely,

[User's Full Name]
[User's Contact Information / Zip Code]`;

        if (letterDraft) letterDraft.value = letter;
        
        if (btnGmail) btnGmail.href = generateGmailLink({ to: toEmail, subject: subjectLine, body: letter });
        if (btnOutlook) btnOutlook.href = generateOutlookLink({ to: toEmail, subject: subjectLine, body: letter });

        btn.innerText = oldText;
        if (overlay) overlay.classList.remove('hidden');
    }, 600);
};

const handleDispute = (row: any) => {
    const disputeOverlay = document.getElementById('dispute-overlay') as HTMLDivElement;
    const disputeDraft = document.getElementById('dispute-draft') as HTMLTextAreaElement;
    const btnDClip = document.getElementById('btn-dispute-copy') as HTMLButtonElement;
    const btnDGmail = document.getElementById('btn-dispute-gmail') as HTMLAnchorElement;
    const btnDOutlook = document.getElementById('btn-dispute-outlook') as HTMLAnchorElement;

    const priceHtml = row.cash_price.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
    const letter = `Dear Billing Department,

I am writing to formally dispute the charges on my recent bill for ${row.description}. 

According to your facility's publicly available Machine-Readable File (MRF) as required by federal law (under the CY ${new Date().getFullYear()} CMS Final Rule), the attested Cash Rate for this procedure at ${row.hospital_name} is ${priceHtml}. 

I was billed a higher rate. Because this cash rate is a publicly attested price point disclosed for consumer transparency, I demand that my bill be adjusted to reflect this rate immediately.

Sincerely,
[Your Name]`;

    if (disputeDraft) disputeDraft.value = letter;
    const subject = `Billing Dispute: ${row.hospital_name} (Cash Rate Parity)`;
    const to = "billing@" + row.hospital_name.toLowerCase().replace(/\s+/g, '') + ".com";
    
    if (btnDGmail) btnDGmail.href = generateGmailLink({ to, subject, body: letter });
    if (btnDOutlook) btnDOutlook.href = generateOutlookLink({ to, subject, body: letter });

    if (btnDClip) {
        btnDClip.onclick = () => {
            copyToClipboard(letter);
            const oldTxt = btnDClip.innerText;
            btnDClip.innerText = "COPIED ✓";
            btnDClip.style.background = "var(--rh-green)";
            setTimeout(() => {
                btnDClip.innerText = oldTxt;
                btnDClip.style.background = "";
            }, 2000);
        };
    }

    if (disputeOverlay) disputeOverlay.classList.remove('hidden');
};

const closeOverlays = () => {
    overlay.classList.add('hidden');
    document.getElementById('dispute-overlay')?.classList.add('hidden');
};

sheetBackdrop.addEventListener('click', closeOverlays);
btnCloseLetter.addEventListener('click', closeOverlays);
document.getElementById('btn-cancel-letter')?.addEventListener('click', closeOverlays);
document.getElementById('btn-close-dispute')?.addEventListener('click', closeOverlays);
document.getElementById('btn-cancel-dispute')?.addEventListener('click', closeOverlays);

btnCopy.addEventListener('click', () => {
    copyToClipboard(letterDraft.value);
    const oldText = btnCopy.innerText;
    btnCopy.innerText = "COPIED ✓";
    setTimeout(() => btnCopy.innerText = oldText, 2000);
});

function generateGmailLink({ to, subject, body }: { to: string, subject: string, body: string }) {
    return `https://mail.google.com/mail/?view=cm&fs=1&to=${to}&su=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

function generateOutlookLink({ to, subject, body }: { to: string, subject: string, body: string }) {
    return `https://outlook.office.com/mail/deeplink/compose?to=${to}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

function copyToClipboard(text: string) {
    navigator.clipboard.writeText(text);
}

function setupDynamicYear() {
    const year = new Date().getFullYear();
    document.querySelectorAll('.current-year').forEach(el => {
        el.textContent = year.toString();
    });
    
    // Attempt meta tag hydration for link previews
    const metaDesc = document.getElementById('meta-desc');
    if (metaDesc) {
        metaDesc.setAttribute('content', metaDesc.getAttribute('content')?.replace(/2026/g, year.toString()) || '');
    }
    const ogDesc = document.getElementById('og-desc');
    if (ogDesc) {
        ogDesc.setAttribute('content', ogDesc.getAttribute('content')?.replace(/2026/g, year.toString()) || '');
    }
    const ldJson = document.getElementById('ld-json');
    if (ldJson) {
        try {
            const data = JSON.parse(ldJson.textContent || '{}');
            if (data.description) {
                data.description = data.description.replace(/2026/g, year.toString());
                ldJson.textContent = JSON.stringify(data, null, 2);
            }
        } catch (e) {}
    }
}
