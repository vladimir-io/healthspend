import { searchPricesWithMeta, getRecommendations } from './db';
import { NPI_CONFIDENCE_THRESHOLD } from './config';

const SEARCH_CONFIDENCE_THRESHOLD = NPI_CONFIDENCE_THRESHOLD;
const SEARCH_PAGE_SIZE = 100;

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
const resultsSummaryEl = document.getElementById('results-summary') as HTMLParagraphElement;
const searchLoadingIndicator = document.getElementById('search-loading-indicator') as HTMLDivElement;
const CONFIDENCE_FLOOR_LABEL = `${Math.round(SEARCH_CONFIDENCE_THRESHOLD * 100)}%`;
const NPI_CONFIDENCE_TOOLTIP = `Strict mode: only rows with NPI confidence >= ${CONFIDENCE_FLOOR_LABEL} are shown by default.`;

let currentResults: any[] = [];
let currentSearchState = {
  query: '',
  state: '',
  zip: '',
  sort: 'price-asc' as 'price-asc' | 'price-desc' | 'score-desc',
  total: 0,
  offset: 0,
  truncated: false,
  dataQualityIssue: null as 'missing_attribution_confidence' | 'missing_attribution_confidence_relaxed' | null,
  loading: false,
};

function showSearchMessage(message: string, tone: 'neutral' | 'error' = 'neutral') {
  const color = tone === 'error' ? 'var(--yc-orange)' : 'var(--text-tertiary)';
  resultsContainer.innerHTML = `
    <div style="padding:44px;text-align:center;color:${color};font-size:0.88rem;">
      ${message}
    </div>`;
  resultsContainer.classList.remove('hidden');
}

function setSearchLoading(loading: boolean) {
  currentSearchState.loading = loading;
  searchLoadingIndicator?.classList.toggle('hidden', !loading);
}

function getSearchDisplayRange() {
  if (currentResults.length === 0) return '0';
  const start = currentSearchState.offset + 1;
  const end = currentSearchState.offset + currentResults.length;
  return `${start.toLocaleString()}–${end.toLocaleString()} of ${currentSearchState.total.toLocaleString()}`;
}

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
      setSearchLoading(false);
      contextBanner.classList.add('hidden');
      if (query.trim().length > 0) {
        showSearchMessage('Keep typing: search runs at 3+ characters.');
      } else {
        resultsContainer.classList.add('hidden');
      }
    }
});

  input.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter') return;
    e.preventDefault();
    const query = input.value.trim();
    const state = stateSelect.value;
    if (query.length < 3) {
      showSearchMessage('Enter at least 3 characters to run search.');
      return;
    }
    performSearch(query, state);
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
  const query = input.value.trim();
  const state = stateSelect.value;
  if (query.length > 2) {
    currentSearchState.offset = 0;
    performSearch(query, state);
  } else if (currentResults.length > 0) {
    applySortAndRender();
  }
});

function applySortAndRender() {
  renderResults(currentResults);
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
    const activeSort = (sortSelect?.value || 'price-asc') as 'price-asc' | 'price-desc' | 'score-desc';
    const searchQuery = (query || '').trim();
    const isSameSearch = searchQuery === currentSearchState.query && state === currentSearchState.state && activeSort === currentSearchState.sort;
    if (!isSameSearch) {
      currentSearchState.offset = 0;
      currentResults = [];
    }
    const isLoadMore = isSameSearch && currentSearchState.offset > 0;
    const searchId = ++searchCounter;
    const marketPanel = document.getElementById('market-rates-panel');
    
    contextBanner.classList.add('hidden');
    marketPanel?.classList.add('hidden');
  resultsContainer.classList.add('hidden');
    setSearchLoading(true);

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
      const minConfidence = SEARCH_CONFIDENCE_THRESHOLD;
      const response = await searchPricesWithMeta(cleanQuery || query, state, searchZip, minConfidence, activeSort, SEARCH_PAGE_SIZE, currentSearchState.offset);
      const results = response.rows;
        if (searchId === searchCounter) {
          setSearchLoading(false);
            currentSearchState = {
              query: cleanQuery || query,
              state,
              zip: searchZip,
              sort: activeSort,
              total: response.total,
              offset: currentSearchState.offset,
              truncated: response.truncated,
              dataQualityIssue: response.dataQualityIssue,
              loading: false,
            };
            currentResults = isLoadMore ? [...currentResults, ...results] : results;
            
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
              if (response.dataQualityIssue === 'missing_attribution_confidence') {
                statsEl.innerText = 'NON-COMPLIANT NODE : INCOMPLETE FILING';
                statsEl.style.color = 'var(--yc-orange)';
              } else if (response.dataQualityIssue === 'missing_attribution_confidence_relaxed') {
                statsEl.innerText = 'FILING NOTE : USING BEST AVAILABLE MATCHING';
                statsEl.style.color = 'var(--text-tertiary)';
              } else {
                statsEl.innerText = isFallback
                  ? (fallbackMessageByReason[fallbackReason] || 'NO LOCAL DATA : SHOWING NATIONAL RESULTS')
                  : '';
                statsEl.style.color = isFallback ? 'var(--yc-orange)' : 'var(--text-tertiary)';
              }
            }

            if (resultsSummaryEl) {
              const resultScope = response.total > currentResults.length
                ? `SHOWING ${getSearchDisplayRange()}`
                : `${response.total.toLocaleString()} RESULTS`;
              const confidenceSegment = response.dataQualityIssue === 'missing_attribution_confidence_relaxed'
                ? 'BEST AVAILABLE MATCHING'
                : `NPI CONFIDENCE >= ${CONFIDENCE_FLOOR_LABEL}`;
              const strictSuffix = response.dataQualityIssue === 'missing_attribution_confidence'
                ? ' · INCOMPLETE FILING HIDDEN'
                : '';
              const summary = `${resultScope} · ${confidenceSegment}${isFallback ? ' · FALLBACK SCOPE APPLIED' : ''}${strictSuffix}`;
              resultsSummaryEl.innerText = summary;
              const titleParts: string[] = [];
              if (response.dataQualityIssue === 'missing_attribution_confidence_relaxed') {
                titleParts.push('Best Available Matching: this filing omits attribution-confidence metadata, so we use the strongest remaining signals.');
              } else {
                titleParts.push(NPI_CONFIDENCE_TOOLTIP);
              }
              if (isFallback) {
                const fallbackTitleByReason: Record<string, string> = {
                  national_scope: 'Fallback Scope Applied: no local matches met this query and confidence scope, so national results are shown.',
                  zip_relaxed_national: 'Fallback Scope Applied: no ZIP-region matches were found, so ZIP was relaxed to national scope.',
                  national_text_match: 'Fallback Scope Applied: no direct CPT match was found, so national text matches are shown.',
                  category_fallback: 'Fallback Scope Applied: no direct procedure match was found, so close category results are shown.',
                };
                titleParts.push(fallbackTitleByReason[fallbackReason] || 'Fallback Scope Applied: local results were insufficient for this query, so a broader scope is shown.');
              }
              resultsSummaryEl.title = titleParts.join(' ');
            }

            // Benchmark must always be computed on full matched scope, not the loaded page slice.
            if (results.length > 0 && marketPanel && response.market) {
              const minEl = document.getElementById('market-min');
              const medEl = document.getElementById('market-median');
              const p10El = document.getElementById('market-p10');
              const p90El = document.getElementById('market-p90');
              const maxEl = document.getElementById('market-max');

              if (minEl) minEl.innerText = response.market.min.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
              if (medEl) medEl.innerText = response.market.median.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
              if (p10El) p10El.innerText = response.market.p10.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
              if (p90El) p90El.innerText = response.market.p90.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
              if (maxEl) maxEl.innerText = response.market.max.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

              marketPanel.classList.remove('hidden');
            }

            contextBanner.classList.remove('hidden');
            applySortAndRender();
        }
    } catch (e) {
      console.error("VFS Search Error:", e);
      setSearchLoading(false);
      contextBanner.classList.remove('hidden');
      currentResults = [];
      if (resultsSummaryEl) {
        resultsSummaryEl.innerText = 'Search temporarily unavailable';
        resultsSummaryEl.title = '';
      }
      showSearchMessage('Search is temporarily unavailable. Refresh and try again, or run schema migration for your local database.', 'error');
    }
}

function renderLoadMoreControl() {
    const existing = resultsContainer.querySelector('.load-more-hub');
    existing?.remove();

    const hasMore = currentSearchState.total > currentResults.length;
    if (!hasMore) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'load-more-hub animate-up';
    wrapper.innerHTML = `
      <button class="load-more-button" type="button">Load ${SEARCH_PAGE_SIZE} more</button>
      <p class="load-more-copy">Showing ${getSearchDisplayRange()} results in this audit slice.</p>
    `;

    wrapper.querySelector('.load-more-button')?.addEventListener('click', () => {
      currentSearchState.offset += SEARCH_PAGE_SIZE;
      performSearch(currentSearchState.query, currentSearchState.state);
    });

    resultsContainer.appendChild(wrapper);
}

function renderResults(results: any[]) {
    const validResults = results; 

    resultsContainer.innerHTML = '';
  resultsContainer.classList.remove('hidden');
  resultsContainer.style.removeProperty('display');
    if (validResults.length === 0) {
      const strictNoData = currentSearchState.dataQualityIssue === 'missing_attribution_confidence';
        resultsContainer.innerHTML = `
            <div style="padding:60px;text-align:center;color:var(--text-tertiary);font-size:0.9rem;">
          ${strictNoData ? 'Incomplete filing detected. This node is hidden under strict confidence policy.' : 'No pricing data found for this search.'}
          <br /><small style="opacity:0.6; margin-top:8px; display:block;">${strictNoData ? `Only records with NPI confidence >= ${CONFIDENCE_FLOOR_LABEL} are shown.` : 'Try a broader procedure or different state.'}</small>
            </div>`;
        return;
    }

  let renderedCount = 0;

    validResults.forEach(row => {
    try {
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
        const attributionConfidence = Math.round(((row.attribution_confidence ?? 1) as number) * 100);
        const confidenceTone = attributionConfidence >= 95 ? 'high' : attributionConfidence >= 85 ? 'mid' : 'low';
        const minNegotiated = Number(row.min_negotiated);
        const maxNegotiated = Number(row.max_negotiated);
        const hasNegotiatedRange = Number.isFinite(minNegotiated) && Number.isFinite(maxNegotiated) && minNegotiated > 0 && maxNegotiated > 0;
        const payer = (row.payer || '').toString().trim();
        const plan = (row.plan || '').toString().trim();
        const hasNegotiatedIntel = hasNegotiatedRange || payer.length > 0 || plan.length > 0;
        const spreadRatio = hasNegotiatedRange && minNegotiated > 0 ? Math.max(1, maxNegotiated / minNegotiated) : 1;

        const evidenceFlags: string[] = [];
        if (row.provider_is_deactivated) evidenceFlags.push('<span class="evidence-pill risk">NPI deactivated</span>');
        if (row.license_proxy_suspected) evidenceFlags.push('<span class="evidence-pill risk">License mismatch</span>');
        if (row.npi_confidence_penalty_reason) evidenceFlags.push('<span class="evidence-pill">Attribution adjusted</span>');
        const hasComplianceSignals = evidenceFlags.length > 0 || !hasAttestedAudit || score < 85;

        const fallbackBadgeText = row.fallbackLabel || 'National Result';
        const fallbackBadge = row.isFallback ? `<span style="font-size:0.55rem; background:rgba(255,165,0,0.1); color:orange; padding:2px 6px; border-radius:4px; margin-left:8px; border:1px solid rgba(255,165,0,0.2); font-family:var(--font-mono); vertical-align:middle; letter-spacing:0.05em; text-transform:uppercase;">${fallbackBadgeText}</span>` : '';

        const perfectScoreClass = score === 100 ? 'perfect-audit-card' : '';
        const perfectScoreBadge = score === 100 ? `<span style="font-size:0.55rem; background:rgba(255,215,0,0.15); color:#FFD700; padding:2px 6px; border-radius:4px; margin-left:8px; border:1px solid rgba(255,215,0,0.3); font-family:var(--font-mono); vertical-align:middle; letter-spacing:0.05em; text-transform:uppercase; box-shadow: 0 0 5px rgba(255,215,0,0.2);">★ Perfect Audit</span>` : '';
        const negotiatedPanel = hasNegotiatedIntel ? `
          <div class="negotiated-panel-wrap">
            <button class="negotiated-toggle" type="button" aria-expanded="false" title="Show negotiated rate context">
              <span>Negotiated Rate Intel</span>
              <span class="negotiated-chevron">▾</span>
            </button>
            <div class="negotiated-panel" aria-hidden="true">
              <div class="negotiated-grid">
                <div class="negotiated-cell">
                  <span class="negotiated-k">Min Negotiated</span>
                  <span class="negotiated-v">${hasNegotiatedRange ? minNegotiated.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }) : 'Unavailable'}</span>
                </div>
                <div class="negotiated-cell">
                  <span class="negotiated-k">Max Negotiated</span>
                  <span class="negotiated-v">${hasNegotiatedRange ? maxNegotiated.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }) : 'Unavailable'}</span>
                </div>
                <div class="negotiated-cell">
                  <span class="negotiated-k">Spread</span>
                  <span class="negotiated-v">${hasNegotiatedRange ? `${spreadRatio.toFixed(2)}x` : 'Unavailable'}</span>
                </div>
                <div class="negotiated-cell">
                  <span class="negotiated-k">Reference Plan</span>
                  <span class="negotiated-v">${plan || payer || 'Unavailable'}</span>
                </div>
              </div>
            </div>
          </div>
        ` : '';

        el.innerHTML = `
            <div class="search-result-card animate-up ${perfectScoreClass}">
                <div>
                    <p class="result-label">Procedure ${fallbackBadge}</p>
                    <p class="result-procedure">${row.description}</p>
                    <p class="result-hospital">${row.hospital_name} &nbsp;·&nbsp; <span class="result-location">${location}</span> ${perfectScoreBadge}</p>
                    <p class="result-attest">${auditText}</p>
                    <div class="result-meta-row">
                      <span class="confidence-pill ${confidenceTone}" title="Confidence for procedure to provider attribution">Confidence ${attributionConfidence}%</span>
                      ${evidenceFlags.join('')}
                    </div>
                    ${negotiatedPanel}
                </div>

                <div class="result-price-block" style="text-align:right;">
                    <p class="result-label">Audit Index</p>
                    <div class="result-score" style="color:${score === 100 ? '#FFD700' : scoreCol}; font-family:var(--font-mono); font-size:1.1rem; font-weight:700;">${score}<span style="font-size:0.6rem; color:var(--text-tertiary); font-weight:400;">/100</span></div>
                    <p class="result-price-label btn-audit-detail" style="cursor:pointer; text-decoration:underline; text-underline-offset:3px; font-weight:700; color:var(--text-secondary); opacity:0.8;">Full Audit</p>
                    <div class="audit-compliance-cta">
                      ${score === 100
                        ? '<p class="audit-quiet-note">No unresolved compliance flags</p>'
                        : hasComplianceSignals
                          ? '<button class="btn-report-link" type="button">Send to CMS</button><p class="audit-quiet-note">Use only when your audit review shows a credible filing issue.</p>'
                          : '<p class="audit-quiet-note">Review Full Audit before reporting.</p>'
                      }
                    </div>
                </div>

                <div class="result-price-block">
                    <p class="result-label">Cash Rate</p>
                    <div class="result-price">${price}</div>
                    <p class="result-price-label">Negotiated Net</p>
                </div>

                <div class="result-actions-col">
                    <button class="btn primary btn-dispute" style="width:100%;">Claim Rate</button>
                </div>
            </div>
        `;

        el.querySelector('.btn-audit-detail')?.addEventListener('click', async () => {
            const { showAuditDetail } = await import('./views/detail.js');
            // We need name/state/city for the Header in detail
            showAuditDetail({ ...row, name: row.hospital_name });
        });
        el.querySelector('.negotiated-toggle')?.addEventListener('click', () => {
          const toggle = el.querySelector('.negotiated-toggle') as HTMLButtonElement | null;
          const panel = el.querySelector('.negotiated-panel') as HTMLDivElement | null;
          if (!toggle || !panel) return;
          const isOpen = toggle.getAttribute('aria-expanded') === 'true';
          toggle.setAttribute('aria-expanded', isOpen ? 'false' : 'true');
          panel.setAttribute('aria-hidden', isOpen ? 'true' : 'false');
          panel.classList.toggle('open', !isOpen);
        });
        el.querySelector('.btn-dispute')?.addEventListener('click', () => handleDispute(row));
        el.querySelector('.btn-report-link')?.addEventListener('click', () => {
          const reason = window.prompt('Briefly describe the compliance issue you observed (e.g., placeholder values, missing dollar amounts, broken MRF link).');
          if (!reason) return;
          const cleanReason = reason.trim();
          if (cleanReason.length < 12) {
            window.alert('Please include a bit more detail before drafting a CMS complaint.');
            return;
          }
          handleDraft(row, cleanReason);
        });
        resultsContainer.appendChild(el);
        renderedCount += 1;
      } catch (err) {
        console.error('Result row render error', err, row);
      }
    });

    if (renderedCount === 0) {
      resultsContainer.innerHTML = `
        <div style="padding:48px 24px;text-align:center;color:var(--yc-orange);font-size:0.9rem;">
          Data was found but could not be rendered in card view.
          <br /><small style="opacity:0.75; margin-top:8px; display:block;">Try reload once. If it persists, there is a malformed row shape in this local dataset.</small>
        </div>`;
      return;
    }

    const apiFooter = document.createElement('div');
    apiFooter.className = 'api-cta-footer animate-up';
    apiFooter.innerHTML = `Need this data in your own stack? &nbsp;&middot;&nbsp; <strong>Use the Healthspend API</strong>`;
    apiFooter.onclick = () => window.location.hash = 'api';
    resultsContainer.appendChild(apiFooter);
    renderLoadMoreControl();
}

const handleDraft = (row: any, observedReason: string) => {
        const subjectLine = `Formal Complaint of Noncompliance: ${row.hospital_name}`;
        const toEmail = "HPTCompliance@cms.hhs.gov";

        const letter = `To the CMS Price Transparency Enforcement Division:

I am submitting a formal complaint regarding suspected noncompliance by ${row.hospital_name} with the Hospital Price Transparency Rule (45 CFR § 180.50).

Observed issue:
${observedReason}

An independent review of this facility's data catalog revealed the use of algorithm-derived models and estimated pricing, specifically regarding CPT Code ${row.cpt_code}. Under the ${new Date().getFullYear()} CMS Final Rule, the use of estimates is strictly prohibited; hospitals are legally mandated to publish actual dollar amounts for all standard charges in a machine-readable format.

I respectfully request that CMS open an immediate investigation into this facility's disclosures and assess Civil Monetary Penalties (CMPs) if this obstruction of public data is confirmed.

Sincerely,

[User's Full Name]
[User's Contact Information / Zip Code]`;

        if (letterDraft) letterDraft.value = letter;
        
        if (btnGmail) btnGmail.href = generateGmailLink({ to: toEmail, subject: subjectLine, body: letter });
        if (btnOutlook) btnOutlook.href = generateOutlookLink({ to: toEmail, subject: subjectLine, body: letter });
        if (overlay) overlay.classList.remove('hidden');
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

According to your facility's publicly available Machine-Readable File (MRF) as required by federal law (under the ${new Date().getFullYear()} CMS Final Rule), the attested Cash Rate for this procedure at ${row.hospital_name} is ${priceHtml}. 

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
