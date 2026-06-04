import { enrichSearchStats, getRecommendations, searchPricesWithMeta } from '../db';
import { NPI_CONFIDENCE_THRESHOLD } from '../config';
import { recordRum } from '../rum';
import { FALLBACK_LABELS, formatResultsSummary } from './copy';
import { handleDispute, handleDraft } from './overlays';

export const SEARCH_CONFIDENCE_THRESHOLD = NPI_CONFIDENCE_THRESHOLD;
const SEARCH_PAGE_SIZE = 100;

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
  resultsContainer.innerHTML = `
    <div class="empty-state empty-state--${tone}">
      <p>${message}</p>
    </div>`;
  resultsContainer.classList.remove('hidden');
  document.getElementById('search-welcome')?.classList.add('hidden');
}

function setSearchLoading(loading: boolean) {
  currentSearchState.loading = loading;
  searchLoadingIndicator?.classList.toggle('hidden', !loading);
  searchLoadingIndicator?.setAttribute('aria-busy', loading ? 'true' : 'false');
}

function getSearchDisplayRange() {
  if (currentResults.length === 0) return '0';
  const start = currentSearchState.offset + 1;
  const end = currentSearchState.offset + currentResults.length;
  return `${start.toLocaleString()}–${end.toLocaleString()} of ${currentSearchState.total.toLocaleString()}`;
}

let debounceTimer: ReturnType<typeof setTimeout> | undefined;
let recsDebounceTimer: any;

input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    const query = input.value;
    const state = stateSelect.value;

    clearTimeout(recsDebounceTimer);
    recsDebounceTimer = setTimeout(() => {
      const recs = getRecommendations(input.value);
      if (recs.length > 0) {
        renderRecommendations(recs);
        recommendationEl.classList.remove('hidden');
      } else {
        recommendationEl.classList.add('hidden');
      }
    }, 100);

    if (query.length > 2) {
        debounceTimer = setTimeout(() => performSearch(query, state), 280);
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

export async function performSearch(query: string, state: string = '') {
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
            const statsEl = document.getElementById('omitted-stats');
            if (statsEl) {
              if (response.dataQualityIssue === 'missing_attribution_confidence') {
                statsEl.innerText = 'Incomplete filing hidden';
                statsEl.style.color = 'var(--yc-orange)';
              } else if (response.dataQualityIssue === 'missing_attribution_confidence_relaxed') {
                statsEl.innerText = 'Best available match';
                statsEl.style.color = 'var(--text-tertiary)';
              } else {
                statsEl.innerText = isFallback
                  ? (FALLBACK_LABELS[fallbackReason] || FALLBACK_LABELS.national_scope)
                  : '';
                statsEl.style.color = isFallback ? 'var(--yc-orange)' : 'var(--text-tertiary)';
              }
            }

            if (resultsSummaryEl) {
              const resultScope =
                response.total > currentResults.length
                  ? `Showing ${getSearchDisplayRange()}`
                  : `${response.total.toLocaleString()} hospitals`;
              const confidenceSegment =
                response.dataQualityIssue === 'missing_attribution_confidence_relaxed'
                  ? 'Best available matching'
                  : `NPI confidence ≥ ${CONFIDENCE_FLOOR_LABEL}`;
              const summary = formatResultsSummary({
                showing: resultScope,
                confidence: confidenceSegment,
                fallback: isFallback,
              });
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
            document.getElementById('search-welcome')?.classList.add('hidden');
            const auditLabel = document.getElementById('results-audit-label');
            if (auditLabel) {
              let label = state ? `${state} prices` : 'National prices';
              if (searchZip) label = `Near ${searchZip}`;
              auditLabel.textContent = label;
            }
            applySortAndRender();

            if (response.statsPending && response.statsQuery) {
              void enrichSearchStats(
                response.statsQuery,
                SEARCH_PAGE_SIZE,
                currentSearchState.offset,
                results.length
              ).then((stats) => {
                if (searchId !== searchCounter) return;
                currentSearchState.total = stats.total;
                currentSearchState.truncated = stats.total > currentSearchState.offset + currentResults.length;
                if (stats.market && marketPanel) {
                  const minEl = document.getElementById('market-min');
                  const medEl = document.getElementById('market-median');
                  const p10El = document.getElementById('market-p10');
                  const p90El = document.getElementById('market-p90');
                  const maxEl = document.getElementById('market-max');
                  if (minEl) minEl.innerText = stats.market.min.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                  if (medEl) medEl.innerText = stats.market.median.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                  if (p10El) p10El.innerText = stats.market.p10.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                  if (p90El) p90El.innerText = stats.market.p90.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                  if (maxEl) maxEl.innerText = stats.market.max.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
                  marketPanel.classList.remove('hidden');
                }
                if (resultsSummaryEl) {
                  resultsSummaryEl.innerText = formatResultsSummary({
                    showing:
                      stats.total > currentResults.length
                        ? `Showing ${getSearchDisplayRange()}`
                        : `${stats.total.toLocaleString()} hospitals`,
                    queryMs: response.diagnostics.queryMs,
                  });
                }
                recordRum({ name: 'search_stats', ms: 0, meta: { total: stats.total } });
              });
            }
        }
    } catch (e) {
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
            <div class="empty-state">
              <p>${strictNoData ? 'This hospital’s filing is incomplete for strict matching.' : 'No published cash rates matched your search.'}</p>
              <small>${strictNoData ? `We only show rows with NPI confidence ≥ ${CONFIDENCE_FLOOR_LABEL}.` : 'Try a broader procedure name, CPT code, or remove the state filter.'}</small>
            </div>`;
        return;
    }

  let renderedCount = 0;

    validResults.forEach((row, index) => {
    try {
        const el = document.createElement('div');
        el.style.setProperty('--card-i', String(index));
        const city     = row.city  || '';
        const state    = row.state || '';
        const score    = row.score ?? 0;
      const hasAttestedAudit = score > 0;
        const location = (city && state) ? `${city}, ${state}` : (city || state || 'Location not available');
        const price    = (row.cash_price ?? 0).toLocaleString('en-US', { style: 'currency', currency: 'USD' });
        const scoreCol = score > 80 ? 'var(--rh-green)' : score > 50 ? 'var(--amber)' : 'var(--yc-orange)';
      const auditText = hasAttestedAudit
        ? 'Published cash line item (hospital transparency filing)'
        : 'Cash rate from filing · full attestation in Audit Index';
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
        const fallbackBadge = row.isFallback ? `<span class="badge-fallback">${fallbackBadgeText}</span>` : '';

        const perfectScoreClass = score === 100 ? 'perfect-audit-card' : '';
        const perfectScoreBadge = score === 100 ? `<span class="badge-perfect">★ Perfect Audit</span>` : '';
        const auditChipClass = score === 100 ? 'result-audit-chip result-audit-chip--perfect' : 'result-audit-chip';
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

        const cmsSecondary = hasComplianceSignals && score !== 100
          ? '<button class="btn-ghost btn-report-link btn-cms-secondary" type="button">Report filing issue</button>'
          : '';

        el.innerHTML = `
            <div class="search-result-card card-stagger ${perfectScoreClass}">
                <div class="result-main-col">
                    <p class="result-label">Procedure ${fallbackBadge}</p>
                    <p class="result-procedure">${row.description}</p>
                    <p class="result-hospital">${row.hospital_name} &nbsp;·&nbsp; <span class="result-location">${location}</span> ${perfectScoreBadge}</p>
                    <p class="result-attest">${auditText}</p>
                    <div class="result-meta-row">
                      <span class="confidence-pill ${confidenceTone}" title="Confidence for procedure to provider attribution">Confidence ${attributionConfidence}%</span>
                      <span class="${auditChipClass}"${score === 100 ? '' : ` style="color:${scoreCol}"`}>Audit ${score}/100</span>
                      ${evidenceFlags.join('')}
                    </div>
                    ${negotiatedPanel}
                </div>

                <aside class="result-cta-col">
                    <p class="result-label">Published cash rate</p>
                    <div class="result-price result-price--hero">${price}</div>
                    <button type="button" class="btn btn-primary-lg btn-dispute">Use this rate</button>
                    <p class="cta-hint">Email template for estimates or billing questions</p>
                    <div class="result-secondary-actions">
                      <button type="button" class="btn-ghost btn-audit-detail">Full audit</button>
                      ${cmsSecondary}
                    </div>
                </aside>
            </div>
        `;

        el.querySelector('.btn-audit-detail')?.addEventListener('click', async () => {
            const { showAuditDetail } = await import('../views/detail.js');
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
      } catch {
        // skip malformed row
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

    renderLoadMoreControl();
}

