import { searchCompliance, getTotalComplianceCount } from '../compliance';
import { showAuditDetail } from './detail';

const STATES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'];

export async function renderScorecard(viewId: string) {
    const container = document.getElementById(viewId);
    if (!container) return;

    let query = '';
    let state = '';
    let page = 1;
    const pageSize = 20;

    container.innerHTML = `
        <div class="audit-hub">
            <div class="hero-section animate-up" style="--stagger:1;">
                <div class="status-pill-container">
                    <div class="live-dot"></div>
                    <span>7,400+ Federal Hospital Nodes</span>
                </div>
                <h1 class="hero-headline">Facility Audit Index</h1>
                    <p class="hero-sub">Nationwide monitoring of federal price transparency compliance across 7,400+ CMS registered hospitals.</p>
            </div>

            <div class="audit-controls animate-up" style="--stagger:2;">
                <div class="search-container" style="flex:1;">
                    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" style="color:var(--yc-orange);flex-shrink:0;"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
                    <div class="search-divider" style="width:1px;height:24px;margin:0;"></div>
                    <input type="text" id="scorecard-search" placeholder="Filter node name or CCN..." />
                    
                    <div class="search-state-wrapper">
                      <select id="scorecard-state" class="state-pill" style="min-width: 60px;">
                          <option value="">NATIONAL</option>
                          ${STATES.map(s => `<option value="${s}">${s}</option>`).join('')}
                      </select>
                      <div class="search-divider"></div>
                      <select id="scorecard-sort" class="state-pill" style="min-width: 130px;">
                          <option value="score-desc">SORT: RANK ↓</option>
                          <option value="score-asc">SORT: RANK ↑</option>
                          <option value="name-asc">SORT: A–Z</option>
                      </select>
                    </div>
                </div>
            </div>

            <div id="scorecard-results-summary" class="results-summary animate-up" style="--stagger:2.5;">
                Loading federal nodes…
            </div>

            <div id="scorecard-results" class="audit-list animate-up" style="--stagger:3;">
                ${[...Array(5)].map(() => `<div class="skeleton-card animate-pulse" style="height:76px;"></div>`).join('')}
            </div>

            <div id="scorecard-pagination" class="pagination-hub animate-up" style="--stagger:4;"></div>
        </div>
    `;

    const resultsList = document.getElementById('scorecard-results') as HTMLDivElement;
    const searchInput = document.getElementById('scorecard-search') as HTMLInputElement;
    const stateFilter = document.getElementById('scorecard-state') as HTMLSelectElement;
    const sortFilter = document.getElementById('scorecard-sort') as HTMLSelectElement;
    const paginationHub = document.getElementById('scorecard-pagination') as HTMLDivElement;
    const summaryEl = document.getElementById('scorecard-results-summary') as HTMLDivElement;

    const scoreColor = (s: number) => s > 80 ? 'var(--rh-green)' : s > 50 ? 'var(--amber)' : 'var(--yc-orange)';

    const updateResults = async () => {
        const offset = (page - 1) * pageSize;
        resultsList.innerHTML = [...Array(4)].map(() =>
            `<div class="skeleton-card animate-pulse" style="height:76px;"></div>`
        ).join('');

        try {
            const [results, total] = await Promise.all([
                searchCompliance(query, state, pageSize, offset, sortFilter?.value || 'score-desc'),
                getTotalComplianceCount(query, state)
            ]);

            summaryEl.innerHTML = `<strong>${total.toLocaleString()} nodes</strong> matched &nbsp;·&nbsp; Region: ${state || 'Federal'} &nbsp;·&nbsp; Page ${page} of ${Math.ceil(total / pageSize)}`;

            if (results.length === 0) {
                resultsList.innerHTML = `<div style="padding:60px;text-align:center;color:var(--text-tertiary);">No facilities match this filter.</div>`;
                paginationHub.innerHTML = '';
                return;
            }

            resultsList.innerHTML = results.map(row => {
                const sc = row.score;
                const col = scoreColor(sc);
                const perfectClass = sc === 100 ? 'perfect-audit-card' : '';
                const complianceItems = [
                    { label: 'Price Pub.', ok: row.txt_exists, title: '§ 180.50' },
                    { label: 'Bot Access', ok: row.robots_ok, title: '§ 180.40' },
                    { label: 'MRF Schema', ok: row.mrf_valid, title: 'CMS v2.0' },
                    { label: 'Shoppable', ok: row.shoppable_exists, title: 'Consumer Tool' },
                ];
                const dots = complianceItems.map(c =>
                    `<div class="status-dot" title="${c.label}: ${c.ok ? 'Pass' : 'Fail'} (${c.title})"
                        style="background:${c.ok ? 'var(--rh-green)' : 'var(--yc-orange)'};
                        box-shadow:0 0 6px ${c.ok ? 'var(--rh-green-glow)' : 'var(--yc-orange-glow)'};"></div>`
                ).join('');

                return `
                    <div class="audit-row ${perfectClass}" data-ccn="${row.ccn}"
                        style="grid-template-columns: 1fr 60px 120px 130px;">
                        <div class="audit-identity">
                            <h3>${row.name}</h3>
                            <p>${row.city}, ${row.state} &nbsp;·&nbsp; <span style="font-family:var(--font-mono);font-size:0.68rem;">CCN ${row.ccn}</span></p>
                        </div>
                        <div class="score-ring" style="color:${col};">
                            <span class="val" style="color:${col};">${sc}</span>
                            <span class="lbl">IDX</span>
                        </div>
                        <div class="status-dots">${dots}</div>
                        <div style="text-align:right;">
                            <button class="brutalist-action" style="border-color:var(--yc-orange);color:var(--yc-orange);">View Audit →</button>
                        </div>
                    </div>`;
            }).join('');

            resultsList.querySelectorAll<HTMLElement>('.audit-row').forEach(el => {
                el.addEventListener('click', () => {
                    const rec = results.find(r => r.ccn === el.dataset.ccn);
                    if (rec) showAuditDetail(rec);
                });
            });

            renderPagination(total);

            const apiFooter = document.createElement('div');
            apiFooter.className = 'api-cta-footer animate-up';
            apiFooter.innerHTML = `Need this data as a structured feed? &nbsp;&middot;&nbsp; <strong>Use the Healthspend API</strong>`;
            apiFooter.onclick = () => window.location.hash = 'api';
            resultsList.appendChild(apiFooter);
        } catch (e) {
            console.error(e);
            resultsList.innerHTML = `<div style="padding:40px;text-align:center;color:var(--red);">Audit engine error. Please reload.</div>`;
        }
    };

    const renderPagination = (total: number) => {
        const totalPages = Math.ceil(total / pageSize);
        paginationHub.innerHTML = `
            <span>Showing ${((page - 1) * pageSize) + 1}–${Math.min(page * pageSize, total)} of ${total.toLocaleString()}</span>
            <div class="pagination-controls">
                <button class="brutalist-action btn-prev" ${page === 1 ? 'disabled' : ''}>← Prev</button>
                <button class="brutalist-action btn-next" ${page >= totalPages ? 'disabled' : ''}>Next →</button>
            </div>`;
        paginationHub.querySelector('.btn-prev')?.addEventListener('click', () => { page--; updateResults(); });
        paginationHub.querySelector('.btn-next')?.addEventListener('click', () => { page++; updateResults(); });
    };

    let debounce: any;
    searchInput.addEventListener('input', () => {
        clearTimeout(debounce);
        debounce = setTimeout(() => { query = searchInput.value; page = 1; updateResults(); }, 280);
    });
    stateFilter.addEventListener('change', () => { state = stateFilter.value; page = 1; updateResults(); });
    sortFilter.addEventListener('change', () => { page = 1; updateResults(); });

    updateResults();
}
