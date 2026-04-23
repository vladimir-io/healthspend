import { searchComplianceIncidents, getTotalIncidentCount, type ComplianceRecord } from '../compliance.js';
import { handleComplaint } from '../mail.js';
import { showAuditDetail } from './detail.js';

const STATES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'];

export function renderIncidents(containerId: string) {
    const container = document.getElementById(containerId);
    if (!container) return;

    let query = '';
    let state = '';
    let page = 1;
    const pageSize = 20;

    container.innerHTML = `
        <div class="hero-section animate-up" style="--stagger:1;">
            <div class="status-pill-container">
                <div class="live-dot" style="background:var(--yc-orange); box-shadow:0 0 8px var(--yc-orange-glow);"></div>
                <span style="color:var(--text-secondary);">7,400+ Federal Hospital Nodes</span>
            </div>
            <h1 class="hero-headline">Incident Log</h1>
                <p class="hero-sub">Nationwide monitoring of federal price transparency compliance across 7,400+ CMS registered hospitals.</p>
        </div>

        <div class="audit-controls animate-up" style="--stagger:2;">
            <div class="search-container" style="flex:1;">
                <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" style="color:var(--violation);flex-shrink:0;"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
                <div class="search-divider" style="width:1px;height:24px;margin:0;"></div>
                <input type="text" id="incidents-search" placeholder="Filter violations by node or CCN..." />
                
                <div class="search-state-wrapper">
                  <select id="incidents-state" class="state-pill" style="min-width: 60px;">
                      <option value="">NATIONAL</option>
                      ${STATES.map(s => `<option value="${s}">${s}</option>`).join('')}
                  </select>
                </div>
            </div>
        </div>

        <div id="incidents-results-summary" class="results-summary animate-up" style="--stagger:2.5;">
            Scanning federal nodes…
        </div>

        <div id="incidents-feed" class="incident-feed animate-up" style="--stagger:3;">
            ${[...Array(5)].map(() => `<div class="skeleton-card animate-pulse" style="height:76px;"></div>`).join('')}
        </div>

        <div id="incidents-pagination" class="pagination-hub animate-up" style="--stagger:4;"></div>
    `;

    const feed = document.getElementById('incidents-feed') as HTMLDivElement;
    const searchInput = document.getElementById('incidents-search') as HTMLInputElement;
    const stateSelect = document.getElementById('incidents-state') as HTMLSelectElement;
    const paginationHub = document.getElementById('incidents-pagination') as HTMLDivElement;
    const summaryEl = document.getElementById('incidents-results-summary') as HTMLDivElement;


    const updateResults = async () => {
        const offset = (page - 1) * pageSize;
        feed.innerHTML = [...Array(3)].map(() =>
            `<div class="skeleton-card animate-pulse" style="height:76px;"></div>`
        ).join('');

        try {
            const [results, total] = await Promise.all([
                searchComplianceIncidents(query, state, pageSize, offset),
                getTotalIncidentCount(query, state)
            ]);

            summaryEl.innerHTML = `<strong style="color:var(--text-primary);">${total.toLocaleString()} violations</strong> detected &nbsp;·&nbsp; Region: ${state || 'Federal'} &nbsp;·&nbsp; Page ${page} of ${Math.ceil(total / pageSize)}`;

            feed.innerHTML = '';
            if (results.length === 0) {
                feed.innerHTML = `<div style="padding:60px;text-align:center;color:var(--text-tertiary);">No violations match this filter.</div>`;
                paginationHub.innerHTML = '';
                return;
            }

            results.forEach((record: ComplianceRecord) => {
                const row = document.createElement('div');
                row.className = 'incident-row animate-up';
                row.style.cssText = 'grid-template-columns: 1fr auto 180px;';

                const badges = [
                    !record.txt_exists && `<span class="tag red">No Price Pub.</span>`,
                    !record.robots_ok && `<span class="tag red">Bot Blocked</span>`,
                    !record.mrf_valid && `<span class="tag amber">Schema Low</span>`,
                ].filter(Boolean).join('');

                row.innerHTML = `
                    <div class="incident-entity">
                        <p class="modal-subtitle" style="font-family:var(--font-mono);font-size:0.75rem;color:var(--text-tertiary);">
                            ${record.city}, ${record.state} &nbsp;·&nbsp; CCN ${record.ccn}
                        </p>
                        <h3 style="margin-top:2px;">${record.name}</h3>
                    </div>
                    <div class="incident-violations" style="justify-content:flex-end; gap:6px;">${badges || '<span class="tag ghost">Minor Issues</span>'}</div>
                    <div style="text-align:right;display:flex;flex-direction:column;gap:8px;align-items:flex-end;">
                        <button class="btn secondary btn-report" style="font-size:0.72rem; padding:6px 14px; border-color:var(--yc-orange); color:var(--yc-orange);">Send to CMS</button>
                        <button class="brutalist-action btn-view" style="font-size:0.6rem; letter-spacing:0.04em;">Audit Trace →</button>
                    </div>
                `;

                row.querySelector('.btn-report')?.addEventListener('click', (e) => {
                    e.stopPropagation();
                    handleComplaint(record);
                });
                row.querySelector('.btn-view')?.addEventListener('click', (e) => {
                    e.stopPropagation();
                    showAuditDetail(record);
                });
                row.addEventListener('click', () => showAuditDetail(record));
                feed.appendChild(row);
            });

            renderPagination(total);

            const apiFooter = document.createElement('div');
            apiFooter.className = 'api-cta-footer animate-up';
            apiFooter.innerHTML = `Need this feed as a structured API? &nbsp;&middot;&nbsp; <strong>Use the Healthspend API</strong>`;
            apiFooter.onclick = () => window.location.hash = 'api';
            feed.appendChild(apiFooter);
        } catch {
            feed.innerHTML = `<div style="padding:40px;text-align:center;color:var(--yc-orange);">Audit engine error. Please reload.</div>`;
        }
    };

    const renderPagination = (total: number) => {
        const totalPages = Math.ceil(total / pageSize);
        paginationHub.innerHTML = `
            <span>Showing ${((page - 1) * pageSize) + 1}–${Math.min(page * pageSize, total)} of ${total.toLocaleString()} violations</span>
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
    stateSelect.addEventListener('change', () => { state = stateSelect.value; page = 1; updateResults(); });

    updateResults();
}


