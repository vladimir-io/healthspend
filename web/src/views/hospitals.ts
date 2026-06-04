import {
  searchCompliance,
  getTotalComplianceCount,
  searchComplianceIncidents,
  getTotalIncidentCount,
  type ComplianceRecord,
} from '../compliance';
import { handleComplaint } from '../mail.js';
import { getHospitalsTab, setHospitalsTab, type HospitalsTab } from '../app/nav';
import { showAuditDetail } from './detail';

const STATES = [
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
  'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA',
  'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
];

const LIST_CHECKS = [
  { label: 'Star', ok: (r: ComplianceRecord) => r.txt_exists, title: 'CMS overall star rating reported' },
  { label: 'Emergency', ok: (r: ComplianceRecord) => r.robots_ok, title: 'Emergency services disclosed' },
  { label: 'HCAHPS', ok: (r: ComplianceRecord) => r.mrf_valid, title: 'Patient experience measures submitted' },
  { label: 'Readmit', ok: (r: ComplianceRecord) => r.shoppable_exists, title: 'Readmission measures reported' },
];

function scoreColor(s: number) {
  return s > 80 ? 'var(--rh-green)' : s > 50 ? 'var(--amber)' : 'var(--yc-orange)';
}

export async function renderHospitals(viewId: string): Promise<void> {
  const container = document.getElementById(viewId);
  if (!container) return;

  let tab: HospitalsTab = getHospitalsTab();
  let query = '';
  let state = '';
  let page = 1;
  const pageSize = 20;

  container.innerHTML = `
    <div class="audit-hub hospitals-hub">
      <header class="hero-section animate-up" style="--stagger:1;">
        <div class="status-pill-container">
          <div class="live-dot"></div>
          <span>7,400+ hospitals · CMS reporting signals</span>
        </div>
        <h1 class="hero-headline">Hospital directory</h1>
        <p class="hero-sub">Browse transparency scores from public CMS data. Use <strong>Filing issues</strong> when a machine-readable price file is broken — not for billing disputes (use <a href="#search">Prices</a>).</p>
      </header>

      <div class="hospitals-tabs animate-up" style="--stagger:1.5;" role="tablist" aria-label="Hospital lists">
        <button type="button" class="hospitals-tab is-active" data-h-tab="all" role="tab" aria-selected="true">All hospitals</button>
        <button type="button" class="hospitals-tab" data-h-tab="failing" role="tab" aria-selected="false">Filing issues</button>
      </div>

      <div class="audit-controls animate-up" style="--stagger:2;">
        <div class="search-container" style="flex:1;">
          <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" style="color:var(--yc-orange);flex-shrink:0;"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <div class="search-divider" style="width:1px;height:24px;margin:0;"></div>
          <input type="text" id="hospitals-search" placeholder="Hospital name or CCN…" autocomplete="off" />
          <div class="search-state-wrapper">
            <select id="hospitals-state" class="state-pill" style="min-width:60px;">
              <option value="">NATIONAL</option>
              ${STATES.map((s) => `<option value="${s}">${s}</option>`).join('')}
            </select>
            <div class="search-divider"></div>
            <select id="hospitals-sort" class="state-pill hospitals-sort-all" style="min-width:130px;">
              <option value="score-desc">Score: high → low</option>
              <option value="score-asc">Score: low → high</option>
              <option value="name-asc">Name A–Z</option>
            </select>
          </div>
        </div>
      </div>

      <div id="hospitals-summary" class="results-summary animate-up" style="--stagger:2.5;">Loading…</div>
      <div id="hospitals-results" class="audit-list animate-up" style="--stagger:3;">
        ${[...Array(4)].map(() => `<div class="skeleton-card animate-pulse" style="height:76px;"></div>`).join('')}
      </div>
      <div id="hospitals-pagination" class="pagination-hub animate-up" style="--stagger:4;"></div>
    </div>
  `;

  const resultsList = document.getElementById('hospitals-results') as HTMLDivElement;
  const searchInput = document.getElementById('hospitals-search') as HTMLInputElement;
  const stateFilter = document.getElementById('hospitals-state') as HTMLSelectElement;
  const sortFilter = document.getElementById('hospitals-sort') as HTMLSelectElement;
  const paginationHub = document.getElementById('hospitals-pagination') as HTMLDivElement;
  const summaryEl = document.getElementById('hospitals-summary') as HTMLDivElement;
  const tabButtons = container.querySelectorAll<HTMLButtonElement>('[data-h-tab]');

  const syncTabs = () => {
    tabButtons.forEach((btn) => {
      const active = btn.dataset.hTab === tab;
      btn.classList.toggle('is-active', active);
      btn.setAttribute('aria-selected', active ? 'true' : 'false');
    });
    sortFilter?.classList.toggle('hidden', tab === 'failing');
  };

  const renderAllRow = (row: ComplianceRecord) => {
    const sc = row.score;
    const col = scoreColor(sc);
    const dots = LIST_CHECKS.map((c) =>
      `<div class="status-dot" title="${c.label}: ${c.ok(row) ? 'Reported' : 'Gap'} — ${c.title}"
        style="background:${c.ok(row) ? 'var(--rh-green)' : 'var(--yc-orange)'};
        box-shadow:0 0 6px ${c.ok(row) ? 'var(--rh-green-glow)' : 'var(--yc-orange-glow)'};"></div>`
    ).join('');
    return `
      <div class="audit-row ${sc === 100 ? 'perfect-audit-card' : ''}" data-ccn="${row.ccn}"
        style="grid-template-columns:1fr 56px 108px 120px;">
        <div class="audit-identity">
          <h3>${row.name}</h3>
          <p>${row.city}, ${row.state} · <span style="font-family:var(--font-mono);font-size:0.68rem;">CCN ${row.ccn}</span></p>
        </div>
        <div class="score-ring" style="color:${col};"><span class="val" style="color:${col};">${sc}</span><span class="lbl">score</span></div>
        <div class="status-dots" aria-label="Reporting signals">${dots}</div>
        <div style="text-align:right;"><button type="button" class="brutalist-action btn-hospital-detail" style="border-color:var(--yc-orange);color:var(--yc-orange);">Details</button></div>
      </div>`;
  };

  const renderFailingRow = (record: ComplianceRecord) => {
    const gaps = LIST_CHECKS.filter((c) => !c.ok(record)).map((c) => `<span class="tag red">${c.label}</span>`).join('');
    return `
      <div class="incident-row audit-row" data-ccn="${record.ccn}" style="grid-template-columns:1fr auto 168px;">
        <div class="audit-identity">
          <p style="font-family:var(--font-mono);font-size:0.72rem;color:var(--text-tertiary);margin-bottom:4px;">${record.city}, ${record.state} · CCN ${record.ccn}</p>
          <h3>${record.name}</h3>
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end;">${gaps || '<span class="tag ghost">Review</span>'}</div>
        <div style="text-align:right;display:flex;flex-direction:column;gap:8px;align-items:flex-end;">
          <button type="button" class="btn secondary btn-hospital-cms" style="font-size:0.72rem;padding:6px 14px;border-color:var(--yc-orange);color:var(--yc-orange);">Send to CMS</button>
          <button type="button" class="brutalist-action btn-hospital-detail" style="font-size:0.62rem;">Details</button>
        </div>
      </div>`;
  };

  const wireRows = (results: ComplianceRecord[]) => {
    resultsList.querySelectorAll<HTMLElement>('.audit-row').forEach((el) => {
      const rec = results.find((r) => r.ccn === el.dataset.ccn);
      if (!rec) return;
      el.querySelector('.btn-hospital-detail')?.addEventListener('click', (e) => {
        e.stopPropagation();
        void showAuditDetail(rec);
      });
      el.querySelector('.btn-hospital-cms')?.addEventListener('click', (e) => {
        e.stopPropagation();
        handleComplaint(rec);
      });
      if (tab === 'failing') {
        el.addEventListener('click', () => void showAuditDetail(rec));
      } else {
        el.addEventListener('click', () => void showAuditDetail(rec));
      }
    });
  };

  const renderPagination = (total: number) => {
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const noun = tab === 'failing' ? 'facilities with issues' : 'hospitals';
    paginationHub.innerHTML = `
      <span>${((page - 1) * pageSize) + 1}–${Math.min(page * pageSize, total)} of ${total.toLocaleString()} ${noun}</span>
      <div class="pagination-controls">
        <button type="button" class="brutalist-action btn-prev" ${page === 1 ? 'disabled' : ''}>← Prev</button>
        <button type="button" class="brutalist-action btn-next" ${page >= totalPages ? 'disabled' : ''}>Next →</button>
      </div>`;
    paginationHub.querySelector('.btn-prev')?.addEventListener('click', () => { page--; void updateResults(); });
    paginationHub.querySelector('.btn-next')?.addEventListener('click', () => { page++; void updateResults(); });
  };

  const updateResults = async () => {
    syncTabs();
    const offset = (page - 1) * pageSize;
    resultsList.innerHTML = [...Array(4)]
      .map(() => `<div class="skeleton-card animate-pulse" style="height:76px;"></div>`)
      .join('');

    try {
      if (tab === 'failing') {
        const [results, total] = await Promise.all([
          searchComplianceIncidents(query, state, pageSize, offset),
          getTotalIncidentCount(query, state),
        ]);
        summaryEl.innerHTML = `<strong>${total.toLocaleString()}</strong> hospitals with weak transparency reporting · ${state || 'US'} · page ${page}`;
        if (results.length === 0) {
          resultsList.innerHTML = `<div class="empty-state"><p>No hospitals match this filter.</p></div>`;
          paginationHub.innerHTML = '';
          return;
        }
        resultsList.innerHTML = results.map(renderFailingRow).join('');
        wireRows(results);
        renderPagination(total);
        return;
      }

      const [results, total] = await Promise.all([
        searchCompliance(query, state, pageSize, offset, sortFilter?.value || 'score-desc'),
        getTotalComplianceCount(query, state),
      ]);
      summaryEl.innerHTML = `<strong>${total.toLocaleString()}</strong> hospitals · ${state || 'US'} · page ${page}`;
      if (results.length === 0) {
        resultsList.innerHTML = `<div class="empty-state"><p>No hospitals match this filter.</p></div>`;
        paginationHub.innerHTML = '';
        return;
      }
      resultsList.innerHTML = results.map(renderAllRow).join('');
      wireRows(results);
      renderPagination(total);
    } catch (e) {
      console.error(e);
      resultsList.innerHTML = `<div class="empty-state empty-state--error"><p>Could not load hospital data. Check your connection and reload.</p></div>`;
    }
  };

  tabButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const next = btn.dataset.hTab as HospitalsTab;
      if (next === tab) return;
      tab = next;
      setHospitalsTab(tab);
      page = 1;
      const url = new URL(window.location.href);
      if (tab === 'failing') url.searchParams.set('tab', 'failing');
      else url.searchParams.delete('tab');
      window.history.replaceState(null, '', `${url.pathname}${url.search}${url.hash}`);
      void updateResults();
    });
  });

  window.addEventListener('hs:hospitals-tab', ((e: CustomEvent<HospitalsTab>) => {
    tab = e.detail;
    page = 1;
    syncTabs();
    void updateResults();
  }) as EventListener);

  let debounce: ReturnType<typeof setTimeout>;
  searchInput.addEventListener('input', () => {
    clearTimeout(debounce);
    debounce = setTimeout(() => { query = searchInput.value; page = 1; void updateResults(); }, 280);
  });
  stateFilter.addEventListener('change', () => { state = stateFilter.value; page = 1; void updateResults(); });
  sortFilter?.addEventListener('change', () => { page = 1; void updateResults(); });

  syncTabs();
  await updateResults();
}
