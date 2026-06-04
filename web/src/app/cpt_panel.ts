import { closeSheet, openSheet } from './micro.js';
import { performSearch } from './search_controller';

export function setupCptPanel(): void {
  const overlay = document.getElementById('cpt-overlay')!;
  const backdrop = document.getElementById('cpt-backdrop')!;
  const btnOpen = document.getElementById('btn-open-cpt')!;
  const btnClose = document.getElementById('btn-close-cpt')!;
  const filterInput = document.getElementById('cpt-filter') as HTMLInputElement;
  const pillsEl = document.getElementById('cpt-category-pills')!;
  const tableEl = document.getElementById('cpt-table-container')!;
  const input = document.getElementById('search-input') as HTMLInputElement;
  const stateSelect = document.getElementById('search-state') as HTMLSelectElement;

  let activeCategory = '';
  const open = () => openSheet(overlay);
  const close = () => closeSheet(overlay);
  btnOpen.addEventListener('click', () => {
    void renderTable('', '');
    open();
  });
  btnClose.addEventListener('click', close);
  backdrop.addEventListener('click', close);

  async function renderTable(filter: string, category: string) {
    const { CPT_CATALOG, CPT_CATEGORIES } = await import('../cpt_catalog.js');

    if (!pillsEl.children.length) {
      pillsEl.innerHTML = ['All', ...CPT_CATEGORIES]
        .map(
          (cat) =>
            `<button class="cpt-pill ${cat === 'All' ? 'active' : ''}" data-cat="${cat === 'All' ? '' : cat}">${cat}</button>`
        )
        .join('');
      pillsEl.querySelectorAll('.cpt-pill').forEach((pill) => {
        pill.addEventListener('click', () => {
          activeCategory = (pill as HTMLElement).dataset.cat!;
          pillsEl.querySelectorAll('.cpt-pill').forEach((p) => p.classList.remove('active'));
          pill.classList.add('active');
          void renderTable(filterInput.value, activeCategory);
        });
      });
    }

    const norm = filter.toLowerCase();
    const filtered = CPT_CATALOG.filter(
      (e) =>
        (!category || e.category === category) &&
        (!norm ||
          e.plain.toLowerCase().includes(norm) ||
          e.code.includes(norm) ||
          e.technical.toLowerCase().includes(norm))
    );

    if (filtered.length === 0) {
      tableEl.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-tertiary);">No codes match.</div>`;
      return;
    }

    tableEl.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:0.82rem;">
      <thead><tr style="border-bottom:2px solid var(--border-medium);">
        <th style="text-align:left;padding:10px 14px;">CPT</th>
        <th style="text-align:left;padding:10px 14px;">Plain English</th>
        <th style="padding:10px 14px;"></th>
      </tr></thead>
      <tbody>${filtered
        .map(
          (e) => `<tr class="cpt-row" data-code="${e.code}" data-plain="${e.plain}">
        <td style="padding:12px 14px;font-family:var(--font-mono);color:var(--yc-orange);">${e.code}</td>
        <td style="padding:12px 14px;">${e.plain}</td>
        <td style="padding:12px 14px;text-align:right;"><button class="brutalist-action cpt-search-btn">Search</button></td>
      </tr>`
        )
        .join('')}</tbody></table>`;

    tableEl.querySelectorAll('.cpt-search-btn').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const row = (btn as HTMLElement).closest('.cpt-row') as HTMLElement;
        const q = row.dataset.code!;
        close();
        input.value = row.dataset.plain!;
        void performSearch(q, stateSelect?.value ?? '');
        window.scrollTo({ top: 0, behavior: 'smooth' });
      });
    });
  }

  filterInput.addEventListener('input', () => {
    void renderTable(filterInput.value, activeCategory);
  });
}
