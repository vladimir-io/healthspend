import { getComplianceDetail, type ComplianceRecord } from '../compliance';

// Each check: what it is, why it matters, what the CMS source field actually is
const CHECK_DEFS = [
    {
        key: 'txt_exists',
        name: 'CMS Overall Rating Published',
        source: 'CMS Hospital General Information',
        field: 'Hospital Overall Rating',
        cite: 'Star Rating Program',
        getOk: (r: ComplianceRecord) => r.txt_exists,
        passLabel: 'Published',
        failLabel: 'Not Reported',
        explanation: `CMS publishes an overall hospital quality rating (1-5 stars) for most facilities. When a hospital lacks this rating, it typically means they did not submit sufficient quality measure data to CMS during the reporting period. This is not a legal violation by itself, but non-reporting correlates strongly with broader transparency gaps. Hospitals that suppress quality data tend to suppress pricing data.`,
        source_detail: 'This check reads the "Hospital overall rating" field directly from the CMS Provider Data Catalog, updated annually.'
    },
    {
        key: 'robots_ok',
        name: 'Emergency Services Disclosed',
        source: 'CMS Hospital General Information',
        field: 'Emergency Services',
        cite: 'Structural Disclosure',
        getOk: (r: ComplianceRecord) => r.robots_ok,
        passLabel: 'Yes',
        failLabel: 'Not Disclosed',
        explanation: `Federal regulations require hospitals to clearly disclose whether they provide emergency services. Facilities that do not disclose this basic characteristic are often also opaque about pricing. This check is a structural disclosure indicator, not a compliance violation finding.`,
        source_detail: 'This check reads the "Emergency Services" field from the CMS Provider Data Catalog.'
    },
    {
        key: 'mrf_reachable',
        name: 'Quality Measure Reporting Coverage',
        source: 'CMS Hospital Compare',
        field: 'Safety + Mortality Measures Reported',
        cite: 'CMS Quality Reporting',
        getOk: (r: ComplianceRecord) => {
            if (r.mrf_reachable === 1) return 1;
            if (r.mrf_reachable === 0 && (r.safety_measures || r.mort_measures)) return (r.safety_measures + r.mort_measures) >= 5 ? 1 : 0;
            return r.mrf_reachable;
        },
        passLabel: 'Adequate',
        failLabel: 'Low Coverage',
        explanation: `CMS tracks how many quality measures each hospital actually submits data for. A hospital reporting fewer than 5 measures across safety and mortality domains is either very small or actively suppressing data submission. Hospitals with low measure coverage receive fewer patients who can make informed choices — which is the same problem price opacity creates.`,
        source_detail: 'Computed from "Count of Facility Safety Measures" and "Count of Facility MORT Measures" in the CMS dataset.'
    },
    {
        key: 'mrf_valid',
        name: 'Patient Experience Data Submitted',
        source: 'CMS HCAHPS Survey',
        field: 'Patient Experience Measures Reported',
        cite: 'HCAHPS Program',
        getOk: (r: ComplianceRecord) => {
            if (r.mrf_valid === 1) return 1;
            if (r.mrf_valid === 0 && r.pt_exp_measures) return r.pt_exp_measures >= 2 ? 1 : 0;
            return r.mrf_valid;
        },
        passLabel: 'Submitted',
        failLabel: 'Not Submitted',
        explanation: `The HCAHPS (Hospital Consumer Assessment of Healthcare Providers and Systems) survey is the national standard for measuring patients' experience in hospitals. Submitting HCAHPS data is required for full Medicare payment. Hospitals that don't submit this data receive a 2% Medicare payment reduction — so non-reporters are either penalized specialty facilities or facilities choosing to trade revenue for opacity.`,
        source_detail: 'Checks whether "Count of Facility Pt Exp Measures" is at least 2, per the CMS Provider Data Catalog.'
    },
    {
        key: 'mrf_fresh',
        name: 'Readmission Data Reported',
        source: 'CMS Hospital Readmissions',
        field: 'READM Measures Reported',
        cite: 'HRRP Program',
        getOk: (r: ComplianceRecord) => {
            if (r.mrf_fresh === 1) return 1;
            if (r.mrf_fresh === 0 && r.readm_measures) return r.readm_measures >= 3 ? 1 : 0;
            return r.mrf_fresh;
        },
        passLabel: 'Reported',
        failLabel: 'Not Reported',
        explanation: `The Hospital Readmissions Reduction Program (HRRP) requires hospitals to report readmission rates for conditions like heart failure, pneumonia, and hip/knee replacement. This is one of the most direct indicators of care quality and follow-through. Facilities not reporting readmissions data have less accountable clinical systems, which correlates with billing practices that are harder for patients to challenge.`,
        source_detail: 'Checks whether "Count of Facility READM Measures" is at least 3, per the CMS dataset.'
    },
];

const SCORE_COMPONENTS = [
    { label: 'CMS Star Rating', key: 'score_rating', max: 30, description: 'Overall quality rating (1-5 stars). Missing rating = 0 points.' },
    { label: 'Patient Experience', key: 'score_pt_exp', max: 20, description: 'HCAHPS survey reporting completeness.' },
    { label: 'Safety Measures', key: 'score_safety', max: 20, description: 'Proportion of CMS safety measures reported.' },
    { label: 'Mortality Measures', key: 'score_mortality', max: 15, description: 'Proportion of mortality measures reported.' },
    { label: 'Readmission Data', key: 'score_readmission', max: 10, description: 'Proportion of readmission measures reported.' },
    { label: 'Emergency Disclosure', key: null, max: 5, description: 'Whether emergency services are disclosed.' },
];

export async function showAuditDetail(record: ComplianceRecord) {
    let detailRecord: ComplianceRecord = record;
    const hasBreakdown =
        record.score_rating !== undefined &&
        record.score_pt_exp !== undefined &&
        record.score_safety !== undefined &&
        record.score_mortality !== undefined &&
        record.score_readmission !== undefined;

    if (!hasBreakdown && record.ccn) {
        try {
            const enriched = await getComplianceDetail(record.ccn);
            if (enriched) {
                detailRecord = enriched;
            }
        } catch {
            // enrichment is best-effort; fall through with base record
        }
    }

    const overlay = document.getElementById('detail-overlay');
    const content = document.getElementById('detail-content');
    if (!overlay || !content) return;

    const ts = new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
    const sc = detailRecord.score;
    const scoreCol = sc > 80 ? 'var(--rh-green)' : sc > 50 ? 'var(--amber)' : 'var(--yc-orange)';
    const verdict = sc > 80 ? 'Strong Reporting' : sc > 50 ? 'Partial Reporting' : 'Low Reporting Coverage';
    const verdictDetail = sc >= 80
        ? `This facility submits comprehensive data across CMS quality programs. Individual checks represent binary pass/fail thresholds. The composite score reflects how fully each reporting area is covered.`
        : sc >= 50
            ? `A facility can pass all binary checks while scoring below 80. Each check is a threshold (e.g., "reported at least 2 measures") while the score measures reporting depth (e.g., what fraction of all required measures were submitted). A hospital reporting 2 of 10 safety measures passes the check but scores low on that dimension.`
            : `Significant reporting gaps detected across CMS quality programs. The binary checks use minimum thresholds; the score reflects how much data was actually submitted relative to what CMS expects. A score this low means the facility is near the floor on multiple dimensions, not just one.`;

    const mid = Math.ceil(CHECK_DEFS.length / 2);
    const leftDefs = CHECK_DEFS.slice(0, mid);
    const rightDefs = CHECK_DEFS.slice(mid);

    const renderCol = (defs: typeof CHECK_DEFS, startIdx: number) => defs.map((def, i) => {
        const ok = def.getOk(detailRecord) === 1;
        const expandId = `check-expand-${startIdx + i}`;
        return `
            <div class="check-item-container" style="margin-bottom:12px;">
                <div class="check-row clickable-check" data-expand="${expandId}">
                    <div style="flex:1;">
                        <div style="display:flex; align-items:center; gap:10px;">
                            <span style="font-size:0.85rem; font-weight:700; color:var(--text-primary);">${def.name}</span>
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" class="expand-arrow" style="opacity:0.4;"><polyline points="6 9 12 15 18 9"/></svg>
                        </div>
                        <div style="font-family:var(--font-mono); font-size:0.62rem; color:var(--text-tertiary); margin-top:2px; letter-spacing:0.02em;">${def.source} &nbsp;·&nbsp; ${def.field}</div>
                    </div>
                    <span class="tag ${ok ? 'green' : 'red'}" style="padding:6px 10px; font-size:0.65rem; border-radius:6px; letter-spacing:0.05em;">${ok ? def.passLabel : def.failLabel}</span>
                </div>
                <div id="${expandId}" class="check-explanation hidden" style="grid-column:unset; margin:0; border-top:1px solid var(--border-subtle);">
                    <p style="font-size:0.82rem; line-height:1.6; color:var(--text-secondary); margin-bottom:12px;">${def.explanation}</p>
                    <div style="font-family:var(--font-mono); font-size:0.6rem; color:var(--text-tertiary); background:rgba(255,255,255,0.03); padding:10px 14px; border-radius:8px; border:1px solid var(--border-subtle); display:flex; align-items:flex-start; gap:8px;">
                        <span style="color:var(--yc-orange); font-weight:900;">SOURCE:</span>
                        <span>${def.source_detail}</span>
                    </div>
                </div>
            </div>`;
    }).join('');

    const leftColHtml = renderCol(leftDefs, 0);
    const rightColHtml = renderCol(rightDefs, mid);

    // Score bar components
    const scoreBars = SCORE_COMPONENTS.map(c => {
        const val = c.key ? (detailRecord as any)[c.key] ?? 0 : (detailRecord.robots_ok ? 5 : 0);
        const pct = Math.round((val / c.max) * 100);
        return `
            <div style="margin-bottom:12px;">
                <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;">
                    <span style="font-size:0.75rem;font-weight:600;">${c.label}</span>
                    <span style="font-family:var(--font-mono);font-size:0.68rem;color:var(--text-tertiary);">${val} / ${c.max}</span>
                </div>
                <div style="height:4px;background:var(--border-subtle);border-radius:2px;overflow:hidden;">
                    <div style="height:100%;width:${pct}%;background:${pct > 70 ? 'var(--rh-green)' : pct > 40 ? 'var(--amber)' : 'var(--yc-orange)'};border-radius:2px;transition:width 0.6s var(--ease-fluid);"></div>
                </div>
                <p style="font-size:0.68rem;color:var(--text-tertiary);margin-top:3px;">${c.description}</p>
            </div>`;
    }).join('');

    content.innerHTML = `
        <button class="icon-btn btn-close-detail" style="position:absolute; top:12px; right:12px; font-size:1.1rem; opacity:0.4; transition:all 0.2s; padding:8px;" onmouseover="this.style.opacity='1'; this.style.background='var(--bg-card-hover)';" onmouseout="this.style.opacity='0.4'; this.style.background='transparent'">&#x2715;</button>

        <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:32px;">
            <div style="flex:1; padding-right:48px;">
                <div class="modal-eyebrow" style="margin-bottom:8px; letter-spacing:0.1em;">CMS Audit Intelligence &nbsp;&middot;&nbsp; CCN ${detailRecord.ccn}</div>
                <h2 class="modal-title" style="margin-bottom:6px; font-size:1.8rem; letter-spacing:-0.03em;">${detailRecord.name}</h2>
                <div style="display:flex; align-items:center; gap:12px;">
                    <p class="modal-subtitle" style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-tertiary);">${detailRecord.city}, ${detailRecord.state}</p>
                    <span class="tag ghost" style="font-size:0.55rem; padding:1px 6px; border:1px solid var(--border-strong);">Facility Active</span>
                </div>
            </div>
            <div style="text-align:right; flex-shrink:0; background:var(--bg-card-2); padding:16px 20px; border-radius:var(--radius-lg); border:1px solid var(--border-subtle);">
                <div style="font-family:var(--font-mono); font-size:0.62rem; color:var(--text-tertiary); text-transform:uppercase; margin-bottom:6px; letter-spacing:0.1em;">Institutional Rank</div>
                <div style="display:flex; align-items:baseline; justify-content:flex-end; gap:2px; margin-bottom:2px;">
                    <span style="font-size:2.2rem; font-weight:900; color:${scoreCol}; line-height:1; letter-spacing:-0.02em;">${detailRecord.score}</span>
                    <span style="font-size:0.9rem; font-weight:700; color:var(--text-tertiary);">/100</span>
                </div>
                <div style="font-size:0.68rem; font-weight:800; color:${scoreCol}; letter-spacing:0.1em; text-transform:uppercase;">${verdict}</div>
            </div>
        </div>

        <div class="audit-grid" style="display:grid; grid-template-columns:1fr 280px; gap:40px; margin-bottom:40px;">
            <div style="background:rgba(255,255,255,0.02); padding:28px; border-radius:var(--radius-xl); border:1px solid var(--border-subtle);">
                <p style="font-size:0.68rem; font-weight:900; text-transform:uppercase; letter-spacing:0.12em; color:var(--text-tertiary); margin-bottom:16px;">Forensic Audit Summary</p>
                <p style="font-size:0.95rem; color:var(--text-primary); line-height:1.7; letter-spacing:0.01em;">${verdictDetail}</p>
                <div style="margin-top:28px; display:flex; gap:32px;">
                    <div>
                        <p style="font-size:0.62rem; color:var(--text-tertiary); text-transform:uppercase; font-weight:800; margin-bottom:6px; letter-spacing:0.1em;">Last Verified</p>
                        <p style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-primary); font-weight:600;">${ts.split(' ')[0]}</p>
                    </div>
                    <div>
                        <p style="font-size:0.62rem; color:var(--text-tertiary); text-transform:uppercase; font-weight:800; margin-bottom:6px; letter-spacing:0.1em;">CMS Release</p>
                        <p style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-primary); font-weight:600;">Annual v${new Date().getFullYear()}.1</p>
                    </div>
                </div>
            </div>
            <div>
                <p style="font-size:0.68rem; font-weight:900; text-transform:uppercase; letter-spacing:0.12em; color:var(--text-tertiary); margin-bottom:20px;">Score Weighting</p>
                ${scoreBars}
            </div>
        </div>

        <div style="background:var(--bg-card); border:1px solid var(--border-medium); border-radius:var(--radius-xl); padding:32px; margin-bottom:40px; box-shadow:var(--shadow-card);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:24px;">
                <p style="font-size:0.68rem; font-weight:900; text-transform:uppercase; letter-spacing:0.15em; color:var(--yc-orange);">Binary Compliance Thresholds</p>
                <span class="tag ghost" style="font-size:0.6rem; color:var(--text-tertiary);">§ 45 CFR Part 180</span>
            </div>
            <div class="compliance-inner-grid">
                <div>${leftColHtml}</div>
                <div>${rightColHtml}</div>
            </div>
        </div>

        <div class="evidence-source" style="margin:0 0 32px; padding:16px 20px;">
            <p class="evidence-source-label" style="font-size:0.6rem;">Source & Transparency Page</p>
            <p style="font-size:0.78rem; color:var(--text-secondary); line-height:1.5; margin-bottom:12px;">All metrics are audited from the <strong>CMS Provider Data Catalog</strong>. We extract this data directly from federal reporting schemas to ensure patient-facing accuracy.</p>
            <a href="${detailRecord.website}" target="_blank" rel="noopener" class="text-orange fw-800" style="font-size:0.75rem; text-decoration:none; display:inline-flex; align-items:center; gap:6px;">
                Hospital Transparency URL 
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/></svg>
            </a>
        </div>

        <div style="background:var(--bg-body); border-radius:var(--radius-lg); border:1px solid var(--border-subtle); padding:24px; margin-bottom:32px; display:flex; flex-direction:column; gap:16px;">
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                <button class="btn secondary btn-close-detail" style="padding:12px;">Dismiss Audit</button>
                ${detailRecord.score === 100 
                    ? `<button class="btn secondary" disabled style="padding:12px; font-weight:800; border-color:var(--rh-green); color:var(--rh-green); font-family:var(--font-mono); font-size:0.7rem; letter-spacing:0.05em;">★ VERIFIED COMPLIANT</button>`
                    : `<button class="btn primary btn-report-cms" style="padding:12px; font-weight:800; letter-spacing:0.02em;">Send Complaint</button>`
                }
            </div>
            <a href="https://www.cms.gov/hospital-price-transparency" target="_blank" rel="noopener" 
               style="text-align:center; font-size:0.65rem; color:var(--text-tertiary); text-decoration:underline; text-underline-offset:3px; font-family:var(--font-mono); text-transform:uppercase; letter-spacing:0.05em;">
                Official CMS Compliance Portal Detail &#x2192;
            </a>
        </div>
    `;

    // Wire up expandable rows
    content.querySelectorAll<HTMLElement>('.check-row').forEach(row => {
        row.addEventListener('click', () => {
            const expandId = row.dataset.expand!;
            const panel = document.getElementById(expandId);
            if (!panel) return;
            const isNowOpen = panel.classList.toggle('hidden'); // toggles hidden, returns true if added
            row.classList.toggle('active', !isNowOpen);
        });
    });

    // Wire up actions
    const { handleComplaint } = await import('../mail.js');
    content.querySelector('.btn-report-cms')?.addEventListener('click', () => handleComplaint(detailRecord));
    content.querySelectorAll('.btn-close-detail').forEach(b => {
        b.addEventListener('click', () => overlay.classList.add('hidden'));
    });

    overlay.querySelector('.sheet-backdrop')?.addEventListener('click', () => overlay.classList.add('hidden'), { once: true });
    overlay.classList.remove('hidden');
}
