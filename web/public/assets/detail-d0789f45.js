import{g as p,D as m,_}from"./main-a364c182.js";setTimeout(()=>p(m),0);async function H(e,t,n=20,a=0,s="score-desc"){const l=await p(m);let r=`
    SELECT h.name, h.state, h.city, h.website, c.*
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE 1=1
  `;const c=[];return e&&(r+=" AND (h.name LIKE ? OR h.ccn LIKE ?)",c.push(`%${e}%`,`%${e}%`)),t&&(r+=" AND h.state = ?",c.push(t)),r+=` ORDER BY ${{"score-desc":"c.score DESC","score-asc":"c.score ASC","name-asc":"h.name ASC"}[s]||"c.score DESC"} LIMIT ? OFFSET ?`,c.push(n,a),l.db.query(r,c)}async function T(e,t){const n=await p(m);let a=`
    SELECT COUNT(*) as total
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE 1=1
  `;const s=[];return e&&(a+=" AND (h.name LIKE ? OR h.ccn LIKE ?)",s.push(`%${e}%`,`%${e}%`)),t&&(a+=" AND h.state = ?",s.push(t)),(await n.db.query(a,s))[0].total}async function I(e,t,n=20,a=0){const s=await p(m);let l=`
    SELECT h.name, h.state, h.city, h.website, c.*
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE c.score < 65
  `;const r=[];return e&&(l+=" AND (h.name LIKE ? OR h.ccn LIKE ?)",r.push(`%${e}%`,`%${e}%`)),t&&(l+=" AND h.state = ?",r.push(t)),l+=" ORDER BY c.score ASC LIMIT ? OFFSET ?",r.push(n,a),s.db.query(l,r)}async function P(e,t){const n=await p(m);let a=`
    SELECT COUNT(*) as total
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE c.score < 65
  `;const s=[];return e&&(a+=" AND (h.name LIKE ? OR h.ccn LIKE ?)",s.push(`%${e}%`,`%${e}%`)),t&&(a+=" AND h.state = ?",s.push(t)),(await n.db.query(a,s))[0].total}async function L(e){const t=await p(m),n=`
    SELECT h.name, h.state, h.city, h.website, c.*
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE c.ccn = ?
  `,a=await t.db.query(n,[e]);return a.length>0?a[0]:null}const u=[{key:"txt_exists",name:"CMS Overall Rating Published",source:"CMS Hospital General Information",field:"Hospital Overall Rating",cite:"Star Rating Program",getOk:e=>e.txt_exists,passLabel:"Published",failLabel:"Not Reported",explanation:"CMS publishes an overall hospital quality rating (1-5 stars) for most facilities. When a hospital lacks this rating, it typically means they did not submit sufficient quality measure data to CMS during the reporting period. This is not a legal violation by itself, but non-reporting correlates strongly with broader transparency gaps. Hospitals that suppress quality data tend to suppress pricing data.",source_detail:'This check reads the "Hospital overall rating" field directly from the CMS Provider Data Catalog, updated annually.'},{key:"robots_ok",name:"Emergency Services Disclosed",source:"CMS Hospital General Information",field:"Emergency Services",cite:"Structural Disclosure",getOk:e=>e.robots_ok,passLabel:"Yes",failLabel:"Not Disclosed",explanation:"Federal regulations require hospitals to clearly disclose whether they provide emergency services. Facilities that do not disclose this basic characteristic are often also opaque about pricing. This check is a structural disclosure indicator, not a compliance violation finding.",source_detail:'This check reads the "Emergency Services" field from the CMS Provider Data Catalog.'},{key:"mrf_reachable",name:"Quality Measure Reporting Coverage",source:"CMS Hospital Compare",field:"Safety + Mortality Measures Reported",cite:"CMS Quality Reporting",getOk:e=>e.mrf_reachable,passLabel:"Adequate",failLabel:"Low Coverage",explanation:"CMS tracks how many quality measures each hospital actually submits data for. A hospital reporting fewer than 5 measures across safety and mortality domains is either very small or actively suppressing data submission. Hospitals with low measure coverage receive fewer patients who can make informed choices — which is the same problem price opacity creates.",source_detail:'Computed from "Count of Facility Safety Measures" and "Count of Facility MORT Measures" in the CMS dataset.'},{key:"mrf_valid",name:"Patient Experience Data Submitted",source:"CMS HCAHPS Survey",field:"Patient Experience Measures Reported",cite:"HCAHPS Program",getOk:e=>e.mrf_valid,passLabel:"Submitted",failLabel:"Not Submitted",explanation:"The HCAHPS (Hospital Consumer Assessment of Healthcare Providers and Systems) survey is the national standard for measuring patients' experience in hospitals. Submitting HCAHPS data is required for full Medicare payment. Hospitals that don't submit this data receive a 2% Medicare payment reduction — so non-reporters are either penalized specialty facilities or facilities choosing to trade revenue for opacity.",source_detail:'Checks whether "Count of Facility Pt Exp Measures" is at least 2, per the CMS Provider Data Catalog.'},{key:"mrf_fresh",name:"Readmission Data Reported",source:"CMS Hospital Readmissions",field:"READM Measures Reported",cite:"HRRP Program",getOk:e=>e.mrf_fresh,passLabel:"Reported",failLabel:"Not Reported",explanation:"The Hospital Readmissions Reduction Program (HRRP) requires hospitals to report readmission rates for conditions like heart failure, pneumonia, and hip/knee replacement. This is one of the most direct indicators of care quality and follow-through. Facilities not reporting readmissions data have less accountable clinical systems, which correlates with billing practices that are harder for patients to challenge.",source_detail:'Checks whether "Count of Facility READM Measures" is at least 3, per the CMS dataset.'}],$=[{label:"CMS Star Rating",key:"score_rating",max:30,description:"Overall quality rating (1-5 stars). Missing rating = 0 points."},{label:"Patient Experience",key:"score_pt_exp",max:20,description:"HCAHPS survey reporting completeness."},{label:"Safety Measures",key:"score_safety",max:20,description:"Proportion of CMS safety measures reported."},{label:"Mortality Measures",key:"score_mortality",max:15,description:"Proportion of mortality measures reported."},{label:"Readmission Data",key:"score_readmission",max:10,description:"Proportion of readmission measures reported."},{label:"Emergency Disclosure",key:null,max:5,description:"Whether emergency services are disclosed."}];async function D(e){var v,b;let t=e;if(!(e.score_rating!==void 0&&e.score_pt_exp!==void 0&&e.score_safety!==void 0&&e.score_mortality!==void 0&&e.score_readmission!==void 0)&&e.ccn)try{const i=await L(e.ccn);i&&(t=i)}catch(i){console.warn("[HS] Failed to fetch enriched compliance detail:",i)}const a=document.getElementById("detail-overlay"),s=document.getElementById("detail-content");if(!a||!s)return;const l=new Date().toISOString().replace("T"," ").slice(0,19)+" UTC",r=t.score,c=r>80?"var(--rh-green)":r>50?"var(--amber)":"var(--yc-orange)",y=r>80?"Strong Reporting":r>50?"Partial Reporting":"Low Reporting Coverage",C=r>=80?"This facility submits comprehensive data across CMS quality programs. Individual checks represent binary pass/fail thresholds. The composite score reflects how fully each reporting area is covered.":r>=50?'A facility can pass all binary checks while scoring below 80. Each check is a threshold (e.g., "reported at least 2 measures") while the score measures reporting depth (e.g., what fraction of all required measures were submitted). A hospital reporting 2 of 10 safety measures passes the check but scores low on that dimension.':"Significant reporting gaps detected across CMS quality programs. The binary checks use minimum thresholds; the score reflects how much data was actually submitted relative to what CMS expects. A score this low means the facility is near the floor on multiple dimensions, not just one.",h=Math.ceil(u.length/2),S=u.slice(0,h),k=u.slice(h),f=(i,d)=>i.map((o,g)=>{const x=o.getOk(t)===1,w=`check-expand-${d+g}`;return`
            <div class="check-item-container" style="margin-bottom:12px;">
                <div class="check-row clickable-check" data-expand="${w}">
                    <div style="flex:1;">
                        <div style="display:flex; align-items:center; gap:10px;">
                            <span style="font-size:0.85rem; font-weight:700; color:var(--text-primary);">${o.name}</span>
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" class="expand-arrow" style="opacity:0.4;"><polyline points="6 9 12 15 18 9"/></svg>
                        </div>
                        <div style="font-family:var(--font-mono); font-size:0.62rem; color:var(--text-tertiary); margin-top:2px; letter-spacing:0.02em;">${o.source} &nbsp;·&nbsp; ${o.field}</div>
                    </div>
                    <span class="tag ${x?"green":"red"}" style="padding:6px 10px; font-size:0.65rem; border-radius:6px; letter-spacing:0.05em;">${x?o.passLabel:o.failLabel}</span>
                </div>
                <div id="${w}" class="check-explanation hidden" style="grid-column:unset; margin:0; border-top:1px solid var(--border-subtle);">
                    <p style="font-size:0.82rem; line-height:1.6; color:var(--text-secondary); margin-bottom:12px;">${o.explanation}</p>
                    <div style="font-family:var(--font-mono); font-size:0.6rem; color:var(--text-tertiary); background:rgba(255,255,255,0.03); padding:10px 14px; border-radius:8px; border:1px solid var(--border-subtle); display:flex; align-items:flex-start; gap:8px;">
                        <span style="color:var(--yc-orange); font-weight:900;">SOURCE:</span>
                        <span>${o.source_detail}</span>
                    </div>
                </div>
            </div>`}).join(""),E=f(S,0),M=f(k,h),R=$.map(i=>{const d=i.key?t[i.key]??0:t.robots_ok?5:0,o=Math.round(d/i.max*100);return`
            <div style="margin-bottom:12px;">
                <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;">
                    <span style="font-size:0.75rem;font-weight:600;">${i.label}</span>
                    <span style="font-family:var(--font-mono);font-size:0.68rem;color:var(--text-tertiary);">${d} / ${i.max}</span>
                </div>
                <div style="height:4px;background:var(--border-subtle);border-radius:2px;overflow:hidden;">
                    <div style="height:100%;width:${o}%;background:${o>70?"var(--rh-green)":o>40?"var(--amber)":"var(--yc-orange)"};border-radius:2px;transition:width 0.6s var(--ease-fluid);"></div>
                </div>
                <p style="font-size:0.68rem;color:var(--text-tertiary);margin-top:3px;">${i.description}</p>
            </div>`}).join("");s.innerHTML=`
        <button class="icon-btn btn-close-detail" style="position:absolute; top:12px; right:12px; font-size:1.1rem; opacity:0.4; transition:all 0.2s; padding:8px;" onmouseover="this.style.opacity='1'; this.style.background='var(--bg-card-hover)';" onmouseout="this.style.opacity='0.4'; this.style.background='transparent'">&#x2715;</button>

        <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:32px;">
            <div style="flex:1; padding-right:48px;">
                <div class="modal-eyebrow" style="margin-bottom:8px; letter-spacing:0.1em;">CMS Audit Intelligence &nbsp;&middot;&nbsp; CCN ${t.ccn}</div>
                <h2 class="modal-title" style="margin-bottom:6px; font-size:1.8rem; letter-spacing:-0.03em;">${t.name}</h2>
                <div style="display:flex; align-items:center; gap:12px;">
                    <p class="modal-subtitle" style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-tertiary);">${t.city}, ${t.state}</p>
                    <span class="tag ghost" style="font-size:0.55rem; padding:1px 6px; border:1px solid var(--border-strong);">Facility Active</span>
                </div>
            </div>
            <div style="text-align:right; flex-shrink:0; background:var(--bg-card-2); padding:16px 20px; border-radius:var(--radius-lg); border:1px solid var(--border-subtle);">
                <div style="font-family:var(--font-mono); font-size:0.62rem; color:var(--text-tertiary); text-transform:uppercase; margin-bottom:6px; letter-spacing:0.1em;">Institutional Rank</div>
                <div style="display:flex; align-items:baseline; justify-content:flex-end; gap:2px; margin-bottom:2px;">
                    <span style="font-size:2.2rem; font-weight:900; color:${c}; line-height:1; letter-spacing:-0.02em;">${t.score}</span>
                    <span style="font-size:0.9rem; font-weight:700; color:var(--text-tertiary);">/100</span>
                </div>
                <div style="font-size:0.68rem; font-weight:800; color:${c}; letter-spacing:0.1em; text-transform:uppercase;">${y}</div>
            </div>
        </div>

        <div class="audit-grid" style="display:grid; grid-template-columns:1fr 280px; gap:40px; margin-bottom:40px;">
            <div style="background:rgba(255,255,255,0.02); padding:28px; border-radius:var(--radius-xl); border:1px solid var(--border-subtle);">
                <p style="font-size:0.68rem; font-weight:900; text-transform:uppercase; letter-spacing:0.12em; color:var(--text-tertiary); margin-bottom:16px;">Forensic Audit Summary</p>
                <p style="font-size:0.95rem; color:var(--text-primary); line-height:1.7; letter-spacing:0.01em;">${C}</p>
                <div style="margin-top:28px; display:flex; gap:32px;">
                    <div>
                        <p style="font-size:0.62rem; color:var(--text-tertiary); text-transform:uppercase; font-weight:800; margin-bottom:6px; letter-spacing:0.1em;">Last Verified</p>
                        <p style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-primary); font-weight:600;">${l.split(" ")[0]}</p>
                    </div>
                    <div>
                        <p style="font-size:0.62rem; color:var(--text-tertiary); text-transform:uppercase; font-weight:800; margin-bottom:6px; letter-spacing:0.1em;">CMS Release</p>
                        <p style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-primary); font-weight:600;">Annual v${new Date().getFullYear()}.1</p>
                    </div>
                </div>
            </div>
            <div>
                <p style="font-size:0.68rem; font-weight:900; text-transform:uppercase; letter-spacing:0.12em; color:var(--text-tertiary); margin-bottom:20px;">Score Weighting</p>
                ${R}
            </div>
        </div>

        <div style="background:var(--bg-card); border:1px solid var(--border-medium); border-radius:var(--radius-xl); padding:32px; margin-bottom:40px; box-shadow:var(--shadow-card);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:24px;">
                <p style="font-size:0.68rem; font-weight:900; text-transform:uppercase; letter-spacing:0.15em; color:var(--yc-orange);">Binary Compliance Thresholds</p>
                <span class="tag ghost" style="font-size:0.6rem; color:var(--text-tertiary);">§ 45 CFR Part 180</span>
            </div>
            <div class="compliance-inner-grid">
                <div>${E}</div>
                <div>${M}</div>
            </div>
        </div>

        <div class="evidence-source" style="margin:0 0 32px; padding:16px 20px;">
            <p class="evidence-source-label" style="font-size:0.6rem;">Source & Transparency Page</p>
            <p style="font-size:0.78rem; color:var(--text-secondary); line-height:1.5; margin-bottom:12px;">All metrics are audited from the <strong>CMS Provider Data Catalog</strong>. We extract this data directly from federal reporting schemas to ensure patient-facing accuracy.</p>
            <a href="${t.website}" target="_blank" rel="noopener" class="text-orange fw-800" style="font-size:0.75rem; text-decoration:none; display:inline-flex; align-items:center; gap:6px;">
                Hospital Transparency URL 
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/></svg>
            </a>
        </div>

        <div style="background:var(--bg-body); border-radius:var(--radius-lg); border:1px solid var(--border-subtle); padding:24px; margin-bottom:32px; display:flex; flex-direction:column; gap:16px;">
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                <button class="btn secondary btn-close-detail" style="padding:12px;">Dismiss Audit</button>
                ${t.score===100?'<button class="btn secondary" disabled style="padding:12px; font-weight:800; border-color:var(--rh-green); color:var(--rh-green); font-family:var(--font-mono); font-size:0.7rem; letter-spacing:0.05em;">★ VERIFIED COMPLIANT</button>':'<button class="btn primary btn-report-cms" style="padding:12px; font-weight:800; letter-spacing:0.02em;">Send Complaint</button>'}
            </div>
            <a href="https://www.cms.gov/hospital-price-transparency" target="_blank" rel="noopener" 
               style="text-align:center; font-size:0.65rem; color:var(--text-tertiary); text-decoration:underline; text-underline-offset:3px; font-family:var(--font-mono); text-transform:uppercase; letter-spacing:0.05em;">
                Official CMS Compliance Portal Detail &#x2192;
            </a>
        </div>
    `,s.querySelectorAll(".check-row").forEach(i=>{i.addEventListener("click",()=>{const d=i.dataset.expand,o=document.getElementById(d);if(!o)return;const g=o.classList.toggle("hidden");i.classList.toggle("active",!g)})});const{handleComplaint:O}=await _(()=>import("./mail-484b852f.js"),[]);(v=s.querySelector(".btn-report-cms"))==null||v.addEventListener("click",()=>O(t)),s.querySelectorAll(".btn-close-detail").forEach(i=>{i.addEventListener("click",()=>a.classList.add("hidden"))}),(b=a.querySelector(".sheet-backdrop"))==null||b.addEventListener("click",()=>a.classList.add("hidden"),{once:!0}),a.classList.remove("hidden")}const z=Object.freeze(Object.defineProperty({__proto__:null,showAuditDetail:D},Symbol.toStringTag,{value:"Module"}));export{D as a,I as b,P as c,z as d,T as g,H as s};
