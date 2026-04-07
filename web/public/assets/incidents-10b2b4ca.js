import{b as N,c as A,a as f}from"./detail-d0789f45.js";import{handleComplaint as E}from"./mail-484b852f.js";import"./main-a364c182.js";const S=["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"];function H(L){const v=document.getElementById(L);if(!v)return;let g="",d="",e=1;const a=20;v.innerHTML=`
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
                      ${S.map(n=>`<option value="${n}">${n}</option>`).join("")}
                  </select>
                </div>
            </div>
        </div>

        <div id="incidents-results-summary" class="results-summary animate-up" style="--stagger:2.5;">
            Scanning federal nodes…
        </div>

        <div id="incidents-feed" class="incident-feed animate-up" style="--stagger:3;">
            ${[...Array(5)].map(()=>'<div class="skeleton-card animate-pulse" style="height:76px;"></div>').join("")}
        </div>

        <div id="incidents-pagination" class="pagination-hub animate-up" style="--stagger:4;"></div>
    `;const o=document.getElementById("incidents-feed"),m=document.getElementById("incidents-search"),y=document.getElementById("incidents-state"),p=document.getElementById("incidents-pagination"),M=document.getElementById("incidents-results-summary"),c=async()=>{const n=(e-1)*a;o.innerHTML=[...Array(3)].map(()=>'<div class="skeleton-card animate-pulse" style="height:76px;"></div>').join("");try{const[l,r]=await Promise.all([N(g,d,a,n),A(g,d)]);if(M.innerHTML=`<strong style="color:var(--text-primary);">${r.toLocaleString()} violations</strong> detected &nbsp;·&nbsp; Region: ${d||"Federal"} &nbsp;·&nbsp; Page ${e} of ${Math.ceil(r/a)}`,o.innerHTML="",l.length===0){o.innerHTML='<div style="padding:60px;text-align:center;color:var(--text-tertiary);">No violations match this filter.</div>',p.innerHTML="";return}l.forEach(t=>{var b,x;const s=document.createElement("div");s.className="incident-row animate-up",s.style.cssText="grid-template-columns: 1fr auto 180px;";const I=[!t.txt_exists&&'<span class="tag red">No Price Pub.</span>',!t.robots_ok&&'<span class="tag red">Bot Blocked</span>',!t.mrf_valid&&'<span class="tag amber">Schema Low</span>'].filter(Boolean).join("");s.innerHTML=`
                    <div class="incident-entity">
                        <p class="modal-subtitle" style="font-family:var(--font-mono);font-size:0.75rem;color:var(--text-tertiary);">
                            ${t.city}, ${t.state} &nbsp;·&nbsp; CCN ${t.ccn}
                        </p>
                        <h3 style="margin-top:2px;">${t.name}</h3>
                    </div>
                    <div class="incident-violations" style="justify-content:flex-end; gap:6px;">${I||'<span class="tag ghost">Minor Issues</span>'}</div>
                    <div style="text-align:right;display:flex;flex-direction:column;gap:8px;align-items:flex-end;">
                        <button class="btn secondary btn-report" style="font-size:0.72rem; padding:6px 14px; border-color:var(--yc-orange); color:var(--yc-orange);">Send to CMS</button>
                        <button class="brutalist-action btn-view" style="font-size:0.6rem; letter-spacing:0.04em;">Audit Trace →</button>
                    </div>
                `,(b=s.querySelector(".btn-report"))==null||b.addEventListener("click",u=>{u.stopPropagation(),E(t)}),(x=s.querySelector(".btn-view"))==null||x.addEventListener("click",u=>{u.stopPropagation(),f(t)}),s.addEventListener("click",()=>f(t)),o.appendChild(s)}),T(r);const i=document.createElement("div");i.className="api-cta-footer animate-up",i.innerHTML="Need this feed as a structured API? &nbsp;&middot;&nbsp; <strong>Use the Healthspend API</strong>",i.onclick=()=>window.location.hash="api",o.appendChild(i)}catch(l){console.error(l),o.innerHTML='<div style="padding:40px;text-align:center;color:var(--yc-orange);">Audit engine error. Please reload.</div>'}},T=n=>{var r,i;const l=Math.ceil(n/a);p.innerHTML=`
            <span>Showing ${(e-1)*a+1}–${Math.min(e*a,n)} of ${n.toLocaleString()} violations</span>
            <div class="pagination-controls">
                <button class="brutalist-action btn-prev" ${e===1?"disabled":""}>← Prev</button>
                <button class="brutalist-action btn-next" ${e>=l?"disabled":""}>Next →</button>
            </div>`,(r=p.querySelector(".btn-prev"))==null||r.addEventListener("click",()=>{e--,c()}),(i=p.querySelector(".btn-next"))==null||i.addEventListener("click",()=>{e++,c()})};let h;m.addEventListener("input",()=>{clearTimeout(h),h=setTimeout(()=>{g=m.value,e=1,c()},280)}),y.addEventListener("change",()=>{d=y.value,e=1,c()}),c()}export{H as renderIncidents};
