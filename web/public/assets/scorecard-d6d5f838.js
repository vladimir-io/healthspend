import{s as N,g as I,a as S}from"./detail-d0789f45.js";import"./main-a364c182.js";const E=["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"];async function P(f){const h=document.getElementById(f);if(!h)return;let m="",p="",t=1;const i=20;h.innerHTML=`
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
                          ${E.map(s=>`<option value="${s}">${s}</option>`).join("")}
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
                ${[...Array(5)].map(()=>'<div class="skeleton-card animate-pulse" style="height:76px;"></div>').join("")}
            </div>

            <div id="scorecard-pagination" class="pagination-hub animate-up" style="--stagger:4;"></div>
        </div>
    `;const o=document.getElementById("scorecard-results"),y=document.getElementById("scorecard-search"),b=document.getElementById("scorecard-state"),u=document.getElementById("scorecard-sort"),v=document.getElementById("scorecard-pagination"),$=document.getElementById("scorecard-results-summary"),A=s=>s>80?"var(--rh-green)":s>50?"var(--amber)":"var(--yc-orange)",r=async()=>{const s=(t-1)*i;o.innerHTML=[...Array(4)].map(()=>'<div class="skeleton-card animate-pulse" style="height:76px;"></div>').join("");try{const[a,c]=await Promise.all([N(m,p,i,s,(u==null?void 0:u.value)||"score-desc"),I(m,p)]);if($.innerHTML=`<strong>${c.toLocaleString()} nodes</strong> matched &nbsp;·&nbsp; Region: ${p||"Federal"} &nbsp;·&nbsp; Page ${t} of ${Math.ceil(c/i)}`,a.length===0){o.innerHTML='<div style="padding:60px;text-align:center;color:var(--text-tertiary);">No facilities match this filter.</div>',v.innerHTML="";return}o.innerHTML=a.map(e=>{const l=e.score,g=A(l),L=l===100?"perfect-audit-card":"",M=[{label:"Price Pub.",ok:e.txt_exists,title:"§ 180.50"},{label:"Bot Access",ok:e.robots_ok,title:"§ 180.40"},{label:"MRF Schema",ok:e.mrf_valid,title:"CMS v2.0"},{label:"Shoppable",ok:e.shoppable_exists,title:"Consumer Tool"}].map(d=>`<div class="status-dot" title="${d.label}: ${d.ok?"Pass":"Fail"} (${d.title})"
                        style="background:${d.ok?"var(--rh-green)":"var(--yc-orange)"};
                        box-shadow:0 0 6px ${d.ok?"var(--rh-green-glow)":"var(--yc-orange-glow)"};"></div>`).join("");return`
                    <div class="audit-row ${L}" data-ccn="${e.ccn}"
                        style="grid-template-columns: 1fr 60px 120px 130px;">
                        <div class="audit-identity">
                            <h3>${e.name}</h3>
                            <p>${e.city}, ${e.state} &nbsp;·&nbsp; <span style="font-family:var(--font-mono);font-size:0.68rem;">CCN ${e.ccn}</span></p>
                        </div>
                        <div class="score-ring" style="color:${g};">
                            <span class="val" style="color:${g};">${l}</span>
                            <span class="lbl">IDX</span>
                        </div>
                        <div class="status-dots">${M}</div>
                        <div style="text-align:right;">
                            <button class="brutalist-action" style="border-color:var(--yc-orange);color:var(--yc-orange);">View Audit →</button>
                        </div>
                    </div>`}).join(""),o.querySelectorAll(".audit-row").forEach(e=>{e.addEventListener("click",()=>{const l=a.find(g=>g.ccn===e.dataset.ccn);l&&S(l)})}),T(c);const n=document.createElement("div");n.className="api-cta-footer animate-up",n.innerHTML="Need this data as a structured feed? &nbsp;&middot;&nbsp; <strong>Use the Healthspend API</strong>",n.onclick=()=>window.location.hash="api",o.appendChild(n)}catch(a){console.error(a),o.innerHTML='<div style="padding:40px;text-align:center;color:var(--red);">Audit engine error. Please reload.</div>'}},T=s=>{var c,n;const a=Math.ceil(s/i);v.innerHTML=`
            <span>Showing ${(t-1)*i+1}–${Math.min(t*i,s)} of ${s.toLocaleString()}</span>
            <div class="pagination-controls">
                <button class="brutalist-action btn-prev" ${t===1?"disabled":""}>← Prev</button>
                <button class="brutalist-action btn-next" ${t>=a?"disabled":""}>Next →</button>
            </div>`,(c=v.querySelector(".btn-prev"))==null||c.addEventListener("click",()=>{t--,r()}),(n=v.querySelector(".btn-next"))==null||n.addEventListener("click",()=>{t++,r()})};let x;y.addEventListener("input",()=>{clearTimeout(x),x=setTimeout(()=>{m=y.value,t=1,r()},280)}),b.addEventListener("change",()=>{p=b.value,t=1,r()}),u.addEventListener("change",()=>{t=1,r()}),r()}export{P as renderScorecard};
