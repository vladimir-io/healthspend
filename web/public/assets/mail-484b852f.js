function u(e){const t=[];e.txt_exists||t.push('Failure to publish a machine-readable file (MRF) discoverable via the required "cms-hpt.txt" index file, in violation of 45 CFR §180.50(d)(2).'),e.robots_ok||t.push(`Active use of a "robots.txt" directive to instruct web crawlers to disallow access to the hospital's MRF, a deliberate obstruction of public access in violation of 45 CFR §180.60.`),(e.mrf_machine_readable===!1||e.mrf_machine_readable===0)&&t.push("Publication of an MRF file that is not machine-readable, containing malformed data, non-standard schemas, or non-parseable formatting, in violation of 45 CFR §180.50(b)."),(e.waf_blocked===!0||e.waf_blocked===1)&&t.push("Deployment of a Web Application Firewall (WAF) or bot-mitigation technology that blocks automated access to legally mandated public pricing data, in violation of 45 CFR §180.60."),t.length===0&&t.push("Failure to meet the minimum standards required under the Hospital Price Transparency Rule (45 CFR Part 180) as determined by an independent compliance audit.");const o=t.map((a,c)=>`  ${c+1}. ${a}`).join(`

`);return`${new Date().toLocaleDateString("en-US",{year:"numeric",month:"long",day:"numeric"})}

Centers for Medicare & Medicaid Services
Price Transparency Enforcement
HPTCompliance@cms.hhs.gov

Re: Formal Complaint: Hospital Price Transparency Non-Compliance: ${e.name} (CCN: ${e.ccn})

To the CMS Price Transparency Enforcement Division,

I am formally submitting this complaint regarding the failure of ${e.name}, located in ${e.city}, ${e.state} (CMS Certification Number: ${e.ccn}), to comply with the Hospital Price Transparency Rule enacted under the Affordable Care Act.

An independent audit conducted via publicly available HTTP inspection has identified ${t.length} specific violation${t.length>1?"s":""} of federal regulations:

${o}

This hospital has received a compliance score of ${e.score}/100 in this audit. Under 42 CFR §180.70, CMS has authority to impose Civil Monetary Penalties (CMPs) of up to $300 per day for hospitals with fewer than 30 beds, and up to $5,500 per day for larger institutions. I request that CMS:

  1. Open a formal investigation into the above violations.
  2. Issue a corrective action plan (CAP) to ${e.name} within 30 days.
  3. Impose Civil Monetary Penalties if compliance is not achieved within the CAP window.
  4. Publish the outcome of this investigation on the CMS website for public accountability.

The right to access hospital pricing information is a federal mandate. Obstruction of that right harms patients' ability to make informed healthcare decisions. I respectfully request CMS enforce this law with the full authority provided to it.

Sincerely,

[Your Name]`}function h({to:e,subject:t,body:o}){return`https://mail.google.com/mail/?view=cm&fs=1&to=${encodeURIComponent(e)}&su=${encodeURIComponent(t)}&body=${encodeURIComponent(o)}`}function b({to:e,subject:t,body:o}){return`https://outlook.live.com/mail/0/deeplink/compose?to=${encodeURIComponent(e)}&subject=${encodeURIComponent(t)}&body=${encodeURIComponent(o)}`}function y({to:e,subject:t,body:o}){return`https://compose.mail.yahoo.com/?to=${encodeURIComponent(e)}&subj=${encodeURIComponent(t)}&body=${encodeURIComponent(o)}`}function f(e){return navigator.clipboard&&window.isSecureContext?(navigator.clipboard.writeText(e),!0):!1}function C(e){const t=document.getElementById("letter-overlay");if(!t)return;const o={name:e.name,ccn:e.ccn,state:e.state,city:e.city,score:e.score,txt_exists:e.txt_exists,robots_ok:e.robots_ok},i=u(o),a="HPTCompliance@cms.hhs.gov",c=`Official Complaint: Hospital Noncompliance : CCN ${e.ccn}`,l=document.getElementById("letter-draft"),d=document.getElementById("btn-gmail"),n=document.getElementById("btn-copy"),s=document.getElementById("btn-close-letter"),r=t.querySelector(".sheet-backdrop");l&&(l.value=i),d&&(d.href=h({to:a,subject:c,body:i})),n&&(n.onclick=()=>{f(i);const p=n.innerText;n.innerText="Copied ✓",setTimeout(()=>{n.innerText=p},2e3)});const m=()=>t.classList.add("hidden");s==null||s.addEventListener("click",m,{once:!0}),r==null||r.addEventListener("click",m,{once:!0}),t.classList.remove("hidden")}export{f as copyToClipboard,h as generateGmailLink,b as generateOutlookLink,y as generateYahooLink,C as handleComplaint};
