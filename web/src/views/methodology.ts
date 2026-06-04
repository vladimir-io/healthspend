const CMS_HPT = 'https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency';
const CMS_COMPARE = 'https://www.medicare.gov/care-compare/';
const GITHUB = 'https://github.com/vladimir-io/healthspend';

export async function renderMethodology(containerId: string): Promise<void> {
  const el = document.getElementById(containerId);
  if (!el) return;

  el.innerHTML = `
    <article class="method-page">
      <header class="method-hero animate-up">
        <div class="status-pill-container">
          <div class="live-dot" aria-hidden="true"></div>
          <span>Open methodology · updated with CMS rules</span>
        </div>
        <h1 class="method-hero__title">How we score hospitals and surface prices</h1>
        <p class="method-hero__lede">Every price row and audit score traces back to a public filing — CMS registry data plus each hospital’s machine-readable file. No estimates, no black-box models.</p>
      </header>

      <div class="method-layout">
        <aside class="method-sidebar" aria-label="On this page">
          <nav class="method-toc">
            <p class="method-toc__label">On this page</p>
            <a href="#method-sources" class="method-toc__link">Data sources</a>
            <a href="#method-score" class="method-toc__link">Audit index</a>
            <a href="#method-npi" class="method-toc__link">NPI confidence</a>
            <a href="#method-verify" class="method-toc__link method-toc__link--accent">Before you pay</a>
          </nav>
        </aside>

        <div class="method-main">
          <section id="method-sources" class="method-section card-stagger" style="--card-i: 0">
            <div class="method-section__head">
              <span class="method-section__num" aria-hidden="true">01</span>
              <div>
                <h2>Data sources</h2>
                <p class="method-section__intro">Three public layers are merged nightly into our SQLite ledger.</p>
              </div>
            </div>
            <div class="method-source-grid">
              <div class="method-source-card">
                <span class="method-source-card__tag">Registry</span>
                <h3>CMS Hospital General Information</h3>
                <p>Facility identity, CCN, and quality-reporting baseline for every audited node.</p>
              </div>
              <div class="method-source-card">
                <span class="method-source-card__tag">Pricing</span>
                <h3>Hospital MRFs</h3>
                <p>Cash and negotiated rates from files published under <a href="${CMS_HPT}" target="_blank" rel="noopener">45 CFR § 180.50</a>.</p>
              </div>
              <div class="method-source-card">
                <span class="method-source-card__tag">Attribution</span>
                <h3>NPPES</h3>
                <p>Provider linkage so procedure prices can be matched to the correct NPI where the file allows.</p>
              </div>
            </div>
          </section>

          <section id="method-score" class="method-section card-stagger" style="--card-i: 1">
            <div class="method-section__head">
              <span class="method-section__num" aria-hidden="true">02</span>
              <div>
                <h2>Facility Audit Index (0–100)</h2>
                <p class="method-section__intro">A single score per hospital blending CMS signals with deterministic file checks.</p>
              </div>
            </div>
            <div class="method-score-panel">
              <div class="method-score-visual" aria-hidden="true">
                <div class="method-score-ring">
                  <span class="method-score-ring__value">100</span>
                  <span class="method-score-ring__label">max</span>
                </div>
                <div class="method-score-bar">
                  <span style="width: 100%"></span>
                </div>
              </div>
              <ul class="method-checklist">
                <li><span class="method-check" aria-hidden="true">✓</span> MRF reachable and parseable</li>
                <li><span class="method-check" aria-hidden="true">✓</span> Schema matches federal requirements</li>
                <li><span class="method-check" aria-hidden="true">✓</span> Dollar amounts (not placeholders)</li>
                <li><span class="method-check" aria-hidden="true">✓</span> Attestation &amp; accessibility fields</li>
              </ul>
              <p class="method-footnote">Pipeline changes are documented in <a href="${GITHUB}" target="_blank" rel="noopener">our open repo</a> and tracked against current CMS transparency policy.</p>
            </div>
          </section>

          <section id="method-npi" class="method-section card-stagger" style="--card-i: 2">
            <div class="method-section__head">
              <span class="method-section__num" aria-hidden="true">03</span>
              <div>
                <h2>NPI confidence</h2>
                <p class="method-section__intro">Search defaults to high-confidence attribution so you see prices that likely apply to the listed provider.</p>
              </div>
            </div>
            <div class="method-npi-panel">
              <div class="method-npi-meter" role="img" aria-label="Default threshold 95 percent">
                <div class="method-npi-meter__fill" style="width: 95%"></div>
              </div>
              <div class="method-npi-labels">
                <span>0%</span>
                <span class="method-npi-threshold">95% default threshold</span>
                <span>100%</span>
              </div>
              <p class="method-npi-copy">Fewer results usually means <strong>stricter matching</strong>, not a broken search. Lower-confidence rows are deprioritized or hidden unless the dataset forces a fallback.</p>
            </div>
          </section>

          <section id="method-verify" class="method-section method-section--accent card-stagger" style="--card-i: 3">
            <div class="method-section__head">
              <span class="method-section__num" aria-hidden="true">04</span>
              <div>
                <h2>Verify before you pay</h2>
                <p class="method-section__intro">Healthspend is research and education — not legal, medical, or billing advice.</p>
              </div>
            </div>
            <ol class="method-steps">
              <li>
                <span class="method-step__index">1</span>
                <div>
                  <strong>Compare three numbers</strong>
                  <p>Published cash rate, your itemized bill, and your EOB line items.</p>
                </div>
              </li>
              <li>
                <span class="method-step__index">2</span>
                <div>
                  <strong>Confirm with the hospital</strong>
                  <p>Call billing or patient financial services for a written estimate before paying.</p>
                </div>
              </li>
              <li>
                <span class="method-step__index">3</span>
                <div>
                  <strong>Cross-check the facility</strong>
                  <p>Use <a href="${CMS_COMPARE}" target="_blank" rel="noopener">Medicare Care Compare</a> and the live MRF link in Full Audit.</p>
                </div>
              </li>
            </ol>
          </section>
        </div>
      </div>
    </article>
  `;

  el.querySelectorAll<HTMLAnchorElement>('.method-toc__link').forEach((link) => {
    link.addEventListener('click', (e) => {
      const id = link.getAttribute('href')?.slice(1);
      const target = id ? document.getElementById(id) : null;
      if (!target) return;
      e.preventDefault();
      target.scrollIntoView({ behavior: 'smooth', block: 'start' });
      history.replaceState(null, '', `#methodology`);
    });
  });
}
