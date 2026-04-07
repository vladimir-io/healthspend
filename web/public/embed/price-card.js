(function () {
    const scripts = document.getElementsByTagName('script');
    const currentScript = scripts[scripts.length - 1];
    const params = new URLSearchParams(currentScript.src.split('?')[1]);
    const state = (params.get('state') || 'US').toUpperCase();
    const brandColor = '#FF6000';

    const container = document.createElement('div');
    container.style.cssText = `
        font-family: -apple-system, system-ui, sans-serif;
        border: 1px solid ${brandColor};
        border-radius: 12px;
        padding: 24px;
        max-width: 360px;
        background: #fff;
        box-shadow: 0 10px 40px rgba(255, 96, 0, 0.08);
        margin: 20px 0;
    `;

    container.innerHTML = `
        <div style="font-weight:900; color:${brandColor}; font-size:0.75rem; text-transform:uppercase; margin-bottom:12px; letter-spacing:0.12em;">Healthspend &middot; Transparency Node</div>
        <div class="hs-title" style="font-size:1.2rem; font-weight:900; color:#000; line-height:1.1; margin-bottom:18px; letter-spacing:-0.02em;">Live Pricing: ${state}</div>
        
        <div class="hs-data-load" style="font-size:0.9rem; color:#666;">Synchronizing federal MRF audits...</div>
        
        <div class="hs-data-results" style="display:none;"></div>

        <a href="https://healthspend.lol/visibility/state-${state.toLowerCase()}.html" target="_blank" style="display:block; text-align:center; background:${brandColor}; color:#fff; text-decoration:none; padding:14px; border-radius:8px; font-weight:800; font-size:0.85rem; margin-top:24px; transition: opacity 0.2s;">Full Transparency Index &rarr;</a>
    `;

    currentScript.parentNode.insertBefore(container, currentScript);

    const dataUrl = `https://healthspend.lol/embed/data/${state.toLowerCase()}.json`;

    fetch(dataUrl)
        .then(response => {
            if (!response.ok) throw new Error('Network response not ok');
            return response.json();
        })
        .then(data => {
            const resultsEl = container.querySelector('.hs-data-results');
            const titleEl = container.querySelector('.hs-title');
            titleEl.innerText = `Live Pricing: ${data.full_name}`;

            let rowsHtml = "";
            // Show up to 4 procedures for the widget
            data.procedures.slice(0, 4).forEach(proc => {
                rowsHtml += `
                <div style="display:flex; justify-content:space-between; font-size:0.85rem; padding:10px 0; border-bottom:1px solid #f8f8f8;">
                    <span style="color:#444; font-weight:500;">${proc.label}</span>
                    <span style="font-weight:900; color:${brandColor}; font-family:ui-monospace, monospace;">$${proc.price.toLocaleString()}</span>
                </div>`;
            });

            resultsEl.innerHTML = rowsHtml + `
                <p style="font-size:0.7rem; color:#999; margin-top:16px; line-height:1.4;">
                    Audited from the latest 2026 CMS machine-readable clinical ledgers.
                </p>`;

            container.querySelector('.hs-data-load').style.display = 'none';
            resultsEl.style.display = 'block';
        })
        .catch(err => {
            console.error('Healthspend Widget Error:', err);
            container.querySelector('.hs-data-load').innerHTML = 
                `<div style="color:#d33; font-weight:700;">Data Sync Error</div>
                 <div style="font-size:0.8rem; margin-top:4px;">Node ${state} pricing currently offline.</div>`;
        });
})();