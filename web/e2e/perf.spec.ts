import { test, expect, type Page } from '@playwright/test';
import { existsSync } from 'node:fs';
import { join } from 'node:path';

const dbPath = join(process.cwd(), 'public', 'audit_data.db');
const hasDb = existsSync(dbPath);

const P95_COLD_MS = Number(process.env.HS_PERF_P95_COLD_MS ?? 25_000);
const P95_WARM_MS = Number(process.env.HS_PERF_P95_WARM_MS ?? 8_000);

async function emulateSlow3G(page: Page) {
  const session = await page.context().newCDPSession(page);
  await session.send('Network.enable');
  await session.send('Network.emulateNetworkConditions', {
    offline: false,
    downloadThroughput: (400 * 1024) / 8,
    uploadThroughput: (400 * 1024) / 8,
    latency: 400,
  });
}

test.describe('search perf budget', () => {
  test.skip(!hasDb, 'Requires web/public/audit_data.db');

  test('cold search within budget (Slow 3G)', async ({ page }) => {
    await emulateSlow3G(page);
    await page.goto('/#search');
    const t0 = Date.now();
    await page.fill('#search-input', 'MRI');
    await page.waitForSelector('.search-result-card', { timeout: 90_000 });
    expect(Date.now() - t0).toBeLessThan(P95_COLD_MS);
  });

  test('warm repeat search within budget', async ({ page }) => {
    await page.goto('/#search');
    await page.fill('#search-input', '80053');
    await page.waitForSelector('.search-result-card', { timeout: 90_000 });

    await emulateSlow3G(page);
    const samples: number[] = [];
    for (const q of ['colonoscopy', 'knee']) {
      await page.fill('#search-input', q);
      const t0 = Date.now();
      await page.waitForFunction(
        () => {
          const summary = document.getElementById('results-summary')?.textContent ?? '';
          return summary.length > 10 && !summary.includes('Search ready');
        },
        { timeout: 60_000 }
      );
      await page.waitForSelector('.search-result-card', { timeout: 60_000 });
      samples.push(Date.now() - t0);
    }
    const p95 = Math.max(...samples);
    expect(p95).toBeLessThan(P95_WARM_MS);
  });
});
