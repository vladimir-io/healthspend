/**
 * Cold-start benchmark: measures shell load without user interaction.
 * Usage: node scripts/cold_start_benchmark.mjs [baseUrl]
 */
import { chromium } from '@playwright/test';
import { execSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const baseUrl = process.argv[2] ?? 'http://127.0.0.1:4173';
const webRoot = process.cwd();

function bundleStats() {
  const assets = join(webRoot, 'dist', 'assets');
  const files = readdirSync(assets);
  const main = files.find((f) => f.startsWith('main-') && f.endsWith('.js'));
  const css = files.find((f) => f.startsWith('index-') && f.endsWith('.css'));
  const chunks = files.filter((f) => f.endsWith('.js') && !f.includes('worker'));
  const gzip = (p) => {
    try {
      return Number(execSync(`gzip -c "${p}" | wc -c`, { encoding: 'utf8' }).trim());
    } catch {
      return 0;
    }
  };
  return {
    mainKb: main ? Math.round(readFileSync(join(assets, main)).length / 1024) : 0,
    mainGzipKb: main ? Math.round(gzip(join(assets, main)) / 1024) : 0,
    cssKb: css ? Math.round(readFileSync(join(assets, css)).length / 1024) : 0,
    chunkCount: chunks.length,
  };
}

async function runBrowserProbe() {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const requests = [];
  const t0 = Date.now();

  page.on('request', (req) => {
    requests.push({ url: req.url(), ms: Date.now() - t0, type: req.resourceType() });
  });

  await page.goto(`${baseUrl}/#search`, { waitUntil: 'domcontentloaded', timeout: 60_000 });
  const domMs = Date.now() - t0;

  await page.waitForTimeout(2500);
  const probeMs = Date.now() - t0;

  const inputReady = await page.isEnabled('#search-input');
  const hospitalsLoaded = requests.some((r) => r.url.includes('hospitals'));
  const methodologyLoaded = requests.some((r) => r.url.includes('methodology'));
  const dbRequests = requests.filter((r) => r.url.includes('.db') || r.url.includes('huggingface'));
  const wasmRequests = requests.filter((r) => r.url.includes('.wasm'));
  const fontRequests = requests.filter((r) => r.url.includes('fonts.googleapis') || r.url.includes('fonts.gstatic'));
  const jsChunks = [...new Set(requests.filter((r) => r.type === 'script' && r.url.includes('/assets/')).map((r) => r.url.split('/').pop()))];

  await browser.close();

  return {
    domMs,
    probeMs,
    inputReady,
    hospitalsLoaded,
    methodologyLoaded,
    dbRequestCount: dbRequests.length,
    wasmRequestCount: wasmRequests.length,
    fontRequestCount: fontRequests.length,
    jsChunks,
    totalRequests: requests.length,
  };
}

const bundles = bundleStats();
const probe = await runBrowserProbe();

console.log(JSON.stringify({ bundles, probe }, null, 2));
