/**
 * Privacy-preserving Real User Monitoring.
 * Aggregates timing locally; optional beacon to /api/v1/rum (static ingest) when configured.
 */

export type RumEventName =
  | 'cold_start'
  | 'db_warm'
  | 'search'
  | 'search_stats'
  | 'fallback'
  | 'page_view';

export type RumPayload = {
  name: RumEventName;
  ms: number;
  meta?: Record<string, string | number | boolean>;
};

const BUFFER_KEY = 'hs_rum_v1';
const MAX_BUFFER = 120;
const PERF_SAMPLE_RATE = 0.15;
const ENGAGEMENT_EVENTS = new Set<RumEventName>(['search', 'search_stats', 'page_view', 'fallback']);

function sessionContext(): Record<string, string> {
  const params = new URLSearchParams(location.search);
  const ctx: Record<string, string> = {};
  const utmSource = params.get('utm_source');
  const utmMedium = params.get('utm_medium');
  const utmCampaign = params.get('utm_campaign');
  const state = params.get('state');
  if (utmSource) ctx.utm_source = utmSource.slice(0, 80);
  if (utmMedium) ctx.utm_medium = utmMedium.slice(0, 80);
  if (utmCampaign) ctx.utm_campaign = utmCampaign.slice(0, 80);
  if (state) ctx.state = state.slice(0, 4).toUpperCase();
  if (document.referrer) {
    try {
      const ref = new URL(document.referrer);
      if (ref.hostname !== location.hostname) {
        ctx.referrer = ref.hostname.slice(0, 120);
      }
    } catch {
      /* ignore */
    }
  }
  return ctx;
}

function shouldBeacon(name: RumEventName): boolean {
  if (ENGAGEMENT_EVENTS.has(name)) return true;
  return Math.random() <= PERF_SAMPLE_RATE;
}

type RumBuffer = { events: RumPayload[]; updatedAt: string };

function readBuffer(): RumBuffer {
  try {
    const raw = localStorage.getItem(BUFFER_KEY);
    if (!raw) return { events: [], updatedAt: '' };
    return JSON.parse(raw) as RumBuffer;
  } catch {
    return { events: [], updatedAt: '' };
  }
}

function writeBuffer(buf: RumBuffer) {
  try {
    localStorage.setItem(BUFFER_KEY, JSON.stringify(buf));
  } catch {
    /* quota */
  }
}

export function recordRum(event: RumPayload): void {
  const buf = readBuffer();
  buf.events.push(event);
  if (buf.events.length > MAX_BUFFER) {
    buf.events = buf.events.slice(-MAX_BUFFER);
  }
  buf.updatedAt = new Date().toISOString();
  writeBuffer(buf);

  if (import.meta.env.DEV) {
    console.debug('[rum]', event.name, Math.round(event.ms), event.meta ?? '');
  }

  if (!shouldBeacon(event.name)) return;

  const body = JSON.stringify({
    v: 1,
    ...event,
    meta: { ...sessionContext(), ...(event.meta ?? {}) },
    path: location.pathname + location.hash,
    ts: Date.now(),
  });

  try {
    if (navigator.sendBeacon) {
      navigator.sendBeacon('/api/v1/rum', body);
      return;
    }
    void fetch('/api/v1/rum', {
      method: 'POST',
      body,
      keepalive: true,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch {
    /* no endpoint in static deploy */
  }
}

export function recordPageView(route: string): void {
  recordRum({ name: 'page_view', ms: 0, meta: { route } });
}

export function getRumSummary(): {
  count: number;
  medianSearchMs: number;
  fallbackRate: number;
} {
  const buf = readBuffer();
  const searches = buf.events.filter((e) => e.name === 'search').map((e) => e.ms);
  const fallbacks = buf.events.filter((e) => e.name === 'fallback').length;
  const sorted = [...searches].sort((a, b) => a - b);
  const median =
    sorted.length === 0
      ? 0
      : sorted[Math.floor(sorted.length / 2)] ?? 0;
  return {
    count: buf.events.length,
    medianSearchMs: Math.round(median),
    fallbackRate: searches.length ? fallbacks / searches.length : 0,
  };
}

let coldStartMarked = false;

export function markColdStart(): void {
  if (coldStartMarked) return;
  coldStartMarked = true;
  const nav = performance.getEntriesByType('navigation')[0] as
    | PerformanceNavigationTiming
    | undefined;
  const ms = nav?.domContentLoadedEventEnd ?? performance.now();
  recordRum({ name: 'cold_start', ms, meta: { nav: nav?.type ?? 'unknown' } });
}
