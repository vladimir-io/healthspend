/**
 * Privacy-preserving Real User Monitoring.
 * Aggregates timing locally; optional beacon to /api/v1/rum (static ingest) when configured.
 */

export type RumEventName =
  | 'cold_start'
  | 'db_warm'
  | 'search'
  | 'search_stats'
  | 'fallback';

export type RumPayload = {
  name: RumEventName;
  ms: number;
  meta?: Record<string, string | number | boolean>;
};

const BUFFER_KEY = 'hs_rum_v1';
const MAX_BUFFER = 120;
const SAMPLE_RATE = 0.15;

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

  if (Math.random() > SAMPLE_RATE) return;
  const body = JSON.stringify({
  v: 1,
    ...event,
    path: location.pathname + location.hash,
    ts: Date.now(),
  });
  try {
    if (navigator.sendBeacon) {
      navigator.sendBeacon('/api/v1/rum', body);
    }
  } catch {
    /* no endpoint in static deploy */
  }
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
