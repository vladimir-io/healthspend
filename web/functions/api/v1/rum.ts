/** Ingest RUM beacons into KV daily rollups (queryable via rum_analytics_report.py). */

interface RumBody {
  v?: number;
  name?: string;
  ms?: number;
  path?: string;
  ts?: number;
  meta?: Record<string, string | number | boolean>;
}

interface Env {
  RUM_COUNTERS: KVNamespace;
}

const ALLOWED = new Set([
  'cold_start',
  'db_warm',
  'search',
  'search_stats',
  'fallback',
  'page_view',
]);

const TTL_SECONDS = 60 * 60 * 24 * 120; // 120 days

function dayKey(ts: number): string {
  return new Date(ts).toISOString().slice(0, 10);
}

function safeSegment(value: string, max = 80): string {
  return value.replace(/[^a-zA-Z0-9#_.:=/-]/g, '_').slice(0, max);
}

async function bump(env: Env, key: string, delta = 1): Promise<void> {
  const prev = parseInt((await env.RUM_COUNTERS.get(key)) || '0', 10);
  await env.RUM_COUNTERS.put(key, String(prev + delta), { expirationTtl: TTL_SECONDS });
}

async function bumpSum(env: Env, key: string, amount: number): Promise<void> {
  const prev = parseFloat((await env.RUM_COUNTERS.get(key)) || '0');
  await env.RUM_COUNTERS.put(key, String(prev + amount), { expirationTtl: TTL_SECONDS });
}

export const onRequestPost: PagesFunction<Env> = async (context) => {
  let body: RumBody;
  try {
    body = (await context.request.json()) as RumBody;
  } catch {
    return new Response(null, { status: 204 });
  }

  const name = typeof body.name === 'string' ? body.name.slice(0, 40) : '';
  if (!ALLOWED.has(name)) {
    return new Response(null, { status: 204 });
  }

  const path = typeof body.path === 'string' ? body.path.slice(0, 200) : '/';
  const ms = typeof body.ms === 'number' && Number.isFinite(body.ms) ? body.ms : 0;
  const day = dayKey(typeof body.ts === 'number' ? body.ts : Date.now());
  const env = context.env;

  await bump(env, `d:${day}:event:${name}`);
  await bump(env, `d:${day}:path:${safeSegment(path, 120)}`);

  if (name === 'search' && ms > 0) {
    await bump(env, `d:${day}:search:n`);
    await bumpSum(env, `d:${day}:search:ms_sum`, ms);
  }

  if (name === 'db_warm' && ms > 0) {
    const tier =
      body.meta && typeof body.meta.tier === 'string' ? safeSegment(body.meta.tier, 20) : 'unknown';
    await bump(env, `d:${day}:db_warm:${tier}`);
    await bumpSum(env, `d:${day}:db_warm:${tier}:ms_sum`, ms);
  }

  if (body.meta && typeof body.meta === 'object') {
    const src = body.meta.utm_source;
    const ref = body.meta.referrer;
    if (typeof src === 'string' && src) {
      await bump(env, `d:${day}:src:${safeSegment(src, 60)}`);
    }
    if (typeof ref === 'string' && ref) {
      await bump(env, `d:${day}:ref:${safeSegment(ref, 60)}`);
    }
    const route = body.meta.route;
    if (typeof route === 'string' && route) {
      await bump(env, `d:${day}:route:${safeSegment(route, 30)}`);
    }
  }

  return new Response(null, {
    status: 204,
    headers: { 'Cache-Control': 'no-store' },
  });
};
