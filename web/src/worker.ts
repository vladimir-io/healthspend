import { createDbWorker } from 'sql.js-httpvfs';
import { DB_URL, DB_URL_CHAIN, DB_VFS_ADAPTER } from './config';
import { recordRum } from './rum';

const workerUrl = new URL('sql.js-httpvfs/dist/sqlite.worker.js', import.meta.url);
const wasmUrl   = new URL('sql.js-httpvfs/dist/sql-wasm.wasm', import.meta.url);

const pool = new Map<string, any>();

async function resolveFileSize(dbUrl: string): Promise<number | undefined> {
  const match = dbUrl.match(/datasets\/([^/]+\/[^/]+)\/resolve\/main\/(.*)/);
  if (!match) return undefined;
  try {
    const res = await fetch(`https://huggingface.co/api/datasets/${match[1]}/tree/main`);
    if (!res.ok) return undefined;
    const tree = await res.json();
    const entry = tree.find((f: any) => f.path === match[2]);
    return entry?.size;
  } catch {
    return undefined;
  }
}

async function openWorker(dbUrl: string, chunkSize: number) {
  const length = await resolveFileSize(dbUrl);
  const config: any = { serverMode: 'full', url: dbUrl, requestChunkSize: chunkSize };
  if (length) config.length = length;

  const instance = await createDbWorker(
    [{ from: 'inline', config }],
    workerUrl.toString(),
    wasmUrl.toString()
  );

  return {
    db: {
      query: async (sql: string, params?: any[]) => {
        const results = await (instance as any).db.query(sql, params);
        return Array.isArray(results) ? results : [];
      }
    },
    close: () => {}
  };
}

export async function getSharedWorker(dbUrl: string = DB_URL, chunkSize: number = 262144) {
  const chain = dbUrl === DB_URL ? DB_URL_CHAIN : [dbUrl];
  let lastError: unknown;
  for (const url of chain) {
    const key = `${DB_VFS_ADAPTER}:${url}:${chunkSize}`;
    if (pool.has(key)) return pool.get(key);
    try {
      const t0 = performance.now();
      const instance = await openWorker(url, chunkSize);
      pool.set(key, instance);
      recordRum({
        name: 'db_warm',
        ms: performance.now() - t0,
        meta: { tier: url.includes('audit_hot') ? 'hot' : 'full' },
      });
      return instance;
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError ?? new Error('Failed to open database');
}

/** Prefetch worker + SQLite on search focus (non-blocking). */
export function prefetchDatabase(dbUrl: string = DB_URL): void {
  void getSharedWorker(dbUrl).then((w) => w.db.query('SELECT 1')).catch(() => undefined);
}