import { createDbWorker } from 'sql.js-httpvfs';
import { DB_VFS_ADAPTER } from './config';

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

export async function getSharedWorker(dbUrl: string, chunkSize: number = 65536) {
  const key = `${DB_VFS_ADAPTER}:${dbUrl}:${chunkSize}`;
  if (!pool.has(key)) {
    pool.set(key, await openWorker(dbUrl, chunkSize));
  }
  return pool.get(key);
}