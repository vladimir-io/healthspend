import { createDbWorker } from 'sql.js-httpvfs';
import { DB_VFS_ADAPTER } from './config';

const workerUrl = new URL('sql.js-httpvfs/dist/sqlite.worker.js', import.meta.url);
const wasmUrl = new URL('sql.js-httpvfs/dist/sql-wasm.wasm', import.meta.url);

const workers = new Map<string, any>();

async function createSqlJsHttpVfsWorker(dbUrl: string, chunkSize: number) {
  const sqlDb = await createDbWorker(
    [{
      from: 'inline',
      config: {
        serverMode: 'full',
        url: dbUrl,
        requestChunkSize: chunkSize,
      },
    }],
    workerUrl.toString(),
    wasmUrl.toString()
  );

  // Wrap sql.js-httpvfs API to expose a `.query()` method that works with our db.ts
  return {
    db: {
      query: async (sql: string, params?: any[]) => {
        try {
          const sqlDbAny = sqlDb as any;
          const results = await sqlDbAny.db?.exec?.(sql, params);
          
          if (Array.isArray(results) && results.length > 0 && results[0]?.columns && results[0]?.values) {
            const { columns, values } = results[0];
            return values.map((row: any[]) => {
              const obj: Record<string, any> = {};
              columns.forEach((col: string, i: number) => {
                obj[col] = row[i];
              });
              return obj;
            });
          }
          return [];
        } catch (err) {
          console.error('[Healthspend] Query execution error:', err, { sql, params });
          throw err;
        }
      }
    },
    close: () => {
      // sql.js-httpvfs doesn't expose a close method on the db object
      // but we can maintain the API contract
    }
  };
}

async function createTurboliteWorker(dbUrl: string, chunkSize: number) {
  const moduleName = 'turbolite';

  // Locked API target: turbolite@0.2.19 exposes a Node Database class.
  // Because the HealthSpend UI runs in browsers, this adapter currently
  // falls back to sql.js-httpvfs in browser contexts.
  if (typeof window !== 'undefined') {
    console.warn('[Healthspend] turbolite@0.2.19 is Node-only in current integration; using sql.js-httpvfs fallback in browser.');
    return createSqlJsHttpVfsWorker(dbUrl, chunkSize);
  }

  try {
    // Concrete package lock-in path for Node runtimes.
    const turbo: any = await import(/* @vite-ignore */ moduleName);
    if (typeof turbo.Database === 'function') {
      const db = new turbo.Database(dbUrl);
      if (typeof db.query === 'function' && typeof db.exec === 'function') {
        return {
          db: {
            query: async (sql: string, params?: any[]) => {
              const rows = await db.query(sql, params);
              return Array.isArray(rows) ? rows : [];
            }
          },
          close: () => db.close(),
        };
      }
    }

    console.warn('[Healthspend] turbolite@0.2.19 loaded but Database API was not usable. Falling back to sql.js-httpvfs.');
    return createSqlJsHttpVfsWorker(dbUrl, chunkSize);
  } catch (err) {
    console.warn('[Healthspend] Turbolite adapter unavailable, using sql.js-httpvfs fallback.', err);
    return createSqlJsHttpVfsWorker(dbUrl, chunkSize);
  }
}

export async function getSharedWorker(dbUrl: string, chunkSize: number = 65536) {
  const cacheKey = `${DB_VFS_ADAPTER}:${dbUrl}:${chunkSize}`;
  if (!workers.has(cacheKey)) {
    const worker = DB_VFS_ADAPTER === 'turbolite'
      ? await createTurboliteWorker(dbUrl, chunkSize)
      : await createSqlJsHttpVfsWorker(dbUrl, chunkSize);
    workers.set(cacheKey, worker);
  }

  return workers.get(cacheKey);
}