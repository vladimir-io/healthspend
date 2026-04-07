import { createDbWorker } from 'sql.js-httpvfs';

const workerUrl = new URL('sql.js-httpvfs/dist/sqlite.worker.js', import.meta.url);
const wasmUrl = new URL('sql.js-httpvfs/dist/sql-wasm.wasm', import.meta.url);

const workers = new Map<string, any>();

export async function getSharedWorker(dbUrl: string, chunkSize: number = 16384) {
  if (!workers.has(dbUrl)) {
    const worker = await createDbWorker(
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
    workers.set(dbUrl, worker);
  }

  return workers.get(dbUrl);
}