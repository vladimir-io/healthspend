import { prefetchDatabase } from '../worker';

/** Warm the SQLite worker when the browser is idle — not on critical path. */
export function scheduleIdleDatabaseWarm(): void {
  const run = () => prefetchDatabase();
  if (typeof requestIdleCallback === 'function') {
    requestIdleCallback(run, { timeout: 5000 });
  } else {
    setTimeout(run, 2500);
  }
}

/** Deep links and explicit search need the DB immediately. */
export function warmDatabaseNow(): void {
  prefetchDatabase();
}
