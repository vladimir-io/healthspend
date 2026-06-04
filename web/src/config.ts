/// <reference types="vite/client" />

const HF_DATASET = 'vladimir-io/healthspend-data';
const HF_BASE = `https://huggingface.co/datasets/${HF_DATASET}/resolve/main`;

export const FULL_DB_URL = import.meta.env.DEV
  ? '/audit_data.db'
  : `${HF_BASE}/audit_data.db`;

export const HOT_DB_URL = import.meta.env.DEV
  ? '/audit_hot.db'
  : `${HF_BASE}/audit_hot.db`;

/** Primary URL; worker falls back to FULL_DB_URL in production if hot shard is unavailable. */
export const DB_URL = import.meta.env.DEV
  ? import.meta.env.VITE_USE_HOT_DB === 'true'
    ? '/audit_hot.db'
    : '/audit_data.db'
  : HOT_DB_URL;

export const DB_URL_CHAIN: string[] = import.meta.env.DEV
  ? [DB_URL]
  : [HOT_DB_URL, FULL_DB_URL];

export const NPI_CONFIDENCE_THRESHOLD = 0.95;
export const DB_VFS_ADAPTER: string = 'sqljs-httpvfs';

/** Max sequential fallback query rounds before stopping (latency guard). */
export const MAX_SEARCH_FALLBACK_ROUNDS = 3;

/** Defer expensive COUNT / market aggregates until after first paint. */
export const DEFER_SEARCH_STATS_DEFAULT = true;
