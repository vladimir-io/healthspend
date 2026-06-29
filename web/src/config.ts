/// <reference types="vite/client" />

const HF_DATASET = 'vladimir-io/healthspend-data';
const HF_BASE = `https://huggingface.co/datasets/${HF_DATASET}/resolve/main`;

export const FULL_DB_URL = import.meta.env.DEV
  ? '/audit_data.db'
  : `${HF_BASE}/audit_data.db`;

export const HOT_DB_URL = import.meta.env.DEV
  ? '/audit_hot.db'
  : `${HF_BASE}/audit_hot.db`;

/** Fast shard for anonymous search — full ledger loads only on explicit upgrade. */
export const DB_URL = import.meta.env.DEV
  ? import.meta.env.VITE_USE_FULL_DB === 'true'
    ? '/audit_data.db'
    : '/audit_hot.db'
  : HOT_DB_URL;

/** Never auto-download the full ledger after a hot open failure. */
export const DB_URL_CHAIN: string[] = [DB_URL];

export const HOT_CPT_CODES = new Set([
  '27447', '27130', '70551', '73721', '74177', '70450', '71045', '71250',
  '80053', '85025', '45378', '45380', '99283', '99285', '59400', '12001',
  '90686', '96372', '99213', '76700',
]);

export const NPI_CONFIDENCE_THRESHOLD = 0.95;
export const DB_VFS_ADAPTER: string = 'sqljs-httpvfs';

/** Max sequential fallback query rounds before stopping (latency guard). */
export const MAX_SEARCH_FALLBACK_ROUNDS = 3;

/** Defer expensive COUNT / market aggregates until after first paint. */
export const DEFER_SEARCH_STATS_DEFAULT = true;
