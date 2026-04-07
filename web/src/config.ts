/// <reference types="vite/client" />

const HF_DB_URL = "https://data.healthspend.lol/audit_data.db";

const isProd = import.meta.env.PROD;

export const DB_URL = isProd ? HF_DB_URL : "/audit_data.db";

console.log(`[Healthspend] Database initialized from ${isProd ? 'Remote CDN' : 'Local Node'}: ${DB_URL}`);
