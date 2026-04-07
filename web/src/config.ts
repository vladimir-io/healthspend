/// <reference types="vite/client" />

const HOT_DB_URL_OVERRIDE = import.meta.env.VITE_HOT_DB_URL as string | undefined;
const FORCE_REMOTE_DB = (import.meta.env.VITE_FORCE_REMOTE_DB as string | undefined) === 'true';
const VFS_ADAPTER_OVERRIDE = import.meta.env.VITE_DB_VFS as string | undefined;

const isProd = import.meta.env.PROD;
const LOCAL_DB_URL = "/audit_data.db";

export const DB_URL = isProd
	? (FORCE_REMOTE_DB && HOT_DB_URL_OVERRIDE ? HOT_DB_URL_OVERRIDE : LOCAL_DB_URL)
	: (HOT_DB_URL_OVERRIDE || LOCAL_DB_URL);
export const NPI_CONFIDENCE_THRESHOLD = 0.95;
export const DB_VFS_ADAPTER = (VFS_ADAPTER_OVERRIDE || "sqljs-httpvfs").toLowerCase();

console.log(`[Healthspend] Database initialized from ${DB_URL.startsWith('/') ? 'Local static asset' : 'Remote dataset'}: ${DB_URL} (vfs=${DB_VFS_ADAPTER})`);
