/// <reference types="vite/client" />

export const DB_URL = import.meta.env.DEV 
  ? "/audit_data.db"
  : "https://huggingface.co/datasets/vladimir-io/healthspend-data/resolve/main/audit_data.db";
export const NPI_CONFIDENCE_THRESHOLD = 0.95;
export const DB_VFS_ADAPTER: string = "sqljs-httpvfs";
