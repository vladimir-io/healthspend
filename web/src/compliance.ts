import { getSharedWorker } from './worker.js';

import { DB_URL } from './config';
let metricsCache: { totalAudited: number, totalHiding: number } | null = null;

// Pre-connect on module load
setTimeout(() => getSharedWorker(DB_URL), 0);

export interface ComplianceRecord {
  ccn: string;
  name: string;
  state: string;
  city: string;
  website: string;
  score: number;
  txt_exists: number;
  robots_ok: number;
  mrf_reachable: number;
  mrf_valid: number;
  mrf_fresh: number;
  shoppable_exists: number;
  // Score component breakdown (matches ingestion script weights)
  score_rating: number;       // CMS Overall Star Rating -> 30 pts max
  score_pt_exp: number;       // Patient Experience completeness -> 20 pts max
  score_safety: number;       // Safety measure completeness -> 20 pts max
  score_mortality: number;    // Mortality measure completeness -> 15 pts max
  score_readmission: number;  // Readmission completeness -> 10 pts max
  // Raw CMS measure counts
  pt_exp_measures: number;
  safety_measures: number;
  mort_measures: number;
  readm_measures: number;
  last_checked: string;
  evidence_json: string;
}

export async function searchCompliance(query: string, state: string, limit: number = 20, offset: number = 0, orderBy: string = 'score-desc'): Promise<ComplianceRecord[]> {
  const w = await getSharedWorker(DB_URL);
  let sql = `
    SELECT h.name, h.state, h.city, h.website, c.*
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE 1=1
  `;
  const params: any[] = [];
  
  if (query) {
    sql += ` AND (h.name LIKE ? OR h.ccn LIKE ?)`;
    params.push(`%${query}%`, `%${query}%`);
  }
  if (state) {
    sql += ` AND h.state = ?`;
    params.push(state);
  }
  
  const orderMap: Record<string, string> = {
    'score-desc': 'c.score DESC',
    'score-asc':  'c.score ASC',
    'name-asc':   'h.name ASC'
  };
  sql += ` ORDER BY ${orderMap[orderBy] || 'c.score DESC'} LIMIT ? OFFSET ?`;
  params.push(limit, offset);
  return w.db.query(sql, params) as Promise<ComplianceRecord[]>;
}

export async function getTotalComplianceCount(query: string, state: string): Promise<number> {
  const w = await getSharedWorker(DB_URL);
  let sql = `
    SELECT COUNT(*) as total
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE 1=1
  `;
  const params: any[] = [];
  
  if (query) {
    sql += ` AND (h.name LIKE ? OR h.ccn LIKE ?)`;
    params.push(`%${query}%`, `%${query}%`);
  }
  if (state) {
    sql += ` AND h.state = ?`;
    params.push(state);
  }
  
  const result = await w.db.query(sql, params) as any[];
  return result[0].total;
}

export async function getComplianceIncidents(limit: number = 50): Promise<ComplianceRecord[]> {
  return searchComplianceIncidents('', '', limit, 0);
}

export async function searchComplianceIncidents(query: string, state: string, limit: number = 20, offset: number = 0): Promise<ComplianceRecord[]> {
  const w = await getSharedWorker(DB_URL);
  let sql = `
    SELECT h.name, h.state, h.city, h.website, c.*
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE c.score < 65
  `;
  const params: any[] = [];
  
  if (query) {
    sql += ` AND (h.name LIKE ? OR h.ccn LIKE ?)`;
    params.push(`%${query}%`, `%${query}%`);
  }
  if (state) {
    sql += ` AND h.state = ?`;
    params.push(state);
  }
  
  sql += ` ORDER BY c.score ASC LIMIT ? OFFSET ?`;
  params.push(limit, offset);
  return w.db.query(sql, params) as Promise<ComplianceRecord[]>;
}

export async function getTotalIncidentCount(query: string, state: string): Promise<number> {
  const w = await getSharedWorker(DB_URL);
  let sql = `
    SELECT COUNT(*) as total
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE c.score < 65
  `;
  const params: any[] = [];
  
  if (query) {
    sql += ` AND (h.name LIKE ? OR h.ccn LIKE ?)`;
    params.push(`%${query}%`, `%${query}%`);
  }
  if (state) {
    sql += ` AND h.state = ?`;
    params.push(state);
  }
  
  const result = await w.db.query(sql, params) as any[];
  return result[0].total;
}

export async function getComplianceDetail(ccn: string): Promise<ComplianceRecord | null> {
  const w = await getSharedWorker(DB_URL);
  const sql = `
    SELECT h.name, h.state, h.city, h.website, c.*
    FROM compliance c
    JOIN hospitals h ON c.ccn = h.ccn
    WHERE c.ccn = ?
  `;
  const results = await w.db.query(sql, [ccn]) as ComplianceRecord[];
  return results.length > 0 ? results[0] : null;
}

export async function getGlobalMetrics(): Promise<{ totalAudited: number, totalHiding: number }> {
  if (metricsCache) return metricsCache;

  const w = await getSharedWorker(DB_URL);
  const sql = `
    SELECT 
      COUNT(*) as totalAudited, 
      SUM(CASE WHEN score < 60 THEN 1 ELSE 0 END) as totalHiding 
    FROM compliance
  `;
  const result = await w.db.query(sql);
  metricsCache = result[0] as { totalAudited: number, totalHiding: number };
  return metricsCache;
}
