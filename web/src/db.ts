import { getSharedWorker } from './worker.js';
import { CPT_CATALOG, PLAIN_TO_CODE } from './cpt_catalog.js';
export { CPT_CATALOG, CPT_CATEGORIES, CODE_TO_PLAIN } from './cpt_catalog.js';
import { DB_URL } from './config';

const cache: Map<string, any[]> = new Map();

type FallbackReason =
  | 'national_scope'
  | 'zip_relaxed_national'
  | 'national_text_match'
  | 'category_fallback';

const AUDIT_NODES: Record<string, string> = {
  'SURGERY': '27447',
  'IMAGING': '70551',
  'LABS': '80053',
  'EMERGENCY': '99283',
  'MATERNITY': '59400',
  'GENERAL': '12001',
  'COLON': '45378',
  'CARDIAC': '99285',
  'XRAY': '71045',
  'CT': '74177',
  'HIP': '27130'
};

const CATEGORY_FALLBACK: Record<string, string> = {
  'Emergency': AUDIT_NODES.EMERGENCY,
  'Imaging': AUDIT_NODES.IMAGING,
  'Lab Work': AUDIT_NODES.LABS,
  'Surgery': AUDIT_NODES.SURGERY,
  'Maternity': AUDIT_NODES.MATERNITY,
  'Cardiology': AUDIT_NODES.CARDIAC,
  'Mental Health': '90791',
  'Physical Therapy': AUDIT_NODES.GENERAL,
  'Preventive': '99213',
  'Sleep': '95810'
};

const BASE_MAPPING: Record<string, string> = {
  'knee replacement': '27447',
  'hip replacement': '27130',
  'mri': '70551',
  'brain mri': '70551',
  'mri brain': '70551',
  'ct scan': '74177',
  'cat scan': '74177',
  'xray': '71045',
  'x-ray': '71045',
  'chest x-ray': '71045',
  'blood work': '80053',
  'metabolic panel': '80053',
  'cmp': '80053',
  'colonoscopy': '45378',
  'emergency': '99283',
  'er': '99283',
  'er visit': '99283',
  'childbirth': '59400',
  'birth': '59400',
  'labor': '59400',
  'stitches': '12001',
  'wound': '12001',
  'flu shot': '90686',
  'flu vaccine': '90686',
  'influenza vaccine': '90686',
  'shot': '96372',
  'injection': '96372'
};

CPT_CATALOG.forEach(entry => {
    if (!BASE_MAPPING[entry.code]) {
    BASE_MAPPING[entry.code] = entry.code;
    }
});

export const SMART_MAPPING: Record<string, string> = {
  ...PLAIN_TO_CODE,
  ...BASE_MAPPING,
};

function markFallback(rows: any[], reason: FallbackReason, label: string): any[] {
  return rows.map((r) => ({ ...r, isFallback: true, fallbackReason: reason, fallbackLabel: label }));
}

function normalizeQuery(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function resolveMappedCode(rawQuery: string): string {
  const norm = normalizeQuery(rawQuery);
  if (!norm) return '';

  const direct = SMART_MAPPING[norm] || BASE_MAPPING[norm];
  if (direct) return direct;

  if (/^[a-z]?\d{4,5}$/i.test(norm)) {
    return norm.toUpperCase();
  }

  const catalogMatch = CPT_CATALOG.find(entry => {
    const plain = entry.plain.toLowerCase();
    const technical = entry.technical.toLowerCase();
    return plain === norm || technical === norm || norm === entry.code;
  });
  if (catalogMatch) return catalogMatch.code;

  const semanticMatch = Object.entries(SMART_MAPPING)
    .filter(([key]) => key.length > 3 && (norm.includes(key) || key.includes(norm)))
    .sort((a, b) => b[0].length - a[0].length)[0];
  if (semanticMatch) return semanticMatch[1];

  return norm;
}

export function getRecommendations(query: string) {
  const norm = query.toLowerCase().trim();
  if (norm.length < 1) return [];
  
  const results: { query: string; code: string; plain: string }[] = [];
  const seenCodes = new Set<string>();

  const entries = Object.entries(SMART_MAPPING)
    .filter(([key]) => key.toLowerCase().includes(norm))
    .sort((a, b) => a[0].localeCompare(b[0]));

  for (const [key, code] of entries) {
    if (!seenCodes.has(code)) {
      seenCodes.add(code);
      const entry = CPT_CATALOG.find(e => e.code === code);
      results.push({ query: key, code, plain: entry?.plain || key });
      if (results.length >= 8) break;
    }
  }
  return results;
}

function buildQuery(query: string, state: string = '', zip: string = '') {
  const norm = normalizeQuery(query);
  let mappedCpt = resolveMappedCode(norm);
  
  let sql = `
    SELECT 
      p.*, h.ccn, h.website, h.zip_code,
      COALESCE(h.city, p.hospital_name) as city,
      h.state as state,
      COALESCE(c.score, 0) as score
    FROM prices p
    LEFT JOIN hospitals h ON h.ccn = p.ein
    LEFT JOIN compliance c ON c.ccn = h.ccn
    WHERE p.cash_price IS NOT NULL
      AND p.cash_price > 0
  `;
  const params: any[] = [];

  if (mappedCpt.length > 0) {
    if (/^[A-Z]?\d{4,5}$/i.test(mappedCpt)) {
      sql += ` AND p.cpt_code = ?`;
      params.push(mappedCpt);
    } else {
      sql += ` AND (p.description LIKE ? OR p.cpt_code LIKE ?)`;
      params.push(`%${mappedCpt}%`, `%${mappedCpt}%`);
    }
  }

  if (state) {
    sql += ` AND h.state = ?`;
    params.push(state.toUpperCase());
  }

  if (zip) {
    const zipPrefix = zip.substring(0, 3);
    if (zipPrefix.length === 3) {
      sql += ` AND h.zip_code LIKE ?`;
      params.push(`${zipPrefix}%`);
    }
  }

  sql += ` ORDER BY p.cash_price ASC LIMIT 100`;
  return { sql, params, mapped: mappedCpt };
}

export async function searchPrices(query: string, state: string = '', zip: string = '') {
  const cacheKey = `${query}:${state}:${zip}`;
  if (cache.has(cacheKey)) return cache.get(cacheKey)!;

  const w = await getSharedWorker(DB_URL);
  const normQuery = normalizeQuery(query);
  let results: any[] = [];

  const queryObj = buildQuery(normQuery, state, zip);
  results = await w.db.query(queryObj.sql, queryObj.params) as any[];

  // Local fuzzy fallback: allow semantic text matching in-state before widening scope
  if (results.length === 0 && state) {
    const localFuzzy = await w.db.query(
      `SELECT 
         p.*, h.ccn, h.website, h.zip_code,
         COALESCE(h.city, p.hospital_name) as city,
         h.state as state,
         COALESCE(c.score, 0) as score
       FROM prices p
       LEFT JOIN hospitals h ON h.ccn = p.ein
       LEFT JOIN compliance c ON c.ccn = h.ccn
       WHERE p.cash_price IS NOT NULL
         AND p.cash_price > 0
         AND h.state = ?
         AND LOWER(p.description) LIKE ?
       ORDER BY p.cash_price ASC
       LIMIT 100`,
      [state.toUpperCase(), `%${normQuery}%`]
    ) as any[];

    if (localFuzzy.length > 0) {
      results = localFuzzy;
    }
  }

  if (results.length === 0 && (state || zip)) {
    const national = buildQuery(normQuery, '', zip);
    results = await w.db.query(national.sql, national.params) as any[];
    if (results.length > 0) {
      results = markFallback(results, 'national_scope', 'National Result');
    }
  }

  if (results.length === 0 && zip) {
    const nationalNoZip = buildQuery(normQuery, '', '');
    results = await w.db.query(nationalNoZip.sql, nationalNoZip.params) as any[];
    if (results.length > 0) {
      results = markFallback(results, 'zip_relaxed_national', 'National (ZIP Relaxed)');
    }
  }

  if (results.length === 0) {
    const nationalText = await w.db.query(
      `SELECT 
         p.*, h.ccn, h.website, h.zip_code,
         COALESCE(h.city, p.hospital_name) as city,
         h.state as state,
         COALESCE(c.score, 0) as score
       FROM prices p
       LEFT JOIN hospitals h ON h.ccn = p.ein
       LEFT JOIN compliance c ON c.ccn = h.ccn
       WHERE p.cash_price IS NOT NULL
         AND p.cash_price > 0
         AND LOWER(p.description) LIKE ?
       ORDER BY p.cash_price ASC
       LIMIT 100`,
      [`%${normQuery}%`]
    ) as any[];
    if (nationalText.length > 0) {
      results = markFallback(nationalText, 'national_text_match', 'National Text Match');
    }
  }

  if (results.length === 0) {
      const entry = CPT_CATALOG.find(e => {
        const plain = e.plain.toLowerCase();
        const technical = e.technical.toLowerCase();
        return e.code === normQuery || plain === normQuery || plain.includes(normQuery) || technical.includes(normQuery);
      });
      if (entry) {
          const fallbackCode = CATEGORY_FALLBACK[entry.category] || AUDIT_NODES.GENERAL;
            const catQuery = buildQuery(fallbackCode, '', zip);
          results = await w.db.query(catQuery.sql, catQuery.params) as any[];
          if (results.length > 0) {
            results = markFallback(results, 'category_fallback', 'Category Fallback');
          }
      }
  }

  // No generic baseline fallback: better to return no result than unrelated procedures.
  
  cache.set(cacheKey, results); 
  return results;
}

export async function getMarketRates(cptCode: string, zipPrefix: string = ''): Promise<any> {
  const w = await getSharedWorker(DB_URL);
  const norm = normalizeQuery(cptCode);
  let mappedCpt = resolveMappedCode(norm);
  if (!/^[A-Z]?\d{4,5}$/i.test(mappedCpt)) mappedCpt = AUDIT_NODES.LABS;

  let sql = `
    SELECT 
      MIN(cash_price) as min,
      AVG(cash_price) as median,
      MAX(cash_price) as max
    FROM prices p
    LEFT JOIN hospitals h ON h.ccn = p.ein
    WHERE p.cpt_code = ?
  `;
  const params: any[] = [mappedCpt];
  if (zipPrefix) {
    sql += ` AND h.zip_code LIKE ?`;
    params.push(`${zipPrefix.substring(0, 3)}%`);
  }

  const res = await w.db.query(sql, params) as any[];
  if (!res || res.length === 0 || res[0].min === null) {
    return null;
  }
  return {
    cpt_code: mappedCpt,
    market_zip: zipPrefix,
    cash_rate: {
      min: res[0].min,
      median: Math.round(res[0].median),
      max: res[0].max
    }
  };
}
