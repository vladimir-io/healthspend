import { getSharedWorker } from './worker.js';
import { CPT_CATALOG, PLAIN_TO_CODE } from './cpt_catalog.js';
export { CPT_CATALOG, CPT_CATEGORIES, CODE_TO_PLAIN } from './cpt_catalog.js';
import { DB_URL, NPI_CONFIDENCE_THRESHOLD } from './config';

const DEFAULT_CONFIDENCE_THRESHOLD = NPI_CONFIDENCE_THRESHOLD;

const cache: Map<string, SearchResponse> = new Map();
let pricesHasAttributionConfidence: boolean | null = null;

type SearchSort = 'price-asc' | 'price-desc' | 'score-desc';

export type SearchResponse = {
  rows: any[];
  total: number;
  truncated: boolean;
  cap: number;
  dataQualityIssue: 'missing_attribution_confidence' | 'missing_attribution_confidence_relaxed' | null;
  market: {
    min: number;
    median: number;
    p10: number;
    p90: number;
    max: number;
  } | null;
};

const DEFAULT_SEARCH_PAGE_SIZE = 100;

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
  'severe er': '99285',
  'heart attack': '99285',
  'cardiac emergency': '99285',
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

async function hasAttributionConfidenceColumn(): Promise<boolean> {
  if (pricesHasAttributionConfidence !== null) {
    return pricesHasAttributionConfidence;
  }

  try {
    const w = await getSharedWorker(DB_URL);
    const cols = await w.db.query(`PRAGMA table_info(prices)`) as any[];
    pricesHasAttributionConfidence = cols.some((c) => c && c.name === 'attribution_confidence');
  } catch (_err) {
    pricesHasAttributionConfidence = false;
  }

  return pricesHasAttributionConfidence;
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
    .filter(([key]) => {
      const k = key.toLowerCase();
      if (norm.length <= 2) {
        return k.startsWith(norm) || k.split(' ').some((w) => w.startsWith(norm));
      }
      return k.includes(norm);
    })
    .sort((a, b) => {
      const ak = a[0].toLowerCase();
      const bk = b[0].toLowerCase();

      const aStarts = ak.startsWith(norm) ? 1 : 0;
      const bStarts = bk.startsWith(norm) ? 1 : 0;
      if (aStarts !== bStarts) return bStarts - aStarts;

      const aWordStarts = ak.split(' ').some((w) => w.startsWith(norm)) ? 1 : 0;
      const bWordStarts = bk.split(' ').some((w) => w.startsWith(norm)) ? 1 : 0;
      if (aWordStarts !== bWordStarts) return bWordStarts - aWordStarts;

      return ak.localeCompare(bk);
    });

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

function resolveOrderBy(sort: SearchSort): string {
  if (sort === 'price-desc') return 'p.cash_price DESC';
  if (sort === 'score-desc') return 'score DESC, p.cash_price DESC';
  return 'p.cash_price ASC';
}

function buildQuery(query: string, state: string = '', zip: string = '', minConfidence: number = DEFAULT_CONFIDENCE_THRESHOLD, withAttributionConfidence: boolean = true, sort: SearchSort = 'price-asc', limit: number = DEFAULT_SEARCH_PAGE_SIZE, offset: number = 0) {
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

  if (withAttributionConfidence) {
    sql += ` AND COALESCE(p.attribution_confidence, 1.0) >= ?`;
    params.push(minConfidence);
  }

  if (mappedCpt.length > 0) {
    if (/^[A-Z]?\d{4,5}$/i.test(mappedCpt)) {
      sql += ` AND (p.cpt_code = ? OR p.cpt_code LIKE ? OR p.cpt_code LIKE ?)`;
      params.push(mappedCpt, `${mappedCpt}-%`, `${mappedCpt} %`);
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

  sql += ` ORDER BY ${resolveOrderBy(sort)} LIMIT ${limit} OFFSET ${offset}`;
  return { sql, params, mapped: mappedCpt };
}

async function countForQuery(w: any, sql: string, params: any[]): Promise<number> {
  const countSql = `SELECT COUNT(1) as total FROM (${sql.replace(/ORDER BY[\s\S]*$/, '')}) q`;
  const rows = await w.db.query(countSql, params) as any[];
  return Number(rows?.[0]?.total || 0);
}

async function marketForQuery(w: any, sql: string, params: any[]) {
  const baseSql = sql.replace(/ORDER BY[\s\S]*$/, '');
  const marketSql = `
    WITH filtered AS (
      ${baseSql}
    ), ranked AS (
      SELECT
        cash_price,
        ROW_NUMBER() OVER (ORDER BY cash_price) as rn,
        COUNT(1) OVER () as cnt
      FROM filtered
      WHERE cash_price IS NOT NULL
    )
    SELECT
      MIN(cash_price) as min,
      MAX(cash_price) as max,
      AVG(CASE WHEN rn IN ((cnt + 1) / 2, (cnt + 2) / 2) THEN cash_price END) as median,
      MIN(CASE WHEN rn >= CAST(CEIL(cnt * 0.10) AS INT) THEN cash_price END) as p10,
      MIN(CASE WHEN rn >= CAST(CEIL(cnt * 0.90) AS INT) THEN cash_price END) as p90
    FROM ranked
  `;
  const rows = await w.db.query(marketSql, params) as any[];
  const first = rows?.[0];
  if (!first || first.min == null || first.max == null || first.median == null || first.p10 == null || first.p90 == null) {
    return null;
  }
  return {
    min: Number(first.min),
    median: Number(first.median),
    p10: Number(first.p10),
    p90: Number(first.p90),
    max: Number(first.max),
  };
}

export async function searchPricesWithMeta(query: string, state: string = '', zip: string = '', minConfidence: number = DEFAULT_CONFIDENCE_THRESHOLD, sort: SearchSort = 'price-asc', limit: number = DEFAULT_SEARCH_PAGE_SIZE, offset: number = 0): Promise<SearchResponse> {
  const cacheKey = `${query}:${state}:${zip}:${minConfidence.toFixed(2)}:${sort}:${limit}:${offset}`;
  if (cache.has(cacheKey)) {
    return cache.get(cacheKey)!;
  }

  const w = await getSharedWorker(DB_URL);
  const withAttributionConfidence = await hasAttributionConfidenceColumn();
  const normQuery = normalizeQuery(query);
  let results: any[] = [];
  let finalSql = '';
  let finalParams: any[] = [];

  const queryObj = buildQuery(normQuery, state, zip, minConfidence, withAttributionConfidence, sort, limit, offset);
  finalSql = queryObj.sql;
  finalParams = queryObj.params;
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
         ${withAttributionConfidence ? 'AND COALESCE(p.attribution_confidence, 1.0) >= ?' : ''}
         AND h.state = ?
         AND LOWER(p.description) LIKE ?
       ORDER BY ${resolveOrderBy(sort)}
       LIMIT ${limit} OFFSET ${offset}`,
      withAttributionConfidence
        ? [minConfidence, state.toUpperCase(), `%${normQuery}%`]
        : [state.toUpperCase(), `%${normQuery}%`]
    ) as any[];

    if (localFuzzy.length > 0) {
      finalSql = `SELECT 
         p.*, h.ccn, h.website, h.zip_code,
         COALESCE(h.city, p.hospital_name) as city,
         h.state as state,
         COALESCE(c.score, 0) as score
       FROM prices p
       LEFT JOIN hospitals h ON h.ccn = p.ein
       LEFT JOIN compliance c ON c.ccn = h.ccn
       WHERE p.cash_price IS NOT NULL
         AND p.cash_price > 0
         ${withAttributionConfidence ? 'AND COALESCE(p.attribution_confidence, 1.0) >= ?' : ''}
         AND h.state = ?
         AND LOWER(p.description) LIKE ?
      ORDER BY ${resolveOrderBy(sort)}
      LIMIT ${limit} OFFSET ${offset}`;
      finalParams = withAttributionConfidence
        ? [minConfidence, state.toUpperCase(), `%${normQuery}%`]
        : [state.toUpperCase(), `%${normQuery}%`];
      results = localFuzzy;
    }
  }

  if (results.length === 0 && (state || zip)) {
    const national = buildQuery(normQuery, '', zip, minConfidence, withAttributionConfidence, sort, limit, offset);
    results = await w.db.query(national.sql, national.params) as any[];
    if (results.length > 0) {
      finalSql = national.sql;
      finalParams = national.params;
      results = markFallback(results, 'national_scope', 'National Result');
    }
  }

  if (results.length === 0 && zip) {
    const nationalNoZip = buildQuery(normQuery, '', '', minConfidence, withAttributionConfidence, sort, limit, offset);
    results = await w.db.query(nationalNoZip.sql, nationalNoZip.params) as any[];
    if (results.length > 0) {
      finalSql = nationalNoZip.sql;
      finalParams = nationalNoZip.params;
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
         ${withAttributionConfidence ? 'AND COALESCE(p.attribution_confidence, 1.0) >= ?' : ''}
         AND LOWER(p.description) LIKE ?
       ORDER BY ${resolveOrderBy(sort)}
       LIMIT 100`,
      withAttributionConfidence
        ? [minConfidence, `%${normQuery}%`]
        : [`%${normQuery}%`]
    ) as any[];
    if (nationalText.length > 0) {
      finalSql = `SELECT 
         p.*, h.ccn, h.website, h.zip_code,
         COALESCE(h.city, p.hospital_name) as city,
         h.state as state,
         COALESCE(c.score, 0) as score
       FROM prices p
       LEFT JOIN hospitals h ON h.ccn = p.ein
       LEFT JOIN compliance c ON c.ccn = h.ccn
       WHERE p.cash_price IS NOT NULL
         AND p.cash_price > 0
         ${withAttributionConfidence ? 'AND COALESCE(p.attribution_confidence, 1.0) >= ?' : ''}
         AND LOWER(p.description) LIKE ?
      ORDER BY ${resolveOrderBy(sort)}
      LIMIT ${limit} OFFSET ${offset}`;
      finalParams = withAttributionConfidence
        ? [minConfidence, `%${normQuery}%`]
        : [`%${normQuery}%`];
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
            const catQuery = buildQuery(fallbackCode, '', zip, minConfidence, withAttributionConfidence, sort, limit, offset);
          results = await w.db.query(catQuery.sql, catQuery.params) as any[];
          if (results.length > 0) {
            finalSql = catQuery.sql;
            finalParams = catQuery.params;
            results = markFallback(results, 'category_fallback', 'Category Fallback');
          }
      }
  }

  // No generic baseline fallback: better to return no result than unrelated procedures.

  const total = finalSql ? await countForQuery(w, finalSql, finalParams) : results.length;
  const market = finalSql ? await marketForQuery(w, finalSql, finalParams) : null;
  const response: SearchResponse = {
    rows: results,
    total,
    truncated: total > offset + results.length,
    cap: limit,
    dataQualityIssue: withAttributionConfidence ? null : 'missing_attribution_confidence_relaxed',
    market,
  };
  cache.set(cacheKey, response);
  return response;
}

export async function searchPrices(query: string, state: string = '', zip: string = '', minConfidence: number = DEFAULT_CONFIDENCE_THRESHOLD, sort: SearchSort = 'price-asc', limit: number = DEFAULT_SEARCH_PAGE_SIZE, offset: number = 0) {
  const response = await searchPricesWithMeta(query, state, zip, minConfidence, sort, limit, offset);
  return response.rows;
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
