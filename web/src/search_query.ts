/** Pure SQL builder for price search — shared by db.ts and quality tests. */

import { normalizeQuery, resolveSearch } from './search_resolve.js';

export type SearchSort = 'price-asc' | 'price-desc' | 'score-desc';

const DEFAULT_SEARCH_PAGE_SIZE = 100;

export function toFtsMatchFromTokens(phrase: string): string {
  const norm = phrase
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const parts = norm.split(' ').filter((t) => t.length > 0);
  if (parts.length === 0) return '';
  return parts
    .map((t) => {
      const esc = t.replace(/"/g, '""');
      return `"${esc}"*`;
    })
    .join(' AND ');
}

export function resolveOrderBy(sort: SearchSort): string {
  if (sort === 'price-desc') return 'p.cash_price DESC';
  if (sort === 'score-desc') return 'score DESC, p.cash_price DESC';
  return 'p.cash_price ASC';
}

export function buildSearchQuery(
  query: string,
  state: string = '',
  zip: string = '',
  minConfidence: number = 0.95,
  withAttributionConfidence: boolean = true,
  sort: SearchSort = 'price-asc',
  limit: number = DEFAULT_SEARCH_PAGE_SIZE,
  offset: number = 0,
  useFts: boolean = false
): { sql: string; params: unknown[]; mapped: string; usedFts: boolean } {
  const norm = normalizeQuery(query);
  const resolved = resolveSearch(norm);
  const mappedCpts = resolved.cpts;
  const mappedCpt = mappedCpts[0] ?? '';
  const isNumericCpt =
    mappedCpts.length > 0 && mappedCpts.every((c) => /^[A-Z]?\d{4,5}$/i.test(c));
  const ftsQ =
    useFts && !isNumericCpt && mappedCpt.length > 0
      ? toFtsMatchFromTokens(mappedCpt) || toFtsMatchFromTokens(norm)
      : '';
  const usedFts = Boolean(ftsQ);

  const baseFrom = usedFts
    ? `FROM prices p
    INNER JOIN prices_fts fts ON fts.rowid = p.rowid
    LEFT JOIN hospitals h ON h.ccn = p.ein
    LEFT JOIN compliance c ON c.ccn = h.ccn`
    : `FROM prices p
    LEFT JOIN hospitals h ON h.ccn = p.ein
    LEFT JOIN compliance c ON c.ccn = h.ccn`;

  let sql = `
    SELECT
      p.*, h.ccn, h.website, h.zip_code,
      COALESCE(h.city, p.hospital_name) as city,
      h.state as state,
      COALESCE(c.score, 0) as score
    ${baseFrom}
    WHERE p.cash_price IS NOT NULL
      AND p.cash_price > 0
  `;
  const params: unknown[] = [];

  if (withAttributionConfidence) {
    sql += ` AND COALESCE(p.attribution_confidence, 1.0) >= ?`;
    params.push(minConfidence);
  }

  if (mappedCpts.length > 0) {
    if (isNumericCpt) {
      if (mappedCpts.length === 1) {
        sql += ` AND (p.cpt_code = ? OR p.cpt_code LIKE ? OR p.cpt_code LIKE ?)`;
        params.push(mappedCpts[0], `${mappedCpts[0]}-%`, `${mappedCpts[0]} %`);
      } else {
        sql += ` AND p.cpt_code IN (${mappedCpts.map(() => '?').join(', ')})`;
        params.push(...mappedCpts);
      }
    } else if (usedFts && ftsQ) {
      sql += ` AND (prices_fts MATCH ? OR p.cpt_code LIKE ?)`;
      params.push(ftsQ, `%${mappedCpt}%`);
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
  return { sql, params, mapped: resolved.displayLabel || mappedCpt, usedFts };
}

export function countSqlFromSearchQuery(sql: string): string {
  return `SELECT COUNT(1) as total FROM (${sql.replace(/ORDER BY[\s\S]*$/, '')}) q`;
}
