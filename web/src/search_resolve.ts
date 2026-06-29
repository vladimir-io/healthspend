/**
 * Search query resolution — CPT mapping, multi-intent bundles, ZIP disambiguation.
 * Single source of truth for search quality tests and the live search path.
 */

import { CODE_TO_PLAIN, CPT_CATALOG, PLAIN_TO_CODE } from './cpt_catalog.js';

/** Top procedures in audit_hot.db — keep aligned with scripts/build_hot_db.py */
export const HOT_CPT_CODES = new Set([
  '27447', '27130', '70551', '73721', '74177', '70450', '71045', '71250',
  '80053', '85025', '45378', '45380', '99283', '99285', '59400', '12001',
  '90686', '96372', '99213', '76700',
]);

export type ResolvedSearch = {
  cpts: string[];
  displayLabel: string;
  intentKey: string;
};

const AUDIT_NODES: Record<string, string> = {
  SURGERY: '27447',
  IMAGING: '70551',
  LABS: '80053',
  EMERGENCY: '99283',
  MATERNITY: '59400',
  GENERAL: '12001',
  COLON: '45378',
  CARDIAC: '99285',
  XRAY: '71045',
  CT: '74177',
  HIP: '27130',
};

export const CATEGORY_FALLBACK: Record<string, string> = {
  Emergency: AUDIT_NODES.EMERGENCY,
  Imaging: AUDIT_NODES.IMAGING,
  'Lab Work': AUDIT_NODES.LABS,
  Surgery: AUDIT_NODES.SURGERY,
  Maternity: AUDIT_NODES.MATERNITY,
  Cardiology: AUDIT_NODES.CARDIAC,
  'Mental Health': '90791',
  'Physical Therapy': AUDIT_NODES.GENERAL,
  Preventive: '99213',
  Sleep: '95810',
};

const BASE_MAPPING: Record<string, string> = {
  'knee replacement': '27447',
  knee: '27447',
  'hip replacement': '27130',
  hip: '27130',
  mri: '70551',
  'brain mri': '70551',
  'mri brain': '70551',
  'ct scan': '74177',
  'cat scan': '74177',
  xray: '71045',
  'x-ray': '71045',
  'chest x-ray': '71045',
  'blood work': '80053',
  'metabolic panel': '80053',
  cmp: '80053',
  colonoscopy: '45378',
  emergency: '99283',
  er: '99283',
  'er visit': '99283',
  'emergency room': '99283',
  'severe er': '99285',
  'heart attack': '99285',
  'cardiac emergency': '99285',
  'chest pain': '99284',
  childbirth: '59400',
  birth: '59400',
  labor: '59400',
  stitches: '12001',
  wound: '12001',
  'flu shot': '90686',
  'flu vaccine': '90686',
  'influenza vaccine': '90686',
  shot: '96372',
  injection: '96372',
};

CPT_CATALOG.forEach((entry) => {
  if (!BASE_MAPPING[entry.code]) {
    BASE_MAPPING[entry.code] = entry.code;
  }
  if (!BASE_MAPPING[entry.plain.toLowerCase()]) {
    BASE_MAPPING[entry.plain.toLowerCase()] = entry.code;
  }
});

/** Multi-CPT clinical intents (OR search — broader hospital coverage). */
export const INTENT_MULTI: Record<string, string[]> = {
  'heart attack': ['99285', '99284', '99283', '93000', '93306', '71045'],
  'cardiac emergency': ['99285', '99284', '93000', '93306'],
  'chest pain': ['99285', '99284', '71045', '93000', '93306'],
};

export const INTENT_LABELS: Record<string, string> = {
  'heart attack': 'Heart attack — ER & cardiac workup',
  'cardiac emergency': 'Cardiac emergency — ER & cardiac workup',
  'chest pain': 'Chest pain — ER & cardiac workup',
};

export const SMART_MAPPING: Record<string, string> = {
  ...PLAIN_TO_CODE,
  ...BASE_MAPPING,
};

const KNOWN_CPT_CODES = new Set([
  ...CPT_CATALOG.map((e) => e.code),
  ...HOT_CPT_CODES,
]);

export function normalizeQuery(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function isKnownCptCode(token: string): boolean {
  return KNOWN_CPT_CODES.has(token) || /^\d{5}$/.test(token);
}

/** Prevent CPT codes (e.g. 99285) from being parsed as ZIP codes. */
export function extractZipFromQuery(query: string): { zip: string; cleanQuery: string } {
  const trimmed = query.trim();
  const zipMatch = trimmed.match(/\b(\d{5})\b/);
  if (!zipMatch) {
    return { zip: '', cleanQuery: trimmed };
  }

  const candidate = zipMatch[1];
  const withoutZip = trimmed.replace(/\b\d{5}\b/, ' ').replace(/\s+/g, ' ').trim();

  if (withoutZip.length === 0 && isKnownCptCode(candidate)) {
    return { zip: '', cleanQuery: candidate };
  }

  const resolved = resolveSearch(withoutZip || candidate);
  if (
    withoutZip.length === 0 &&
    resolved.cpts.length === 1 &&
    resolved.cpts[0] === candidate
  ) {
    return { zip: '', cleanQuery: candidate };
  }

  if (withoutZip.length > 0 && isKnownCptCode(candidate) && resolved.cpts.length > 0) {
    return { zip: '', cleanQuery: trimmed };
  }

  return { zip: candidate, cleanQuery: withoutZip };
}

function resolveSingleMappedCode(norm: string): string {
  if (!norm) return '';

  const direct = SMART_MAPPING[norm] || BASE_MAPPING[norm];
  if (direct) return direct;

  if (/^[a-z]?\d{4,5}$/i.test(norm)) {
    return norm.toUpperCase();
  }

  const catalogMatch = CPT_CATALOG.find((entry) => {
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

export function resolveSearch(rawQuery: string): ResolvedSearch {
  const norm = normalizeQuery(rawQuery);
  if (!norm) {
    return { cpts: [], displayLabel: '', intentKey: '' };
  }

  if (INTENT_MULTI[norm]) {
    return {
      cpts: INTENT_MULTI[norm],
      displayLabel: INTENT_LABELS[norm] || norm,
      intentKey: norm,
    };
  }

  const code = resolveSingleMappedCode(norm);
  const isCpt = /^[A-Z]?\d{4,5}$/i.test(code);
  const displayLabel = isCpt ? CODE_TO_PLAIN[code] || `CPT ${code}` : code;

  return {
    cpts: isCpt || code ? [code] : [],
    displayLabel,
    intentKey: norm,
  };
}

/** @deprecated use resolveSearch — returns primary CPT for legacy callers */
export function resolveMappedCode(rawQuery: string): string {
  const { cpts } = resolveSearch(rawQuery);
  return cpts[0] ?? '';
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
      const entry = CPT_CATALOG.find((e) => e.code === code);
      results.push({ query: key, code, plain: entry?.plain || key });
      if (results.length >= 8) break;
    }
  }
  return results;
}

export { AUDIT_NODES };
