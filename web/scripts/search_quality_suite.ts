#!/usr/bin/env npx tsx
/**
 * Execute search quality cases against audit_data.db using the same resolver + SQL
 * builder as production. Fails CI when mappings or coverage regress.
 */
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import sqlite3 from 'sqlite3';
import { buildSearchQuery, countSqlFromSearchQuery } from '../src/search_query.js';
import { SEARCH_QUALITY_CASES } from '../src/search_quality_cases.js';
import { extractZipFromQuery, resolveSearch } from '../src/search_resolve.js';

const webRoot = join(import.meta.dirname, '..');
const dbPath = join(webRoot, 'public', 'audit_data.db');
const minConfidence = 0.95;

function sqliteGet(sql: string, params: unknown[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(dbPath);
    db.get(sql, params, (err, row: { total?: number } | undefined) => {
      db.close();
      if (err) reject(err);
      else resolve(Number(row?.total ?? 0));
    });
  });
}

async function runCase(query: string, state: string, zip: string): Promise<number> {
  const { sql, params, usedFts } = buildSearchQuery(
    query,
    state,
    zip,
    minConfidence,
    true,
    'price-asc',
    1,
    0,
    false
  );
  if (usedFts) {
    const fts = buildSearchQuery(query, state, zip, minConfidence, true, 'price-asc', 1, 0, true);
    const ftsCount = await sqliteGet(countSqlFromSearchQuery(fts.sql), fts.params);
    if (ftsCount > 0) return ftsCount;
  }
  return sqliteGet(countSqlFromSearchQuery(sql), params);
}

async function main(): Promise<number> {
  if (!existsSync(dbPath)) {
    console.error(`Missing ${dbPath} — download audit_data.db before running search quality suite.`);
    return 1;
  }

  console.log(`Search quality suite — ${SEARCH_QUALITY_CASES.length} cases — ${dbPath}\n`);
  const failures: string[] = [];

  for (const c of SEARCH_QUALITY_CASES) {
    const state = c.state ?? '';
    const { zip, cleanQuery } = extractZipFromQuery(c.query);
    const effectiveZip = c.zip ?? zip;
    const effectiveQuery = cleanQuery || c.query;

    if (c.forbidZipParse && effectiveZip) {
      failures.push(`${c.name}: CPT/ZIP collision — parsed ZIP ${effectiveZip} from "${c.query}"`);
      continue;
    }

    const resolved = resolveSearch(effectiveQuery);
    if (c.expectCpts?.length) {
      const missing = c.expectCpts.filter((code) => !resolved.cpts.includes(code));
      if (missing.length) {
        failures.push(
          `${c.name}: expected CPT(s) [${c.expectCpts.join(', ')}] got [${resolved.cpts.join(', ')}]`
        );
      }
    }
    if (c.labelIncludes && !resolved.displayLabel.toLowerCase().includes(c.labelIncludes.toLowerCase())) {
      failures.push(`${c.name}: label "${resolved.displayLabel}" missing "${c.labelIncludes}"`);
    }

    try {
      const count = await runCase(effectiveQuery, state, effectiveZip);
      const ok = count >= c.minRows;
      console.log(`  ${ok ? '✓' : '✗'} ${c.name}: ${count} rows (min ${c.minRows})`);
      if (!ok) failures.push(`${c.name}: ${count} rows < min ${c.minRows}`);
    } catch (err) {
      failures.push(`${c.name}: query error — ${err instanceof Error ? err.message : String(err)}`);
      console.log(`  ✗ ${c.name}: ERROR`);
    }
  }

  if (failures.length) {
    console.error('\nFAILURES:');
    failures.forEach((f) => console.error(`  - ${f}`));
    return 1;
  }

  console.log('\n✅ Search quality suite passed');
  return 0;
}

main().then((code) => process.exit(code));
