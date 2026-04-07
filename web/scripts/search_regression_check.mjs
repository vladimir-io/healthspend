import { execSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const projectRoot = join(process.cwd(), '..');
const webRoot = process.cwd();
const dbPath = join(webRoot, 'public', 'audit_data.db');
const dbSource = readFileSync(join(webRoot, 'src', 'db.ts'), 'utf8');

const failures = [];
const warnings = [];
const infos = [];

function assert(condition, message) {
  if (!condition) failures.push(message);
}

function warn(condition, message) {
  if (!condition) warnings.push(message);
}

function info(message) {
  infos.push(message);
}

function sqlScalar(query) {
  const cmd = `sqlite3 "${dbPath}" "${query.replace(/"/g, '""')}"`;
  const out = execSync(cmd, { encoding: 'utf8' }).trim();
  return out;
}

function asInt(v) {
  const parsed = Number.parseInt(v, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

console.log('Running search regression checks...');

// Logic invariants (deterministic, source-level)
assert(dbSource.includes('BASE_MAPPING[entry.code] = entry.code;'), 'CPT self-mapping invariant missing');
assert(dbSource.includes('No generic baseline fallback'), 'Generic baseline fallback guard missing');
assert(dbSource.includes('h.zip_code LIKE ?'), 'ZIP regional filter invariant missing');
assert(dbSource.includes('fallbackReason'), 'Fallback reason metadata invariant missing');

// Data invariants (deterministic against current DB snapshot)
const negativeOrZero = asInt(sqlScalar('SELECT COUNT(*) FROM prices WHERE cash_price IS NULL OR cash_price <= 0;'));
assert(negativeOrZero === 0, `Invalid price rows present (<=0 or NULL): ${negativeOrZero}`);

const totalRows = asInt(sqlScalar('SELECT COUNT(*) FROM prices;'));
const matchedRows = asInt(sqlScalar('SELECT SUM(CASE WHEN h.ccn IS NOT NULL THEN 1 ELSE 0 END) FROM prices p LEFT JOIN hospitals h ON h.ccn = p.ein;'));
assert(totalRows > 0, 'No price rows in database');
assert(matchedRows === totalRows, `Hospital join mismatch: ${matchedRows}/${totalRows} matched`);

// Scenario matrix (warnings only; these can legitimately vary by data refresh)
const hasFluCptGlobal = asInt(sqlScalar("SELECT COUNT(*) FROM prices WHERE cpt_code='90686'")) > 0;
if (!hasFluCptGlobal) {
  info('Global CPT 90686 coverage is 0 in current snapshot; flu-shot local checks are informational only.');
}

const scenarios = [
  { name: 'flu shot NJ local', query: "SELECT COUNT(*) FROM prices p LEFT JOIN hospitals h ON h.ccn=p.ein WHERE p.cpt_code='90686' AND h.state='NJ'", requireRows: hasFluCptGlobal },
  { name: 'ct scan NJ local', query: "SELECT COUNT(*) FROM prices p LEFT JOIN hospitals h ON h.ccn=p.ein WHERE p.cpt_code='74177' AND h.state='NJ'" },
  { name: 'brain mri national', query: "SELECT COUNT(*) FROM prices WHERE cpt_code='70551'" },
  { name: 'metabolic panel national', query: "SELECT COUNT(*) FROM prices WHERE cpt_code='80053'" },
];

for (const scenario of scenarios) {
  const n = asInt(sqlScalar(scenario.query));
  const requireRows = scenario.requireRows !== false;
  if (requireRows) {
    warn(n > 0, `Scenario has no rows: ${scenario.name}`);
  } else if (n === 0) {
    info(`Scenario skipped as expected (dataset coverage): ${scenario.name}`);
  }
  console.log(`  ${scenario.name}: ${n}`);
}

if (warnings.length > 0) {
  console.log('\nWarnings:');
  warnings.forEach((w) => console.log(`  - ${w}`));
}

if (infos.length > 0) {
  console.log('\nInfo:');
  infos.forEach((m) => console.log(`  - ${m}`));
}

if (failures.length > 0) {
  console.error('\nFAILURES:');
  failures.forEach((f) => console.error(`  - ${f}`));
  process.exit(1);
}

console.log('\n✅ Search regression checks passed');
