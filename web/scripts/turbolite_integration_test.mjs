import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Database } from 'turbolite';

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'healthspend-turbolite-'));
const dbPath = path.join(tempDir, 'turbolite-integration.db');

try {
  const db = new Database(dbPath);
  db.exec(`
    CREATE TABLE IF NOT EXISTS smoke (id INTEGER PRIMARY KEY, name TEXT);
    DELETE FROM smoke;
    INSERT INTO smoke (id, name) VALUES (1, 'ok');
  `);

  const rows = db.query('SELECT id, name FROM smoke ORDER BY id ASC');
  assert.ok(Array.isArray(rows), 'rows should be an array');
  assert.equal(rows.length, 1, 'expected one row in smoke table');
  assert.equal(rows[0].id, 1, 'expected id=1');
  assert.equal(rows[0].name, 'ok', 'expected name=ok');

  db.close();
  console.log('turbolite integration test passed');
} finally {
  try {
    fs.rmSync(tempDir, { recursive: true, force: true });
  } catch {
    // noop cleanup fallback
  }
}
