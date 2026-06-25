const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");
const log = require("./logger");

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !v.trim()) {
    log.error(`[db] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v.trim();
}

function hasTable(db, tableName) {
  const row = db
    .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1")
    .get(tableName);
  return !!row;
}

function normalizeKnownCollateralMetadata(db) {
  try {
    if (hasTable(db, "loan_contracts")) {
      db.prepare(
        `
        UPDATE loan_contracts
        SET coll_symbol = 'STXRP',
            coll_decimals = 6,
            updated_at = datetime('now')
        WHERE lower(contract_key) LIKE '%stxrp%'
          AND (coll_symbol IS NOT 'STXRP' OR coll_decimals IS NOT 6)
      `
      ).run();
    }

    if (hasTable(db, "stability_pools")) {
      db.prepare(
        `
        UPDATE stability_pools
        SET coll_symbol = 'STXRP',
            coll_decimals = 6,
            updated_at = datetime('now')
        WHERE lower(pool_key) LIKE '%stxrp%'
          AND (coll_symbol IS NOT 'STXRP' OR coll_decimals IS NOT 6)
      `
      ).run();
    }
  } catch (err) {
    log.warn("[db] Failed to normalize known collateral metadata:", err.message || err);
  }
}

function openDatumDb() {
  const datumPath = requireEnv("DATUM_DB_PATH");
  const sentinelPath = requireEnv("SENTINEL_DB_PATH");

  const dir = path.dirname(datumPath);
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch (err) {
    log.error("[db] Failed to create datum DB directory:", err.message || err);
    process.exit(1);
  }

  try {
    fs.accessSync(dir, fs.constants.W_OK);
  } catch (err) {
    log.error(`[db] Datum DB directory not writable: ${dir}`);
    process.exit(1);
  }

  if (fs.existsSync(datumPath)) {
    try {
      fs.accessSync(datumPath, fs.constants.W_OK);
    } catch (err) {
      try {
        fs.chmodSync(datumPath, 0o664);
      } catch (chmodErr) {
        log.error(`[db] Datum DB file not writable and chmod failed: ${datumPath}`);
        process.exit(1);
      }
    }
  }

  const db = new Database(datumPath, { fileMustExist: false, readonly: false });
  db.pragma("foreign_keys = ON");
  db.exec("PRAGMA main.query_only = OFF;");
  try {
    db.exec("CREATE TABLE IF NOT EXISTS __datum_write_test(id INTEGER);");
    db.exec("DELETE FROM __datum_write_test;");
  } catch (err) {
    log.error("[db] Datum DB is not writable:", err.message || err);
    process.exit(1);
  }

  // Attach Sentinel DB (read-only by convention; we do not write to it).
  try {
    const safePath = sentinelPath.replace(/'/g, "''");
    db.exec(`ATTACH DATABASE '${safePath}' AS sentinel;`);
  } catch (err) {
    log.error("[db] Failed to ATTACH sentinel DB:", err.message || err);
    process.exit(1);
  }

  normalizeKnownCollateralMetadata(db);

  return db;
}

module.exports = { openDatumDb };
