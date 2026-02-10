const path = require("path");
const Database = require("better-sqlite3");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
  quiet: true,
});

const logger = require("../utils/logger");

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    logger.error(`[checkDB] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const DB_PATH = requireEnv("DATUM_DB_PATH");
const LIMIT = parseInt(process.env.CHECKDB_LIMIT || "25", 10);
const LARGE_TABLE_ROWS = parseInt(process.env.CHECKDB_LARGE || "50", 10);

function pickOrderColumn(colNames) {
  const recencyCols = [
    "id",
    "block_number",
    "created_at",
    "updated_at",
    "snapshot_at",
    "timestamp",
  ];
  return recencyCols.find((c) => colNames.includes(c)) || null;
}

const db = new Database(DB_PATH, { fileMustExist: true });
db.pragma("foreign_keys = ON");

logger.info(`[checkDB] Reading tables from ${DB_PATH}`);
logger.info(`[checkDB] LIMIT=${LIMIT}, CHECKDB_LARGE=${LARGE_TABLE_ROWS}\n`);

try {
  const tables = db
    .prepare(
      `
      SELECT name
      FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `
    )
    .all()
    .map((r) => r.name);

  if (tables.length === 0) {
    logger.info("[checkDB] No user tables found.");
  }

  for (const table of tables) {
    try {
      const columns = db.prepare(`PRAGMA table_info("${table}")`).all();
      const colNames = columns.map((c) => c.name);
      const orderCol = pickOrderColumn(colNames);

      const { cnt } = db
        .prepare(`SELECT COUNT(*) AS cnt FROM "${table}"`)
        .get();

      logger.info(`\n[checkDB] Table: ${table} — total rows: ${cnt}`);

      let query;
      let params;
      if (orderCol) {
        query = `
          SELECT * FROM (
            SELECT * FROM "${table}"
            ORDER BY "${orderCol}" DESC
            LIMIT ?
          ) sub
          ORDER BY "${orderCol}" ASC
        `;
        params = [LIMIT];
        logger.info(`[checkDB] Showing newest ${LIMIT} by "${orderCol}"`);
      } else {
        query = `SELECT * FROM "${table}" LIMIT ?`;
        params = [LIMIT];
        logger.info(`[checkDB] Showing first ${LIMIT} rows`);
      }

      const rows = db.prepare(query).all(...params);
      if (rows.length === 0) {
        logger.info("[checkDB] No data found.");
      } else {
        console.table(rows);
      }
    } catch (err) {
      logger.error(`[checkDB] Error reading table ${table}:`, err.message);
    }
  }

  logger.info("\n[checkDB] Done.");
} catch (err) {
  logger.error("[checkDB] Failed to enumerate tables:", err.message);
} finally {
  db.close();
  logger.info("[checkDB] DB closed.");
}
