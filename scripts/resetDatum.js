const path = require("path");
const Database = require("better-sqlite3");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
  quiet: true,
});

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`[resetDatum] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const DB_PATH = requireEnv("DATUM_DB_PATH");

function main() {
  const db = new Database(DB_PATH);
  try {
    db.exec(`
      DELETE FROM scan_cursors;
      DELETE FROM loan_nft_transfers;
      DELETE FROM tracked_troves;
      DELETE FROM redemption_events;
      DELETE FROM trove_events;
      DELETE FROM sp_cursors;
      DELETE FROM sp_deposit_ops;
      DELETE FROM sp_deposit_updates;
    `);
    console.log("[resetDatum] Cleared scan cursors and event tables.");
  } finally {
    db.close();
  }
}

main();
