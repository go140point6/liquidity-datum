const fs = require("fs");
const path = require("path");

function initSchema(db) {
  const schemaPath = path.join(__dirname, "schema.sql");
  const sql = fs.readFileSync(schemaPath, "utf8");
  db.exec(sql);

  const addColumnIfMissing = (table, column, colDef) => {
    const cols = db.prepare(`PRAGMA table_info(${table})`).all().map((r) => r.name);
    if (cols.includes(column)) return;
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${colDef}`);
  };

  addColumnIfMissing("loan_nft_transfers", "block_timestamp", "INTEGER");
  addColumnIfMissing("redemption_events", "block_timestamp", "INTEGER");
  addColumnIfMissing("trove_events", "block_timestamp", "INTEGER");
  addColumnIfMissing("sp_deposit_ops", "block_timestamp", "INTEGER");
  addColumnIfMissing("sp_deposit_updates", "block_timestamp", "INTEGER");
}

module.exports = { initSchema };
