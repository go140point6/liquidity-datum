PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS loan_contracts (
  contract_key TEXT PRIMARY KEY,
  protocol TEXT NOT NULL,
  address_eip55 TEXT NOT NULL,
  default_start_block INTEGER NOT NULL,
  trove_manager_address TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scan_cursors (
  cursor_key TEXT PRIMARY KEY,
  start_block INTEGER NOT NULL DEFAULT 0,
  last_scanned_block INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS loan_nft_transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contract_key TEXT NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp INTEGER,
  tx_hash TEXT NOT NULL,
  log_index INTEGER NOT NULL,
  from_addr TEXT NOT NULL,
  to_addr TEXT NOT NULL,
  token_id TEXT NOT NULL,
  is_burned INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (contract_key, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS tracked_troves (
  contract_key TEXT NOT NULL,
  token_id TEXT NOT NULL,
  first_seen_block INTEGER,
  last_seen_block INTEGER,
  is_burned INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (contract_key, token_id)
);

CREATE TABLE IF NOT EXISTS redemption_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contract_key TEXT NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp INTEGER,
  tx_hash TEXT NOT NULL,
  log_index INTEGER NOT NULL,
  attempted_bold TEXT NOT NULL,
  actual_bold TEXT NOT NULL,
  eth_sent TEXT NOT NULL,
  eth_fee TEXT NOT NULL,
  price TEXT NOT NULL,
  redemption_price TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (contract_key, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS trove_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contract_key TEXT NOT NULL,
  event_name TEXT NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp INTEGER,
  tx_hash TEXT NOT NULL,
  log_index INTEGER NOT NULL,
  trove_id TEXT NOT NULL,
  data_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (contract_key, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS stability_pools (
  pool_key TEXT PRIMARY KEY,
  protocol TEXT NOT NULL,
  address_eip55 TEXT NOT NULL,
  default_start_block INTEGER NOT NULL,
  coll_symbol TEXT NOT NULL,
  coll_decimals INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sp_cursors (
  cursor_key TEXT PRIMARY KEY,
  start_block INTEGER NOT NULL DEFAULT 0,
  last_scanned_block INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sp_deposit_ops (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pool_key TEXT NOT NULL,
  depositor TEXT NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp INTEGER,
  tx_hash TEXT NOT NULL,
  log_index INTEGER NOT NULL,
  operation INTEGER NOT NULL,
  deposit_loss TEXT NOT NULL,
  topup_or_withdrawal TEXT NOT NULL,
  yield_gain_since TEXT NOT NULL,
  yield_gain_claimed TEXT NOT NULL,
  coll_gain_since TEXT NOT NULL,
  coll_gain_claimed TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (pool_key, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS sp_deposit_updates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pool_key TEXT NOT NULL,
  depositor TEXT NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp INTEGER,
  tx_hash TEXT NOT NULL,
  log_index INTEGER NOT NULL,
  new_deposit TEXT NOT NULL,
  stashed_coll TEXT NOT NULL,
  snapshot_p TEXT NOT NULL,
  snapshot_s TEXT NOT NULL,
  snapshot_b TEXT NOT NULL,
  snapshot_scale TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (pool_key, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_trove_events_contract_block
  ON trove_events(contract_key, block_number);
CREATE INDEX IF NOT EXISTS idx_redemption_contract_block
  ON redemption_events(contract_key, block_number);
CREATE INDEX IF NOT EXISTS idx_sp_ops_pool_block
  ON sp_deposit_ops(pool_key, block_number);
