const fs = require("fs");
const path = require("path");
const { ethers } = require("ethers");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
  quiet: true,
});

const { initSchema } = require("../db");
const { openDatumDb } = require("../utils/db");
const { acquireLock, releaseLock } = require("../utils/lock");
const stabilityPoolAbi = require("../abi/stabilityPool.json");
const log = require("../utils/logger");

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    log.error(`[scanStabilityPool] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const { buildProvider } = require("../utils/rpc");
const FLR_SCAN_BLOCKS = Number(requireEnv("DATUM_FLR_SCAN_BLOCKS"));
const FLR_PAUSE_MS = Number(requireEnv("DATUM_FLR_SCAN_PAUSE_MS"));
const OVERLAP_BLOCKS = Number(requireEnv("DATUM_SCAN_OVERLAP_BLOCKS"));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterMs(err) {
  const msg = String(err?.message || "");
  const m = msg.match(/retry in\s+(\d+)\s*s/i);
  if (!m) return null;
  const sec = Number(m[1]);
  return Number.isFinite(sec) && sec > 0 ? sec * 1000 : null;
}

function isRateLimitError(err) {
  const msg = String(err?.message || "").toLowerCase();
  return msg.includes("rate limit") || msg.includes("too many requests") || msg.includes("-32090");
}

async function getLogsWithRetry(provider, filter, { maxAttempts = 6 } = {}) {
  let attempt = 0;
  let backoffMs = 750;
  while (attempt < maxAttempts) {
    attempt++;
    try {
      return { ok: true, logs: await provider.getLogs(filter) };
    } catch (err) {
      const retryAfter = parseRetryAfterMs(err);
      const shouldRetry = isRateLimitError(err) || retryAfter != null;
      log.warn(`getLogs failed (attempt ${attempt}/${maxAttempts}): ${err.message}`);
      if (!shouldRetry || attempt >= maxAttempts) return { ok: false, error: err };
      await sleep(retryAfter ?? backoffMs);
      backoffMs = Math.min(backoffMs * 2, 10000);
    }
  }
  return { ok: false, error: new Error("exhausted retries") };
}

function getStableLogIndex(lg) {
  if (Number.isInteger(lg?.index) && lg.index >= 0) return lg.index;
  const li = lg?.logIndex;
  if (typeof li === "number" && Number.isInteger(li) && li >= 0) return li;
  if (typeof li === "string") {
    const n = li.startsWith("0x") ? Number.parseInt(li, 16) : Number.parseInt(li, 10);
    if (Number.isInteger(n) && n >= 0) return n;
  }
  return null;
}

async function resolveBlockTimestamps(provider, logs) {
  const uniqueBlocks = [];
  const seen = new Set();
  for (const lg of logs) {
    const bn = lg.blockNumber;
    if (!Number.isInteger(bn)) continue;
    if (seen.has(bn)) continue;
    seen.add(bn);
    uniqueBlocks.push(bn);
  }

  const map = new Map();
  await Promise.all(
    uniqueBlocks.map(async (bn) => {
      const blk = await provider.getBlock(bn);
      if (blk && Number.isInteger(blk.timestamp)) {
        map.set(bn, blk.timestamp);
      }
    })
  );
  return map;
}

function scanWindowCount(fromBlock, latestBlock, maxBlocks) {
  return Math.ceil((latestBlock - fromBlock + 1) / (maxBlocks + 1));
}

function readJson(p) {
  const raw = fs.readFileSync(p, "utf8");
  return JSON.parse(raw);
}

function openDb() {
  const db = openDatumDb();
  initSchema(db);
  return db;
}

function loadWallets(db) {
  return db
    .prepare(
      `
      SELECT address_eip55
      FROM sentinel.user_wallets
      WHERE is_enabled = 1
        AND chain_id = 'FLR'
    `
    )
    .all();
}

function ensurePool(db, pool) {
  const upsert = db.prepare(`
    INSERT INTO stability_pools (
      pool_key, protocol, address_eip55, default_start_block, coll_symbol, coll_decimals
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(pool_key) DO UPDATE SET
      protocol = excluded.protocol,
      address_eip55 = excluded.address_eip55,
      default_start_block = excluded.default_start_block,
      coll_symbol = excluded.coll_symbol,
      coll_decimals = excluded.coll_decimals,
      updated_at = datetime('now')
  `);
  upsert.run(
    pool.key,
    pool.protocol,
    ethers.getAddress(pool.address),
    pool.default_start_block,
    pool.coll_symbol,
    pool.coll_decimals
  );
}

function ensureCursor(db, cursorKey, startBlock) {
  db.prepare(
    `
    INSERT INTO sp_cursors (cursor_key, start_block, last_scanned_block)
    VALUES (?, ?, 0)
    ON CONFLICT(cursor_key) DO NOTHING
  `
  ).run(cursorKey, startBlock);
}

function updateCursor(db, cursorKey, lastBlock) {
  db.prepare(
    `
    UPDATE sp_cursors
    SET last_scanned_block = ?, updated_at = datetime('now')
    WHERE cursor_key = ?
  `
  ).run(lastBlock, cursorKey);
}

async function scanPool(db, provider, pool, wallets) {
  ensurePool(db, pool);
  const cursorKey = `sp:${pool.key}:deposit_ops`;
  ensureCursor(db, cursorKey, pool.default_start_block);

  const cursor = db
    .prepare("SELECT start_block, last_scanned_block FROM sp_cursors WHERE cursor_key = ?")
    .get(cursorKey);
  const startBlock = cursor.start_block;
  const lastScanned = cursor.last_scanned_block;
  const latestBlock = await provider.getBlockNumber();
  let fromBlock = lastScanned > 0 ? Math.max(startBlock, lastScanned - OVERLAP_BLOCKS) : startBlock;
  if (fromBlock > latestBlock) return;

  const iface = new ethers.Interface(stabilityPoolAbi);
  const depositOp = iface.getEvent("DepositOperation").topicHash;
  const depositUpdated = iface.getEvent("DepositUpdated").topicHash;
  const totalWindows = scanWindowCount(fromBlock, latestBlock, FLR_SCAN_BLOCKS);
  log.info(`\n=== FLR STABILITY_POOL ${pool.key} ===`);
  log.info(`  start_block=${startBlock} last_scanned=${lastScanned}`);
  log.info(`  latestBlock=${latestBlock}`);
  log.debug(
    `  windows=${totalWindows} window_size=${FLR_SCAN_BLOCKS} overlap=${OVERLAP_BLOCKS} pause=${FLR_PAUSE_MS}ms`
  );

  const insert = db.prepare(`
    INSERT INTO sp_deposit_ops (
      pool_key, depositor, block_number, block_timestamp, tx_hash, log_index,
      operation, deposit_loss, topup_or_withdrawal, yield_gain_since,
      yield_gain_claimed, coll_gain_since, coll_gain_claimed
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pool_key, tx_hash, log_index) DO NOTHING
  `);
  const insertUpdate = db.prepare(`
    INSERT INTO sp_deposit_updates (
      pool_key, depositor, block_number, block_timestamp, tx_hash, log_index,
      new_deposit, stashed_coll, snapshot_p, snapshot_s, snapshot_b, snapshot_scale
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pool_key, tx_hash, log_index) DO NOTHING
  `);

  let lastGoodBlock = fromBlock - 1;
  for (let b = fromBlock; b <= latestBlock; b += FLR_SCAN_BLOCKS + 1) {
    const toBlock = Math.min(b + FLR_SCAN_BLOCKS, latestBlock);
    const windowIndex = Math.floor((b - fromBlock) / (FLR_SCAN_BLOCKS + 1)) + 1;
    log.debug(`      [${windowIndex}/${totalWindows}] blocks ${b} → ${toBlock}`);
    const res = await getLogsWithRetry(provider, {
      address: ethers.getAddress(pool.address),
      fromBlock: b,
      toBlock,
      topics: [[depositOp, depositUpdated]],
    });
    if (!res.ok) break;

    const blockTsMap = await resolveBlockTimestamps(provider, res.logs);
    const items = [];
    for (const lg of res.logs) {
      const li = getStableLogIndex(lg);
      if (li == null) continue;
      const txHash = lg.transactionHash;
      if (!txHash) continue;
      const blockTimestamp = blockTsMap.get(lg.blockNumber) ?? null;

      let parsed;
      try {
        parsed = iface.parseLog({ topics: lg.topics, data: lg.data });
      } catch {
        continue;
      }

      if (parsed.name === "DepositOperation") {
        items.push({
          kind: "op",
          depositor: parsed.args._depositor,
          blockNumber: lg.blockNumber,
          blockTimestamp,
          txHash,
          logIndex: li,
          operation: parsed.args._operation.toString(),
          depositLoss: parsed.args._depositLossSinceLastOperation.toString(),
          topupOrWithdrawal: parsed.args._topUpOrWithdrawal.toString(),
          yieldGainSince: parsed.args._yieldGainSinceLastOperation.toString(),
          yieldGainClaimed: parsed.args._yieldGainClaimed.toString(),
          collGainSince: parsed.args._ethGainSinceLastOperation.toString(),
          collGainClaimed: parsed.args._ethGainClaimed.toString(),
        });
      } else if (parsed.name === "DepositUpdated") {
        items.push({
          kind: "update",
          depositor: parsed.args._depositor,
          blockNumber: lg.blockNumber,
          blockTimestamp,
          txHash,
          logIndex: li,
          newDeposit: parsed.args._newDeposit.toString(),
          stashedColl: parsed.args._stashedColl.toString(),
          snapshotP: parsed.args._snapshotP.toString(),
          snapshotS: parsed.args._snapshotS.toString(),
          snapshotB: parsed.args._snapshotB.toString(),
          snapshotScale: parsed.args._snapshotScale.toString(),
        });
      }
    }

    if (items.length) {
      const tx = db.transaction((arr) => {
        for (const it of arr) {
          if (it.kind === "op") {
            insert.run(
              pool.key,
              it.depositor,
              it.blockNumber,
              it.blockTimestamp,
              it.txHash,
              it.logIndex,
              it.operation,
              it.depositLoss,
              it.topupOrWithdrawal,
              it.yieldGainSince,
              it.yieldGainClaimed,
              it.collGainSince,
              it.collGainClaimed
            );
          } else {
            insertUpdate.run(
              pool.key,
              it.depositor,
              it.blockNumber,
              it.blockTimestamp,
              it.txHash,
              it.logIndex,
              it.newDeposit,
              it.stashedColl,
              it.snapshotP,
              it.snapshotS,
              it.snapshotB,
              it.snapshotScale
            );
          }
        }
      });
      tx(items);
    }

    log.debug(`        logs=${res.logs.length} items=${items.length}`);
    if (FLR_PAUSE_MS > 0) log.debug(`        pause ${FLR_PAUSE_MS}ms`);

    lastGoodBlock = toBlock;
    if (FLR_PAUSE_MS > 0) await sleep(FLR_PAUSE_MS);
  }

  if (lastGoodBlock >= fromBlock) {
    updateCursor(db, cursorKey, lastGoodBlock);
    log.info(`  ✅ advanced cursor to ${lastGoodBlock} (scanned ${latestBlock - fromBlock + 1} blocks)`);
  }
}

async function main() {
  const lockPath = acquireLock("scan-stability-pool");
  if (!lockPath) {
    log.warn("[scanStabilityPool] another instance is running, exiting");
    return;
  }
  const safeRelease = () => releaseLock(lockPath);
  process.once("exit", safeRelease);
  process.once("SIGINT", () => {
    safeRelease();
    process.exit(130);
  });
  process.once("SIGTERM", () => {
    safeRelease();
    process.exit(143);
  });

  const cfg = readJson(path.join(__dirname, "..", "data", "stability_pools.json"));
  const pools = cfg?.chains?.FLR?.contracts || [];
  if (!pools.length) {
    log.error("[scanStabilityPool] No pools found.");
    process.exit(1);
  }

  const provider = buildProvider();
  await provider.getNetwork();

  const db = openDb();
  try {
    const wallets = loadWallets(db);
    for (const pool of pools) {
      await scanPool(db, provider, pool, wallets);
    }
  } finally {
    db.close();
  }
}

main().catch((err) => {
  log.error("[scanStabilityPool] FATAL:", err);
  process.exit(1);
});
