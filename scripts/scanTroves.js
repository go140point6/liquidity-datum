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
const troveNftAbi = require("../abi/troveNFT.json");
const troveManagerAbi = require("../abi/troveManager.json");
const log = require("../utils/logger");

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    log.error(`[scanRedemptions] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const { buildProvider } = require("../utils/rpc");
const FLR_SCAN_BLOCKS = Number(requireEnv("DATUM_FLR_SCAN_BLOCKS"));
const FLR_PAUSE_MS = Number(requireEnv("DATUM_FLR_SCAN_PAUSE_MS"));
const OVERLAP_BLOCKS = Number(requireEnv("DATUM_SCAN_OVERLAP_BLOCKS"));

const TRANSFER_TOPIC = ethers.id("Transfer(address,address,uint256)");
const BURN_ADDRS = new Set([
  "0x0000000000000000000000000000000000000000",
  "0x000000000000000000000000000000000000dead",
]);

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

function addressFromTopic(t) {
  return ethers.getAddress("0x" + t.slice(26));
}

function tokenIdFromTopic(t) {
  return BigInt(t).toString();
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

function isBurn(addrLower) {
  return BURN_ADDRS.has(addrLower);
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

function readJson(p) {
  const raw = fs.readFileSync(p, "utf8");
  return JSON.parse(raw);
}

function scanWindowCount(fromBlock, latestBlock, maxBlocks) {
  return Math.ceil((latestBlock - fromBlock + 1) / (maxBlocks + 1));
}

function openDb() {
  const db = openDatumDb();
  initSchema(db);
  return db;
}

async function ensureContracts(db, provider, contracts) {
  const upsert = db.prepare(`
    INSERT INTO loan_contracts (contract_key, protocol, address_eip55, default_start_block, trove_manager_address)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(contract_key) DO UPDATE SET
      protocol = excluded.protocol,
      address_eip55 = excluded.address_eip55,
      default_start_block = excluded.default_start_block,
      trove_manager_address = COALESCE(excluded.trove_manager_address, loan_contracts.trove_manager_address),
      updated_at = datetime('now')
  `);

  for (const c of contracts) {
    const nft = new ethers.Contract(c.address, troveNftAbi, provider);
    const tmAddr = await nft.troveManager();
    upsert.run(c.key, c.protocol, ethers.getAddress(c.address), c.default_start_block, tmAddr);
  }
}

function ensureCursor(db, cursorKey, startBlock) {
  db.prepare(
    `
    INSERT INTO scan_cursors (cursor_key, start_block, last_scanned_block)
    VALUES (?, ?, 0)
    ON CONFLICT(cursor_key) DO NOTHING
  `
  ).run(cursorKey, startBlock);
}

function updateCursor(db, cursorKey, lastBlock) {
  db.prepare(
    `
    UPDATE scan_cursors
    SET last_scanned_block = ?, updated_at = datetime('now')
    WHERE cursor_key = ?
  `
  ).run(lastBlock, cursorKey);
}

async function scanLoanNftTransfers(db, provider, contract) {
  const cursorKey = `loan_nft:${contract.key}:transfer`;
  ensureCursor(db, cursorKey, contract.default_start_block);
  const cursor = db
    .prepare("SELECT start_block, last_scanned_block FROM scan_cursors WHERE cursor_key = ?")
    .get(cursorKey);
  const startBlock = cursor.start_block;
  const lastScanned = cursor.last_scanned_block;
  const latestBlock = await provider.getBlockNumber();
  let fromBlock = lastScanned > 0 ? Math.max(startBlock, lastScanned - OVERLAP_BLOCKS) : startBlock;
  if (fromBlock > latestBlock) return;

  const totalWindows = scanWindowCount(fromBlock, latestBlock, FLR_SCAN_BLOCKS);
  let lastGoodBlock = fromBlock - 1;
  let windowIndex = 0;
  log.info(`\n=== FLR LOAN_NFT ${contract.key} ===`);
  log.info(`  start_block=${startBlock} last_scanned=${lastScanned}`);
  log.info(`  latestBlock=${latestBlock}`);
  log.info("  mode=all-transfers");
  log.debug(
    `  windows=${totalWindows} window_size=${FLR_SCAN_BLOCKS} overlap=${OVERLAP_BLOCKS} pause=${FLR_PAUSE_MS}ms`
  );

  const insertTransfer = db.prepare(`
    INSERT INTO loan_nft_transfers (
      contract_key, block_number, block_timestamp, tx_hash, log_index, from_addr, to_addr, token_id, is_burned
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(contract_key, tx_hash, log_index) DO NOTHING
  `);

  for (let b = fromBlock; b <= latestBlock; b += FLR_SCAN_BLOCKS + 1) {
    const toBlock = Math.min(b + FLR_SCAN_BLOCKS, latestBlock);
    windowIndex++;

    log.debug(`      [${windowIndex}/${totalWindows}] blocks ${b} → ${toBlock}`);

    const res = await getLogsWithRetry(provider, {
      address: ethers.getAddress(contract.address),
      fromBlock: b,
      toBlock,
      topics: [TRANSFER_TOPIC],
    });
    if (!res.ok) break;

    const blockTsMap = await resolveBlockTimestamps(provider, res.logs);
    const events = [];
    for (const lg of res.logs) {
      if (!lg.topics || lg.topics.length < 4) continue;
      const li = getStableLogIndex(lg);
      if (li == null) continue;
      const txHash = lg.transactionHash;
      if (!txHash) continue;
      const from = addressFromTopic(lg.topics[1]);
      const to = addressFromTopic(lg.topics[2]);
      const fromLower = from.toLowerCase();
      const toLower = to.toLowerCase();
      const tokenId = tokenIdFromTopic(lg.topics[3]);
      const burned = isBurn(toLower);
      events.push({
        blockNumber: lg.blockNumber,
        blockTimestamp: blockTsMap.get(lg.blockNumber) ?? null,
        txHash,
        logIndex: li,
        fromLower,
        toLower,
        tokenId,
        isBurned: burned,
      });
    }

    if (events.length) {
      const tx = db.transaction((items) => {
        for (const e of items) {
          insertTransfer.run(
            contract.key,
            e.blockNumber,
            e.blockTimestamp,
            e.txHash,
            e.logIndex,
            e.fromLower,
            e.toLower,
            e.tokenId,
            e.isBurned ? 1 : 0
          );
        }
      });
      tx(events);
    }

    log.debug(`        logs=${res.logs.length} matched=${events.length}`);
    if (FLR_PAUSE_MS > 0) log.debug(`        pause ${FLR_PAUSE_MS}ms`);

    lastGoodBlock = toBlock;
    if (FLR_PAUSE_MS > 0) await sleep(FLR_PAUSE_MS);
  }

  if (lastGoodBlock >= fromBlock) {
    updateCursor(db, cursorKey, lastGoodBlock);
    log.info(`  ✅ advanced cursor to ${lastGoodBlock} (scanned ${latestBlock - fromBlock + 1} blocks)`);
  }
}

function buildEventData(parsed) {
  const data = {};
  const inputs = parsed.fragment?.inputs || [];
  for (let i = 0; i < inputs.length; i += 1) {
    const name = inputs[i]?.name;
    if (!name || name === "_troveId") continue;
    const val = parsed.args?.[i];
    data[name] = val?.toString?.() ?? String(val);
  }
  return data;
}

async function scanTroveManagerEvents(db, provider, contract) {
  const cursorKey = `trove_manager:${contract.key}:events`;
  ensureCursor(db, cursorKey, contract.default_start_block);
  const cursor = db
    .prepare("SELECT start_block, last_scanned_block FROM scan_cursors WHERE cursor_key = ?")
    .get(cursorKey);
  const startBlock = cursor.start_block;
  const lastScanned = cursor.last_scanned_block;
  const latestBlock = await provider.getBlockNumber();
  let fromBlock = lastScanned > 0 ? Math.max(startBlock, lastScanned - OVERLAP_BLOCKS) : startBlock;
  if (fromBlock > latestBlock) return;

  const iface = new ethers.Interface(troveManagerAbi);
  const topics = [
    iface.getEvent("Redemption").topicHash,
    iface.getEvent("RedemptionFeePaidToTrove").topicHash,
    iface.getEvent("TroveUpdated").topicHash,
    iface.getEvent("TroveOperation").topicHash,
  ];

  const insertRedemption = db.prepare(`
    INSERT INTO redemption_events (
      contract_key, block_number, block_timestamp, tx_hash, log_index,
      attempted_bold, actual_bold, eth_sent, eth_fee, price, redemption_price
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(contract_key, tx_hash, log_index) DO NOTHING
  `);

  const insertTroveEvent = db.prepare(`
    INSERT INTO trove_events (
      contract_key, event_name, block_number, block_timestamp, tx_hash, log_index, trove_id, data_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(contract_key, tx_hash, log_index) DO NOTHING
  `);

  log.info(`\n=== FLR TROVE_EVENTS ${contract.key} ===`);
  log.info(`  start_block=${startBlock} last_scanned=${lastScanned}`);
  log.info(`  latestBlock=${latestBlock}`);
  log.info("  mode=all-events");
  const totalWindows = scanWindowCount(fromBlock, latestBlock, FLR_SCAN_BLOCKS);
  log.debug(
    `  windows=${totalWindows} window_size=${FLR_SCAN_BLOCKS} overlap=${OVERLAP_BLOCKS} pause=${FLR_PAUSE_MS}ms`
  );

  let lastGoodBlock = fromBlock - 1;
  for (let b = fromBlock; b <= latestBlock; b += FLR_SCAN_BLOCKS + 1) {
    const toBlock = Math.min(b + FLR_SCAN_BLOCKS, latestBlock);
    const windowIndex = Math.floor((b - fromBlock) / (FLR_SCAN_BLOCKS + 1)) + 1;
    log.debug(`      [${windowIndex}/${totalWindows}] blocks ${b} → ${toBlock}`);
    const res = await getLogsWithRetry(provider, {
      address: ethers.getAddress(contract.troveManager),
      fromBlock: b,
      toBlock,
      topics: [topics],
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

      if (parsed.name === "Redemption") {
        const args = parsed.args;
        items.push({
          kind: "redemption",
          blockNumber: lg.blockNumber,
          blockTimestamp,
          txHash,
          logIndex: li,
          attemptedBold: args._attemptedBoldAmount.toString(),
          actualBold: args._actualBoldAmount.toString(),
          ethSent: args._ETHSent.toString(),
          ethFee: args._ETHFee.toString(),
          price: args._price.toString(),
          redemptionPrice: args._redemptionPrice.toString(),
        });
        continue;
      }

      if (
        parsed.name === "RedemptionFeePaidToTrove" ||
        parsed.name === "TroveUpdated" ||
        parsed.name === "TroveOperation"
      ) {
        const troveId = parsed.args._troveId?.toString();
        if (!troveId) continue;
        const data = buildEventData(parsed);
        items.push({
          kind: "trove",
          eventName: parsed.name,
          blockNumber: lg.blockNumber,
          blockTimestamp,
          txHash,
          logIndex: li,
          troveId,
          dataJson: JSON.stringify(data),
        });
      }
    }

    if (items.length) {
      const tx = db.transaction((arr) => {
        for (const it of arr) {
          if (it.kind === "redemption") {
            insertRedemption.run(
              contract.key,
              it.blockNumber,
              it.blockTimestamp,
              it.txHash,
              it.logIndex,
              it.attemptedBold,
              it.actualBold,
              it.ethSent,
              it.ethFee,
              it.price,
              it.redemptionPrice
            );
          } else if (it.kind === "trove") {
            insertTroveEvent.run(
              contract.key,
              it.eventName,
              it.blockNumber,
              it.blockTimestamp,
              it.txHash,
              it.logIndex,
              it.troveId,
              it.dataJson
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
  const lockPath = acquireLock("scan-redemptions");
  if (!lockPath) {
    log.warn("[scanRedemptions] another instance is running, exiting");
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

  log.info("[scanRedemptions] Starting scan...");
  const cfg = readJson(path.join(__dirname, "..", "data", "loan_contracts.json"));
  const contracts = cfg?.chains?.FLR?.contracts || [];
  if (!contracts.length) {
    log.error("[scanRedemptions] No contracts found.");
    process.exit(1);
  }

  log.info("[scanRedemptions] Initializing RPC provider...");
  const provider = buildProvider();
  const network = await Promise.race([
    provider.getNetwork(),
    new Promise((_, reject) => setTimeout(() => reject(new Error("RPC init timeout")), 15000)),
  ]);
  log.info(`[scanRedemptions] RPC ready: chainId=${network.chainId}`);

  const db = openDb();
  try {
    await ensureContracts(db, provider, contracts);
    for (const c of contracts) {
      log.info(`[scanRedemptions] Scan transfers: ${c.key}`);
      await scanLoanNftTransfers(db, provider, c);
    }
    for (const c of contracts) {
      const row = db
        .prepare("SELECT trove_manager_address FROM loan_contracts WHERE contract_key = ?")
        .get(c.key);
      if (!row?.trove_manager_address) continue;
      log.info(`[scanRedemptions] Scan trove events: ${c.key}`);
      await scanTroveManagerEvents(db, provider, {
        ...c,
        troveManager: row.trove_manager_address,
      });
    }
  } finally {
    db.close();
  }
}

main().catch((err) => {
  log.error("[scanRedemptions] FATAL:", err);
  process.exit(1);
});
