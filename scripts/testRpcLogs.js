const path = require("path");
const { ethers } = require("ethers");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
  quiet: true,
});

const { buildProvider } = require("../utils/rpc");
const log = require("../utils/logger");

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    log.error(`[testRpcLogs] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const BLOCKS = Number(requireEnv("DATUM_FLR_SCAN_BLOCKS"));
const OVERLAP = Number(requireEnv("DATUM_SCAN_OVERLAP_BLOCKS"));

if (!Number.isInteger(BLOCKS) || BLOCKS <= 0) {
  log.error("[testRpcLogs] DATUM_FLR_SCAN_BLOCKS must be a positive integer");
  process.exit(1);
}
if (!Number.isInteger(OVERLAP) || OVERLAP < 0) {
  log.error("[testRpcLogs] DATUM_SCAN_OVERLAP_BLOCKS must be a non-negative integer");
  process.exit(1);
}

async function main() {
  const provider = buildProvider();
  const latest = await provider.getBlockNumber();
  const toBlock = latest;
  const fromBlock = Math.max(0, latest - BLOCKS);

  const t0 = Date.now();
  log.info(`[testRpcLogs] Testing eth_getLogs window ${fromBlock} → ${toBlock}`);

  const res = await provider.getLogs({
    fromBlock,
    toBlock,
  });

  const ms = Date.now() - t0;
  log.info(`[testRpcLogs] Success. logs=${res.length} elapsed_ms=${ms}`);
  log.info(`[testRpcLogs] If this fails, reduce DATUM_FLR_SCAN_BLOCKS or add pause.`);
}

main().catch((err) => {
  log.error("[testRpcLogs] Failed:", err?.message || err);
  process.exit(1);
});
