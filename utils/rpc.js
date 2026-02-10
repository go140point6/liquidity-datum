const { ethers } = require("ethers");
const log = require("./logger");

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !v.trim()) {
    log.error(`[rpc] Missing required env var: ${name}`);
    process.exit(1);
  }
  return v.trim();
}

function buildProvider() {
  const url = requireEnv("DATUM_FLR_SCAN_RPC");

  const options = { batchMaxCount: 1 };

  return new ethers.JsonRpcProvider(url, undefined, options);
}

module.exports = { buildProvider };
