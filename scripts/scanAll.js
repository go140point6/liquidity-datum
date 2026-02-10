const path = require("path");
const { spawn } = require("child_process");
const log = require("../utils/logger");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
  quiet: true,
});

function runScript(label, scriptPath) {
  return new Promise((resolve, reject) => {
    log.info(`[scanAll] Starting ${label}...`);
    const child = spawn(process.execPath, [scriptPath], {
      stdio: "inherit",
      env: process.env,
    });
    child.on("error", (err) => reject(err));
    child.on("exit", (code, signal) => {
      if (signal) {
        return reject(new Error(`${label} exited via signal ${signal}`));
      }
      if (code !== 0) {
        return reject(new Error(`${label} exited with code ${code}`));
      }
      log.info(`[scanAll] Completed ${label}.`);
      return resolve();
    });
  });
}

async function main() {
  try {
    await runScript("scanTroves", path.join(__dirname, "scanTroves.js"));
    await runScript("scanStabilityPool", path.join(__dirname, "scanStabilityPool.js"));
    log.info("[scanAll] DONE");
  } catch (err) {
    log.error("[scanAll] FATAL:", err);
    process.exit(1);
  }
}

main();
