const fs = require("fs");
const path = require("path");
const log = require("./logger");

function ensureDir(dir) {
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch (err) {
    log.error(`[lock] Failed to create lock dir: ${dir}`, err.message || err);
    return false;
  }
  return true;
}

function acquireLock(name) {
  const lockDir = path.join(__dirname, "..", "locks");
  if (!ensureDir(lockDir)) return null;
  const lockPath = path.join(lockDir, `${name}.lock`);

  try {
    const fd = fs.openSync(lockPath, "wx");
    fs.writeFileSync(
      fd,
      JSON.stringify({ pid: process.pid, created_at: new Date().toISOString() }) + "\n"
    );
    fs.closeSync(fd);
    log.debug(`[LOCK] Acquired lock: ${lockPath}`);
    return lockPath;
  } catch (err) {
    if (err.code === "EEXIST") {
      log.warn(`[LOCK] Lock already exists: ${lockPath}`);
      return null;
    }
    log.error(`[LOCK] Failed to acquire lock: ${lockPath}`, err.message || err);
    return null;
  }
}

function releaseLock(lockPath) {
  if (!lockPath) return;
  try {
    fs.unlinkSync(lockPath);
    log.debug(`[LOCK] Released lock: ${lockPath}`);
  } catch (err) {
    log.warn(`[LOCK] Failed to release lock: ${lockPath}`, err.message || err);
  }
}

module.exports = { acquireLock, releaseLock };
