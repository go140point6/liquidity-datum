// ./utils/lock.js

const fs = require("fs");
const path = require("path");

const LOCK_DIR = path.join(__dirname, "..", "locks");
const STALE_MS = 30 * 60 * 1000; // 30 minutes

function ensureDir() {
  try {
    fs.mkdirSync(LOCK_DIR, { recursive: true });
  } catch (err) {
    console.error("[LOCK] Failed to create lock directory:", err);
    throw err;
  }
}

/**
 * Attempt to acquire a filesystem lock.
 *
 * @param {string} name - Logical lock name (file will be `${name}.lock`)
 * @returns {string|null} lockPath if acquired, otherwise null
 */
function acquireLock(name) {
  if (!name || typeof name !== "string") {
    throw new Error("acquireLock(name) requires a non-empty string name.");
  }

  ensureDir();

  const lockPath = path.join(LOCK_DIR, `${name}.lock`);
  const now = Date.now();

  // Check for existing lock
  try {
    const stat = fs.statSync(lockPath);
    const age = now - stat.mtimeMs;

    if (age < STALE_MS) {
      // Active lock
      return null;
    }

    // Stale lock — attempt cleanup
    try {
      fs.unlinkSync(lockPath);
    } catch {
      return null;
    }
  } catch (err) {
    // statSync failed → file likely doesn't exist, continue
    if (err.code !== "ENOENT") {
      return null;
    }
  }

  // Try to create the lock atomically
  try {
    fs.writeFileSync(
      lockPath,
      JSON.stringify(
        {
          pid: process.pid,
          startedAt: new Date().toISOString(),
        },
        null,
        2
      ),
      { flag: "wx" } // fail if file exists
    );

    return lockPath;
  } catch (err) {
    // Another process beat us to it
    return null;
  }
}

/**
 * Release a previously acquired lock.
 *
 * @param {string|null} lockPath
 */
function releaseLock(lockPath) {
  if (!lockPath) return;

  try {
    fs.unlinkSync(lockPath);
  } catch {
    // Best-effort cleanup; ignore errors
  }
}

module.exports = {
  acquireLock,
  releaseLock,
};
