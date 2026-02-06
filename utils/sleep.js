// ./utils/sleep.js

/**
 * Pause execution for a given number of milliseconds.
 *
 * @param {number} ms - Duration to sleep in milliseconds (must be >= 0)
 * @returns {Promise<void>}
 */
function sleep(ms) {
  if (!Number.isFinite(ms) || ms < 0) {
    throw new Error(`sleep(ms) requires a non-negative number. Received: ${ms}`);
  }

  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  sleep,
};
