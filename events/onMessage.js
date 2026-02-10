// ./events/onMessage.js

const log = require("../utils/logger");

/**
 * MessageCreate handler (currently no-op).
 */
async function onMessage(message) {
  if (message.author?.bot) return;

  // Intentionally no-op (no logging for user messages).
}

module.exports = { onMessage };
