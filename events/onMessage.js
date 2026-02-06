// ./events/onMessage.js

const log = require("../utils/logger");

/**
 * MessageCreate handler (currently no-op).
 */
async function onMessage(message) {
  if (message.author?.bot) return;

  log.debug(`Message from ${message.author.tag}: ${message.content}`);
  // Example:
  // await message.reply("Bingo");
}

module.exports = { onMessage };
