// ./index.js
require("dotenv").config();

const { Client, Events } = require("discord.js");
const { GatewayIntentBits } = require("./config/GatewayIntentBits");
const { onReady } = require("./events/onReady");
const { onInteraction } = require("./events/onInteraction");
const { onMessage } = require("./events/onMessage");
const { validateEnv } = require("./utils/validateEnv");
const log = require("./utils/logger");

function fatal(message, error) {
  log.error(message);
  if (error) log.error(error);
  process.exit(1);
}

process.on("unhandledRejection", (reason) => {
  fatal("Unhandled promise rejection", reason);
});

process.on("uncaughtException", (err) => {
  fatal("Uncaught exception", err);
});

(async () => {
  validateEnv();

  const client = new Client({ intents: GatewayIntentBits });
  module.exports = client;

  client.once(Events.ClientReady, async () => {
    try {
      await onReady(client);
    } catch (err) {
      fatal("Error in onReady handler", err);
    }
  });

  client.on(Events.InteractionCreate, async (interaction) => {
    try {
      await onInteraction(interaction);
    } catch (err) {
      log.error("InteractionCreate handler failed:", err);
    }
  });

  client.on(Events.MessageCreate, async (message) => {
    try {
      await onMessage(message);
    } catch (err) {
      log.error("MessageCreate handler failed:", err);
    }
  });

  try {
    await client.login(process.env.BOT_TOKEN);
    log.startup("Discord client login succeeded.");
  } catch (err) {
    fatal("Discord client login failed (check BOT_TOKEN and bot permissions).", err);
  }
})();