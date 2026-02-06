// ./events/onReady.js

const fs = require("node:fs");
const path = require("node:path");
const { REST, Routes, Collection } = require("discord.js");
const axios = require("axios");
const log = require("../utils/logger");

function requireEnv(name) {
  const val = process.env[name];
  if (!val || !val.trim()) {
    log.error(`Missing required env var ${name}. Add it to your .env file.`);
    process.exit(1);
  }
  return val.trim();
}

function requireIntEnv(name) {
  const raw = requireEnv(name);
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) {
    log.error(`Env var ${name} must be a positive integer. Got: "${raw}"`);
    process.exit(1);
  }
  return n;
}

async function onReady(client) {
  log.startup(`Ready! Logged in as ${client.user.tag}`);

  const BOT_TOKEN = requireEnv("BOT_TOKEN");
  const CLIENT_ID = requireEnv("CLIENT_ID");
  const GUILD_ID = requireEnv("GUILD_ID");
  const PRICE_INTERVAL_MIN = requireIntEnv("PRICE_INTERVAL_MIN");

  const PRICE_INTERVAL_MS = PRICE_INTERVAL_MIN * 60 * 1000;

  client.commands = new Collection();
  const commands = [];

  const commandsPath = path.join(__dirname, "..", "commands");
  let commandFiles = [];
  try {
    commandFiles = fs.readdirSync(commandsPath).filter((f) => f.endsWith(".js"));
  } catch (err) {
    log.error(`Failed to read commands directory: ${commandsPath}`, err);
    process.exit(1);
  }

  for (const file of commandFiles) {
    const filePath = path.join(commandsPath, file);

    let command;
    try {
      command = require(filePath);
    } catch (err) {
      log.warn(`Failed to load command file: ${filePath}`, err);
      continue;
    }

    if (command?.data && command?.execute) {
      client.commands.set(command.data.name, command);
      commands.push(command.data.toJSON());
      log.debug(`Loaded command: ${command.data.name}`);
    } else {
      log.warn(
        `Command at ${filePath} is missing required "data" or "execute" property.`
      );
    }
  }

  const rest = new REST({ version: "10" }).setToken(BOT_TOKEN);

  try {
    const data = await rest.put(
      Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID),
      { body: commands }
    );
    log.startup(`Successfully loaded ${data.length} application (/) commands.`);
  } catch (err) {
    log.error("Failed to register application commands:", err);
  }

  // ===== XRP EXAMPLE scheduler =====

  async function getXRP() {
    try {
      const res = await axios.get(
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=ripple"
      );

      const price = res?.data?.[0]?.current_price;
      if (typeof price !== "number") {
        log.warn("CoinGecko response missing current_price for XRP.");
        return;
      }

      const currentXRP = price.toFixed(4);
      log.info(`XRP current price: ${currentXRP}`);

      // Keep compatibility with your existing pattern
      module.exports.currentXRP = currentXRP;
      // Also stash on client for convenience
      client.currentXRP = currentXRP;
    } catch (err) {
      log.warn(
        "CoinGecko API call failed:",
        err?.response?.status,
        err?.response?.statusText
      );
    }
  }

  async function runXRPOnce() {
    const t0 = Date.now();
    log.debug("▶️  XRP price fetch start");

    try {
      await getXRP();
    } catch (e) {
      log.error("❌ getXRP failed:", e);
    }

    const elapsed = Date.now() - t0;
    log.debug(`⏹️  XRP price fetch end (elapsed ${elapsed} ms)`);
    return elapsed;
  }

  function startXRPScheduler(intervalMs) {
    let running = false;
    let timeoutId = null;

    async function tick() {
      if (running) return;
      running = true;

      try {
        const elapsed = await runXRPOnce();
        const nextDelay = Math.max(0, intervalMs - elapsed);

        if (nextDelay === 0) {
          log.warn(
            `XRP fetch duration (${elapsed} ms) ≥ interval (${intervalMs} ms). Scheduling next immediately.`
          );
        }

        timeoutId = setTimeout(() => {
          running = false;
          tick();
        }, nextDelay);
      } catch (err) {
        log.error("Unexpected XRP scheduler error:", err);
        running = false;
        timeoutId = setTimeout(tick, intervalMs);
      }
    }

    tick();
    return () => {
      if (timeoutId) clearTimeout(timeoutId);
      timeoutId = null;
    };
  }

  client.stopXRPScheduler = startXRPScheduler(PRICE_INTERVAL_MS);
}

module.exports = { onReady };
