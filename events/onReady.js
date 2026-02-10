// ./events/onReady.js

const fs = require("node:fs");
const path = require("node:path");
const { REST, Routes, Collection } = require("discord.js");
const log = require("../utils/logger");

function requireEnv(name) {
  const val = process.env[name];
  if (!val || !val.trim()) {
    log.error(`Missing required env var ${name}. Add it to your .env file.`);
    process.exit(1);
  }
  return val.trim();
}

async function onReady(client) {
  log.startup(`Ready! Logged in as ${client.user.tag}`);

  const BOT_TOKEN = requireEnv("BOT_TOKEN");
  const CLIENT_ID = requireEnv("CLIENT_ID");
  const GUILD_ID = requireEnv("GUILD_ID");

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
}

module.exports = { onReady };
