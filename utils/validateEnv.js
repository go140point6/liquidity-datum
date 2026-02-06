// ./utils/validateEnv.js

const log = require("./logger");

function validateEnv() {
  // Add any additional required env vars here
  const requiredVars = ["BOT_TOKEN", "CLIENT_ID", "GUILD_ID", "PRICE_INTERVAL_MIN"];

  const missing = requiredVars.filter(
    (key) => !process.env[key] || !process.env[key].trim()
  );

  if (missing.length > 0) {
    log.error(
      "Missing required environment variables:\n" +
        missing.map((v) => `  - ${v}`).join("\n") +
        "\n\nFix your .env file and restart the bot."
    );
    process.exit(1);
  }

  // Validate PRICE_INTERVAL_MIN is a positive integer (since you use it in onReady)
  const raw = process.env.PRICE_INTERVAL_MIN.trim();
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) {
    log.error(`PRICE_INTERVAL_MIN must be a positive integer. Got: "${raw}"`);
    process.exit(1);
  }
}

module.exports = { validateEnv };
