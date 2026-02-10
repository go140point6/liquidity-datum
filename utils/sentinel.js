const { MessageFlags } = require("discord.js");
const log = require("./logger");

function getUserIdByDiscordId(db, discordId) {
  const row = db
    .prepare(
      `
      SELECT id
      FROM sentinel.users
      WHERE discord_id = ?
      LIMIT 1
    `
    )
    .get(discordId);
  return row?.id ?? null;
}

function getUserWallets(db, discordId) {
  const userId = getUserIdByDiscordId(db, discordId);
  if (!userId) return [];

  return db
    .prepare(
      `
      SELECT w.chain_id, w.address_eip55, w.label, w.is_enabled
      FROM sentinel.user_wallets w
      WHERE w.user_id = ? AND w.is_enabled = 1
      ORDER BY w.chain_id, w.address_eip55
    `
    )
    .all(userId);
}

async function requireWalletsOrReply(interaction, wallets) {
  if (wallets.length > 0) return true;
  const payload = {
    content:
      "No wallets found for your Discord user in Sentinel DB. " +
      "Ask an admin to add your wallet in Sentinel first.",
    flags: MessageFlags.Ephemeral,
  };
  if (interaction.deferred || interaction.replied) {
    await interaction.editReply(payload);
  } else {
    await interaction.reply(payload);
  }
  return false;
}

module.exports = { getUserWallets, requireWalletsOrReply };
