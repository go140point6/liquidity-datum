// ./events/onInteraction.js

const log = require("../utils/logger");

async function onInteraction(interaction) {
  try {
    if (interaction.isChatInputCommand()) {
      const command = interaction.client.commands?.get(interaction.commandName);

      if (!command) {
        log.warn(`No command handler found for /${interaction.commandName}`);

        if (!interaction.replied && !interaction.deferred) {
          const { MessageFlags } = require("discord.js");
          await interaction.reply({
            content: "⚠️ This command is not recognized.",
            flags: MessageFlags.Ephemeral,
          });
        }
        return;
      }

      await command.execute(interaction);
      return;
    }

    if (interaction.isButton()) {
      log.debug(`Button interaction received: ${interaction.customId}`);
      return;
    }

    // ignore other interaction types for now
  } catch (err) {
    log.error(`Interaction handler failed (type=${interaction.type})`, err);

    try {
      if (interaction.replied || interaction.deferred) {
        const { MessageFlags } = require("discord.js");
        await interaction.followUp({
          content: "❌ An unexpected error occurred while handling this interaction.",
          flags: MessageFlags.Ephemeral,
        });
      } else {
        const { MessageFlags } = require("discord.js");
        await interaction.reply({
          content: "❌ An unexpected error occurred while handling this interaction.",
          flags: MessageFlags.Ephemeral,
        });
      }
    } catch {
      // ignore secondary failures
    }
  }
}

module.exports = { onInteraction };
