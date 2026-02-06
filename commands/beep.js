// ./commands/beep.js

const { SlashCommandBuilder } = require("discord.js");
const log = require("../utils/logger");

module.exports = {
  data: new SlashCommandBuilder()
    .setName("beep")
    .setDescription("Replies with Boop!"),

  /**
   * @param {import('discord.js').ChatInputCommandInteraction} interaction
   */
  async execute(interaction) {
    log.info(`/${interaction.commandName} used by ${interaction.user?.tag}`);
    await interaction.reply({ content: "Boop!" });
  },
};
