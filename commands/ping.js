// ./commands/ping.js

const { SlashCommandBuilder, EmbedBuilder } = require("discord.js");
const log = require("../utils/logger");

module.exports = {
  data: new SlashCommandBuilder()
    .setName("ping")
    .setDescription("Replies with Pong!"),

  /**
   * @param {import('discord.js').ChatInputCommandInteraction} interaction
   */
  async execute(interaction) {
    log.info(`/${interaction.commandName} used by ${interaction.user?.tag}`);

    const embed = new EmbedBuilder()
      .setTitle("Ping!")
      .addFields({ name: "Ping", value: "Pong!" });

    await interaction.reply({ embeds: [embed] });
  },
};
