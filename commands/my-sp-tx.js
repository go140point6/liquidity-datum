const { SlashCommandBuilder, EmbedBuilder, MessageFlags, AttachmentBuilder } = require("discord.js");
const { ethers } = require("ethers");
const log = require("../utils/logger");
const { openDatumDb } = require("../utils/db");
const { getUserWallets, requireWalletsOrReply } = require("../utils/sentinel");
const { toCsv } = require("../utils/csv");

const CDP_SYMBOL = "CDP";
const CDP_DECIMALS = 18;
const DATA_STALE_MINUTES = Number(process.env.DATUM_DATA_STALE_MINUTES || "0");

function formatAmount(value, decimals) {
  if (value == null) return "";
  return ethers.formatUnits(value, decimals);
}

function formatSigned(value, decimals) {
  if (value == null) return "";
  const neg = value < 0n;
  const abs = neg ? -value : value;
  const s = ethers.formatUnits(abs, decimals);
  return neg ? `-${s}` : s;
}

function formatNumber(value, decimals) {
  if (value == null || !Number.isFinite(value)) return "";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
}

function buildPeriod(period) {
  const now = new Date();
  const year = now.getUTCFullYear();
  if (period === "ALL") return { label: "ALL", start: null, end: null };
  if (period === "YTD") {
    const start = Date.UTC(year, 0, 1, 0, 0, 0);
    return { label: `YTD ${year}`, start, end: now.getTime() };
  }
  const y = Number(period);
  if (Number.isInteger(y)) {
    const start = Date.UTC(y, 0, 1, 0, 0, 0);
    const end = Date.UTC(y, 11, 31, 23, 59, 59);
    return { label: String(y), start, end };
  }
  return { label: "ALL", start: null, end: null };
}

function parseSqliteTimestamp(ts) {
  if (!ts) return null;
  const raw = String(ts);
  const iso = raw.includes("T") ? raw : raw.replace(" ", "T");
  const ms = Date.parse(iso.endsWith("Z") ? iso : `${iso}Z`);
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

function getOpLabel(code) {
  const n = Number(code);
  if (n === 0) return "provideToSP";
  if (n === 1) return "withdrawFromSP";
  if (n === 2) return "claimAllCollGains";
  return String(code);
}

module.exports = {
  data: new SlashCommandBuilder()
    .setName("my-sp-tx")
    .setDescription("Export your stability pool transactions (CSV).")
    .addStringOption((opt) => {
      const year = new Date().getUTCFullYear();
      const choices = [
        { name: "YTD", value: "YTD" },
        { name: String(year - 1), value: String(year - 1) },
        { name: String(year - 2), value: String(year - 2) },
        { name: "ALL", value: "ALL" },
      ];
      return opt
        .setName("period")
        .setDescription("Time range")
        .setRequired(true)
        .addChoices(...choices);
    }),

  /**
   * @param {import('discord.js').ChatInputCommandInteraction} interaction
   */
  async execute(interaction) {
    log.debug(`Executing /${interaction.commandName} for ${interaction.user?.tag}`);

    const db = openDatumDb();
    try {
      await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      const period = interaction.options.getString("period", true);
      const range = buildPeriod(period);
      const wallets = getUserWallets(db, interaction.user.id);
      const ok = await requireWalletsOrReply(interaction, wallets);
      if (!ok) return;

      const walletLower = wallets.map((w) => w.address_eip55.toLowerCase());
      const placeholders = walletLower.map(() => "?").join(",");

      const rows = db
        .prepare(
          `
          SELECT s.pool_key, s.depositor, s.block_number, s.block_timestamp, s.tx_hash, s.log_index,
                 s.operation, s.deposit_loss, s.topup_or_withdrawal,
                 s.yield_gain_since, s.yield_gain_claimed,
                 s.coll_gain_since, s.coll_gain_claimed,
                 p.coll_symbol, p.coll_decimals
          FROM sp_deposit_ops s
          LEFT JOIN stability_pools p ON p.pool_key = s.pool_key
          WHERE lower(s.depositor) IN (${placeholders})
          ORDER BY s.block_number DESC, s.log_index DESC
        `
        )
        .all(...walletLower);

      const rowsOut = [];
      const summaryByPool = new Map();
      let minTs = null;
      let maxTs = null;

      for (const r of rows) {
        const blockTs = r.block_timestamp;
        if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
        if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

        const collDecimals = Number.isFinite(r.coll_decimals) ? r.coll_decimals : 18;
        const collSymbol = r.coll_symbol || "COLL";

        const depositLoss = BigInt(r.deposit_loss);
        const collGain = BigInt(r.coll_gain_since);

        if (blockTs != null) {
          minTs = minTs == null ? blockTs : Math.min(minTs, blockTs);
          maxTs = maxTs == null ? blockTs : Math.max(maxTs, blockTs);
        }

        const cdpReduced = Number(ethers.formatUnits(depositLoss, CDP_DECIMALS));
        const collReceived = Number(ethers.formatUnits(collGain, collDecimals));

        const key = r.pool_key;
        if (!summaryByPool.has(key)) {
          summaryByPool.set(key, {
            poolKey: key,
            collSymbol,
            count: 0,
            cdpTotal: 0,
            collTotal: 0,
          });
        }
        if (depositLoss > 0n || collGain > 0n) {
          const agg = summaryByPool.get(key);
          agg.count += 1;
          agg.cdpTotal += cdpReduced;
          agg.collTotal += collReceived;
        }

        rowsOut.push({
          tx_type: "SP",
          datetime_utc: blockTs ? new Date(blockTs * 1000).toISOString() : "",
          tx_hash: r.tx_hash,
          block_number: r.block_number,
          contract_key: r.pool_key,
          trove_or_pool_id: r.pool_key,
          wallet: r.depositor,
          sold_amount: formatAmount(depositLoss, CDP_DECIMALS),
          sold_symbol: CDP_SYMBOL,
          bought_amount: formatAmount(collGain, collDecimals),
          bought_symbol: collSymbol,
          debt_delta_cdp: formatSigned(-depositLoss, CDP_DECIMALS),
          coll_delta: formatSigned(collGain, collDecimals),
          coll_symbol: collSymbol,
          operation_code: r.operation,
          operation_label: getOpLabel(r.operation),
          cdp_loss: formatAmount(depositLoss, CDP_DECIMALS),
          cdp_topup_withdrawal: formatSigned(BigInt(r.topup_or_withdrawal), CDP_DECIMALS),
          cdp_yield_gain_since: formatAmount(BigInt(r.yield_gain_since), CDP_DECIMALS),
          cdp_yield_gain_claimed: formatAmount(BigInt(r.yield_gain_claimed), CDP_DECIMALS),
          coll_gain_since: formatAmount(BigInt(r.coll_gain_since), collDecimals),
          coll_gain_claimed: formatAmount(BigInt(r.coll_gain_claimed), collDecimals),
          trade_cdp_spent: formatAmount(depositLoss, CDP_DECIMALS),
          trade_coll_received: formatAmount(collGain, collDecimals),
        });
      }

      rowsOut.sort((a, b) => b.block_number - a.block_number);

      const headers = [
        "tx_type",
        "datetime_utc",
        "tx_hash",
        "block_number",
        "contract_key",
        "trove_or_pool_id",
        "wallet",
        "sold_amount",
        "sold_symbol",
        "bought_amount",
        "bought_symbol",
        "debt_delta_cdp",
        "coll_delta",
        "coll_symbol",
        "operation_code",
        "operation_label",
        "cdp_loss",
        "cdp_topup_withdrawal",
        "cdp_yield_gain_since",
        "cdp_yield_gain_claimed",
        "coll_gain_since",
        "coll_gain_claimed",
        "trade_cdp_spent",
        "trade_coll_received",
      ];

      const csv = toCsv(
        headers,
        rowsOut.map((r) => headers.map((h) => r[h]))
      );

      const filename = `sp_tx_${interaction.user.id}_${Date.now()}.csv`;
      const attachment = new AttachmentBuilder(Buffer.from(csv, "utf8"), {
        name: filename,
      });

      const scanRow = db.prepare("SELECT MAX(updated_at) AS updated_at FROM sp_cursors").get();
      const dataCapturedTs = parseSqliteTimestamp(scanRow?.updated_at);
      const nowTs = Math.floor(Date.now() / 1000);
      const isStale =
        DATA_STALE_MINUTES > 0 && dataCapturedTs != null
          ? nowTs - dataCapturedTs > DATA_STALE_MINUTES * 60
          : false;
      const staleSuffix = isStale ? " ⚠️ Data may be stale." : "";

      const rangeLabel =
        range.start == null
          ? "ALL"
          : `${new Date(range.start).toISOString().slice(0, 10)} → ${new Date(range.end).toISOString().slice(0, 10)}`;

      const summaryRows = Array.from(summaryByPool.values())
        .filter((s) => s.count > 0)
        .map((s) => {
        const left = `${s.collSymbol} (${s.count})`;
        const mid = `${formatNumber(s.cdpTotal, 2)} ${CDP_SYMBOL}`;
        const right = `${formatNumber(s.collTotal, 4)} ${s.collSymbol}`;
        return [left, mid, right];
      });

      const col1 = summaryRows.length ? summaryRows.map((r) => r[0]).join("\n") : "NONE";
      const col2 = summaryRows.length ? summaryRows.map((r) => r[1]).join("\n") : "";
      const col3 = summaryRows.length ? summaryRows.map((r) => r[2]).join("\n") : "";

      const hasTxNoExchanges = rowsOut.length > 0 && summaryRows.length === 0;
      const noTx = rowsOut.length === 0;

      const embed = new EmbedBuilder()
        .setTitle("Datum — My Stability Pool TX")
        .setThumbnail(interaction.client.user.displayAvatarURL())
        .setDescription(`Period: ${range.label}`)
        .addFields({ name: "Range", value: rangeLabel })
        .addFields(
          { name: "SP Exchanges", value: col1, inline: true },
          { name: "Total CDP Reduced", value: col2, inline: true },
          { name: "Total Coll Received", value: col3, inline: true }
        );

      if (hasTxNoExchanges) {
        embed.addFields({
          name: "Note",
          value: "This period has SP deposits/withdrawals only (no CDP→collateral exchanges).",
          inline: false,
        });
      } else if (noTx) {
        embed.addFields({
          name: "Note",
          value: "No stability pool transactions found for this period.",
          inline: false,
        });
      }

      embed
        .addFields({
          name: "Data Captured",
          value: dataCapturedTs ? `<t:${dataCapturedTs}:f>${staleSuffix}` : "unknown",
          inline: false,
        })
        .setTimestamp(new Date(nowTs * 1000));

      await interaction.editReply({
        embeds: [embed],
        files: rowsOut.length ? [attachment] : [],
      });
    } finally {
      db.close();
    }
  },
};
