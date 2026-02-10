const { SlashCommandBuilder, EmbedBuilder, MessageFlags, AttachmentBuilder } = require("discord.js");
const { ethers } = require("ethers");
const log = require("../utils/logger");
const { openDatumDb } = require("../utils/db");
const { getUserWallets, requireWalletsOrReply } = require("../utils/sentinel");
const { toCsv } = require("../utils/csv");

const CDP_SYMBOL = "CDP";
const CDP_DECIMALS = 18;
const DATA_STALE_MINUTES = Number(process.env.DATUM_DATA_STALE_MINUTES || "0");

const OPERATION_LABELS = {
  0: "openTrove",
  1: "closeTrove",
  2: "adjustTrove",
  3: "adjustTroveInterestRate",
  4: "applyPendingDebt",
  5: "liquidate",
  6: "redeemCollateral",
  7: "openTroveAndJoinBatch",
  8: "setInterestBatchManager",
  9: "removeFromBatch",
};

function getCollMeta(contractKey) {
  const key = String(contractKey || "").toLowerCase();
  if (key.includes("fxrp")) return { symbol: "FXRP", decimals: 6 };
  if (key.includes("wflr")) return { symbol: "WFLR", decimals: 18 };
  return { symbol: "COLL", decimals: 18 };
}

function parseSigned(value) {
  if (value == null) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function formatSigned(value, decimals) {
  if (value == null) return "";
  const neg = value < 0n;
  const abs = neg ? -value : value;
  const s = ethers.formatUnits(abs, decimals);
  return neg ? `-${s}` : s;
}

function formatAmount(value, decimals) {
  if (value == null) return "";
  return ethers.formatUnits(value, decimals);
}

function formatPct(value) {
  if (value == null) return "";
  const n = Number(ethers.formatUnits(value, 18)) * 100;
  if (!Number.isFinite(n)) return "";
  return n.toFixed(4);
}

function formatNumber(value, decimals) {
  if (value == null || !Number.isFinite(value)) return "";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
}

function parseJsonSafe(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
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

module.exports = {
  data: new SlashCommandBuilder()
    .setName("my-liquidations")
    .setDescription("Export your liquidation transactions (CSV).")
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

      const troves = db
        .prepare(
          `
          SELECT DISTINCT contract_key, token_id
          FROM loan_nft_transfers
          WHERE from_addr IN (${placeholders})
             OR to_addr IN (${placeholders})
        `
        )
        .all(...walletLower, ...walletLower);

      if (!troves.length) {
        const embed = new EmbedBuilder()
          .setTitle("Datum — My Liquidations")
          .setDescription("No troves found for your wallets in Datum DB.");
        await interaction.editReply({ embeds: [embed] });
        return;
      }

      const byContract = new Map();
      for (const t of troves) {
        if (!byContract.has(t.contract_key)) byContract.set(t.contract_key, new Set());
        byContract.get(t.contract_key).add(t.token_id);
      }

      const filtered = [];
      for (const [contractKey, tokenSet] of byContract.entries()) {
        const ids = Array.from(tokenSet);
        const idPlaceholders = ids.map(() => "?").join(",");
        const rows = db
          .prepare(
            `
            SELECT te.contract_key, te.trove_id, te.block_number, te.block_timestamp,
                   te.tx_hash, te.log_index, te.data_json
            FROM trove_events te
            WHERE te.contract_key = ?
              AND te.trove_id IN (${idPlaceholders})
              AND te.event_name = 'TroveOperation'
            ORDER BY te.block_number DESC, te.log_index DESC
          `
          )
          .all(contractKey, ...ids);

        for (const row of rows) {
          let parsed = null;
          try {
            parsed = JSON.parse(row.data_json);
          } catch {
            continue;
          }
          const op = Number(parsed?._operation);
          if (op === 5) {
            filtered.push({
              ...row,
              operation: OPERATION_LABELS[op] || String(op),
            });
          }
        }
      }

      const rowsOut = [];
      const summaryByContract = new Map();
      let minTs = null;
      let maxTs = null;

      for (const r of filtered) {
        const blockTs = r.block_timestamp;
        if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
        if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

        const op = parseJsonSafe(r.data_json);
        const collMeta = getCollMeta(r.contract_key);
        const debtDelta = parseSigned(op?._debtChangeFromOperation);
        const collDelta = parseSigned(op?._collChangeFromOperation);

        const debtDeltaAbs = debtDelta != null && debtDelta < 0n ? -debtDelta : debtDelta;
        const collDeltaAbs = collDelta != null && collDelta < 0n ? -collDelta : collDelta;

        const datetime = blockTs ? new Date(blockTs * 1000).toISOString() : "";
        if (blockTs != null) {
          minTs = minTs == null ? blockTs : Math.min(minTs, blockTs);
          maxTs = maxTs == null ? blockTs : Math.max(maxTs, blockTs);
        }

        const debtAmount = debtDeltaAbs ? Number(ethers.formatUnits(debtDeltaAbs, CDP_DECIMALS)) : 0;
        const collAmount = collDeltaAbs ? Number(ethers.formatUnits(collDeltaAbs, collMeta.decimals)) : 0;

        const key = r.contract_key;
        if (!summaryByContract.has(key)) {
          summaryByContract.set(key, {
            contractKey: key,
            collSymbol: collMeta.symbol,
            count: 0,
            debtTotal: 0,
            collTotal: 0,
          });
        }
        const agg = summaryByContract.get(key);
        agg.count += 1;
        agg.debtTotal += debtAmount;
        agg.collTotal += collAmount;

        rowsOut.push({
          tx_type: "LIQUIDATION",
          datetime_utc: datetime,
          tx_hash: r.tx_hash,
          block_number: r.block_number,
          contract_key: r.contract_key,
          trove_or_pool_id: r.trove_id,
          wallet: "",
          sold_amount: formatAmount(collDeltaAbs, collMeta.decimals),
          sold_symbol: collMeta.symbol,
          bought_amount: formatAmount(debtDeltaAbs, CDP_DECIMALS),
          bought_symbol: CDP_SYMBOL,
          debt_delta_cdp: formatSigned(debtDelta, CDP_DECIMALS),
          coll_delta: formatSigned(collDelta, collMeta.decimals),
          coll_symbol: collMeta.symbol,
          op_code: "5",
          op_label: "liquidate",
          debt_now_cdp: "",
          coll_now: "",
          ir_pct: "",
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
        "op_code",
        "op_label",
        "debt_now_cdp",
        "coll_now",
        "ir_pct",
      ];

      const csv = toCsv(
        headers,
        rowsOut.map((r) => headers.map((h) => r[h]))
      );

      const filename = `liquidations_${interaction.user.id}_${Date.now()}.csv`;
      const attachment = new AttachmentBuilder(Buffer.from(csv, "utf8"), {
        name: filename,
      });

      const scanRow = db.prepare("SELECT MAX(updated_at) AS updated_at FROM scan_cursors").get();
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

      const summaryRows = Array.from(summaryByContract.values()).map((s) => {
        const left = `${s.collSymbol} (${s.count})`;
        const mid = `${formatNumber(s.debtTotal, 2)} ${CDP_SYMBOL}`;
        const right = `${formatNumber(s.collTotal, 4)} ${s.collSymbol}`;
        return [left, mid, right];
      });

      const col1 = summaryRows.length ? summaryRows.map((r) => r[0]).join("\n") : "NONE";
      const col2 = summaryRows.length ? summaryRows.map((r) => r[1]).join("\n") : "";
      const col3 = summaryRows.length ? summaryRows.map((r) => r[2]).join("\n") : "";

      const noTx = rowsOut.length === 0;

      const embed = new EmbedBuilder()
        .setTitle("Datum — My Liquidations")
        .setThumbnail(interaction.client.user.displayAvatarURL())
        .setDescription(`Period: ${range.label}`)
        .addFields({ name: "Range", value: rangeLabel })
        .addFields(
          { name: "Liquidations", value: col1, inline: true },
          { name: "Total Debt Reduced", value: col2, inline: true },
          { name: "Total Coll Liquidated", value: col3, inline: true }
        );

      if (noTx) {
        embed.addFields({
          name: "Note",
          value: "No liquidation transactions found for this period.",
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
