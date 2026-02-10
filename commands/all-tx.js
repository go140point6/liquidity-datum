const { SlashCommandBuilder, EmbedBuilder, MessageFlags, AttachmentBuilder } = require("discord.js");
const { ethers } = require("ethers");
const log = require("../utils/logger");
const { openDatumDb } = require("../utils/db");
const { getUserWallets, requireWalletsOrReply } = require("../utils/sentinel");
const { toCsv } = require("../utils/csv");

const CDP_SYMBOL = "CDP";
const CDP_DECIMALS = 18;
const DATA_STALE_MINUTES = Number(process.env.DATUM_DATA_STALE_MINUTES || "0");

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

function getOpLabel(code) {
  const n = Number(code);
  if (n === 0) return "provideToSP";
  if (n === 1) return "withdrawFromSP";
  if (n === 2) return "claimAllCollGains";
  return String(code);
}

module.exports = {
  data: new SlashCommandBuilder()
    .setName("all-tx")
    .setDescription("Export all transaction types combined (CSV).")
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

      const byContract = new Map();
      for (const t of troves) {
        if (!byContract.has(t.contract_key)) byContract.set(t.contract_key, new Set());
        byContract.get(t.contract_key).add(t.token_id);
      }

      const combined = [];
      const redemptionSummary = new Map();
      const liquidationSummary = new Map();
      const spSummary = new Map();
      let minTs = null;
      let maxTs = null;
      for (const [contractKey, tokenSet] of byContract.entries()) {
        const ids = Array.from(tokenSet);
        const idPlaceholders = ids.map(() => "?").join(",");
        const rows = db
          .prepare(
            `
            SELECT te.contract_key, te.trove_id, te.block_number, te.block_timestamp,
                   te.tx_hash, te.log_index, te.event_name, te.data_json
            FROM trove_events te
            WHERE te.contract_key = ?
              AND te.trove_id IN (${idPlaceholders})
            ORDER BY te.block_number DESC, te.log_index DESC
          `
          )
          .all(contractKey, ...ids);
        const grouped = new Map();
        for (const row of rows) {
          const key = `${row.contract_key}:${row.trove_id}:${row.tx_hash}`;
          if (!grouped.has(key)) grouped.set(key, []);
          grouped.get(key).push(row);
        }

        for (const group of grouped.values()) {
          group.sort((a, b) => a.log_index - b.log_index);
          const troveOp = group.find((r) => r.event_name === "TroveOperation");
          if (!troveOp) continue;
          const op = parseJsonSafe(troveOp.data_json);
          const opCode = Number(op?._operation);
          if (![5, 6].includes(opCode)) continue;

          const updated = [...group]
            .filter((r) => r.event_name === "TroveUpdated")
            .sort((a, b) => b.log_index - a.log_index)[0];
          const updatedData = updated ? parseJsonSafe(updated.data_json) : null;

          const feeRow = group.find((r) => r.event_name === "RedemptionFeePaidToTrove");
          const feeData = feeRow ? parseJsonSafe(feeRow.data_json) : null;

          const blockTs = troveOp.block_timestamp;
          if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
          if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

          if (blockTs != null) {
            minTs = minTs == null ? blockTs : Math.min(minTs, blockTs);
            maxTs = maxTs == null ? blockTs : Math.max(maxTs, blockTs);
          }

          const collMeta = getCollMeta(troveOp.contract_key);
          const debtDelta = parseSigned(op?._debtChangeFromOperation);
          const collDelta = parseSigned(op?._collChangeFromOperation);
          const fee = parseSigned(feeData?._ETHFee);

          const debtDeltaAbs = debtDelta != null && debtDelta < 0n ? -debtDelta : debtDelta;
          const collDeltaAbs = collDelta != null && collDelta < 0n ? -collDelta : collDelta;
          const sold = collDeltaAbs != null && fee != null ? collDeltaAbs - fee : collDeltaAbs;
          const soldNonNeg = sold != null && sold > 0n ? sold : 0n;

          const debtAmount = debtDeltaAbs ? Number(ethers.formatUnits(debtDeltaAbs, CDP_DECIMALS)) : 0;
          const collAmount = collDeltaAbs
            ? Number(ethers.formatUnits(collDeltaAbs, collMeta.decimals))
            : 0;

          const summaryMap = opCode === 6 ? redemptionSummary : liquidationSummary;
          if (!summaryMap.has(troveOp.contract_key)) {
            summaryMap.set(troveOp.contract_key, {
              contractKey: troveOp.contract_key,
              collSymbol: collMeta.symbol,
              count: 0,
              debtTotal: 0,
              collTotal: 0,
            });
          }
          const agg = summaryMap.get(troveOp.contract_key);
          agg.count += 1;
          agg.debtTotal += debtAmount;
          agg.collTotal += collAmount;

          combined.push({
            tx_type: opCode === 6 ? "REDEMPTION" : "LIQUIDATION",
            datetime_utc: blockTs ? new Date(blockTs * 1000).toISOString() : "",
            tx_hash: troveOp.tx_hash,
            block_number: troveOp.block_number,
            contract_key: troveOp.contract_key,
            trove_or_pool_id: troveOp.trove_id,
            wallet: "",
            sold_amount: formatAmount(soldNonNeg, collMeta.decimals),
            sold_symbol: collMeta.symbol,
            bought_amount: formatAmount(debtDeltaAbs, CDP_DECIMALS),
            bought_symbol: CDP_SYMBOL,
            debt_delta_cdp: formatSigned(debtDelta, CDP_DECIMALS),
            coll_delta: formatSigned(collDelta, collMeta.decimals),
            coll_symbol: collMeta.symbol,
            op_code: String(opCode),
            op_label: opCode === 6 ? "redeemCollateral" : "liquidate",
            debt_now_cdp: updatedData?._debt ? formatAmount(BigInt(updatedData._debt), CDP_DECIMALS) : "",
            coll_now: updatedData?._coll ? formatAmount(BigInt(updatedData._coll), collMeta.decimals) : "",
            ir_pct: updatedData?._annualInterestRate
              ? formatPct(BigInt(updatedData._annualInterestRate))
              : "",
            operation_code: "",
            operation_label: "",
            cdp_loss: "",
            cdp_topup_withdrawal: "",
            cdp_yield_gain_since: "",
            cdp_yield_gain_claimed: "",
            coll_gain_since: "",
            coll_gain_claimed: "",
            trade_cdp_spent: "",
            trade_coll_received: "",
          });
        }
      }

      const spRows = db
        .prepare(
          `
          SELECT s.pool_key, s.depositor, s.block_number, s.block_timestamp,
                 s.tx_hash, s.log_index,
                 s.operation AS event_name,
                 s.deposit_loss, s.topup_or_withdrawal,
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

      for (const r of spRows) {
        const blockTs = r.block_timestamp;
        if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
        if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

        if (blockTs != null) {
          minTs = minTs == null ? blockTs : Math.min(minTs, blockTs);
          maxTs = maxTs == null ? blockTs : Math.max(maxTs, blockTs);
        }

        const collDecimals = Number.isFinite(r.coll_decimals) ? r.coll_decimals : 18;
        const collSymbol = r.coll_symbol || "COLL";

        const depositLoss = BigInt(r.deposit_loss);
        const collGain = BigInt(r.coll_gain_since);

        if (depositLoss > 0n || collGain > 0n) {
          if (!spSummary.has(r.pool_key)) {
            spSummary.set(r.pool_key, {
              poolKey: r.pool_key,
              collSymbol,
              count: 0,
              cdpTotal: 0,
              collTotal: 0,
            });
          }
          const agg = spSummary.get(r.pool_key);
          agg.count += 1;
          agg.cdpTotal += Number(ethers.formatUnits(depositLoss, CDP_DECIMALS));
          agg.collTotal += Number(ethers.formatUnits(collGain, collDecimals));
        }

        combined.push({
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
          op_code: "",
          op_label: "",
          debt_now_cdp: "",
          coll_now: "",
          ir_pct: "",
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

      combined.sort((a, b) => b.block_number - a.block_number);

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
        combined.map((r) => headers.map((h) => r[h]))
      );

      const filename = `all_tx_${interaction.user.id}_${Date.now()}.csv`;
      const attachment = new AttachmentBuilder(Buffer.from(csv, "utf8"), {
        name: filename,
      });

      const scanRow = db
        .prepare(
          `
          SELECT MAX(updated_at) AS updated_at
          FROM (
            SELECT updated_at FROM scan_cursors
            UNION ALL
            SELECT updated_at FROM sp_cursors
          )
        `
        )
        .get();
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

      const toSummaryRows = (map) => {
        const rows = Array.from(map.values());
        if (!rows.length) return { col1: "NONE", col2: "", col3: "" };
        const col1 = rows.map((s) => `${s.collSymbol} (${s.count})`).join("\n");
        const col2 = rows.map((s) => `${formatNumber(s.debtTotal, 2)} ${CDP_SYMBOL}`).join("\n");
        const col3 = rows.map((s) => `${formatNumber(s.collTotal, 4)} ${s.collSymbol}`).join("\n");
        return { col1, col2, col3 };
      };

      const redCols = toSummaryRows(redemptionSummary);
      const liqCols = toSummaryRows(liquidationSummary);
      const spCols = (() => {
        const rows = Array.from(spSummary.values());
        if (!rows.length) return { col1: "NONE", col2: "", col3: "" };
        const col1 = rows.map((s) => `${s.collSymbol} (${s.count})`).join("\n");
        const col2 = rows.map((s) => `${formatNumber(s.cdpTotal, 2)} ${CDP_SYMBOL}`).join("\n");
        const col3 = rows.map((s) => `${formatNumber(s.collTotal, 4)} ${s.collSymbol}`).join("\n");
        return { col1, col2, col3 };
      })();

      const embed = new EmbedBuilder()
        .setTitle("Datum — All TX")
        .setThumbnail(interaction.client.user.displayAvatarURL())
        .setDescription(`Period: ${range.label}`)
        .addFields({ name: "Range", value: rangeLabel })
        .addFields(
          { name: "Redemptions", value: redCols.col1, inline: true },
          { name: "Total Debt Reduced", value: redCols.col2, inline: true },
          { name: "Total Coll Redeemed", value: redCols.col3, inline: true },
          { name: "Liquidations", value: liqCols.col1, inline: true },
          { name: "Total Debt Reduced", value: liqCols.col2, inline: true },
          { name: "Total Coll Liquidated", value: liqCols.col3, inline: true },
          { name: "SP Exchanges", value: spCols.col1, inline: true },
          { name: "Total CDP Reduced", value: spCols.col2, inline: true },
          { name: "Total Coll Received", value: spCols.col3, inline: true }
        );

      const noTx = combined.length === 0;
      const hasNonExchangeOnly =
        combined.length > 0 &&
        redemptionSummary.size === 0 &&
        liquidationSummary.size === 0 &&
        spSummary.size === 0;

      if (noTx) {
        embed.addFields({
          name: "Note",
          value: "No transactions found for this period.",
          inline: false,
        });
      } else if (hasNonExchangeOnly) {
        embed.addFields({
          name: "Note",
          value:
            "This period has transactions, but no redemptions, liquidations, or SP exchanges.",
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
        files: combined.length ? [attachment] : [],
      });
    } finally {
      db.close();
    }
  },
};
