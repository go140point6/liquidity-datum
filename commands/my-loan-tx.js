const { SlashCommandBuilder, EmbedBuilder, MessageFlags, AttachmentBuilder } = require("discord.js");
const { ethers } = require("ethers");
const log = require("../utils/logger");
const { openDatumDb } = require("../utils/db");
const { getUserWallets, requireWalletsOrReply } = require("../utils/sentinel");
const { toCsv } = require("../utils/csv");

const CDP_SYMBOL = "CDP";
const CDP_DECIMALS = 18;
const DATA_STALE_MINUTES = Number(process.env.DATUM_DATA_STALE_MINUTES || "0");
const LOAN_OP_CODES = new Set([0, 1, 2, 3, 4, 7, 8, 9]);

function getCollMeta(contractKey) {
  const key = String(contractKey || "").toLowerCase();
  if (key.includes("fxrp")) return { symbol: "FXRP", decimals: 6 };
  if (key.includes("wflr")) return { symbol: "WFLR", decimals: 18 };
  return { symbol: "COLL", decimals: 18 };
}

function getTroveOpLabel(code) {
  const n = Number(code);
  return (
    {
      0: "openTrove",
      1: "closeTrove",
      2: "adjustTrove",
      3: "adjustTroveInterestRate",
      4: "applyPendingDebt",
      7: "openTroveAndJoinBatch",
      8: "setInterestBatchManager",
      9: "removeFromBatch",
    }[n] || String(code)
  );
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

module.exports = {
  data: new SlashCommandBuilder()
    .setName("my-loan-tx")
    .setDescription("Export loan operations (open/close/adjust) as CSV.")
    .addStringOption((opt) => {
      const year = new Date().getUTCFullYear();
      const choices = [
        { name: "YTD", value: "YTD" },
        { name: String(year - 1), value: String(year - 1) },
        { name: String(year - 2), value: String(year - 2) },
        { name: "ALL", value: "ALL" },
      ];
      return opt.setName("period").setDescription("Time range").setRequired(true).addChoices(...choices);
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
        await interaction.editReply({
          embeds: [new EmbedBuilder().setTitle("Datum - My Loan TX").setDescription("No troves found for your wallets in Datum DB.")],
        });
        return;
      }

      const byContract = new Map();
      for (const t of troves) {
        if (!byContract.has(t.contract_key)) byContract.set(t.contract_key, new Set());
        byContract.get(t.contract_key).add(t.token_id);
      }

      const rawRows = [];
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
              AND te.event_name IN ('TroveOperation','TroveUpdated')
            ORDER BY te.block_number DESC, te.log_index DESC
          `
          )
          .all(contractKey, ...ids);
        rawRows.push(...rows);
      }

      const grouped = new Map();
      for (const row of rawRows) {
        const key = `${row.contract_key}:${row.trove_id}:${row.tx_hash}`;
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push(row);
      }

      const rowsOut = [];
      const summaryByContract = new Map();
      const redemptionByContract = new Map();
      const liquidationByContract = new Map();
      for (const rows of grouped.values()) {
        rows.sort((a, b) => a.log_index - b.log_index);
        const troveOp = rows.find((r) => r.event_name === "TroveOperation");
        if (!troveOp) continue;
        const op = parseJsonSafe(troveOp.data_json);
        const opCode = Number(op?._operation);
        if (!Number.isInteger(opCode)) continue;

        const blockTs = troveOp.block_timestamp;
        if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
        if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

        if (opCode === 5 || opCode === 6) {
          const debtDelta = parseSigned(op?._debtChangeFromOperation);
          const debtDeltaAbs = debtDelta != null && debtDelta < 0n ? -debtDelta : 0n;
          const debtAmount = Number(ethers.formatUnits(debtDeltaAbs, CDP_DECIMALS));
          const map = opCode === 6 ? redemptionByContract : liquidationByContract;
          map.set(troveOp.contract_key, (map.get(troveOp.contract_key) || 0) + debtAmount);
          continue;
        }

        if (!LOAN_OP_CODES.has(opCode)) continue;

        const updated = [...rows]
          .filter((r) => r.event_name === "TroveUpdated")
          .sort((a, b) => b.log_index - a.log_index)[0];
        const updatedData = updated ? parseJsonSafe(updated.data_json) : null;

        const collMeta = getCollMeta(troveOp.contract_key);
        const debtDelta = parseSigned(op?._debtChangeFromOperation);
        const collDelta = parseSigned(op?._collChangeFromOperation);
        const feeRaw = parseSigned(op?._debtIncreaseFromUpfrontFee) || 0n;
        const redistDebtRaw = parseSigned(op?._debtIncreaseFromRedist) || 0n;
        const borrowedRaw = debtDelta != null && debtDelta > 0n ? debtDelta : 0n;
        const repaidRaw = debtDelta != null && debtDelta < 0n ? -debtDelta : 0n;

        if (!summaryByContract.has(troveOp.contract_key)) {
          summaryByContract.set(troveOp.contract_key, {
            collSymbol: collMeta.symbol,
            count: 0,
            borrowedTotal: 0,
            repaidTotal: 0,
            feeTotal: 0,
            feeOpenTotal: 0,
            feeAdjustTotal: 0,
            feeIrChangeTotal: 0,
            feeOtherTotal: 0,
            inferredInterestTotal: 0,
          });
        }
        const agg = summaryByContract.get(troveOp.contract_key);
        agg.count += 1;
        agg.borrowedTotal += Number(ethers.formatUnits(borrowedRaw, CDP_DECIMALS));
        agg.repaidTotal += Number(ethers.formatUnits(repaidRaw, CDP_DECIMALS));
        const feeAmt = Number(ethers.formatUnits(feeRaw, CDP_DECIMALS));
        agg.feeTotal += feeAmt;
        if (opCode === 0) agg.feeOpenTotal += feeAmt;
        else if (opCode === 2) agg.feeAdjustTotal += feeAmt;
        else if (opCode === 3) agg.feeIrChangeTotal += feeAmt;
        else if (feeAmt > 0) agg.feeOtherTotal += feeAmt;
        const debtNowRaw = updatedData?._debt ? BigInt(updatedData._debt) : null;

        rowsOut.push({
          tx_type: "LOAN_OP",
          datetime_utc: blockTs ? new Date(blockTs * 1000).toISOString() : "",
          tx_hash: troveOp.tx_hash,
          block_number: troveOp.block_number,
          contract_key: troveOp.contract_key,
          trove_or_pool_id: troveOp.trove_id,
          wallet: "",
          sold_amount: formatAmount(repaidRaw, CDP_DECIMALS),
          sold_symbol: CDP_SYMBOL,
          bought_amount: formatAmount(borrowedRaw, CDP_DECIMALS),
          bought_symbol: CDP_SYMBOL,
          debt_delta_cdp: formatSigned(debtDelta, CDP_DECIMALS),
          coll_delta: formatSigned(collDelta, collMeta.decimals),
          coll_symbol: collMeta.symbol,
          op_code: String(opCode),
          op_label: getTroveOpLabel(opCode),
          debt_now_cdp: updatedData?._debt ? formatAmount(BigInt(updatedData._debt), CDP_DECIMALS) : "",
          coll_now: updatedData?._coll ? formatAmount(BigInt(updatedData._coll), collMeta.decimals) : "",
          ir_pct: updatedData?._annualInterestRate ? formatPct(BigInt(updatedData._annualInterestRate)) : "",
          upfront_fee_cdp: formatAmount(feeRaw, CDP_DECIMALS),
          debt_redist_cdp: formatAmount(redistDebtRaw, CDP_DECIMALS),
          estimated_loan_interest_cost_cdp: formatAmount(0n, CDP_DECIMALS),
          _debtDeltaRaw: debtDelta || 0n,
          _feeRaw: feeRaw,
          _redistRaw: redistDebtRaw,
          _debtNowRaw: debtNowRaw,
          _troveKey: `${troveOp.contract_key}:${troveOp.trove_id}`,
        });
      }

      rowsOut.sort((a, b) => b.block_number - a.block_number);

      const prevDebtByTrove = new Map();
      const ascLoanRows = [...rowsOut].sort((a, b) => a.block_number - b.block_number);
      for (const row of ascLoanRows) {
        const nowDebt = row._debtNowRaw;
        if (nowDebt == null) continue;
        const key = row._troveKey;
        const prevDebt = prevDebtByTrove.get(key);
        if (prevDebt != null) {
          const residual =
            nowDebt -
            prevDebt -
            (row._debtDeltaRaw || 0n) -
            (row._feeRaw || 0n) -
            (row._redistRaw || 0n);
          if (residual > 0n) {
            const agg = summaryByContract.get(row.contract_key);
            if (agg) {
              agg.inferredInterestTotal += Number(ethers.formatUnits(residual, CDP_DECIMALS));
            }
            row.estimated_loan_interest_cost_cdp = formatAmount(residual, CDP_DECIMALS);
          }
        }
        prevDebtByTrove.set(key, nowDebt);
      }
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
        "upfront_fee_cdp",
        "debt_redist_cdp",
        "estimated_loan_interest_cost_cdp",
      ];
      const csv = toCsv(headers, rowsOut.map((r) => headers.map((h) => r[h])));
      const attachment = new AttachmentBuilder(Buffer.from(csv, "utf8"), {
        name: `loan_tx_${interaction.user.id}_${Date.now()}.csv`,
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
          : `${new Date(range.start).toISOString().slice(0, 10)} -> ${new Date(range.end).toISOString().slice(0, 10)}`;

      const summaryRowsWithBreakdown = Array.from(summaryByContract.entries()).map(([contractKey, s]) => {
        const repaidByRed = redemptionByContract.get(contractKey) || 0;
        const repaidByLiq = liquidationByContract.get(contractKey) || 0;
        return {
          contractKey,
          collSymbol: s.collSymbol,
          count: s.count,
          borrowed: s.borrowedTotal,
          directRepaid: s.repaidTotal,
          repaidByRed,
          repaidByLiq,
          effective: s.repaidTotal + repaidByRed + repaidByLiq,
          fee: s.feeTotal,
        };
      });

      const col1 = summaryRowsWithBreakdown.length
        ? summaryRowsWithBreakdown.map((r) => `${r.collSymbol} (${r.count})`).join("\n")
        : "NONE";
      const col2 = summaryRowsWithBreakdown.length
        ? summaryRowsWithBreakdown.map((r) => `${formatNumber(r.borrowed, 2)} ${CDP_SYMBOL}`).join("\n")
        : "";
      const col3 = summaryRowsWithBreakdown.length
        ? summaryRowsWithBreakdown.map((r) => `${formatNumber(r.effective, 2)} ${CDP_SYMBOL}`).join("\n")
        : "";
      const feeLines = summaryRowsWithBreakdown.length
        ? summaryRowsWithBreakdown.map((r) => `${r.collSymbol}: ${formatNumber(r.fee, 2)} ${CDP_SYMBOL}`).join("\n")
        : "";
      const feeBreakdown = summaryRowsWithBreakdown.length
        ? summaryRowsWithBreakdown
            .map((r) => {
              const s = summaryByContract.get(r.contractKey);
              if (!s) return null;
              return `${r.collSymbol}: open ${formatNumber(s.feeOpenTotal, 2)} | adjust ${formatNumber(
                s.feeAdjustTotal,
                2
              )} | IR-change ${formatNumber(s.feeIrChangeTotal, 2)} | other ${formatNumber(
                s.feeOtherTotal,
                2
              )} ${CDP_SYMBOL}`;
            })
            .filter(Boolean)
            .join("\n")
        : "";
      const interestApplied = summaryRowsWithBreakdown.length
        ? Array.from(summaryByContract.values())
            .map((s) => `${s.collSymbol}: ${formatNumber(s.inferredInterestTotal, 2)} ${CDP_SYMBOL}`)
            .join("\n")
        : "";
      const breakdown = summaryRowsWithBreakdown.length
        ? summaryRowsWithBreakdown
            .map(
              (r) =>
                `${r.collSymbol}: direct ${formatNumber(r.directRepaid, 2)} + redemption ${formatNumber(
                  r.repaidByRed,
                  2
                )} + liquidation ${formatNumber(r.repaidByLiq, 2)} ${CDP_SYMBOL}`
            )
            .join("\n")
        : "";

      const embed = new EmbedBuilder()
        .setTitle("Datum - My Loan TX")
        .setThumbnail(interaction.client.user.displayAvatarURL())
        .setDescription(`Period: ${range.label}`)
        .addFields({ name: "Range", value: rangeLabel })
        .addFields(
          { name: "Loan Ops", value: col1, inline: true },
          { name: "Total Borrowed", value: col2, inline: true },
          { name: "Total Debt Reduced", value: col3, inline: true }
        );

      if (feeLines) {
        embed.addFields({ name: "Loan Op Fees (Total)", value: feeLines, inline: false });
        embed.addFields({ name: "Fee Breakdown (Totals)", value: feeBreakdown || "n/a", inline: false });
        embed.addFields({
          name: "Estimated Loan Interest Cost",
          value: interestApplied || "n/a",
          inline: false,
        });
        embed.addFields({ name: "Loan Debt Reduction Breakdown", value: breakdown, inline: false });
      } else if (rowsOut.length === 0) {
        embed.addFields({
          name: "Note",
          value: "No loan operation transactions found for this period.",
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
