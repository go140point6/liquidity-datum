const { SlashCommandBuilder, EmbedBuilder, MessageFlags, AttachmentBuilder } = require("discord.js");
const { ethers } = require("ethers");
const log = require("../utils/logger");
const { openDatumDb } = require("../utils/db");
const { getLoanCollMeta, loadLoanCollMetaMap } = require("../utils/loanCollateral");
const { getPrimefiMarketMeta, loadPrimefiMarketMetaMap, parseSqliteTimestamp: parsePrimefiTs } = require("../utils/primefi");
const { addLoanProviderOption } = require("../utils/loanProviders");
const { getUserWalletsByChain } = require("../utils/sentinel");
const { toCsv } = require("../utils/csv");

const CDP_SYMBOL = "CDP";
const CDP_DECIMALS = 18;
const DATA_STALE_MINUTES = Number(process.env.DATUM_DATA_STALE_MINUTES || "0");

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

function getTroveOpLabel(code) {
  const n = Number(code);
  return (
    {
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
    }[n] || String(code)
  );
}

module.exports = {
  data: addLoanProviderOption(
    new SlashCommandBuilder()
      .setName("all-tx")
      .setDescription("Export all transaction types combined (CSV).")
  , { includeAll: true }).addStringOption((opt) => {
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
      const provider = interaction.options.getString("provider", true);
      const period = interaction.options.getString("period", true);
      const range = buildPeriod(period);

      const combined = [];
      const loanOpsSummary = new Map();
      const redemptionSummary = new Map();
      const liquidationSummary = new Map();
      const spSummary = new Map();
      let enosysDataCapturedTs = null;
      if (provider === "enosys" || provider === "all") {
        const loanMetaMap = loadLoanCollMetaMap(db);
        const wallets = getUserWalletsByChain(db, interaction.user.id, "FLR");
        if (provider === "enosys" && wallets.length === 0) {
          await interaction.editReply({
            content:
              "No wallets found for your Discord user in Sentinel DB. Ask an admin to add your wallet in Sentinel first.",
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        if (wallets.length > 0) {
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
              if (!Number.isInteger(opCode)) continue;

              const updated = [...group]
                .filter((r) => r.event_name === "TroveUpdated")
                .sort((a, b) => b.log_index - a.log_index)[0];
              const updatedData = updated ? parseJsonSafe(updated.data_json) : null;

              const feeRow = group.find((r) => r.event_name === "RedemptionFeePaidToTrove");
              const feeData = feeRow ? parseJsonSafe(feeRow.data_json) : null;

              const blockTs = troveOp.block_timestamp;
              if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
              if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

              const collMeta = getLoanCollMeta(loanMetaMap, troveOp.contract_key);
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

              if (opCode === 5 || opCode === 6) {
                const summaryMap = opCode === 6 ? redemptionSummary : liquidationSummary;
                if (!summaryMap.has(troveOp.contract_key)) {
                  summaryMap.set(troveOp.contract_key, {
                    contractKey: troveOp.contract_key,
                    collSymbol: collMeta.symbol,
                    count: 0,
                    debtTotal: 0,
                    collTotal: 0,
                    debtSymbol: CDP_SYMBOL,
                  });
                }
                const agg = summaryMap.get(troveOp.contract_key);
                agg.count += 1;
                agg.debtTotal += debtAmount;
                agg.collTotal += collAmount;
              } else if ([0, 1, 2, 3, 4, 7, 8, 9].includes(opCode)) {
                const feeRaw = parseSigned(op?._debtIncreaseFromUpfrontFee);
                const repaidRaw = debtDelta != null && debtDelta < 0n ? -debtDelta : 0n;
                const borrowedRaw = debtDelta != null && debtDelta > 0n ? debtDelta : 0n;
                if (!loanOpsSummary.has(troveOp.contract_key)) {
                  loanOpsSummary.set(troveOp.contract_key, {
                    contractKey: troveOp.contract_key,
                    collSymbol: collMeta.symbol,
                    debtSymbol: CDP_SYMBOL,
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
                const agg = loanOpsSummary.get(troveOp.contract_key);
                agg.count += 1;
                agg.borrowedTotal += Number(ethers.formatUnits(borrowedRaw, CDP_DECIMALS));
                agg.repaidTotal += Number(ethers.formatUnits(repaidRaw, CDP_DECIMALS));
                const feeAmt = Number(ethers.formatUnits(feeRaw || 0n, CDP_DECIMALS));
                agg.feeTotal += feeAmt;
                if (opCode === 0) agg.feeOpenTotal += feeAmt;
                else if (opCode === 2) agg.feeAdjustTotal += feeAmt;
                else if (opCode === 3) agg.feeIrChangeTotal += feeAmt;
                else if (feeAmt > 0) agg.feeOtherTotal += feeAmt;
              } else {
                continue;
              }

              const txType = opCode === 6 ? "REDEMPTION" : opCode === 5 ? "LIQUIDATION" : "LOAN_OP";
              const feeRaw = parseSigned(op?._debtIncreaseFromUpfrontFee);
              const debtRedistRaw = parseSigned(op?._debtIncreaseFromRedist);
              const soldCdp = debtDelta != null && debtDelta < 0n ? -debtDelta : null;
              const boughtCdp = debtDelta != null && debtDelta > 0n ? debtDelta : null;

              combined.push({
                tx_type: txType,
                datetime_utc: blockTs ? new Date(blockTs * 1000).toISOString() : "",
                tx_hash: troveOp.tx_hash,
                block_number: troveOp.block_number,
                contract_key: troveOp.contract_key,
                trove_or_pool_id: troveOp.trove_id,
                wallet: "",
                sold_amount:
                  txType === "LOAN_OP" ? formatAmount(soldCdp, CDP_DECIMALS) : formatAmount(soldNonNeg, collMeta.decimals),
                sold_symbol: txType === "LOAN_OP" ? CDP_SYMBOL : collMeta.symbol,
                bought_amount:
                  txType === "LOAN_OP" ? formatAmount(boughtCdp, CDP_DECIMALS) : formatAmount(debtDeltaAbs, CDP_DECIMALS),
                bought_symbol: CDP_SYMBOL,
                debt_delta_cdp: formatSigned(debtDelta, CDP_DECIMALS),
                coll_delta: formatSigned(collDelta, collMeta.decimals),
                coll_symbol: collMeta.symbol,
                op_code: String(opCode),
                op_label: getTroveOpLabel(opCode),
                debt_now_cdp: updatedData?._debt ? formatAmount(BigInt(updatedData._debt), CDP_DECIMALS) : "",
                coll_now: updatedData?._coll ? formatAmount(BigInt(updatedData._coll), collMeta.decimals) : "",
                ir_pct: updatedData?._annualInterestRate
                  ? formatPct(BigInt(updatedData._annualInterestRate))
                  : "",
                operation_code: "",
                operation_label: "",
                cdp_loss: txType === "LOAN_OP" ? formatAmount(feeRaw, CDP_DECIMALS) : "",
                cdp_topup_withdrawal: "",
                cdp_yield_gain_since: "",
                cdp_yield_gain_claimed: "",
                coll_gain_since: "",
                coll_gain_claimed: "",
                trade_cdp_spent: txType === "LOAN_OP" ? formatAmount(debtRedistRaw, CDP_DECIMALS) : "",
                trade_coll_received: "",
                upfront_fee_cdp: txType === "LOAN_OP" ? formatAmount(feeRaw, CDP_DECIMALS) : "",
                debt_redist_cdp: txType === "LOAN_OP" ? formatAmount(debtRedistRaw, CDP_DECIMALS) : "",
                estimated_loan_interest_cost_cdp:
                  txType === "LOAN_OP" ? formatAmount(0n, CDP_DECIMALS) : "",
                _debtDeltaRaw: txType === "LOAN_OP" ? debtDelta || 0n : 0n,
                _feeRaw: txType === "LOAN_OP" ? feeRaw || 0n : 0n,
                _redistRaw: txType === "LOAN_OP" ? debtRedistRaw || 0n : 0n,
                _debtNowRaw:
                  txType === "LOAN_OP" && updatedData?._debt ? BigInt(updatedData._debt) : null,
                _troveKey:
                  txType === "LOAN_OP" ? `${troveOp.contract_key}:${troveOp.trove_id}` : "",
              });
            }
          }

          const spRows = db
            .prepare(
              `
              SELECT s.pool_key, s.depositor, s.block_number, s.block_timestamp,
                     s.tx_hash, s.log_index,
                     s.operation,
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
              upfront_fee_cdp: "",
              debt_redist_cdp: "",
              estimated_loan_interest_cost_cdp: "",
            });
          }
        }

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
        enosysDataCapturedTs = parseSqliteTimestamp(scanRow?.updated_at);
      }

      let primefiDataCapturedTs = null;
      if (provider === "primefi" || provider === "all") {
        const wallets = getUserWalletsByChain(db, interaction.user.id, "XDC");
        if (provider === "primefi" && wallets.length === 0) {
          await interaction.editReply({
            content:
              "No wallets found for your Discord user in Sentinel DB. Ask an admin to add your wallet in Sentinel first.",
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        if (wallets.length > 0) {
          const marketMetaMap = loadPrimefiMarketMetaMap(db);
          const walletLower = wallets.map((w) => w.address_eip55.toLowerCase());
          const placeholders = walletLower.map(() => "?").join(",");
          const rows = db
            .prepare(
              `
              SELECT market_key, protocol, block_number, block_timestamp, tx_hash, log_index, event_name, user_lower, event_json
              FROM sentinel.primefi_market_events
              WHERE user_lower IN (${placeholders})
                AND event_name IN ('Deposit', 'Withdraw', 'Borrow', 'Repay', 'LiquidationCall')
              ORDER BY block_number DESC, log_index DESC
            `
            )
            .all(...walletLower);

          for (const row of rows) {
            const blockTs = row.block_timestamp;
            if (range.start != null && (blockTs == null || blockTs * 1000 < range.start)) continue;
            if (range.end != null && (blockTs == null || blockTs * 1000 > range.end)) continue;

            const event = parseJsonSafe(row.event_json);
            if (!event) continue;
            const meta = getPrimefiMarketMeta(marketMetaMap, row.market_key);

            if (row.event_name === "LiquidationCall") {
              const debtToCover = parseSigned(event.debtToCoverRaw);
              const collLiquidated = parseSigned(event.liquidatedCollateralAmountRaw);
              const debtAmount = debtToCover ? Number(ethers.formatUnits(debtToCover, 6)) : 0;
              const collAmount = collLiquidated ? Number(ethers.formatUnits(collLiquidated, 18)) : 0;

              if (!liquidationSummary.has(row.market_key)) {
                liquidationSummary.set(row.market_key, {
                  contractKey: row.market_key,
                  collSymbol: meta.collSymbol,
                  debtSymbol: meta.debtSymbol,
                  count: 0,
                  debtTotal: 0,
                  collTotal: 0,
                });
              }
              const agg = liquidationSummary.get(row.market_key);
              agg.count += 1;
              agg.debtTotal += debtAmount;
              agg.collTotal += collAmount;

              combined.push({
                tx_type: "LIQUIDATION",
                datetime_utc: blockTs ? new Date(blockTs * 1000).toISOString() : "",
                tx_hash: row.tx_hash,
                block_number: row.block_number,
                contract_key: row.market_key,
                trove_or_pool_id: row.market_key,
                wallet: row.user_lower,
                sold_amount: formatAmount(collLiquidated, 18),
                sold_symbol: meta.collSymbol,
                bought_amount: formatAmount(debtToCover, 6),
                bought_symbol: meta.debtSymbol,
                debt_delta_cdp: "",
                coll_delta: formatSigned(collLiquidated ? -collLiquidated : null, 18),
                coll_symbol: meta.collSymbol,
                op_code: row.event_name,
                op_label: "liquidationCall",
                debt_now_cdp: "",
                coll_now: "",
                ir_pct: "",
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
                upfront_fee_cdp: "",
                debt_redist_cdp: "",
                estimated_loan_interest_cost_cdp: "",
              });
              continue;
            }

            const amountRaw = parseSigned(event.amountRaw);
            if (!loanOpsSummary.has(row.market_key)) {
              loanOpsSummary.set(row.market_key, {
                contractKey: row.market_key,
                collSymbol: meta.collSymbol,
                debtSymbol: meta.debtSymbol,
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
            const agg = loanOpsSummary.get(row.market_key);
            agg.count += 1;

            let soldAmount = "";
            let soldSymbol = "";
            let boughtAmount = "";
            let boughtSymbol = "";
            let debtDelta = "";
            let collDelta = "";

            if (row.event_name === "Borrow") {
              boughtAmount = formatAmount(amountRaw, 6);
              boughtSymbol = meta.debtSymbol;
              debtDelta = formatSigned(amountRaw, 6);
              agg.borrowedTotal += amountRaw ? Number(ethers.formatUnits(amountRaw, 6)) : 0;
            } else if (row.event_name === "Repay") {
              soldAmount = formatAmount(amountRaw, 6);
              soldSymbol = meta.debtSymbol;
              debtDelta = formatSigned(amountRaw ? -amountRaw : null, 6);
              agg.repaidTotal += amountRaw ? Number(ethers.formatUnits(amountRaw, 6)) : 0;
            } else if (row.event_name === "Deposit") {
              soldAmount = formatAmount(amountRaw, 18);
              soldSymbol = meta.collSymbol;
              collDelta = formatSigned(amountRaw, 18);
            } else if (row.event_name === "Withdraw") {
              boughtAmount = formatAmount(amountRaw, 18);
              boughtSymbol = meta.collSymbol;
              collDelta = formatSigned(amountRaw ? -amountRaw : null, 18);
            }

            combined.push({
              tx_type: "LOAN_OP",
              datetime_utc: blockTs ? new Date(blockTs * 1000).toISOString() : "",
              tx_hash: row.tx_hash,
              block_number: row.block_number,
              contract_key: row.market_key,
              trove_or_pool_id: row.market_key,
              wallet: row.user_lower,
              sold_amount: soldAmount,
              sold_symbol: soldSymbol,
              bought_amount: boughtAmount,
              bought_symbol: boughtSymbol,
              debt_delta_cdp: debtDelta,
              coll_delta: collDelta,
              coll_symbol: meta.collSymbol,
              op_code: row.event_name,
              op_label: row.event_name,
              debt_now_cdp: "",
              coll_now: "",
              ir_pct: "",
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
              upfront_fee_cdp: "",
              debt_redist_cdp: "",
              estimated_loan_interest_cost_cdp: "",
            });
          }
        }

        const scanRow = db
          .prepare("SELECT MAX(last_scanned_at) AS last_scanned_at FROM sentinel.primefi_market_event_cursors")
          .get();
        primefiDataCapturedTs = parsePrimefiTs(scanRow?.last_scanned_at);
      }

      combined.sort((a, b) => b.block_number - a.block_number);

      const prevDebtByTrove = new Map();
      const ascLoanRows = combined
        .filter((r) => r.tx_type === "LOAN_OP")
        .sort((a, b) => a.block_number - b.block_number);
      for (const row of ascLoanRows) {
        const nowDebt = row._debtNowRaw;
        if (nowDebt == null) continue;
        const prevDebt = prevDebtByTrove.get(row._troveKey);
        if (prevDebt != null) {
          const residual =
            nowDebt -
            prevDebt -
            (row._debtDeltaRaw || 0n) -
            (row._feeRaw || 0n) -
            (row._redistRaw || 0n);
          if (residual > 0n) {
            const agg = loanOpsSummary.get(row.contract_key);
            if (agg) {
              agg.inferredInterestTotal += Number(ethers.formatUnits(residual, CDP_DECIMALS));
            }
            row.estimated_loan_interest_cost_cdp = formatAmount(residual, CDP_DECIMALS);
          }
        }
        prevDebtByTrove.set(row._troveKey, nowDebt);
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
        "upfront_fee_cdp",
        "debt_redist_cdp",
        "estimated_loan_interest_cost_cdp",
      ];

      const csv = toCsv(
        headers,
        combined.map((r) => headers.map((h) => r[h]))
      );

      const filename = `all_tx_${interaction.user.id}_${Date.now()}.csv`;
      const attachment = new AttachmentBuilder(Buffer.from(csv, "utf8"), {
        name: filename,
      });

      const nowTs = Math.floor(Date.now() / 1000);
      const selectedCaptured = [];
      if (provider === "enosys" || provider === "all") selectedCaptured.push(enosysDataCapturedTs);
      if (provider === "primefi" || provider === "all") selectedCaptured.push(primefiDataCapturedTs);

      const validCaptured = selectedCaptured.filter((ts) => ts != null);
      const dataCapturedTs = validCaptured.length ? Math.max(...validCaptured) : null;
      const isStale =
        DATA_STALE_MINUTES > 0 && validCaptured.length > 0
          ? validCaptured.some((ts) => nowTs - ts > DATA_STALE_MINUTES * 60)
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
        const col2 = rows.map((s) => `${formatNumber(s.debtTotal, 2)} ${(s.debtSymbol || CDP_SYMBOL)}`).join("\n");
        const col3 = rows.map((s) => `${formatNumber(s.collTotal, 4)} ${s.collSymbol}`).join("\n");
        return { col1, col2, col3 };
      };

      const redCols = toSummaryRows(redemptionSummary);
      const liqCols = toSummaryRows(liquidationSummary);
      const loanCols = (() => {
        const keys = new Set([
          ...loanOpsSummary.keys(),
          ...redemptionSummary.keys(),
          ...liquidationSummary.keys(),
        ]);
        const rows = Array.from(keys).map((key) => {
          const loan = loanOpsSummary.get(key) || {
            contractKey: key,
            collSymbol: getLoanCollMeta(loanMetaMap, key).symbol,
            debtSymbol: CDP_SYMBOL,
            count: 0,
            borrowedTotal: 0,
            repaidTotal: 0,
            feeTotal: 0,
            feeOpenTotal: 0,
            feeAdjustTotal: 0,
            feeIrChangeTotal: 0,
            feeOtherTotal: 0,
            inferredInterestTotal: 0,
          };
          const red = redemptionSummary.get(key);
          const liq = liquidationSummary.get(key);
          const repaidByRed = red ? red.debtTotal : 0;
          const repaidByLiq = liq ? liq.debtTotal : 0;
          const effectiveRepaid = loan.repaidTotal + repaidByRed + repaidByLiq;
          return {
            collSymbol: loan.collSymbol,
            debtSymbol: loan.debtSymbol,
            count: loan.count,
            borrowedTotal: loan.borrowedTotal,
            repaidTotal: loan.repaidTotal,
            repaidByRed,
            repaidByLiq,
            effectiveRepaid,
            feeTotal: loan.feeTotal,
            feeOpenTotal: loan.feeOpenTotal,
            feeAdjustTotal: loan.feeAdjustTotal,
            feeIrChangeTotal: loan.feeIrChangeTotal,
            feeOtherTotal: loan.feeOtherTotal,
            inferredInterestTotal: loan.inferredInterestTotal,
          };
        });
        if (!rows.length) return { col1: "NONE", col2: "", col3: "", fees: "" };
        const col1 = rows.map((s) => `${s.collSymbol} (${s.count})`).join("\n");
        const col2 = rows.map((s) => `${formatNumber(s.borrowedTotal, 2)} ${(s.debtSymbol || CDP_SYMBOL)}`).join("\n");
        const col3 = rows.map((s) => `${formatNumber(s.effectiveRepaid, 2)} ${(s.debtSymbol || CDP_SYMBOL)}`).join("\n");
        const fees = rows
          .filter((s) => s.feeTotal > 0)
          .map((s) => `${s.collSymbol}: ${formatNumber(s.feeTotal, 2)} ${(s.debtSymbol || CDP_SYMBOL)}`)
          .join("\n");
        const breakdown = rows
          .map(
            (s) =>
              `${s.collSymbol}: direct ${formatNumber(s.repaidTotal, 2)} + redemption ${formatNumber(
                s.repaidByRed,
                2
              )} + liquidation ${formatNumber(s.repaidByLiq, 2)} ${(s.debtSymbol || CDP_SYMBOL)}`
          )
          .join("\n");
        const feeBreakdown = rows
          .filter((s) => s.feeTotal > 0)
          .map(
            (s) =>
              `${s.collSymbol}: open ${formatNumber(s.feeOpenTotal, 2)} | adjust ${formatNumber(
                s.feeAdjustTotal,
                2
              )} | IR-change ${formatNumber(s.feeIrChangeTotal, 2)} | other ${formatNumber(
                s.feeOtherTotal,
                2
              )} ${(s.debtSymbol || CDP_SYMBOL)}`
          )
          .join("\n");
        const interestApplied = rows
          .filter((s) => s.inferredInterestTotal > 0)
          .map(
            (s) =>
              `${s.collSymbol}: ${formatNumber(s.inferredInterestTotal, 2)} ${(s.debtSymbol || CDP_SYMBOL)}`
          )
          .join("\n");
        return { col1, col2, col3, fees, breakdown, feeBreakdown, interestApplied };
      })();
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
        .setDescription(`Provider: ${provider === "all" ? "All" : provider === "primefi" ? "PrimeFi" : "Enosys"}\nPeriod: ${range.label}`)
        .addFields({ name: "Range", value: rangeLabel })
        .addFields(
          { name: "Loan Ops", value: loanCols.col1, inline: true },
          { name: "Total Borrowed", value: loanCols.col2, inline: true },
          { name: "Total Debt Reduced", value: loanCols.col3, inline: true },
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

      if (loanCols.fees) {
        embed.addFields({
          name: "Loan Op Fees (Total)",
          value: loanCols.fees,
          inline: false,
        });
        embed.addFields({
          name: "Fee Breakdown (Totals)",
          value: loanCols.feeBreakdown,
          inline: false,
        });
        embed.addFields({
          name: "Estimated Loan Interest Cost",
          value: loanCols.interestApplied,
          inline: false,
        });
        embed.addFields({
          name: "Loan Debt Reduction Breakdown",
          value: loanCols.breakdown,
          inline: false,
        });
      }

      const noTx = combined.length === 0;
      const hasNonExchangeOnly =
        combined.length > 0 &&
        loanOpsSummary.size === 0 &&
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
