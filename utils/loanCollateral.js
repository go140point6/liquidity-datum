function inferLoanCollMeta(contractKey) {
  const key = String(contractKey || "").toLowerCase();
  if (key.includes("stxrp")) return { symbol: "STXRP", decimals: 18 };
  if (key.includes("sflr")) return { symbol: "SFLR", decimals: 18 };
  if (key.includes("fxrp")) return { symbol: "FXRP", decimals: 6 };
  if (key.includes("wflr")) return { symbol: "WFLR", decimals: 18 };
  return { symbol: "COLL", decimals: 18 };
}

function loadLoanCollMetaMap(db) {
  const columns = db.prepare("PRAGMA table_info(loan_contracts)").all().map((row) => row.name);
  const hasSymbol = columns.includes("coll_symbol");
  const hasDecimals = columns.includes("coll_decimals");
  const selectList = [
    "contract_key",
    hasSymbol ? "coll_symbol" : "NULL AS coll_symbol",
    hasDecimals ? "coll_decimals" : "NULL AS coll_decimals",
  ].join(", ");
  const rows = db.prepare(`SELECT ${selectList} FROM loan_contracts`).all();

  return new Map(
    rows.map((row) => [
      row.contract_key,
      {
        symbol: row.coll_symbol || inferLoanCollMeta(row.contract_key).symbol,
        decimals: Number.isFinite(row.coll_decimals)
          ? row.coll_decimals
          : inferLoanCollMeta(row.contract_key).decimals,
      },
    ])
  );
}

function getLoanCollMeta(metaMap, contractKey) {
  return metaMap.get(contractKey) || inferLoanCollMeta(contractKey);
}

module.exports = {
  getLoanCollMeta,
  inferLoanCollMeta,
  loadLoanCollMetaMap,
};
