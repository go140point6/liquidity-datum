function parseSqliteTimestamp(ts) {
  if (!ts) return null;
  const raw = String(ts);
  const iso = raw.includes("T") ? raw : raw.replace(" ", "T");
  const ms = Date.parse(iso.endsWith("Z") ? iso : `${iso}Z`);
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

function inferPrimefiMarketMeta(marketKey) {
  const suffix = String(marketKey || "").replace(/^primefi_/i, "");
  const parts = suffix.split("_").filter(Boolean);
  const collSymbol = parts[0] ? parts[0].toUpperCase() : "COLL";
  const debtSymbol = parts[1] ? parts[1].toUpperCase() : "DEBT";
  return { collSymbol, debtSymbol };
}

function loadPrimefiMarketMetaMap(db) {
  const rows = db
    .prepare(
      `
      SELECT market_key, snapshot_json
      FROM sentinel.primefi_loan_position_snapshots
    `
    )
    .all();

  const map = new Map();
  for (const row of rows) {
    if (map.has(row.market_key)) continue;
    try {
      const snapshot = JSON.parse(row.snapshot_json);
      map.set(row.market_key, {
        collSymbol: snapshot.collSymbol || inferPrimefiMarketMeta(row.market_key).collSymbol,
        debtSymbol: snapshot.debtSymbol || inferPrimefiMarketMeta(row.market_key).debtSymbol,
      });
    } catch {
      map.set(row.market_key, inferPrimefiMarketMeta(row.market_key));
    }
  }
  return map;
}

function getPrimefiMarketMeta(metaMap, marketKey) {
  return metaMap.get(marketKey) || inferPrimefiMarketMeta(marketKey);
}

module.exports = {
  getPrimefiMarketMeta,
  inferPrimefiMarketMeta,
  loadPrimefiMarketMetaMap,
  parseSqliteTimestamp,
};
