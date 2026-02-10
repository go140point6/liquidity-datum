function csvEscape(v) {
  if (v == null) return "";
  const s = String(v);
  if (s.includes("\"")) {
    const escaped = s.replace(/\"/g, "\"\"");
    return `"${escaped}"`;
  }
  if (s.includes(",") || s.includes("\n")) {
    return `"${s}"`;
  }
  return s;
}

function toCsv(headers, rows) {
  const out = [];
  out.push(headers.map(csvEscape).join(","));
  for (const row of rows) {
    out.push(row.map(csvEscape).join(","));
  }
  return out.join("\n") + "\n";
}

module.exports = { toCsv };
