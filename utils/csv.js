// ./utils/csv.js

const fs = require("fs");
const { parse } = require("csv-parse/sync");

/**
 * Read a CSV file and return an array of row objects.
 *
 * - Returns [] if the file does not exist or is empty
 * - Throws only on truly unexpected parse or I/O errors
 *
 * @param {string} csvPath - Path to the CSV file
 * @returns {Array<Record<string, string>>}
 */
function readCsvRows(csvPath) {
  if (!csvPath || typeof csvPath !== "string") {
    throw new Error("readCsvRows(csvPath) requires a file path string.");
  }

  if (!fs.existsSync(csvPath)) {
    console.warn(`[WARN] CSV file not found: ${csvPath}`);
    return [];
  }

  let content;
  try {
    content = fs.readFileSync(csvPath, "utf8");
  } catch (err) {
    console.error(`[ERROR] Failed to read CSV file: ${csvPath}`);
    throw err;
  }

  if (!content.trim()) {
    return [];
  }

  try {
    return parse(content, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
  } catch (err) {
    console.error(`[ERROR] Failed to parse CSV file: ${csvPath}`);
    throw err;
  }
}

module.exports = {
  readCsvRows,
};
