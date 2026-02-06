// ./utils/intlNumberFormats.js

const DEFAULT_LOCALE = "en-US";

function assertFractionDigits(min, max) {
  if (!Number.isInteger(min) || !Number.isInteger(max) || min < 0 || max < min) {
    throw new Error(
      `Invalid fraction digits: min=${min}, max=${max}. ` +
      "Expected integers where 0 ≤ min ≤ max."
    );
  }
}

function createCurrencyFormatter(currency, minFractionDigits, maxFractionDigits) {
  if (!currency || typeof currency !== "string") {
    throw new Error("Currency code must be a non-empty string (e.g. 'USD').");
  }

  assertFractionDigits(minFractionDigits, maxFractionDigits);

  return new Intl.NumberFormat(DEFAULT_LOCALE, {
    style: "currency",
    currency,
    minimumFractionDigits: minFractionDigits,
    maximumFractionDigits: maxFractionDigits,
  });
}

function createDecimalFormatter(minFractionDigits, maxFractionDigits) {
  assertFractionDigits(minFractionDigits, maxFractionDigits);

  return new Intl.NumberFormat(DEFAULT_LOCALE, {
    style: "decimal",
    minimumFractionDigits: minFractionDigits,
    maximumFractionDigits: maxFractionDigits,
  });
}

function createPercentFormatter(minFractionDigits, maxFractionDigits) {
  assertFractionDigits(minFractionDigits, maxFractionDigits);

  return new Intl.NumberFormat(DEFAULT_LOCALE, {
    style: "percent",
    minimumFractionDigits: minFractionDigits,
    maximumFractionDigits: maxFractionDigits,
  });
}

module.exports = {
  createCurrencyFormatter,
  createDecimalFormatter,
  createPercentFormatter,
};
