// ./utils/logger.js

const DEBUG_LEVEL = Number.parseInt(process.env.DEBUG || "0", 10);

/**
 * Log levels:
 *   0 = errors only
 *   1 = warnings
 *   2 = info
 *   3 = debug
 *
 * startup() always prints, regardless of DEBUG.
 *
 * Color behavior:
 * - Enabled automatically when running in a TTY
 * - Disabled if NO_COLOR is set
 * - Forced on if FORCE_COLOR is set
 */

const isTTY = Boolean(process.stdout.isTTY);
const useColor =
  (process.env.FORCE_COLOR ? true : isTTY) && !process.env.NO_COLOR;

const ANSI = {
  reset: "\x1b[0m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  green: "\x1b[32m",
  gray: "\x1b[90m",
};

function ts() {
  return new Date().toISOString();
}

function formatLabel(label) {
  if (!useColor) return `[${label}]`;

  switch (label) {
    case "STARTUP":
      return `${ANSI.green}[${label}]${ANSI.reset}`;
    case "ERROR":
      return `${ANSI.red}[${label}]${ANSI.reset}`;
    case "WARN":
      return `${ANSI.yellow}[${label}]${ANSI.reset}`;
    case "INFO":
      return `${ANSI.blue}[${label}]${ANSI.reset}`;
    case "DEBUG":
      return `${ANSI.gray}[${label}]${ANSI.reset}`;
    default:
      return `[${label}]`;
  }
}

function _log(label, args) {
  const stamp = useColor
    ? `${ANSI.dim}[${ts()}]${ANSI.reset}`
    : `[${ts()}]`;

  console.log(`${stamp} ${formatLabel(label)}`, ...args);
}

function startup(...args) {
  _log("STARTUP", args);
}

function error(...args) {
  _log("ERROR", args);
}

function warn(...args) {
  if (DEBUG_LEVEL >= 1) _log("WARN", args);
}

function info(...args) {
  if (DEBUG_LEVEL >= 2) _log("INFO", args);
}

function debug(...args) {
  if (DEBUG_LEVEL >= 3) _log("DEBUG", args);
}

module.exports = {
  startup,
  error,
  warn,
  info,
  debug,
};
