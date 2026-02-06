# Logger Utility (`./utils/logger.js`)

This project uses a **tiny, dependency-free logger** that standardizes output across the bot and lets you control verbosity using `.env` variables.

- No external libraries
- ISO timestamps
- Consistent labels (`STARTUP`, `ERROR`, `WARN`, `INFO`, `DEBUG`)
- Optional ANSI colors (great locally; controllable for CI/containers)

---

## File: `./utils/logger.js`

Exports:

- `log.startup(...)` — **always prints** (use for startup/lifecycle confirmations)
- `log.error(...)` — always prints
- `log.warn(...)` — prints when `DEBUG >= 1`
- `log.info(...)` — prints when `DEBUG >= 2`
- `log.debug(...)` — prints when `DEBUG >= 3`

---

## `.env` configuration

### DEBUG (verbosity)

```env
# 0 = errors only
# 1 = warnings + errors
# 2 = info + warnings + errors
# 3 = debug + info + warnings + errors
DEBUG=2
```

`startup()` logs show at **all** debug levels.

### Color control

Colors are enabled automatically when running in a TTY.

Optional overrides:

```env
# Disable colors (CI, log files)
NO_COLOR=1

# Force colors (Docker / piped output)
FORCE_COLOR=1
```

---

## Output example

```
[2026-01-09T16:57:30.742Z] [STARTUP] Ready! Logged in as MyBot#1234
[2026-01-09T16:57:31.026Z] [STARTUP] Loaded 2 application (/) commands.
```

Color mapping:
- STARTUP → green
- ERROR → red
- WARN → yellow
- INFO → blue
- DEBUG → gray

---

## Usage examples

```js
log.startup("Bot is online");
log.error("Database connection failed");
log.warn("Missing optional config value");
log.info("Registered 12 commands");
log.debug("Full interaction object:", interaction);
```

---

## Best practices

- Use `startup` for lifecycle confirmations
- Use `error` for actionable failures
- Use `warn` for recoverable issues
- Use `info` for normal operational milestones
- Use `debug` for high-volume diagnostics
- Never log secrets (tokens, private keys)

---

## Sanity check

- `DEBUG=0` → STARTUP + ERROR
- `DEBUG=1` → STARTUP + ERROR + WARN
- `DEBUG=2` → STARTUP + ERROR + WARN + INFO
- `DEBUG=3` → everything
