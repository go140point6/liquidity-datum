# discordjs-v14-bot-template

A clean, minimal **Discord.js v14 bot template** built for rapid iteration and reuse across multiple bots.

This template focuses on:
- Clear project structure
- Safe startup and error handling
- Environment-variable driven configuration
- Consistent, controllable logging
- Slash-command–first design (Discord.js v14)

---

## What this template provides

- Discord.js v14 client setup
- Slash command loading and registration (guild-scoped for fast iteration)
- Centralized environment validation
- Structured event handlers (`onReady`, `onInteraction`, `onMessage`)
- A tiny custom logger with:
  - log levels
  - always-visible startup messages
  - optional colored output
- Example commands and scheduled task logic for reference

---

## Requirements

- **Node.js 18+**
- A Discord application and bot token

This project includes a `.nvmrc` file. If you use nvm, run:

```bash
nvm use
```

Set the .nvmrc file to your major Node version (i.e. 24, 22, 20, or minimum 18).
Tested on Node 20 and 24, .nvmrc is currently set to 24.

---

## Installation

Clone the repository and install dependencies:

```bash
npm install
```

Create your environment file:

```bash
cp .env-template .env
```

Fill in the required values in `.env`.

Start the bot (to test):

```bash
node start
```

For development, you can also run:

```bash
DEBUG=2 npm run dev
```

RECOMMENDED: Use pm2 or other process manager for production.

---

## Environment configuration

All required configuration is provided via environment variables.

At minimum, you must set:
- `BOT_TOKEN`
- `CLIENT_ID`
- `GUILD_ID`

Optional variables control logging verbosity and example schedulers.

See `.env-template` for the full list.

---

## Logging

This template uses a **custom lightweight logger** instead of raw `console.*`.

Features include:
- Log levels (`STARTUP`, `ERROR`, `WARN`, `INFO`, `DEBUG`)
- Always-visible startup confirmation
- Optional ANSI color output
- Single-point verbosity control via `.env`

Full documentation:
```
./docs/LOGGER.md
```

---

## License

MIT

