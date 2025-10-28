# Discord Activity Monitor Bot

A Discord bot that tracks online presence and custom status for members with a specific role, aggregates daily/weekly/monthly totals, and provides a lightweight web panel for ranking and reporting.

## Purpose
- Monitor whether members use a desired status text and how long they stay online.
- Produce time-based aggregates and rank members by online or desired status duration.
- Offer slash commands for quick, ephemeral summaries.

## Requirements
- Node.js 18+
- Discord Bot Token
- Discord bot intents enabled: `GUILD_MEMBERS`, `GUILD_PRESENCES`

## Environment
- `DISCORD_TOKEN`: Your bot token from Discord Developer Portal.
- `PANEL_SECRET` (optional): If set, panel API requires this token via `X-Panel-Token` header.

## Setup
1. Install dependencies: `npm install`
2. Create `.env` from `.env.example` and set `DISCORD_TOKEN`
3. Configure `config.json`:
   - `guildId`: Target server ID
   - `controlChannelId`: Channel ID where admin-only commands are allowed
   - `reportChannelId`: Channel ID to send summary embeds (falls back to `controlChannelId`)
   - `adminRoleId`: Role ID permitted to run admin commands
   - `watchRoleId`: Role ID for members to be monitored
   - `desiredStatusText`: Keyword to detect in custom status
   - `checkIntervalMinutes`: Presence polling interval (e.g., 2 or 5)
   - `panelPort`: Web panel port (default `3000`)
   - `topNDefault`: Default ranking list size
   - `enableDailySummary`/`enableWeeklySummary`/`enableMonthlySummary`: Auto embed scheduling toggles

## Run
- `npm start`

## Slash Commands
- `/overview` — Current summary: monitored count, online count, non-compliant list.
- `/status user:@mention` — Snapshot of a chosen member (online, custom status, compliance).
- `/report user:@mention [period]` — Daily/weekly/monthly totals and first 20 sessions.
- `/mytime` — Your personal online totals (today/this week/this month), ephemeral.

Admin-only commands are restricted to `controlChannelId` and `adminRoleId`.

## Data
- SQLite at `data/activity.sqlite` is created automatically.
- Tables:
  - `user_sessions(user_id, guild_id, start_ts, end_ts)` — online sessions
  - `status_sessions(user_id, guild_id, status_text, start_ts, end_ts)` — desired status sessions

## Web Panel
- Local panel at `http://localhost:3000/`
- Load ranking, preview embeds, pick a Discord channel, and send embeds.
- Requires `PANEL_SECRET` if configured for panel API auth.

## License
- ISC