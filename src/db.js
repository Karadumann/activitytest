"use strict";

const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

const dataDir = path.join(__dirname, "..", "data");
fs.mkdirSync(dataDir, { recursive: true });
const dbPath = path.join(dataDir, "activity.sqlite");

const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

db.exec(`
CREATE TABLE IF NOT EXISTS user_sessions (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  guild_id TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_start ON user_sessions(user_id, start_ts);
CREATE INDEX IF NOT EXISTS idx_user_sessions_end ON user_sessions(end_ts);

CREATE TABLE IF NOT EXISTS status_sessions (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  guild_id TEXT NOT NULL,
  status_text TEXT,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_status_sessions_user_start ON status_sessions(user_id, start_ts);
CREATE INDEX IF NOT EXISTS idx_status_sessions_end ON status_sessions(end_ts);

CREATE TABLE IF NOT EXISTS daily_aggregates (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  guild_id TEXT NOT NULL,
  date_day TEXT NOT NULL,
  online_ms INTEGER NOT NULL DEFAULT 0,
  status_ms INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(user_id, guild_id, date_day)
);
CREATE INDEX IF NOT EXISTS idx_daily_agg_guild_day ON daily_aggregates(guild_id, date_day);

CREATE TABLE IF NOT EXISTS weekly_aggregates (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  guild_id TEXT NOT NULL,
  week_label TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER NOT NULL,
  online_ms INTEGER NOT NULL DEFAULT 0,
  status_ms INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(user_id, guild_id, week_label)
);
CREATE INDEX IF NOT EXISTS idx_weekly_agg_guild_week ON weekly_aggregates(guild_id, week_label);

CREATE TABLE IF NOT EXISTS monthly_aggregates (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  guild_id TEXT NOT NULL,
  month_label TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER NOT NULL,
  online_ms INTEGER NOT NULL DEFAULT 0,
  status_ms INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(user_id, guild_id, month_label)
);
CREATE INDEX IF NOT EXISTS idx_monthly_agg_guild_month ON monthly_aggregates(guild_id, month_label);
`);

const stmt = {
  getOpenOnline: db.prepare("SELECT * FROM user_sessions WHERE user_id = ? AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1"),
  startOnline: db.prepare("INSERT INTO user_sessions (user_id, guild_id, start_ts) VALUES (?, ?, ?)"),
  endOnlineById: db.prepare("UPDATE user_sessions SET end_ts = ? WHERE id = ?"),

  getOpenStatus: db.prepare("SELECT * FROM status_sessions WHERE user_id = ? AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1"),
  startStatus: db.prepare("INSERT INTO status_sessions (user_id, guild_id, status_text, start_ts) VALUES (?, ?, ?, ?)"),
  endStatusById: db.prepare("UPDATE status_sessions SET end_ts = ? WHERE id = ?"),

  getOnlineBetween: db.prepare(
    "SELECT * FROM user_sessions WHERE user_id = ? AND start_ts <= ? AND (end_ts IS NULL OR end_ts >= ?) ORDER BY start_ts ASC"
  ),
  getStatusBetween: db.prepare(
    "SELECT * FROM status_sessions WHERE user_id = ? AND start_ts <= ? AND (end_ts IS NULL OR end_ts >= ?) ORDER BY start_ts ASC"
  ),
};

const startOnlineSession = (userId, guildId, ts) => {
  const open = stmt.getOpenOnline.get(userId);
  if (!open) stmt.startOnline.run(userId, guildId, ts);
};

const endOnlineSession = (userId, ts) => {
  const open = stmt.getOpenOnline.get(userId);
  if (open) stmt.endOnlineById.run(ts, open.id);
};

const startStatusSession = (userId, guildId, ts, text) => {
  const open = stmt.getOpenStatus.get(userId);
  if (!open) stmt.startStatus.run(userId, guildId, text || null, ts);
};

const endStatusSession = (userId, ts) => {
  const open = stmt.getOpenStatus.get(userId);
  if (open) stmt.endStatusById.run(ts, open.id);
};

const getOnlineSessionsBetween = (userId, startMs, endMs) => {
  return stmt.getOnlineBetween.all(userId, endMs, startMs);
};

const getStatusSessionsBetween = (userId, startMs, endMs) => {
  return stmt.getStatusBetween.all(userId, endMs, startMs);
};

const overlapDuration = (startTs, endTs, rangeStart, rangeEnd, nowMs = Date.now()) => {
  const s = Math.max(startTs, rangeStart);
  const e = Math.min(endTs ?? nowMs, rangeEnd);
  return e > s ? e - s : 0;
};

const sumOverlap = (sessions, rangeStart, rangeEnd) => {
  return sessions.reduce((acc, s) => acc + overlapDuration(s.start_ts, s.end_ts, rangeStart, rangeEnd), 0);
};


const upsertDaily = db.prepare(`
  INSERT INTO daily_aggregates (user_id, guild_id, date_day, online_ms, status_ms, created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(user_id, guild_id, date_day) DO UPDATE SET
    online_ms = excluded.online_ms,
    status_ms = excluded.status_ms,
    updated_at = excluded.updated_at
`);

const upsertWeekly = db.prepare(`
  INSERT INTO weekly_aggregates (user_id, guild_id, week_label, start_ts, end_ts, online_ms, status_ms, created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(user_id, guild_id, week_label) DO UPDATE SET
    start_ts = excluded.start_ts,
    end_ts = excluded.end_ts,
    online_ms = excluded.online_ms,
    status_ms = excluded.status_ms,
    updated_at = excluded.updated_at
`);

const upsertMonthly = db.prepare(`
  INSERT INTO monthly_aggregates (user_id, guild_id, month_label, start_ts, end_ts, online_ms, status_ms, created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(user_id, guild_id, month_label) DO UPDATE SET
    start_ts = excluded.start_ts,
    end_ts = excluded.end_ts,
    online_ms = excluded.online_ms,
    status_ms = excluded.status_ms,
    updated_at = excluded.updated_at
`);

const rankDailyBy = db.prepare(`
  SELECT user_id, online_ms, status_ms FROM daily_aggregates
  WHERE guild_id = ? AND date_day = ?
  ORDER BY CASE WHEN ? = 'online' THEN online_ms ELSE status_ms END DESC
  LIMIT ?
`);

const rankWeeklyBy = db.prepare(`
  SELECT user_id, online_ms, status_ms FROM weekly_aggregates
  WHERE guild_id = ? AND week_label = ?
  ORDER BY CASE WHEN ? = 'online' THEN online_ms ELSE status_ms END DESC
  LIMIT ?
`);

const rankMonthlyBy = db.prepare(`
  SELECT user_id, online_ms, status_ms FROM monthly_aggregates
  WHERE guild_id = ? AND month_label = ?
  ORDER BY CASE WHEN ? = 'online' THEN online_ms ELSE status_ms END DESC
  LIMIT ?
`);

const saveDailyAggregate = (userId, guildId, dateDay, onlineMs, statusMs, nowMs = Date.now()) => {
  upsertDaily.run(userId, guildId, dateDay, onlineMs, statusMs, nowMs, nowMs);
};

const saveWeeklyAggregate = (userId, guildId, weekLabel, startTs, endTs, onlineMs, statusMs, nowMs = Date.now()) => {
  upsertWeekly.run(userId, guildId, weekLabel, startTs, endTs, onlineMs, statusMs, nowMs, nowMs);
};

const saveMonthlyAggregate = (userId, guildId, monthLabel, startTs, endTs, onlineMs, statusMs, nowMs = Date.now()) => {
  upsertMonthly.run(userId, guildId, monthLabel, startTs, endTs, onlineMs, statusMs, nowMs, nowMs);
};

const getTopDaily = (guildId, dateDay, metric, limit) => rankDailyBy.all(guildId, dateDay, metric, limit);
const getTopWeekly = (guildId, weekLabel, metric, limit) => rankWeeklyBy.all(guildId, weekLabel, metric, limit);
const getTopMonthly = (guildId, monthLabel, metric, limit) => rankMonthlyBy.all(guildId, monthLabel, metric, limit);

module.exports = {
  db,
  startOnlineSession,
  endOnlineSession,
  startStatusSession,
  endStatusSession,
  getOnlineSessionsBetween,
  getStatusSessionsBetween,
  sumOverlap,
  saveDailyAggregate,
  saveWeeklyAggregate,
  saveMonthlyAggregate,
  getTopDaily,
  getTopWeekly,
  getTopMonthly,
};