"use strict";

require("dotenv").config();
const {
  Client,
  GatewayIntentBits,
  ActivityType,
  EmbedBuilder
} = require("discord.js");
const config = require("../config.json");
const {
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
  getTopMonthly
} = require("./db");
const cron = require("node-cron");
const express = require("express");

const nowTs = () => Date.now();
const formatTime = (ts) => {
  const d = new Date(ts);
  return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
};
const formatDate = (ts) => new Date(ts).toLocaleDateString("en-US");
const formatDuration = (ms) => {
  const totalMinutes = Math.floor(ms / 60000);
  const h = Math.floor(totalMinutes / 60);
  const m = totalMinutes % 60;
  return `${h} h ${m} min`;
};

const startOfDay = (d = new Date()) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
const endOfDay = (d = new Date()) => startOfDay(d) + 24 * 60 * 60 * 1000 - 1;
const startOfWeek = (d = new Date()) => {
  const day = d.getDay();
  const diffToMonday = (day === 0 ? -6 : 1 - day);
  const monday = new Date(d.getFullYear(), d.getMonth(), d.getDate() + diffToMonday);
  return startOfDay(monday);
};
const endOfWeek = (d = new Date()) => startOfWeek(d) + 7 * 24 * 60 * 60 * 1000 - 1;
const startOfMonth = (d = new Date()) => new Date(d.getFullYear(), d.getMonth(), 1).getTime();
const endOfMonth = (d = new Date()) => new Date(d.getFullYear(), d.getMonth() + 1, 0, 23, 59, 59, 999).getTime();
const isoWeekLabel = (d = new Date()) => {
  const start = new Date(startOfWeek(d));
  const oneJan = new Date(start.getFullYear(), 0, 1);
  const dayOfYear = Math.floor((start - oneJan) / 86400000) + 1;
  const week = Math.ceil(dayOfYear / 7);
  return `${start.getFullYear()}-W${String(week).padStart(2, "0")}`;
};
const monthLabel = (d = new Date()) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;

const isWatchedMember = (member) => member?.roles?.cache?.has(config.watchRoleId);
const getPresence = (member) => member?.presence || null;
const getCustomStatusText = (presence) => {
  if (!presence) return null;
  const custom = presence.activities?.find((a) => a.type === ActivityType.Custom);
  return custom?.state || null;
};
const isOnlineStatus = (presence) => presence?.status === "online";
const hasDesiredStatus = (text) => {
  if (!text) return false;
  const want = (config.desiredStatusText || "").trim().toLowerCase();
  if (!want) return false;
  return text.toLowerCase().includes(want);
};

const handleMemberSnapshot = (member) => {
  try {
    if (!isWatchedMember(member)) return;
    const presence = getPresence(member);
    const ts = nowTs();

    if (isOnlineStatus(presence)) {
      startOnlineSession(member.id, member.guild.id, ts);
    } else {
      endOnlineSession(member.id, ts);
    }

    const customText = getCustomStatusText(presence);
    if (hasDesiredStatus(customText)) {
      startStatusSession(member.id, member.guild.id, ts, customText);
    } else {
      endStatusSession(member.id, ts);
    }
  } catch (err) {
    console.error("Snapshot error:", err);
  }
};

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildPresences
  ]
});

let BOT_GUILD = null;
let CLIENT_READY = false;

client.once("ready", async () => {
  console.log(`Bot logged in: ${client.user.tag}`);
  CLIENT_READY = true;
  if (!config.guildId) {
    console.warn("config.guildId is missing!");
  }

  const guild = client.guilds.cache.get(config.guildId);
  if (guild) {
    BOT_GUILD = guild;
    await guild.members.fetch();

    const commands = [
      {
        name: "overview",
        description: "Show overall monitoring summary"
      },
      {
        name: "status",
        description: "Show a member's current presence and custom status",
        options: [
          {
            name: "user",
            description: "Target user",
            type: 6,
            required: true
          }
        ]
      },
      {
        name: "report",
        description: "Report a member's online/status totals by period",
        options: [
          {
            name: "user",
            description: "Target user",
            type: 6,
            required: true
          },
          {
            name: "period",
            description: "Time range",
            type: 3,
            required: false,
            choices: [
              { name: "daily", value: "daily" },
              { name: "weekly", value: "weekly" },
              { name: "monthly", value: "monthly" }
            ]
          }
        ]
      },
      {
        name: "mytime",
        description: "Show your own online totals (ephemeral)"
      }
    ];
    await client.application.commands.set(commands, config.guildId);
    console.log("Slash commands registered.");

    try {
      const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
      watchers.forEach((m) => handleMemberSnapshot(m));
    } catch (e) {
      console.error("Initial snapshot error:", e);
    }

    const intervalMs = Math.max(1, Number(config.checkIntervalMinutes || 5)) * 60000;
    setInterval(async () => {
      try {
        await guild.members.fetch();
        const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
        watchers.forEach((m) => handleMemberSnapshot(m));
        console.log(`Periodic check done - ${watchers.size} users`);
      } catch (e) {
        console.error("Periodic check error:", e);
      }
    }, intervalMs);

    try {
      if (config.enableDailySummary) {
        cron.schedule("5 0 * * *", async () => {
          await runDailySummary(guild);
        });
      }
      if (config.enableWeeklySummary) {
        cron.schedule("10 0 * * 1", async () => {
          await runWeeklySummary(guild);
        });
      }
      if (config.enableMonthlySummary) {
        cron.schedule("15 0 1 * *", async () => {
          await runMonthlySummary(guild);
        });
      }
      console.log("Schedulers active.");
    } catch (e) {
      console.error("Scheduler setup error:", e);
    }

    try {
      startPanelServer();
    } catch (e) {
      console.error("Panel server failed to start:", e);
    }
  } else {
    console.warn("Bot is not in the specified guild or it's not cached.");
  }
});

client.on("presenceUpdate", (oldPresence, newPresence) => {
  try {
    const member = newPresence?.member;
    if (member && isWatchedMember(member)) {
      handleMemberSnapshot(member);
    }
  } catch (err) {
    console.error("presenceUpdate error:", err);
  }
});


async function computeAndStoreDaily(guild, dateObj) {
  await guild.members.fetch();
  const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
  const startMs = startOfDay(dateObj);
  const endMs = endOfDay(dateObj);
  const dateDay = `${dateObj.getFullYear()}-${String(dateObj.getMonth() + 1).padStart(2, "0")}-${String(dateObj.getDate()).padStart(2, "0")}`;
  watchers.forEach((m) => {
    try {
      const online = sumOverlap(getOnlineSessionsBetween(m.id, startMs, endMs), startMs, endMs);
      const status = sumOverlap(getStatusSessionsBetween(m.id, startMs, endMs), startMs, endMs);
      saveDailyAggregate(m.id, guild.id, dateDay, online, status);
    } catch (e) {
      console.error("Günlük aggregate hata:", e);
    }
  });
  return { watchersCount: watchers.size, startMs, endMs, dateDay };
}

async function computeAndStoreWeekly(guild, startObj) {
  await guild.members.fetch();
  const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
  const startMs = startOfWeek(startObj);
  const endMs = endOfWeek(startObj);
  const label = isoWeekLabel(startObj);
  watchers.forEach((m) => {
    try {
      const online = sumOverlap(getOnlineSessionsBetween(m.id, startMs, endMs), startMs, endMs);
      const status = sumOverlap(getStatusSessionsBetween(m.id, startMs, endMs), startMs, endMs);
      saveWeeklyAggregate(m.id, guild.id, label, startMs, endMs, online, status);
    } catch (e) {
      console.error("Haftalık aggregate hata:", e);
    }
  });
  return { watchersCount: watchers.size, startMs, endMs, label };
}

async function computeAndStoreMonthly(guild, startObj) {
  await guild.members.fetch();
  const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
  const startMs = startOfMonth(startObj);
  const endMs = endOfMonth(startObj);
  const label = monthLabel(startObj);
  watchers.forEach((m) => {
    try {
      const online = sumOverlap(getOnlineSessionsBetween(m.id, startMs, endMs), startMs, endMs);
      const status = sumOverlap(getStatusSessionsBetween(m.id, startMs, endMs), startMs, endMs);
      saveMonthlyAggregate(m.id, guild.id, label, startMs, endMs, online, status);
    } catch (e) {
      console.error("Aylık aggregate hata:", e);
    }
  });
  return { watchersCount: watchers.size, startMs, endMs, label };
}

function msToHours(ms) { return (ms / 3600000).toFixed(2); }

async function sendRankEmbed(guild, period, metric, limit, channelIdOverride) {
  const channelId = channelIdOverride || (config.reportChannelId || config.controlChannelId);
  const channel = guild.channels.cache.get(channelId);
  if (!channel) {
    console.warn("Report channel not found.");
    return;
  }
  const guildId = guild.id;
  let rows = [];
  let title = "";
  const now = new Date();
  if (period === "daily") {
    const dateDay = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
    rows = getTopDaily(guildId, dateDay, metric, limit);
    title = `Daily Top ${limit} (${metric === 'online' ? 'Online' : 'Status'})`;
  } else if (period === "weekly") {
    const label = isoWeekLabel(now);
    rows = getTopWeekly(guildId, label, metric, limit);
    title = `Weekly Top ${limit} (${metric === 'online' ? 'Online' : 'Status'})`;
  } else {
    const label = monthLabel(now);
    rows = getTopMonthly(guildId, label, metric, limit);
    title = `Monthly Top ${limit} (${metric === 'online' ? 'Online' : 'Status'})`;
  }
  if (!rows.length) {
    await channel.send({ content: `No ${period} data available yet.` });
    return;
  }
  const list = await Promise.all(rows.map(async (r, i) => {
    const m = await guild.members.fetch(r.user_id).catch(() => null);
    const name = m ? (m.displayName || m.user.username) : r.user_id;
    const onlineH = msToHours(r.online_ms);
    const statusH = msToHours(r.status_ms);
    return `#${i + 1} ${name} — Online: ${onlineH}h, Status: ${statusH}h`;
  }));
  const embed = new EmbedBuilder()
    .setTitle(title)
    .setDescription(list.join("\n"))
    .setColor(0x5865F2);
  await channel.send({ embeds: [embed] });
}

async function runDailySummary(guild) {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  await computeAndStoreDaily(guild, yesterday);
  await sendRankEmbed(guild, "daily", "status", config.topNDefault || 10);
}
async function runWeeklySummary(guild) {
  const prevWeekStart = new Date();
  prevWeekStart.setDate(prevWeekStart.getDate() - 7);
  await computeAndStoreWeekly(guild, prevWeekStart);
  await sendRankEmbed(guild, "weekly", "status", config.topNDefault || 10);
}
async function runMonthlySummary(guild) {
  const prevMonth = new Date();
  prevMonth.setMonth(prevMonth.getMonth() - 1);
  await computeAndStoreMonthly(guild, prevMonth);
  await sendRankEmbed(guild, "monthly", "status", config.topNDefault || 10);
}

let PANEL_STARTED = false;
function startPanelServer() {
  if (PANEL_STARTED) return;
  const app = express();
  app.use(express.json());
  const secret = process.env.PANEL_SECRET || "";

  const requireAuth = (req, res, next) => {
    const token = req.headers["x-panel-token"] || req.query.token;
    if (!secret || token === secret) return next();
    res.status(401).json({ error: "unauthorized" });
  };

  app.get("/api/status", (req, res) => {
    try {
      const intentsObj = client.options?.intents;
      const hasMembers = intentsObj?.has
        ? intentsObj.has(GatewayIntentBits.GuildMembers)
        : ((intentsObj & GatewayIntentBits.GuildMembers) !== 0);
      const hasPresences = intentsObj?.has
        ? intentsObj.has(GatewayIntentBits.GuildPresences)
        : ((intentsObj & GatewayIntentBits.GuildPresences) !== 0);
      const botReady = CLIENT_READY && !!client.user;
      const guildConnected = !!BOT_GUILD;
      const tokenPresent = !!process.env.DISCORD_TOKEN;
      const panelAuthEnabled = !!secret;

      res.json({
        bot_logged_in: botReady,
        guild_connected: guildConnected,
        intents_ok: hasMembers && hasPresences,
        token_present: tokenPresent,
        panel_started: PANEL_STARTED,
        panel_auth_enabled: panelAuthEnabled,
        guild_id: config.guildId || null,
        panel_port: Number(config.panelPort || 3000)
      });
    } catch (e) {
      console.error("/api/status error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  app.get("/", (req, res) => {
    res.sendFile(require("path").join(__dirname, "panel.html"));
  });

  app.get("/api/rank", requireAuth, async (req, res) => {
    const period = req.query.period || "weekly";
    const metric = req.query.metric === "online" ? "online" : "status";
    const limit = Math.min(50, Math.max(1, Number(req.query.limit || config.topNDefault || 10)));
    const now = new Date();
    const guild = BOT_GUILD;
    if (!guild) return res.status(503).json({ error: "bot_not_ready" });
    const guildId = guild.id;
    let rows = [];
    if (period === "daily") rows = getTopDaily(guildId, `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`, metric, limit);
    else if (period === "weekly") rows = getTopWeekly(guildId, isoWeekLabel(now), metric, limit);
    else rows = getTopMonthly(guildId, monthLabel(now), metric, limit);

    const payload = await Promise.all(rows.map(async (r) => {
      const m = await guild.members.fetch(r.user_id).catch(() => null);
      return {
        user_id: r.user_id,
        name: m ? (m.displayName || m.user.username) : r.user_id,
        online_ms: r.online_ms,
        status_ms: r.status_ms,
      };
    }));
    res.json(payload);
  });

  app.post("/api/send_rank_embed", requireAuth, async (req, res) => {
    const period = req.body.period || "weekly";
    const metric = req.body.metric === "online" ? "online" : "status";
    const limit = Math.min(50, Math.max(1, Number(req.body.limit || config.topNDefault || 10)));
    const channelId = req.body.channel_id || null;
    try {
      const guild = BOT_GUILD;
      if (!guild) return res.status(503).json({ error: "bot_not_ready" });
      await sendRankEmbed(guild, period, metric, limit, channelId);
      res.json({ ok: true });
    } catch (e) {
      console.error("Embed send error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  app.get("/api/channels", requireAuth, async (req, res) => {
    const guild = BOT_GUILD;
    if (!guild) return res.status(503).json({ error: "bot_not_ready" });
    try {
      await guild.channels.fetch();
      const list = guild.channels.cache
        .filter((ch) => typeof ch.isTextBased === "function" ? ch.isTextBased() : true)
        .map((ch) => ({ id: ch.id, name: ch.name }))
        .sort((a, b) => a.name.localeCompare(b.name));
      res.json(list);
    } catch (e) {
      console.error("Channel list error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  const port = Number(config.panelPort || 3000);
  app.listen(port, () => {
    console.log(`Panel ready: http://localhost:${port}/`);
  });
  PANEL_STARTED = true;
}

const isAuthorized = (interaction) => {
  try {
    const inChannel = interaction.channelId === config.controlChannelId;
    const hasRole = interaction.member?.roles?.cache?.has(config.adminRoleId);
    return inChannel && hasRole;
  } catch {
    return false;
  }
};

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) return;
  const { commandName } = interaction;

  const guild = interaction.guild;
  await guild.members.fetch();

  if (commandName === "mytime") {
    try {
      const member = await guild.members.fetch(interaction.user.id).catch(() => null);
      if (!member) {
        return interaction.reply({ content: "Member not found.", ephemeral: true });
      }
      const now = new Date();
      const dStart = startOfDay(now), dEnd = endOfDay(now);
      const wStart = startOfWeek(now), wEnd = endOfWeek(now);
      const mStart = startOfMonth(now), mEnd = endOfMonth(now);

      const dOnline = sumOverlap(getOnlineSessionsBetween(member.id, dStart, dEnd), dStart, dEnd);
      const wOnline = sumOverlap(getOnlineSessionsBetween(member.id, wStart, wEnd), wStart, wEnd);
      const mOnline = sumOverlap(getOnlineSessionsBetween(member.id, mStart, mEnd), mStart, mEnd);

      const lines = [
        `User: <@${member.id}>`,
        `Today Online: ${formatDuration(dOnline)}`,
        `This Week Online: ${formatDuration(wOnline)}`,
        `This Month Online: ${formatDuration(mOnline)}`
      ];
      return interaction.reply({ content: lines.join("\n"), ephemeral: true });
    } catch (e) {
      console.error("mytime command error:", e);
      return interaction.reply({ content: "An error occurred.", ephemeral: true });
    }
  }

  if (!isAuthorized(interaction)) {
    return interaction.reply({
      content: "This command is only available in the designated channel and for members with the admin role.",
      ephemeral: true
    });
  }

  if (commandName === "overview") {
    const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
    const total = watchers.size;
    let onlineCount = 0;
    let withStatusCount = 0;
    const nonCompliant = [];

    watchers.forEach((m) => {
      const presence = getPresence(m);
      const online = isOnlineStatus(presence);
      const custom = getCustomStatusText(presence);
      const compliant = hasDesiredStatus(custom);
      if (online) onlineCount++;
      if (compliant) withStatusCount++;
      if (!compliant) nonCompliant.push(m);
    });

    const desc = [
      `Monitored total: ${total}`,
      `Currently online: ${onlineCount}`,
      `Desired status (current): ${withStatusCount}`
    ].join("\n");

    const embed = new EmbedBuilder()
      .setTitle("Monitoring Overview")
      .setDescription(desc)
      .setColor(0x00AE86)
      .addFields(
        nonCompliant.length
          ? [{ name: "Non-compliant (current)", value: nonCompliant.slice(0, 20).map((m) => `<@${m.id}>`).join(", ") }]
          : []
      );

    return interaction.reply({ embeds: [embed], ephemeral: true });
  }

  if (commandName === "status") {
    const user = interaction.options.getUser("user");
    const member = await guild.members.fetch(user.id).catch(() => null);
    if (!member) {
      return interaction.reply({ content: "Member not found.", ephemeral: true });
    }
    const presence = getPresence(member);
    const online = isOnlineStatus(presence);
    const custom = getCustomStatusText(presence);
    const compliant = hasDesiredStatus(custom);

    const lines = [
      `User: <@${member.id}>`,
      `Online: ${online ? "Yes" : "No"}`,
      `Custom Status: ${custom ? `"${custom}"` : "None"}`,
      `Desired status compliance: ${compliant ? "Yes" : "No"}`
    ];
    return interaction.reply({ content: lines.join("\n"), ephemeral: true });
  }

  if (commandName === "report") {
    const user = interaction.options.getUser("user");
    const member = await guild.members.fetch(user.id).catch(() => null);
    if (!member) {
      return interaction.reply({ content: "Member not found.", ephemeral: true });
    }
    const period = interaction.options.getString("period") || "daily";
    let startMs, endMs, title;
    const now = new Date();
    if (period === "daily") {
      startMs = startOfDay(now);
      endMs = endOfDay(now);
      title = `Daily Report (${formatDate(nowTs())})`;
    } else if (period === "weekly") {
      startMs = startOfWeek(now);
      endMs = endOfWeek(now);
      title = `Weekly Report`;
    } else {
      startMs = startOfMonth(now);
      endMs = endOfMonth(now);
      title = `Monthly Report`;
    }

    const onlineSessions = getOnlineSessionsBetween(member.id, startMs, endMs);
    const statusSessions = getStatusSessionsBetween(member.id, startMs, endMs);
    const onlineTotal = sumOverlap(onlineSessions, startMs, endMs);
    const statusTotal = sumOverlap(statusSessions, startMs, endMs);
    const totalRange = endMs - startMs + 1;
    const offlineTotal = Math.max(0, totalRange - onlineTotal);

    const onlineLines = onlineSessions.slice(0, 20).map((s, i) => `#${i + 1} ${formatTime(s.start_ts)} - ${s.end_ts ? formatTime(s.end_ts) : "(ongoing)"}`);
    const statusLines = statusSessions.slice(0, 20).map((s, i) => `#${i + 1} ${formatTime(s.start_ts)} - ${s.end_ts ? formatTime(s.end_ts) : "(ongoing)"} | ${s.status_text ?? ""}`);

    const content = [
      title,
      `User: <@${member.id}>`,
      `Total Online: ${formatDuration(onlineTotal)}`,
      `Total Offline: ${formatDuration(offlineTotal)}`,
      `Desired Status Duration: ${formatDuration(statusTotal)}`,
      "",
      "Online Sessions (first 20):",
      onlineLines.length ? onlineLines.join("\n") : "(no records)",
      "",
      "Desired Status Sessions (first 20):",
      statusLines.length ? statusLines.join("\n") : "(no records)"
    ].join("\n");

    return interaction.reply({ content, ephemeral: true });
  }
});

const token = process.env.DISCORD_TOKEN;
if (!token) {
  console.error("DISCORD_TOKEN not set in .env! Panel will run; bot not logged in.");
} else {
  client.login(token);
}
try { startPanelServer(); } catch {}