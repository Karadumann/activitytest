"use strict";

require("dotenv").config();
const {
  Client,
  GatewayIntentBits,
  ActivityType,
  EmbedBuilder,
  AttachmentBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle
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
  getTopMonthly,
  clearGuildData
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

function sumIntersection(sessionsA, sessionsB, startMs, endMs) {
  try {
    const norm = (s) => (s || []).map(x => ({
      start: Math.max(startMs, x.start_ts),
      end: Math.min(endMs, x.end_ts ?? Date.now())
    })).filter(x => x.end >= x.start);
    const a = norm(sessionsA);
    const b = norm(sessionsB);
    let i = 0, j = 0, total = 0;
    while (i < a.length && j < b.length) {
      const start = Math.max(a[i].start, b[j].start);
      const end = Math.min(a[i].end, b[j].end);
      if (end >= start) total += (end - start);
      if (a[i].end < b[j].end) i++; else j++;
    }
    return total;
  } catch {
    return 0;
  }
}

const isWatchedMember = (member) => member?.roles?.cache?.has(config.watchRoleId);
const getPresence = (member) => member?.presence || null;
const getCustomStatusText = (presence) => {
  if (!presence) return null;
  const custom = presence.activities?.find((a) => a.type === ActivityType.Custom);
  if (!custom) return null;
  const state = custom?.state || null;
  if (state && state.trim().length > 0) return state;
  const emojiName = custom?.emoji?.name || null;
  return emojiName || null;
};
const isOnlineStatus = (presence) => ["online", "idle", "dnd"].includes(presence?.status);
let ACTIVE_EVENT = null;

const hasDesiredStatus = (text) => {
  if (!text) return false;
  const want = ((ACTIVE_EVENT?.statusText ?? config.desiredStatusText) || "").trim().toLowerCase();
  if (!want) return false;
  return text.toLowerCase().includes(want);
};

const handleMemberSnapshot = (member) => {
  try {
    if (!isWatchedMember(member)) return;
    const presence = getPresence(member);
    const ts = nowTs();
    const online = presence ? isOnlineStatus(presence) : false;

    if (online) {
      startOnlineSession(member.id, member.guild.id, ts);
    } else {
      endOnlineSession(member.id, ts);
    }

    const customText = presence ? getCustomStatusText(presence) : null;
    if (online && hasDesiredStatus(customText)) {
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

const I18N = {
  "en-US": {
    title_daily: "Daily Top {n} ({metric})",
    title_weekly: "Weekly Top {n} ({metric})",
    title_monthly: "Monthly Top {n} ({metric})",
    metric_online: "Online",
    metric_status: "Status",
    metric_split: "Online Split",
    with_status: "w/ Status",
    without_status: "w/o Status",
    no_data_period: "No {period} data available yet.",
    period_daily: "daily",
    period_weekly: "weekly",
    period_monthly: "monthly",
    member_not_found: "Member not found.",
    today: "Today",
    this_week: "This Week",
    this_month: "This Month",
    guild_footer_prefix: "Guild",
    user_label: "User",
  },
  "tr": {
    title_daily: "Günlük Top {n} ({metric})",
    title_weekly: "Haftalık Top {n} ({metric})",
    title_monthly: "Aylık Top {n} ({metric})",
    metric_online: "Çevrimiçi",
    metric_status: "Durum",
    metric_split: "Online Ayrımı",
    with_status: "Durumlu",
    without_status: "Durumsuz",
    no_data_period: "{period} verisi henüz yok.",
    period_daily: "Günlük",
    period_weekly: "Haftalık",
    period_monthly: "Aylık",
    member_not_found: "Üye bulunamadı.",
    today: "Bugün",
    this_week: "Bu Hafta",
    this_month: "Bu Ay",
    guild_footer_prefix: "Sunucu",
    user_label: "Kullanıcı",
  },
};

const pickLocale = (interactionOrGuild) => {
  try {
    const raw = interactionOrGuild?.locale || interactionOrGuild?.preferredLocale || "en-US";
    const loc = String(raw).toLowerCase();
    if (loc.startsWith("tr")) return "tr"; // normalize tr-TR -> tr
    if (loc.startsWith("en")) return "en-US";
    return "en-US";
  } catch {
    return "en-US";
  }
};

const fmtTitle = (L, period, limit, metricLabel) => {
  if (period === "daily") return L.title_daily.replace("{n}", String(limit)).replace("{metric}", metricLabel);
  if (period === "weekly") return L.title_weekly.replace("{n}", String(limit)).replace("{metric}", metricLabel);
  return L.title_monthly.replace("{n}", String(limit)).replace("{metric}", metricLabel);
};

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
          name_localizations: { "tr": "genel-bakis", "en-US": "overview" },
          description: "Show overall monitoring summary",
          description_localizations: { "tr": "Genel izleme özetini göster" }
        },
        {
          name: "status",
          name_localizations: { "tr": "durum", "en-US": "status" },
          description: "Show a member's current presence and custom status",
          description_localizations: { "tr": "Bir üyenin mevcut varlığını ve özel durum metnini göster" },
          options: [
            {
              name: "user",
              name_localizations: { "tr": "kullanici" },
              description: "Target user",
              description_localizations: { "tr": "Hedef kullanıcı" },
              type: 6,
              required: true
            }
          ]
        },
        {
          name: "report",
          name_localizations: { "tr": "rapor", "en-US": "report" },
          description: "Report a member's online/status totals by period",
          description_localizations: { "tr": "Bir üyenin dönem bazında çevrimiçi/durum toplamlarını göster" },
          options: [
            {
              name: "user",
              name_localizations: { "tr": "kullanici" },
              description: "Target user",
              description_localizations: { "tr": "Hedef kullanıcı" },
              type: 6,
              required: true
            },
            {
              name: "period",
              name_localizations: { "tr": "donem" },
              description: "Time range",
              description_localizations: { "tr": "Zaman aralığı" },
              type: 3,
              required: false,
              choices: [
                { name: "daily", name_localizations: { "tr": "Günlük" }, value: "daily" },
                { name: "weekly", name_localizations: { "tr": "Haftalık" }, value: "weekly" },
                { name: "monthly", name_localizations: { "tr": "Aylık" }, value: "monthly" }
              ]
            }
          ]
        },
        {
          name: "mytime",
          name_localizations: { "tr": "benim-surem", "en-US": "mytime" },
          description: "Show your own online totals (ephemeral)",
          description_localizations: { "tr": "Kendi çevrimiçi toplamlarını göster (yalnızca sana görünür)" }
        },
        {
          name: "leader",
          name_localizations: { "tr": "lider", "en-US": "leader" },
          description: "Show Top 15 leaderboard",
          description_localizations: { "tr": "İlk 15 lider tablosunu göster" },
          options: [
            {
              name: "period",
              name_localizations: { "tr": "donem" },
              description: "Time range",
              description_localizations: { "tr": "Zaman aralığı" },
              type: 3,
              required: false,
              choices: [
                { name: "daily", name_localizations: { "tr": "Günlük" }, value: "daily" },
                { name: "weekly", name_localizations: { "tr": "Haftalık" }, value: "weekly" },
                { name: "monthly", name_localizations: { "tr": "Aylık" }, value: "monthly" }
              ]
            },
            {
              name: "metric",
              name_localizations: { "tr": "metrik" },
              description: "Sort metric",
              description_localizations: { "tr": "Sıralama metriği" },
              type: 3,
              required: false,
              choices: [
                { name: "Online", name_localizations: { "tr": "Çevrimiçi Toplam" }, value: "online" },
                { name: "Online w/ Status", name_localizations: { "tr": "Durumlu Çevrimiçi" }, value: "desired_online" }
              ]
            }
          ]
        },
        {
          name: "event",
          name_localizations: { "tr": "etkinlik", "en-US": "event" },
          description: "Manage event mode",
          description_localizations: { "tr": "Etkinlik modunu yönet" },
          options: [
            {
              type: 1,
              name: "start",
              name_localizations: { "tr": "baslat" },
              description: "Start an event",
              description_localizations: { "tr": "Bir etkinlik başlat" },
              options: [
                {
                  name: "status_text",
                  name_localizations: { "tr": "durum_meti" },
                  description: "Required status text for event",
                  description_localizations: { "tr": "Etkinlik için gereken durum metni" },
                  type: 3,
                  required: true
                },
                {
                  name: "duration_hours",
                  name_localizations: { "tr": "sure_saat" },
                  description: "Event window duration in hours",
                  description_localizations: { "tr": "Etkinlik penceresi (saat)" },
                  type: 4,
                  required: false
                },
                {
                  name: "channel",
                  name_localizations: { "tr": "kanal" },
                  description: "Scoreboard channel",
                  description_localizations: { "tr": "Skorboard kanalı" },
                  type: 7,
                  required: false
                },
                {
                  name: "limit",
                  name_localizations: { "tr": "limit" },
                  description: "Top N to show",
                  description_localizations: { "tr": "Gösterilecek Top N" },
                  type: 4,
                  required: false
                }
              ]
            },
            {
              type: 1,
              name: "stop",
              name_localizations: { "tr": "bitir" },
              description: "Stop current event",
              description_localizations: { "tr": "Mevcut etkinliği bitir" }
            },
            {
              type: 1,
              name: "export",
              name_localizations: { "tr": "disa-aktar" },
              description: "Export current event results as CSV",
              description_localizations: { "tr": "Mevcut etkinlik sonuçlarını CSV olarak aktar" }
            }
          ]
        },
        {
          name: "reward",
          name_localizations: { "tr": "odul", "en-US": "reward" },
          description: "Assign a role to Top N of current event",
          description_localizations: { "tr": "Mevcut etkinliğin Top N'ine rol ataması yap" },
          options: [
            {
              name: "role",
              name_localizations: { "tr": "rol" },
              description: "Role to grant",
              description_localizations: { "tr": "Verilecek rol" },
              type: 8,
              required: true
            },
            {
              name: "limit",
              name_localizations: { "tr": "limit" },
              description: "Top N winners",
              description_localizations: { "tr": "Kazanan Top N" },
              type: 4,
              required: false
            }
          ]
        }
      ];
    await client.application.commands.set(commands, config.guildId);
    console.log("Slash commands registered.");

    try {
      await guild.members.fetch({ withPresences: true });
      const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
      watchers.forEach((m) => handleMemberSnapshot(m));
    } catch (e) {
      console.error("Initial snapshot error:", e);
    }

    const intervalMs = Math.max(1, Number(config.checkIntervalMinutes || 5)) * 60000;
    setInterval(async () => {
      try {
        await guild.members.fetch({ withPresences: true });
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
      if (ACTIVE_EVENT) scheduleEventScoreboardUpdate(member.guild, 2000);
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

function formatHMS(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return '0 h 0 min 0 sec';
  const totalSeconds = Math.floor(ms / 1000);
  const h = Math.floor(totalSeconds / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = totalSeconds % 60;
  return `${h} h ${m} min ${s} sec`;
}

function statusLabel(localeSource) {
  return (pickLocale(localeSource) === 'tr') ? 'Status' /* or 'Durum' if preferred */ : 'Status';
}

async function sendRankEmbed(guild, period, metric, limit, channelIdOverride) {
  const channelId = channelIdOverride || (config.reportChannelId || config.controlChannelId);
  const channel = guild.channels.cache.get(channelId);
  if (!channel) {
    console.warn("Report channel not found.");
    return;
  }
  const guildId = guild.id;
  let title = "";
  const now = new Date();
  if (metric !== 'desired_online') {
    let rows = [];
    if (period === "daily") {
      const dateDay = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
      rows = getTopDaily(guildId, dateDay, metric, limit);
      title = `Daily Top ${limit} (${metric === 'online' ? 'Online' : 'Status'})`;
      if (!rows || rows.length === 0) {
        await guild.members.fetch();
        const startMs = startOfDay(now), endMs = endOfDay(now);
        const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
        const computed = await Promise.all(watchers.map(async (m) => {
          try {
            const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
            const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
            const onlineMs = sumOverlap(onlineSessions, startMs, endMs);
            const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
            const onlineWithoutStatusMs = Math.max(0, onlineMs - desiredOnlineMs);
            return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: desiredOnlineMs, online_without_status_ms: onlineWithoutStatusMs };
          } catch {
            return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: 0, online_without_status_ms: 0 };
          }
        }));
        computed.sort((a, b) => (metric === 'online'
          ? (b.desired_online_ms + b.online_without_status_ms) - (a.desired_online_ms + a.online_without_status_ms)
          : (b.desired_online_ms - a.desired_online_ms))
        );
        const top = computed.slice(0, limit);
        const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
        const rankLabel = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
        const fields = top.map((r, i) => ({
          name: `${rankLabel(i)} ${r.name} — ${formatHMS(r.desired_online_ms)}`,
          value: "\u200B",
          inline: true
        }));
        const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
        const embed = new EmbedBuilder()
          .setColor(0x10b981)
          .setTitle(title)
          .setAuthor({ name: guild.name, iconURL: icon || undefined })
          .setFooter({ text: `Guild: ${guild.name} • ${rangeStr}` })
          .setTimestamp(now)
          .addFields(fields);
        if (icon) embed.setThumbnail(icon);
        await channel.send({ embeds: [embed] });
        return;
      }
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
    let startMs, endMs;
    if (period === "daily") { startMs = startOfDay(now); endMs = endOfDay(now); }
    else if (period === "weekly") { startMs = startOfWeek(now); endMs = endOfWeek(now); }
    else { startMs = startOfMonth(now); endMs = endOfMonth(now); }
    const topFields = await Promise.all(rows.map(async (r, i) => {
      const m = await guild.members.fetch(r.user_id).catch(() => null);
      const name = m ? (m.displayName || m.user.username) : r.user_id;
      const onlineSessions = getOnlineSessionsBetween(r.user_id, startMs, endMs);
      const statusSessions = getStatusSessionsBetween(r.user_id, startMs, endMs);
      const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
      const rankLabel = (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
      return {
        name: `${rankLabel} ${name} — ${formatHMS(desiredOnlineMs)}`,
        value: "\u200B",
        inline: true
      };
    }));
    const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
    const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
    const embed = new EmbedBuilder()
      .setColor(0x10b981)
      .setTitle(title)
      .setAuthor({ name: guild.name, iconURL: icon || undefined })
      .setFooter({ text: `Guild: ${guild.name} • ${rangeStr}` })
      .setTimestamp(now)
      .addFields(topFields);
    if (icon) embed.setThumbnail(icon);
    await channel.send({ embeds: [embed] });
    return;
  }

  await guild.members.fetch();
  let startMs, endMs;
  const Lguild = I18N[pickLocale(guild)];
  if (period === "daily") { startMs = startOfDay(now); endMs = endOfDay(now); title = fmtTitle(Lguild, "daily", limit, Lguild.metric_split); }
  else if (period === "weekly") { startMs = startOfWeek(now); endMs = endOfWeek(now); title = fmtTitle(Lguild, "weekly", limit, Lguild.metric_split); }
  else { startMs = startOfMonth(now); endMs = endOfMonth(now); title = fmtTitle(Lguild, "monthly", limit, Lguild.metric_split); }
  const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
  const rows = await Promise.all(watchers.map(async (m) => {
    try {
      const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
      const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
      const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
      const onlineTotalMs = sumOverlap(onlineSessions, startMs, endMs);
      const onlineWithoutStatusMs = Math.max(0, onlineTotalMs - desiredOnlineMs);
      return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: desiredOnlineMs, online_without_status_ms: onlineWithoutStatusMs };
    } catch {
      return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: 0, online_without_status_ms: 0 };
    }
  }));
  rows.sort((a, b) => b.desired_online_ms - a.desired_online_ms);
  const top = rows.slice(0, limit);
  const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
  const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
  const rankLabel = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
  const fields = top.map((r, i) => ({
    name: `${rankLabel(i)} ${r.name} — ${formatHMS(r.desired_online_ms)}`,
    value: "\u200B",
    inline: true
  }));
  const embed = new EmbedBuilder()
    .setColor(0x10b981)
    .setTitle(title)
    .setAuthor({ name: guild.name, iconURL: icon || undefined })
    .setFooter({ text: `${Lguild.guild_footer_prefix}: ${guild.name} • ${rangeStr}` })
    .setTimestamp(now)
    .addFields(fields);
  if (icon) embed.setThumbnail(icon);
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
    const metricReq = (req.query.metric || "status").toLowerCase();
    const metric = ["online", "status", "desired_online"].includes(metricReq) ? metricReq : "status";
    const limit = Math.min(50, Math.max(1, Number(req.query.limit || config.topNDefault || 10)));
    const now = new Date();
    const guild = BOT_GUILD;
    if (!guild) return res.status(503).json({ error: "bot_not_ready" });
    const guildId = guild.id;
    if (metric !== "desired_online") {
      let rows = [];
      if (period === "daily") rows = getTopDaily(guildId, `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`, metric, limit);
      else if (period === "weekly") rows = getTopWeekly(guildId, isoWeekLabel(now), metric, limit);
      else rows = getTopMonthly(guildId, monthLabel(now), metric, limit);

      if (period === "daily" && (!rows || rows.length === 0)) {
        await guild.members.fetch();
        const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
        const startMs = startOfDay(now), endMs = endOfDay(now);
        const computed = await Promise.all(watchers.map(async (m) => {
          try {
            const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
            const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
            const onlineMs = sumOverlap(onlineSessions, startMs, endMs);
            const statusMs = sumOverlap(statusSessions, startMs, endMs);
            const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
            const onlineWithoutStatusMs = Math.max(0, onlineMs - desiredOnlineMs);
            return { user_id: m.id, name: m.displayName || m.user.username, online_ms: onlineMs, status_ms: statusMs, desired_online_ms: desiredOnlineMs, online_without_status_ms: onlineWithoutStatusMs };
          } catch {
            return { user_id: m.id, name: m.displayName || m.user.username, online_ms: 0, status_ms: 0, desired_online_ms: 0, online_without_status_ms: 0 };
          }
        }));
        computed.sort((a, b) => (metric === 'online' ? (b.online_ms - a.online_ms) : (b.status_ms - a.status_ms)));
        return res.json(computed.slice(0, limit));
      }

      const rangeStart = period === "daily" ? startOfDay(now) : (period === "weekly" ? startOfWeek(now) : startOfMonth(now));
      const rangeEnd = period === "daily" ? endOfDay(now) : (period === "weekly" ? endOfWeek(now) : endOfMonth(now));
      const payload = await Promise.all(rows.map(async (r) => {
        const m = await guild.members.fetch(r.user_id).catch(() => null);
        const onlineSessions = getOnlineSessionsBetween(r.user_id, rangeStart, rangeEnd);
        const statusSessions = getStatusSessionsBetween(r.user_id, rangeStart, rangeEnd);
        const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, rangeStart, rangeEnd);
        const onlineTotalMs = r.online_ms;
        const onlineWithoutStatusMs = Math.max(0, onlineTotalMs - desiredOnlineMs);
        return {
          user_id: r.user_id,
          name: m ? (m.displayName || m.user.username) : r.user_id,
          online_ms: r.online_ms,
          status_ms: r.status_ms,
          desired_online_ms: desiredOnlineMs,
          online_without_status_ms: onlineWithoutStatusMs,
        };
      }));
      return res.json(payload);
    }

    await guild.members.fetch();
    const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
    let startMs, endMs;
    if (period === "daily") { startMs = startOfDay(now); endMs = endOfDay(now); }
    else if (period === "weekly") { startMs = startOfWeek(now); endMs = endOfWeek(now); }
    else { startMs = startOfMonth(now); endMs = endOfMonth(now); }

    const rows = await Promise.all(watchers.map(async (m) => {
      try {
        const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
        const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
        const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
        const statusMs = sumOverlap(statusSessions, startMs, endMs);
        const onlineTotalMs = sumOverlap(onlineSessions, startMs, endMs);
        const onlineWithoutStatusMs = Math.max(0, onlineTotalMs - desiredOnlineMs);
        return {
          user_id: m.id,
          name: m.displayName || m.user.username,
          online_ms: desiredOnlineMs,
          status_ms: statusMs,
          desired_online_ms: desiredOnlineMs,
          online_without_status_ms: onlineWithoutStatusMs,
        };
      } catch {
        return { user_id: m.id, name: m.displayName || m.user.username, online_ms: 0, status_ms: 0, desired_online_ms: 0, online_without_status_ms: 0 };
      }
    }));
    rows.sort((a, b) => b.online_ms - a.online_ms);
    res.json(rows.slice(0, limit));
  });

  app.post("/api/send_rank_embed", requireAuth, async (req, res) => {
    const period = req.body.period || "weekly";
    const metricReq = (req.body.metric || "status").toLowerCase();
    const metric = ["online", "status", "desired_online"].includes(metricReq) ? metricReq : "status";
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

  app.post("/api/reconnect", requireAuth, async (req, res) => {
    try {
      const token = process.env.DISCORD_TOKEN || "";
      if (!token) return res.status(400).json({ error: "no_token" });
      try { await client.destroy(); } catch {}
      CLIENT_READY = false;
      BOT_GUILD = null;
      await client.login(token);
      res.json({ ok: true });
    } catch (e) {
      console.error("Reconnect error:", e);
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

  app.get("/api/event/state", requireAuth, async (req, res) => {
    try {
      const ev = ACTIVE_EVENT
        ? {
            startMs: ACTIVE_EVENT.startMs,
            endMs: ACTIVE_EVENT.endMs ?? null,
            statusText: ACTIVE_EVENT.statusText,
            channelId: ACTIVE_EVENT.channelId,
            messageId: ACTIVE_EVENT.messageId ?? null,
            limit: ACTIVE_EVENT.limit || 15,
          }
        : null;
      res.json({ active: !!ACTIVE_EVENT, event: ev });
    } catch (e) {
      console.error("/api/event/state error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  app.post("/api/event/start", requireAuth, async (req, res) => {
    try {
      const guild = BOT_GUILD;
      if (!guild) return res.status(503).json({ error: "bot_not_ready" });
      const statusText = (req.body.status_text || "").trim();
      if (!statusText) return res.status(400).json({ error: "status_text_required" });
      const durationHoursRaw = req.body.duration_hours;
      const durationHours = durationHoursRaw === null || durationHoursRaw === undefined ? null : Number(durationHoursRaw);
      const limit = Math.min(50, Math.max(1, Number(req.body.limit || 15)));
      const channelId = req.body.channel_id || config.reportChannelId;
      const now = Date.now();
      ACTIVE_EVENT = {
        startMs: now,
        endMs: durationHours ? now + durationHours * 3600000 : null,
        statusText,
        channelId,
        messageId: null,
        limit,
        pageIndex: 0,
        milestones: {},
        refreshHandle: null,
        rankSource: (config?.eventLeaderboardSource) || 'panel',
        rankPeriod: (config?.eventSeedPeriod) || 'weekly',
      };
      try {
        await guild.members.fetch({ withPresences: true });
        const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
        watchers.forEach((m) => handleMemberSnapshot(m));
      } catch (snapErr) {
        console.error("Initial event snapshot error:", snapErr);
      }
      await ensureEventScoreboardMessage(guild);
      await updateEventScoreboard(guild);
      try {
        if (ACTIVE_EVENT.refreshHandle) clearInterval(ACTIVE_EVENT.refreshHandle);
      } catch {}
      const refreshSeconds = Math.max(2, Number((config?.eventRefreshSeconds) || 10));
      ACTIVE_EVENT.refreshHandle = setInterval(async () => {
        try {
          if (ACTIVE_EVENT?.endMs && Date.now() > ACTIVE_EVENT.endMs) {
            clearInterval(ACTIVE_EVENT.refreshHandle);
            ACTIVE_EVENT.refreshHandle = null;
          }
          await updateEventScoreboard(guild);
        } catch (e) {
          console.error("Event refresh error:", e);
        }
      }, refreshSeconds * 1000);
      res.json({ ok: true });
    } catch (e) {
      console.error("/api/event/start error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  app.post("/api/event/stop", requireAuth, async (req, res) => {
    try {
      const guild = BOT_GUILD;
      if (!guild) return res.status(503).json({ error: "bot_not_ready" });
      if (!ACTIVE_EVENT) return res.status(400).json({ error: "no_active_event" });
      try {
        if (ACTIVE_EVENT.refreshHandle) clearInterval(ACTIVE_EVENT.refreshHandle);
      } catch {}
      const ended = ACTIVE_EVENT;
      ACTIVE_EVENT = null;
      const channelId = ended.channelId || config.reportChannelId;
      const channel = guild.channels.cache.get(channelId) || (await guild.channels.fetch(channelId).catch(() => null));
      if (channel && ended.messageId) {
        const message = await channel.messages.fetch(ended.messageId).catch(() => null);
        if (message) {
          const L = I18N[pickLocale(guild)];
          const rangeStr = `${formatTime(ended.startMs)} – ${formatTime(ended.endMs ?? Date.now())}`;
          const embed = message.embeds?.[0] ? EmbedBuilder.from(message.embeds[0]) : new EmbedBuilder();
          embed.setFooter({ text: `${L.guild_footer_prefix}: ${guild.name} • ${rangeStr} • (Ended)` }).setTimestamp(Date.now());
          await message.edit({ embeds: [embed] });
        }
      }
      res.json({ ok: true });
    } catch (e) {
      console.error("/api/event/stop error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  app.post("/api/event/reset", requireAuth, async (req, res) => {
    try {
      const guild = BOT_GUILD;
      if (!guild) return res.status(503).json({ error: "bot_not_ready" });
      if (ACTIVE_EVENT) {
        const now = Date.now();
        ACTIVE_EVENT.startMs = now;
        ACTIVE_EVENT.milestones = {};
        try { ACTIVE_EVENT.lastMyTime = null; } catch {}
      }
      try {
        clearGuildData(guild.id);
      } catch (purgeErr) {
        console.error("Guild data purge error:", purgeErr);
        return res.status(500).json({ error: "purge_failed" });
      }
      try {
        await guild.members.fetch({ withPresences: true });
        const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
        watchers.forEach((m) => handleMemberSnapshot(m));
        try {
          const L = I18N[pickLocale(guild)];
          const locale = pickLocale(guild);
          let online = 0, idle = 0, dnd = 0, offline = 0;
          let desiredYes = 0, desiredNo = 0;
          const nonCompliant = [];
          watchers.forEach((m) => {
            const presence = getPresence(m);
            const status = presence?.status || 'offline';
            if (status === 'online') online++; else if (status === 'idle') idle++; else if (status === 'dnd') dnd++; else offline++;
            const custom = getCustomStatusText(presence);
            const compliant = hasDesiredStatus(custom);
            if (compliant) desiredYes++; else { desiredNo++; nonCompliant.push(m); }
          });
          const headLabel = (locale === 'tr') ? 'Reset Özeti' : 'Reset Summary';
          const statusLabelOnline = (locale === 'tr') ? 'Çevrimiçi' : 'Online';
          const statusLabelIdle = (locale === 'tr') ? 'Uzakta' : 'Away';
          const statusLabelDnd = (locale === 'tr') ? 'Rahatsız Etmeyin' : 'DND';
          const statusLabelOffline = (locale === 'tr') ? 'Çevrimdışı' : 'Offline';
          const desiredLabel = (locale === 'tr') ? 'İstenen Durum' : 'Desired Status';
          const yesLabel = (locale === 'tr') ? 'Evet' : 'Yes';
          const noLabel = (locale === 'tr') ? 'Hayır' : 'No';
          const totalLabel = (locale === 'tr') ? 'Toplam İzlenen' : 'Monitored Total';
          const nonCompLabel = (locale === 'tr') ? 'Uymayanlar (anlık)' : 'Non-compliant (current)';
          const desc = [
            `${totalLabel}: ${watchers.size}`,
            `${statusLabelOnline}: ${online} • ${statusLabelIdle}: ${idle} • ${statusLabelDnd}: ${dnd} • ${statusLabelOffline}: ${offline}`,
            `${desiredLabel}: ${yesLabel} ${desiredYes} • ${noLabel} ${desiredNo}`
          ].join("\n");
          const embed = new EmbedBuilder()
            .setTitle(headLabel)
            .setDescription(desc)
            .setColor(0x00AE86)
            .addFields(
              nonCompliant.length
                ? [{ name: nonCompLabel, value: nonCompliant.slice(0, 20).map((m) => `<@${m.id}>`).join(", ") }]
                : []
            )
            .setTimestamp(Date.now());
          const targetChannelId = config.controlChannelId || config.reportChannelId;
          const outChan = guild.channels.cache.get(targetChannelId) || await guild.channels.fetch(targetChannelId).catch(() => null);
          if (outChan) {
            await outChan.send({ embeds: [embed] });
          }
        } catch (summaryErr) {
          console.error("Reset summary error:", summaryErr);
        }
      } catch (snapErr) {
        console.error("Post-reset snapshot error:", snapErr);
      }
      try { if (ACTIVE_EVENT) await updateEventScoreboard(guild); } catch {}
      res.json({ ok: true });
    } catch (e) {
      console.error("/api/event/reset error:", e);
      res.status(500).json({ error: "failed" });
    }
  });

  app.get("/api/event/export", requireAuth, async (req, res) => {
    try {
      const guild = BOT_GUILD;
      if (!guild) return res.status(503).json({ error: "bot_not_ready" });
      if (!ACTIVE_EVENT) return res.status(400).json({ error: "no_active_event" });
      const now = Date.now();
      const startMs = ACTIVE_EVENT.startMs;
      const endMs = ACTIVE_EVENT.endMs ?? now;
      const limit = ACTIVE_EVENT.limit || 15;
      const rankSource = ACTIVE_EVENT?.rankSource || (config?.eventLeaderboardSource) || 'panel';
      const period = ACTIVE_EVENT?.rankPeriod || (config?.eventSeedPeriod) || 'weekly';
      const rows = rankSource === 'panel'
        ? await computePeriodDesiredRank(guild, period, limit)
        : await computeEventRanking(guild, startMs, endMs, limit);
      const header = ["user_id", "username", "desired_online_ms", "online_without_status_ms", "total_online_ms"];
      const lines = [header.join(",")].concat(
        rows.map((r) => {
          const total = (typeof r.total_online_ms === 'number')
            ? r.total_online_ms
            : ((r.desired_online_ms || 0) + (r.online_without_status_ms || 0));
          return [r.user_id, String(r.name || "").replace(/,/g, " "), r.desired_online_ms, r.online_without_status_ms, total].join(",");
        })
      );
      const csv = lines.join("\n");
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader("Content-Disposition", `attachment; filename="event_${formatDate(now)}.csv"`);
      res.send(csv);
    } catch (e) {
      console.error("/api/event/export error:", e);
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
    const hasRole = interaction.member?.roles?.cache?.has(config.adminRoleId);
    return !!hasRole;
  } catch {
    return false;
  }
};

async function computeEventRanking(guild, startMs, endMs, limit) {
  await guild.members.fetch();
  const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
  const rows = await Promise.all(watchers.map(async (m) => {
    try {
      const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
      const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
      const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
      const onlineTotalMs = sumOverlap(onlineSessions, startMs, endMs);
      const onlineWithoutStatusMs = Math.max(0, onlineTotalMs - desiredOnlineMs);
      return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: desiredOnlineMs, online_without_status_ms: onlineWithoutStatusMs, total_online_ms: onlineTotalMs };
    } catch {
      return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: 0, online_without_status_ms: 0, total_online_ms: 0 };
    }
  }));
  rows.sort((a, b) => b.desired_online_ms - a.desired_online_ms);
  return rows.slice(0, limit);
}

async function computePeriodDesiredRank(guild, period, limit) {
  await guild.members.fetch();
  const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
  const now = new Date();
  let startMs, endMs;
  if (period === "daily") { startMs = startOfDay(now); endMs = endOfDay(now); }
  else if (period === "monthly") { startMs = startOfMonth(now); endMs = endOfMonth(now); }
  else { startMs = startOfWeek(now); endMs = endOfWeek(now); }
  const rows = await Promise.all(watchers.map(async (m) => {
    try {
      const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
      const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
      const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
      const statusMs = sumOverlap(statusSessions, startMs, endMs);
      const onlineTotalMs = sumOverlap(onlineSessions, startMs, endMs);
      const onlineWithoutStatusMs = Math.max(0, onlineTotalMs - desiredOnlineMs);
      return {
        user_id: m.id,
        name: m.displayName || m.user.username,
        online_ms: desiredOnlineMs,
        status_ms: statusMs,
        desired_online_ms: desiredOnlineMs,
        online_without_status_ms: onlineWithoutStatusMs,
      };
    } catch {
      return { user_id: m.id, name: m.displayName || m.user.username, online_ms: 0, status_ms: 0, desired_online_ms: 0, online_without_status_ms: 0 };
    }
  }));
  rows.sort((a, b) => b.online_ms - a.online_ms);
  return rows.slice(0, limit);
}

async function ensureEventScoreboardMessage(guild) {
  if (!ACTIVE_EVENT) return;
  const channelId = ACTIVE_EVENT.channelId || config.reportChannelId;
  const channel = guild.channels.cache.get(channelId) || await guild.channels.fetch(channelId).catch(() => null);
  if (!channel) return;
  if (ACTIVE_EVENT.messageId) return; 
  const L = I18N[pickLocale(guild)];
  const title = `🎯 ${ACTIVE_EVENT.statusText} — ${L.metric_split}`;
  const now = Date.now();
  const rangeStr = `${formatTime(ACTIVE_EVENT.startMs)} – ${formatTime(ACTIVE_EVENT.endMs ?? now)}`;
  const usePanel = ACTIVE_EVENT?.rankSource === 'panel';
  const periodKey = (ACTIVE_EVENT?.rankPeriod === 'daily') ? 'today' : (ACTIVE_EVENT?.rankPeriod === 'monthly') ? 'this_month' : 'this_week';
  const periodLabel = L[periodKey];
  const embed = new EmbedBuilder()
    .setColor(0x0ea5e9)
    .setTitle(title)
    .setDescription(usePanel ? `Period: ${periodLabel}` : `Window: ${rangeStr}`)
    .setTimestamp(now);
  const btnLabel = (pickLocale(guild) === 'tr') ? 'Benim Sürem' : 'My Time';
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('event_prev').setLabel('◀').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('event_next').setLabel('▶').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('mytime_self').setLabel(btnLabel).setStyle(ButtonStyle.Primary)
  );
  const msg = await channel.send({ embeds: [embed], components: [row] });
  try { await msg.pin(); } catch {}
  ACTIVE_EVENT.messageId = msg.id;
}

async function updateEventScoreboard(guild) {
  if (!ACTIVE_EVENT) return;
  const channelId = ACTIVE_EVENT.channelId || config.reportChannelId;
  const pageSize = ACTIVE_EVENT.limit || 15;
  const pageIndex = ACTIVE_EVENT.pageIndex || 0;
  const channel = guild.channels.cache.get(channelId) || await guild.channels.fetch(channelId).catch(() => null);
  if (!channel) return;
  await ensureEventScoreboardMessage(guild);
  const message = ACTIVE_EVENT.messageId ? (await channel.messages.fetch(ACTIVE_EVENT.messageId).catch(() => null)) : null;
  const now = Date.now();
  const startMs = ACTIVE_EVENT.startMs;
  const endMs = ACTIVE_EVENT.endMs ?? now;
  const L = I18N[pickLocale(guild)];
  const fetchLimit = Math.max(pageSize, (pageIndex + 1) * pageSize);
  const usePanel = ACTIVE_EVENT?.rankSource === 'panel';
  const top = usePanel
    ? await computePeriodDesiredRank(guild, ACTIVE_EVENT.rankPeriod || "weekly", fetchLimit)
    : await computeEventRanking(guild, startMs, endMs, fetchLimit);
  const startIdx = pageIndex * pageSize;
  const pageRows = top.slice(startIdx, startIdx + pageSize);
  const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
  const rankLabel = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
  const fields = pageRows.map((r, i) => {
    const globalIdx = startIdx + i;
    return {
      name: `${rankLabel(globalIdx)} ${r.name}`,
      value: `🟢 ${L.with_status}: ${formatHMS(r.desired_online_ms)} • ⚪ ${L.without_status}: ${formatHMS(r.online_without_status_ms)}`,
      inline: false
    };
  });
  if (fields.length === 0) fields.push({ name: '—', value: L.no_data_period.replace('{period}', 'event'), inline: false });
  const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
  const periodKey = (ACTIVE_EVENT?.rankPeriod === 'daily') ? 'today' : (ACTIVE_EVENT?.rankPeriod === 'monthly') ? 'this_month' : 'this_week';
  const periodLabel = L[periodKey];
  const embed = new EmbedBuilder()
    .setColor(0x10b981)
    .setTitle(`🏁 Event Leaderboard — ${ACTIVE_EVENT.statusText}`)
    .setAuthor({ name: guild.name, iconURL: icon || undefined })
    .setFooter({ text: `${L.guild_footer_prefix}: ${guild.name} • ${usePanel ? `Period: ${periodLabel}` : rangeStr} • ${(pickLocale(guild)==='tr'?'Sayfa':'Page')} ${pageIndex + 1}` })
    .setTimestamp(now)
    .addFields(fields);
  if (icon) embed.setThumbnail(icon);
  const btnLabel = (pickLocale(guild) === 'tr') ? 'Benim Sürem' : 'My Time';
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('event_prev').setLabel('◀').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('event_next').setLabel('▶').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('mytime_self').setLabel(btnLabel).setStyle(ButtonStyle.Primary)
  );
  if (message) {
    await message.edit({ embeds: [embed], components: [row] });
  } else {
    const newMsg = await channel.send({ embeds: [embed], components: [row] });
    try { await newMsg.pin(); } catch {}
    ACTIVE_EVENT.messageId = newMsg.id;
  }
  try { await announceMilestones(guild, top, channel); } catch {}
}

function scheduleEventScoreboardUpdate(guild, delayMs = 2000) {
  try {
    if (!ACTIVE_EVENT) return;
    if (ACTIVE_EVENT.pendingUpdate) return;
    ACTIVE_EVENT.pendingUpdate = setTimeout(async () => {
      try {
        await updateEventScoreboard(guild);
      } catch (e) {
        console.error("Event schedule update error:", e);
      } finally {
        try { clearTimeout(ACTIVE_EVENT.pendingUpdate); } catch {}
        ACTIVE_EVENT.pendingUpdate = null;
      }
    }, delayMs);
  } catch (e) {
    console.error("scheduleEventScoreboardUpdate error:", e);
  }
}

const DEFAULT_MILESTONE_HOURS = [1, 2, 5, 10];
async function announceMilestones(guild, rankedRows, channel) {
  if (!ACTIVE_EVENT) return;
  if (!ACTIVE_EVENT.milestones) ACTIVE_EVENT.milestones = {};
  for (const r of rankedRows) {
    const ms = r.desired_online_ms;
    const achievedIdx = DEFAULT_MILESTONE_HOURS.findIndex((h) => ms >= h * 3600000);
    const prevIdx = ACTIVE_EVENT.milestones[r.user_id] ?? -1;
    if (achievedIdx > prevIdx) {
      ACTIVE_EVENT.milestones[r.user_id] = achievedIdx;
      const hours = DEFAULT_MILESTONE_HOURS[achievedIdx];
      const hourLabel = hours === 1 ? "hour" : "hours";
      await channel.send(`🎉 <@${r.user_id}> has been online with status for ${hours} ${hourLabel}!`);
    }
  }
}

client.on("interactionCreate", async (interaction) => {
  if (interaction.isButton()) {
    try {
      if (interaction.customId === "event_prev" || interaction.customId === "event_next") {
        const guild = interaction.guild;
        if (!ACTIVE_EVENT) { try { await interaction.deferUpdate(); } catch {} return; }
        const pageSize = ACTIVE_EVENT.limit || 15;
        const now = Date.now();
        const startMs = ACTIVE_EVENT.startMs;
        const endMs = ACTIVE_EVENT.endMs ?? now;
        try {
          const allTop = await computeEventRanking(guild, startMs, endMs, 500);
          const maxPage = Math.max(0, Math.ceil(allTop.length / pageSize) - 1);
          if (interaction.customId === "event_prev") {
            ACTIVE_EVENT.pageIndex = Math.max(0, (ACTIVE_EVENT.pageIndex || 0) - 1);
          } else {
            ACTIVE_EVENT.pageIndex = Math.min(maxPage, (ACTIVE_EVENT.pageIndex || 0) + 1);
          }
        } catch {
          // Fallback without clamping
          if (interaction.customId === "event_prev") {
            ACTIVE_EVENT.pageIndex = Math.max(0, (ACTIVE_EVENT.pageIndex || 0) - 1);
          } else {
            ACTIVE_EVENT.pageIndex = (ACTIVE_EVENT.pageIndex || 0) + 1;
          }
        }
        try { await interaction.deferUpdate(); } catch {}
        try { await updateEventScoreboard(guild); } catch {}
        return;
      }
      if (interaction.customId === "mytime_self") {
        const guild = interaction.guild;
        await guild.members.fetch({ withPresences: true });
        const member = await guild.members.fetch(interaction.user.id).catch(() => null);
        if (!member) return interaction.reply({ content: "Member not found.", ephemeral: true });
        if (!isWatchedMember(member)) return interaction.reply({ content: "This feature is for watched members only.", ephemeral: true });
        const L = I18N[pickLocale(interaction)];
        if (ACTIVE_EVENT) {
          const rankSource = ACTIVE_EVENT?.rankSource || (config?.eventLeaderboardSource) || 'panel';
          if (rankSource === 'panel') {
            const now = new Date();
            const period = ACTIVE_EVENT?.rankPeriod || 'weekly';
            let startMs, endMs;
            if (period === 'daily') { startMs = startOfDay(now); endMs = endOfDay(now); }
            else if (period === 'monthly') { startMs = startOfMonth(now); endMs = endOfMonth(now); }
            else { startMs = startOfWeek(now); endMs = endOfWeek(now); }
            const onlineSessions = getOnlineSessionsBetween(member.id, startMs, endMs);
            const statusSessions = getStatusSessionsBetween(member.id, startMs, endMs);
            const onlineTotal = sumOverlap(onlineSessions, startMs, endMs);
            const desired = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
            const without = Math.max(0, onlineTotal - desired);
            const locale = pickLocale(interaction);
            const periodKey = (period === 'daily') ? (locale==='tr'?'Bugün':'Today') : (period === 'monthly') ? (locale==='tr'?'Bu Ay':'This Month') : (locale==='tr'?'Bu Hafta':'This Week');
            const lines = [
              `${L.user_label}: <@${member.id}>`,
              `${periodKey}: 🟢 ${L.with_status}: ${formatDuration(desired)} • ⚪ ${L.without_status}: ${formatDuration(without)}`
            ];
            return interaction.reply({ content: lines.join("\n"), ephemeral: true });
          } else {
            const startMs = ACTIVE_EVENT.startMs;
            const endMs = ACTIVE_EVENT.endMs || Date.now();
            const onlineSessions = getOnlineSessionsBetween(member.id, startMs, endMs);
            const statusSessions = getStatusSessionsBetween(member.id, startMs, endMs);
            const onlineTotal = sumOverlap(onlineSessions, startMs, endMs);
            const desired = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
            const without = Math.max(0, onlineTotal - desired);
            const locale = pickLocale(interaction);
            const eventLabel = (locale === 'tr') ? 'Etkinlik' : 'Event';
            const lines = [
              `${L.user_label}: <@${member.id}>`,
              `${eventLabel}: 🟢 ${L.with_status}: ${formatDuration(desired)} • ⚪ ${L.without_status}: ${formatDuration(without)}`
            ];
            return interaction.reply({ content: lines.join("\n"), ephemeral: true });
          }
        }

        const now = new Date();
        const dStart = startOfDay(now), dEnd = endOfDay(now);
        const wStart = startOfWeek(now), wEnd = endOfWeek(now);
        const mStart = startOfMonth(now), mEnd = endOfMonth(now);

        const dOnlineSessions = getOnlineSessionsBetween(member.id, dStart, dEnd);
        const dStatusSessions = getStatusSessionsBetween(member.id, dStart, dEnd);
        const dOnlineTotal = sumOverlap(dOnlineSessions, dStart, dEnd);
        const dDesired = sumIntersection(dOnlineSessions, dStatusSessions, dStart, dEnd);
        const dWithout = Math.max(0, dOnlineTotal - dDesired);

        const wOnlineSessions = getOnlineSessionsBetween(member.id, wStart, wEnd);
        const wStatusSessions = getStatusSessionsBetween(member.id, wStart, wEnd);
        const wOnlineTotal = sumOverlap(wOnlineSessions, wStart, wEnd);
        const wDesired = sumIntersection(wOnlineSessions, wStatusSessions, wStart, wEnd);
        const wWithout = Math.max(0, wOnlineTotal - wDesired);

        const mOnlineSessions = getOnlineSessionsBetween(member.id, mStart, mEnd);
        const mStatusSessions = getStatusSessionsBetween(member.id, mStart, mEnd);
        const mOnlineTotal = sumOverlap(mOnlineSessions, mStart, mEnd);
        const mDesired = sumIntersection(mOnlineSessions, mStatusSessions, mStart, mEnd);
        const mWithout = Math.max(0, mOnlineTotal - mDesired);

        const lines = [
          `${L.user_label}: <@${member.id}>`,
          `${L.today}: 🟢 ${L.with_status}: ${formatDuration(dDesired)} • ⚪ ${L.without_status}: ${formatDuration(dWithout)}`,
          `${L.this_week}: 🟢 ${L.with_status}: ${formatDuration(wDesired)} • ⚪ ${L.without_status}: ${formatDuration(wWithout)}`,
          `${L.this_month}: 🟢 ${L.with_status}: ${formatDuration(mDesired)} • ⚪ ${L.without_status}: ${formatDuration(mWithout)}`
        ];
        return interaction.reply({ content: lines.join("\n"), ephemeral: true });
      }
    } catch (e) {
      console.error("button interaction error:", e);
      try { return interaction.reply({ content: "An error occurred.", ephemeral: true }); } catch {}
    }
    return; 
  }

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
      if (!isWatchedMember(member)) {
        return interaction.reply({ content: "Only watched members can use this command.", ephemeral: true });
      }
      const L = I18N[pickLocale(interaction)];
      if (ACTIVE_EVENT) {
        const startMs = ACTIVE_EVENT.startMs;
        const endMs = ACTIVE_EVENT.endMs || Date.now();
        const onlineSessions = getOnlineSessionsBetween(member.id, startMs, endMs);
        const statusSessions = getStatusSessionsBetween(member.id, startMs, endMs);
        const onlineTotal = sumOverlap(onlineSessions, startMs, endMs);
        const desired = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
        const without = Math.max(0, onlineTotal - desired);
        const locale = pickLocale(interaction);
        const eventLabel = (locale === 'tr') ? 'Etkinlik' : 'Event';
        const lines = [
          `${L.user_label}: <@${member.id}>`,
          `${eventLabel}: 🟢 ${L.with_status}: ${formatDuration(desired)} • ⚪ ${L.without_status}: ${formatDuration(without)}`
        ];
        return interaction.reply({ content: lines.join("\n"), ephemeral: true });
      }

      const now = new Date();
      const dStart = startOfDay(now), dEnd = endOfDay(now);
      const wStart = startOfWeek(now), wEnd = endOfWeek(now);
      const mStart = startOfMonth(now), mEnd = endOfMonth(now);

      const dOnlineSessions = getOnlineSessionsBetween(member.id, dStart, dEnd);
      const dStatusSessions = getStatusSessionsBetween(member.id, dStart, dEnd);
      const dOnlineTotal = sumOverlap(dOnlineSessions, dStart, dEnd);
      const dDesired = sumIntersection(dOnlineSessions, dStatusSessions, dStart, dEnd);
      const dWithout = Math.max(0, dOnlineTotal - dDesired);

      const wOnlineSessions = getOnlineSessionsBetween(member.id, wStart, wEnd);
      const wStatusSessions = getStatusSessionsBetween(member.id, wStart, wEnd);
      const wOnlineTotal = sumOverlap(wOnlineSessions, wStart, wEnd);
      const wDesired = sumIntersection(wOnlineSessions, wStatusSessions, wStart, wEnd);
      const wWithout = Math.max(0, wOnlineTotal - wDesired);

      const mOnlineSessions = getOnlineSessionsBetween(member.id, mStart, mEnd);
      const mStatusSessions = getStatusSessionsBetween(member.id, mStart, mEnd);
      const mOnlineTotal = sumOverlap(mOnlineSessions, mStart, mEnd);
      const mDesired = sumIntersection(mOnlineSessions, mStatusSessions, mStart, mEnd);
      const mWithout = Math.max(0, mOnlineTotal - mDesired);

      const lines = [
        `${L.user_label}: <@${member.id}>`,
        `${L.today}: 🟢 ${L.with_status}: ${formatDuration(dDesired)} • ⚪ ${L.without_status}: ${formatDuration(dWithout)}`,
        `${L.this_week}: 🟢 ${L.with_status}: ${formatDuration(wDesired)} • ⚪ ${L.without_status}: ${formatDuration(wWithout)}`,
        `${L.this_month}: 🟢 ${L.with_status}: ${formatDuration(mDesired)} • ⚪ ${L.without_status}: ${formatDuration(mWithout)}`
      ];
      return interaction.reply({ content: lines.join("\n"), ephemeral: true });
    } catch (e) {
      console.error("mytime command error:", e);
      return interaction.reply({ content: "An error occurred.", ephemeral: true });
    }
  }

  if (commandName === "leader") {
    try {
      if (!isAuthorized(interaction)) {
        return interaction.reply({ content: "Unauthorized.", ephemeral: true });
      }
      const period = (interaction.options.getString("period") || "weekly").toLowerCase();
      const metricReq = (interaction.options.getString("metric") || "desired_online").toLowerCase();
      const metric = ["online", "desired_online"].includes(metricReq) ? metricReq : "desired_online";
      const limit = 15;

      const now = new Date();
      const guildId = guild.id;
      await guild.members.fetch();

      const L = I18N[pickLocale(interaction)];
      let title = "";
      if (metric !== "desired_online") {
        let rows = [];
        if (period === "daily") {
          const dateDay = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
          rows = getTopDaily(guildId, dateDay, metric, limit);
          title = fmtTitle(L, "daily", limit, metric === 'online' ? L.metric_online : L.metric_status);
        } else if (period === "weekly") {
          const label = isoWeekLabel(now);
          rows = getTopWeekly(guildId, label, metric, limit);
          title = fmtTitle(L, "weekly", limit, metric === 'online' ? L.metric_online : L.metric_status);
        } else {
          const label = monthLabel(now);
          rows = getTopMonthly(guildId, label, metric, limit);
          title = fmtTitle(L, "monthly", limit, metric === 'online' ? L.metric_online : L.metric_status);
        }

        // Fallback for daily if empty
        if (period === "daily" && (!rows || !rows.length)) {
          const startMs = startOfDay(now), endMs = endOfDay(now);
          const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
          const computed = await Promise.all(watchers.map(async (m) => {
            try {
              const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
              const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
              const onlineMs = sumOverlap(onlineSessions, startMs, endMs);
              const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
              const onlineWithoutStatusMs = Math.max(0, onlineMs - desiredOnlineMs);
              return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: desiredOnlineMs, online_without_status_ms: onlineWithoutStatusMs };
            } catch {
              return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: 0, online_without_status_ms: 0 };
            }
          }));
          computed.sort((a, b) => (metric === 'online'
            ? (b.desired_online_ms + b.online_without_status_ms) - (a.desired_online_ms + a.online_without_status_ms)
            : (b.desired_online_ms - a.desired_online_ms))
          );
          const top = computed.slice(0, limit);
          const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
          const rankLabel = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
          const fields = top.map((r, i) => ({
            name: `${rankLabel(i)} ${r.name} — ${formatHMS(r.desired_online_ms)}`,
            value: "\u200B",
            inline: true
          }));
          const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
          const embed = new EmbedBuilder()
            .setColor(0x10b981)
            .setTitle(title)
            .setAuthor({ name: guild.name, iconURL: icon || undefined })
            .setFooter({ text: `${L.guild_footer_prefix}: ${guild.name} • ${rangeStr}` })
            .setTimestamp(now)
            .addFields(fields);
          if (icon) embed.setThumbnail(icon);
          return interaction.reply({ embeds: [embed], ephemeral: true });
        }

        if (!rows.length) {
          const perLabel = period === "daily" ? L.period_daily : (period === "weekly" ? L.period_weekly : L.period_monthly);
          return interaction.reply({ content: L.no_data_period.replace("{period}", perLabel), ephemeral: true });
        }

        // Aggregated rows: compute intersection and without for display
        let startMs, endMs;
        if (period === "daily") { startMs = startOfDay(now); endMs = endOfDay(now); }
        else if (period === "weekly") { startMs = startOfWeek(now); endMs = endOfWeek(now); }
        else { startMs = startOfMonth(now); endMs = endOfMonth(now); }
        const topFields = await Promise.all(rows.map(async (r, i) => {
          const m = await guild.members.fetch(r.user_id).catch(() => null);
          const name = m ? (m.displayName || m.user.username) : r.user_id;
          const onlineSessions = getOnlineSessionsBetween(r.user_id, startMs, endMs);
          const statusSessions = getStatusSessionsBetween(r.user_id, startMs, endMs);
          const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
          const rankLabel = (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
          return {
            name: `${rankLabel} ${name} — ${formatHMS(desiredOnlineMs)}`,
            value: "\u200B",
            inline: true
          };
        }));
        const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
        const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
        const embed = new EmbedBuilder()
          .setColor(0x10b981)
          .setTitle(title)
          .setAuthor({ name: guild.name, iconURL: icon || undefined })
          .setFooter({ text: `${L.guild_footer_prefix}: ${guild.name} • ${rangeStr}` })
          .setTimestamp(now)
          .addFields(topFields);
        if (icon) embed.setThumbnail(icon);
        return interaction.reply({ embeds: [embed], ephemeral: true });
      }

      let startMs, endMs;
      if (period === "daily") { startMs = startOfDay(now); endMs = endOfDay(now); title = fmtTitle(L, "daily", limit, L.metric_split); }
      else if (period === "weekly") { startMs = startOfWeek(now); endMs = endOfWeek(now); title = fmtTitle(L, "weekly", limit, L.metric_split); }
      else { startMs = startOfMonth(now); endMs = endOfMonth(now); title = fmtTitle(L, "monthly", limit, L.metric_split); }
      const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
      const rows = await Promise.all(watchers.map(async (m) => {
        try {
          const onlineSessions = getOnlineSessionsBetween(m.id, startMs, endMs);
          const statusSessions = getStatusSessionsBetween(m.id, startMs, endMs);
          const desiredOnlineMs = sumIntersection(onlineSessions, statusSessions, startMs, endMs);
          const onlineTotalMs = sumOverlap(onlineSessions, startMs, endMs);
          const onlineWithoutStatusMs = Math.max(0, onlineTotalMs - desiredOnlineMs);
          return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: desiredOnlineMs, online_without_status_ms: onlineWithoutStatusMs };
        } catch {
          return { user_id: m.id, name: m.displayName || m.user.username, desired_online_ms: 0, online_without_status_ms: 0 };
        }
      }));
      rows.sort((a, b) => b.desired_online_ms - a.desired_online_ms);
      const top = rows.slice(0, limit);
      const icon = typeof guild.iconURL === 'function' ? (guild.iconURL({ size: 128 }) || null) : null;
      const rankLabel = (i) => (i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `#${i + 1}`);
      const fields = top.map((r, i) => ({
        name: `${rankLabel(i)} ${r.name} — ${formatHMS(r.desired_online_ms)}`,
        value: "\u200B",
        inline: true
      }));
      const rangeStr = `${formatTime(startMs)} – ${formatTime(endMs)}`;
      const embed = new EmbedBuilder()
        .setColor(0x10b981)
        .setTitle(title)
        .setAuthor({ name: guild.name, iconURL: icon || undefined })
        .setFooter({ text: `${L.guild_footer_prefix}: ${guild.name} • ${rangeStr}` })
        .setTimestamp(now)
        .addFields(fields);
      if (icon) embed.setThumbnail(icon);
      return interaction.reply({ embeds: [embed], ephemeral: true });
    } catch (e) {
      console.error("leader command error:", e);
      return interaction.reply({ content: "An error occurred.", ephemeral: true });
    }
  }

  if (!isAuthorized(interaction)) {
    return interaction.reply({
      content: "Unauthorized.",
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

  if (commandName === "event") {
    if (!isAuthorized(interaction)) {
      return interaction.reply({ content: "Unauthorized.", ephemeral: true });
    }
    const sub = interaction.options.getSubcommand();
  if (sub === "start") {
      try {
        const statusText = interaction.options.getString("status_text", true);
        const durationHours = interaction.options.getInteger("duration_hours") || null;
        const channelObj = interaction.options.getChannel("channel") || null;
        const limit = interaction.options.getInteger("limit") || 15;
        const now = Date.now();
        ACTIVE_EVENT = {
          startMs: now,
          endMs: durationHours ? now + (durationHours * 3600000) : null,
          statusText,
          channelId: channelObj ? channelObj.id : config.reportChannelId,
          messageId: null,
          limit,
          milestones: {},
          refreshHandle: null,
          rankSource: (config?.eventLeaderboardSource) || 'panel',
          rankPeriod: (config?.eventSeedPeriod) || "weekly",
        };
        try {
          await guild.members.fetch({ withPresences: true });
          const watchers = guild.members.cache.filter((m) => isWatchedMember(m));
          watchers.forEach((m) => handleMemberSnapshot(m));
        } catch (snapErr) {
          console.error("Initial event snapshot error:", snapErr);
        }
        await ensureEventScoreboardMessage(guild);
        await updateEventScoreboard(guild);
        try { if (ACTIVE_EVENT.refreshHandle) clearInterval(ACTIVE_EVENT.refreshHandle); } catch {}
        const refreshSeconds = Math.max(2, Number((config?.eventRefreshSeconds) || 10));
        ACTIVE_EVENT.refreshHandle = setInterval(async () => {
          try {
            if (ACTIVE_EVENT?.endMs && Date.now() > ACTIVE_EVENT.endMs) {
              clearInterval(ACTIVE_EVENT.refreshHandle);
              ACTIVE_EVENT.refreshHandle = null;
            }
            await updateEventScoreboard(guild);
          } catch (e) { console.error("Event refresh error:", e); }
        }, refreshSeconds * 1000);
        const hourLabel = durationHours === 1 ? "hour" : "hours";
        return interaction.reply({ content: `Event started. Status: "${statusText}"${durationHours ? ` • Duration: ${durationHours} ${hourLabel}` : ""}.`, ephemeral: true });
      } catch (e) {
        console.error("event start error:", e);
        return interaction.reply({ content: "Error: Failed to start the event.", ephemeral: true });
      }
    } else if (sub === "stop") {
      try {
        if (!ACTIVE_EVENT) {
          return interaction.reply({ content: "No active event.", ephemeral: true });
        }
        try { if (ACTIVE_EVENT.refreshHandle) clearInterval(ACTIVE_EVENT.refreshHandle); } catch {}
        const ended = ACTIVE_EVENT;
        ACTIVE_EVENT = null;
        const channelId = ended.channelId || config.reportChannelId;
        const channel = guild.channels.cache.get(channelId) || await guild.channels.fetch(channelId).catch(() => null);
        if (channel && ended.messageId) {
          const message = await channel.messages.fetch(ended.messageId).catch(() => null);
          if (message) {
            const L = I18N[pickLocale(guild)];
            const rangeStr = `${formatTime(ended.startMs)} – ${formatTime(ended.endMs ?? Date.now())}`;
            const embed = message.embeds?.[0] ? EmbedBuilder.from(message.embeds[0]) : new EmbedBuilder();
            embed.setFooter({ text: `${L.guild_footer_prefix}: ${guild.name} • ${rangeStr} • (Ended)` }).setTimestamp(Date.now());
            await message.edit({ embeds: [embed] });
          }
        }
        return interaction.reply({ content: "Event stopped.", ephemeral: true });
      } catch (e) {
        console.error("event stop error:", e);
        return interaction.reply({ content: "Error: Failed to stop the event.", ephemeral: true });
      }
    } else if (sub === "export") {
      try {
        if (!ACTIVE_EVENT) {
          return interaction.reply({ content: "No active event.", ephemeral: true });
        }
        const now = Date.now();
        const startMs = ACTIVE_EVENT.startMs;
        const endMs = ACTIVE_EVENT.endMs ?? now;
        const limit = ACTIVE_EVENT.limit || 15;
        const rankSource = ACTIVE_EVENT?.rankSource || (config?.eventLeaderboardSource) || 'panel';
        const period = ACTIVE_EVENT?.rankPeriod || (config?.eventSeedPeriod) || 'weekly';
        const rows = rankSource === 'panel'
          ? await computePeriodDesiredRank(guild, period, limit)
          : await computeEventRanking(guild, startMs, endMs, limit);
        const header = ["user_id","username","desired_online_ms","online_without_status_ms","total_online_ms"]; 
        const lines = [header.join(",")].concat(rows.map(r => {
          const total = (typeof r.total_online_ms === 'number')
            ? r.total_online_ms
            : ((r.desired_online_ms || 0) + (r.online_without_status_ms || 0));
          return [r.user_id, (r.name || '').replace(/,/g, " "), r.desired_online_ms, r.online_without_status_ms, total].join(",");
        }));
        const csv = lines.join("\n");
        const buf = Buffer.from(csv, 'utf8');
        const file = new AttachmentBuilder(buf, { name: `event_${formatDate(now)}.csv` });
        return interaction.reply({ content: "CSV ready.", files: [file], ephemeral: true });
      } catch (e) {
        console.error("event export error:", e);
        return interaction.reply({ content: "Error: Failed to generate CSV.", ephemeral: true });
      }
    }
  }

  if (commandName === "reward") {
    if (!isAuthorized(interaction)) {
      return interaction.reply({ content: "Unauthorized.", ephemeral: true });
    }
    try {
      if (!ACTIVE_EVENT) {
        return interaction.reply({ content: "No active event.", ephemeral: true });
      }
      const role = interaction.options.getRole("role", true);
      const limit = interaction.options.getInteger("limit") || 3;
      const now = Date.now();
      const startMs = ACTIVE_EVENT.startMs;
      const endMs = ACTIVE_EVENT.endMs ?? now;
      const rows = await computeEventRanking(guild, startMs, endMs, limit);
      const granted = [];
      for (const r of rows) {
        const m = await guild.members.fetch(r.user_id).catch(() => null);
        if (!m) continue;
        try { await m.roles.add(role.id); granted.push(`<@${r.user_id}>`); } catch (e) { console.error("role add error:", e); }
      }
      return interaction.reply({ content: `Roles granted (${role.name}): ${granted.join(", ")}`, ephemeral: true });
    } catch (e) {
      console.error("reward error:", e);
      return interaction.reply({ content: "Error: Failed to assign roles.", ephemeral: true });
    }
  }
});

const token = process.env.DISCORD_TOKEN;
if (!token) {
  console.error("DISCORD_TOKEN not set in .env! Panel will run; bot not logged in.");
} else {
  client.login(token);
}
try { startPanelServer(); } catch {}