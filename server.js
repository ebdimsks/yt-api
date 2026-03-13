import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import stream from "stream";
import { pipeline } from "stream/promises";
import { setTimeout as wait } from "timers/promises";

const PORT = process.env.PORT || 3000;
const WORKER_SECRET = process.env.WORKER_SECRET;
const UPSTREAM_TIMEOUT_MS = 30000; // 30s
const INSTANCE_BAN_MS = 5 * 60 * 1000;
const ALLOWED_WINDOW = 300;
const CACHE_DIR = path.join(process.cwd(), "cache");
const INSTANCE_REFRESH_MS = 60 * 60 * 1000; // refresh every hour
const MAX_INVIDIOUS_TRIES = 3;
const MAX_PIPED_TRIES = 3;

if (!WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

const app = express();
app.use(express.json());

// ---------------- Instance lists ----------------
const INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.tiekoetter.com",
  "https://yewtu.be",
];

const PIPED_INSTANCES = [
  "https://pipedapi.kavin.rocks",
  "https://pipedapi.leptons.xyz",
  "https://pipedapi.nosebs.ru",
  "https://pipedapi-libre.kavin.rocks",
  "https://piped-api.privacy.com.de",
  "https://pipedapi.adminforge.de",
  "https://api.piped.yt",
  "https://pipedapi.drgns.space",
  "https://pipedapi.owo.si",
  "https://pipedapi.ducks.party",
  "https://piped-api.codespace.cz",
  "https://pipedapi.reallyaweso.me",
  "https://api.piped.private.coffee",
  "https://pipedapi.darkness.services",
  "https://pipedapi.orangenet.cc",
];

// -------------------- innertube client --------------------
let ytPromise = Innertube.create({ client_type: "ANDROID", generate_session_locally: true });
let ytClient;
async function getYtClient() {
  if (!ytClient) ytClient = await ytPromise;
  return ytClient;
}

// -------------------- instance management --------------------
const badInstances = new Map(); // instance -> timestamp when marked bad
const nextIndex = { invidious: 0, piped: 0 };
function markBad(instance) {
  try { badInstances.set(instance, Date.now()); console.warn("[INST] mark bad", instance); } catch {}
}
function isBad(instance) {
  const t = badInstances.get(instance);
  if (!t) return false;
  if (Date.now() - t > INSTANCE_BAN_MS) { badInstances.delete(instance); return false; }
  return true;
}
function getInstancesForProvider(list, providerKey) {
  if (!Array.isArray(list) || list.length === 0) return [];
  const idx = (nextIndex[providerKey] || 0) % list.length;
  nextIndex[providerKey] = (idx + 1) % list.length;
  const rotated = [...list.slice(idx), ...list.slice(0, idx)];
  const good = rotated.filter(i => !isBad(i));
  return good.length ? good : rotated;
}

// Periodically attempt to refresh instance lists from optional endpoints provided via env vars.
// These URLs are optional and best-effort: they are attempted but failure doesn't break server.
async function tryRefreshInstances() {
  try {
    // Example: INVIDIOUS_LIST_URL can point to a JSON array of instances
    if (process.env.INVIDIOUS_LIST_URL) {
      try {
        const res = await fetch(process.env.INVIDIOUS_LIST_URL, { method: "GET", timeout: UPSTREAM_TIMEOUT_MS });
        if (res.ok) {
          const j = await res.json();
          if (Array.isArray(j) && j.length) {
            INVIDIOUS_INSTANCES = j;
            console.info("[INST] refreshed Invidious instances from", process.env.INVIDIOUS_LIST_URL);
          }
        }
      } catch (e) {
        console.warn("[INST] failed to fetch INVIDIOUS_LIST_URL", e?.message || e);
      }
    }
    if (process.env.PIPED_LIST_URL) {
      try {
        const res = await fetch(process.env.PIPED_LIST_URL, { method: "GET", timeout: UPSTREAM_TIMEOUT_MS });
        if (res.ok) {
          const j = await res.json();
          if (Array.isArray(j) && j.length) {
            PIPED_INSTANCES = j;
            console.info("[INST] refreshed Piped instances from", process.env.PIPED_LIST_URL);
          }
        }
      } catch (e) {
        console.warn("[INST] failed to fetch PIPED_LIST_URL", e?.message || e);
      }
    }
  } catch (e) {
    console.warn("[INST] tryRefreshInstances error", e?.message || e);
  } finally {
    // schedule next
    setTimeout(tryRefreshInstances, INSTANCE_REFRESH_MS).unref();
  }
}
// start background refresh
setTimeout(tryRefreshInstances, 5_000).unref();

// -------------------- utilities --------------------
function parseSignatureUrl(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher;
  if (!sc) return null;
  try { const params = new URLSearchParams(sc); return params.get("url") || params.get("u") || null; } catch { return null; }
}

function selectBestProgressive(formats) {
  if (!Array.isArray(formats) || formats.length === 0) return null;
  const norm = formats.map(f => ({
    original: f,
    itag: f.itag,
    url: parseSignatureUrl(f) || f.url || null,
    mime: (f.mime_type || f.mimeType || f.type || "").toLowerCase(),
    has_audio: Boolean(f.has_audio || f.audioBitrate || f.audioQuality || /mp4a|aac|vorbis|opus|audio/.test((f.mime_type||"") + (f.codecs||""))),
    height: Number(f.height || (f.qualityLabel && parseInt((f.qualityLabel||"").replace(/[^0-9]/g,""),10)) || f.resolution || 0) || 0,
    bitrate: Number(f.bitrate || f.audioBitrate || 0) || 0
  }));

  // 1) combined audio+video (progressive)
  const combined = norm.filter(f => f.url && f.has_audio && /video/.test(f.mime || "video"));
  if (combined.length) { combined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate)); return combined[0].original; }

  // 2) codecs suggesting combined
  const codecsCombined = norm.filter(f => f.url && /mp4a|aac|opus|vorbis/.test(f.mime));
  if (codecsCombined.length) { codecsCombined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate)); return codecsCombined[0].original; }

  // 3) any video
  const videos = norm.filter(f => f.url && /video/.test(f.mime || ""));
  if (videos.length) { videos.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate)); return videos[0].original; }

  // 4) any URL
  const any = norm.find(f => f.url);
  return any ? any.original : null;
}

async function fetchWithTimeout(url, opts = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    const res = await fetch(url, { method: opts.method || "GET", headers: opts.headers, signal: controller.signal });
    clearTimeout(timeout);
    return res;
  } catch (e) { clearTimeout(timeout); throw e; }
}

// -------------------- provider fetchers (Invidious → Piped → Innertube) --------------------
async function fetchFromInvidious(id) {
  const instances = getInstancesForProvider(INVIDIOUS_INSTANCES, "invidious");
  if (!instances.length) throw new Error("no invidious instances configured");
  let lastErr = null;
  for (const base of instances) {
    for (let attempt = 0; attempt < MAX_INVIDIOUS_TRIES; attempt++) {
      try {
        const url = `${base.replace(/\/$/, "")}/api/v1/videos/${id}`;
        console.info("[INV] trying", base, "attempt", attempt + 1);
        let resp;
        try { resp = await fetchWithTimeout(url); } catch (e) { console.warn("[INV] fetch fail", base, e?.message || e); markBad(base); lastErr = e; continue; }
        console.info("[INV] status", base, resp.status);
        if (!resp.ok) { markBad(base); lastErr = new Error(`status ${resp.status}`); continue; }
        const data = await resp.json();
        const formats = [];
        if (Array.isArray(data.formatStreams)) for (const f of data.formatStreams) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
        if (Array.isArray(data.adaptiveFormats)) for (const f of data.adaptiveFormats) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
        if (Array.isArray(data.formats)) for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
        console.info("[INV] formats found", base, formats.length);
        if (formats.length) return { provider: "invidious", streaming_data: { formats, adaptive_formats: [] } , rawProviderData: data };
        // if no formats, mark bad (the instance didn't supply usable data)
        console.warn("[INV] no formats from", base);
        markBad(base);
      } catch (e) {
        console.warn("[INV] exception for", base, e?.message || e);
        markBad(base);
        lastErr = e;
      }
    }
  }
  throw lastErr || new Error("invidious all instances failed");
}

async function fetchFromPiped(id) {
  const instances = getInstancesForProvider(PIPED_INSTANCES, "piped");
  if (!instances.length) throw new Error("no piped instances configured");
  let lastErr = null;
  for (const base of instances) {
    for (let attempt = 0; attempt < MAX_PIPED_TRIES; attempt++) {
      try {
        const url = `${base.replace(/\/$/, "")}/streams/${id}`;
        console.info("[PIPED] trying", base, "attempt", attempt+1);
        let resp;
        try { resp = await fetchWithTimeout(url); } catch (e) { console.warn("[PIPED] fetch fail", base, e?.message || e); markBad(base); lastErr = e; continue; }
        console.info("[PIPED] status", base, resp.status);
        if (!resp.ok) { markBad(base); lastErr = new Error(`status ${resp.status}`); continue; }
        const data = await resp.json();
        const formats = [];
        if (Array.isArray(data.videoStreams)) for (const v of data.videoStreams) formats.push({ itag: v.itag, url: v.url, mime_type: v.mimeType || v.type, ...v });
        if (Array.isArray(data.audioStreams)) for (const a of data.audioStreams) formats.push({ itag: a.itag, url: a.url, mime_type: a.mimeType || a.type, ...a });
        if (Array.isArray(data.formats)) for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
        console.info("[PIPED] formats found", base, formats.length);
        if (formats.length) return { provider: "piped", streaming_data: { formats, adaptive_formats: [] }, rawProviderData: data };
        console.warn("[PIPED] no formats from", base);
        markBad(base);
      } catch (e) {
        console.warn("[PIPED] exception for", base, e?.message || e);
        markBad(base);
        lastErr = e;
      }
    }
  }
  throw lastErr || new Error("piped all instances failed");
}

async function fetchFromInnertube(id) {
  const client = await getYtClient();
  try {
    console.info("[YT] innertube getInfo", id);
    const info = await client.getInfo(id);
    if (info && info.streaming_data) return { provider: "innertube", streaming_data: info.streaming_data, rawProviderData: info };
    console.info("[YT] trying getStreamingData", id);
    const sd = await client.getStreamingData(id);
    if (sd) {
      const streaming_data = (sd.formats || sd.adaptive_formats) ? sd : { formats: Array.isArray(sd) ? sd : [sd], adaptive_formats: [] };
      return { provider: "innertube", streaming_data, rawProviderData: sd };
    }
  } catch (e) {
    console.warn("[YT] innertube error", e?.message || e);
    throw new Error("innertube failed: " + String(e?.message || e));
  }
  throw new Error("innertube streaming data unavailable");
}

// Keep the requested order: Invidious -> Piped -> Innertube
async function fetchStreamingInfo(id) {
  console.info("[FLOW] fetchStreamingInfo", id);
  try {
    const r = await fetchFromInvidious(id);
    console.info("[FLOW] selected invidious");
    return r;
  } catch (e) {
    console.warn("[FLOW] Invidious failed:", e?.message || e);
  }
  try {
    const r = await fetchFromPiped(id);
    console.info("[FLOW] selected piped");
    return r;
  } catch (e) {
    console.warn("[FLOW] Piped failed:", e?.message || e);
  }
  console.info("[FLOW] fallback innertube");
  return await fetchFromInnertube(id);
}

// -------------------- Security (HMAC + timestamp) --------------------
function timingSafeEqualHex(aHex, bHex) {
  try { const a = Buffer.from(aHex, "hex"); const b = Buffer.from(bHex, "hex"); if (a.length !== b.length) return false; return crypto.timingSafeEqual(a, b); } catch { return false; }
}
function verifyWorkerAuth(req, res, next) {
  if (req.method === "OPTIONS") return next();
  const tsHeader = req.header("x-proxy-timestamp");
  const sigHeader = req.header("x-proxy-signature");
  if (!tsHeader || !sigHeader) return res.status(401).json({ error: "unauthorized" });
  const ts = Number(tsHeader);
  if (!Number.isFinite(ts)) return res.status(401).json({ error: "unauthorized" });
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - ts) > ALLOWED_WINDOW) return res.status(401).json({ error: "unauthorized" });
  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac("sha256", WORKER_SECRET).update(payload).digest("hex");
  if (!timingSafeEqualHex(expected, sigHeader)) return res.status(401).json({ error: "unauthorized" });
  next();
}

// -------------------- Active stream tracking (dedupe simultaneous downloads) --------------------
/*
  activeMp4Streams: key = `${id}:${itag || 'best'}`, value =
    { pass: PassThrough, clients: Set(res), writePromise: Promise, finished: boolean }
*/
const activeMp4Streams = new Map();

function makeCachePaths(id, itag) {
  const dir = path.join(CACHE_DIR, id);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${itag || "best"}.mp4`);
  const tmp = file + ".download";
  return { dir, file, tmp };
}

// robust stream + cache writer
async function streamAndCacheMp4(videoUrl, cacheFile, req, res, key) {
  // key used for activeMp4Streams
  const id = path.basename(path.dirname(cacheFile));
  // If another stream active for same key, join it
  if (activeMp4Streams.has(key)) {
    const state = activeMp4Streams.get(key);
    state.clients.add(res);
    req.on('close', () => state.clients.delete(res));
    console.info("[STREAM] joined existing active stream", key, "clients", state.clients.size);
    // Set headers for this client (best-effort)
    return;
  }

  const { tmp } = makeCachePaths(id, path.basename(cacheFile, ".mp4"));
  const pass = new stream.PassThrough();
  const clients = new Set([res]);
  const state = { pass, clients, finished: false };
  activeMp4Streams.set(key, state);

  const writeStream = fs.createWriteStream(tmp, { flags: 'a' });

  // prepare headers for upstream; forward range if present
  const headers = {};
  if (req.headers.range) headers.Range = req.headers.range;

  let upstream;
  try {
    console.info("[STREAM] fetching upstream", videoUrl);
    upstream = await fetchWithTimeout(videoUrl, { headers });
  } catch (e) {
    console.error("[STREAM] upstream fetch failed", e?.message || e);
    for (const c of clients) try { c.status(502).end(); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    return;
  }

  if (!upstream.ok && upstream.status !== 206) {
    console.error("[STREAM] upstream bad status", upstream.status);
    for (const c of clients) try { c.status(502).end(); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    return;
  }

  // headers
  const contentType = upstream.headers.get('content-type') || 'video/mp4';
  const contentLength = upstream.headers.get('content-length');
  const contentRange = upstream.headers.get('content-range');

  const setHeadersFor = (r) => {
    try {
      r.setHeader('Content-Type', contentType);
      if (contentLength) r.setHeader('Content-Length', contentLength);
      if (contentRange) r.setHeader('Content-Range', contentRange);
      r.setHeader('Accept-Ranges', 'bytes');
      if (req.headers.range) r.status(206);
    } catch (e) { /* ignore */ }
  };

  // set headers for current client
  setHeadersFor(res);

  // ensure each client gets the headers and data
  for (const c of clients) setHeadersFor(c);

  // pipe upstream -> pass -> writeFile and -> clients
  const upstreamStream = upstream.body;

  // Important: pipe upstream to pass; we then pipe pass to file and every client.
  upstreamStream.pipe(pass);
  pass.pipe(writeStream);

  const pipeToClients = () => {
    for (const c of clients) {
      if (c.writableEnded) continue;
      try {
        // each client needs its own piping from pass
        pass.pipe(c, { end: false });
      } catch (e) {
        console.warn("[STREAM] pipe error to client", e?.message || e);
      }
    }
  };
  pipeToClients();

  upstreamStream.on('end', () => {
    try {
      writeStream.close();
      // rename tmp -> final (best-effort)
      try { fs.renameSync(tmp, cacheFile); } catch (err) { console.warn("[STREAM] rename failed", err?.message || err); }
    } catch (e) { console.warn("[STREAM] onend cleanup", e?.message || e); }
    for (const c of clients) try { if (!c.writableEnded) c.end(); } catch {}
    state.finished = true;
    activeMp4Streams.delete(key);
    console.info("[STREAM] finished", key);
  });

  upstreamStream.on('error', (err) => {
    for (const c of clients) try { c.destroy(err); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    console.error("[STREAM] upstream error", err?.message || err);
  });

  // remove client on disconnect
  req.on('close', () => {
    clients.delete(res);
  });
}

// serve from cache with correct Range handling
function streamFromCache(cacheFile, req, res) {
  const stat = fs.statSync(cacheFile);
  const range = req.headers.range;
  if (!range) {
    res.writeHead(200, { 'Content-Length': stat.size, 'Content-Type': 'video/mp4', 'Accept-Ranges': 'bytes' });
    fs.createReadStream(cacheFile).pipe(res);
    return;
  }
  const parts = range.replace(/bytes=/, '').split('-');
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
  if (isNaN(start) || isNaN(end) || start > end || start < 0) return res.status(416).end();
  const chunkSize = (end - start) + 1;
  res.writeHead(206, { 'Content-Range': `bytes ${start}-${end}/${stat.size}`, 'Accept-Ranges': 'bytes', 'Content-Length': chunkSize, 'Content-Type': 'video/mp4' });
  fs.createReadStream(cacheFile, { start, end }).pipe(res);
}

// -------------------- API: /api/stream --------------------
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'id required' });

    const base = path.join(CACHE_DIR, id);
    if (!fs.existsSync(base)) fs.mkdirSync(base, { recursive: true });

    // We'll choose an itag-specific file name; if not found pick best => 'best.mp4'
    // For simplicity: choose the "chosen" format's itag or "best"
    let info;
    try {
      info = await fetchStreamingInfo(id);
    } catch (e) {
      console.error("[API] fetchStreamingInfo failed", e?.message || e);
      return res.status(502).json({ error: 'no streaming info' });
    }

    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    console.info("[API] total formats", formats.length, "provider", info.provider);

    if (!formats.length) return res.status(404).json({ error: 'no formats' });

    const chosen = selectBestProgressive(formats);
    if (!chosen) {
      console.warn("[API] no suitable progressive format");
      return res.status(404).json({ error: 'no suitable format' });
    }

    const rawUrl = parseSignatureUrl(chosen) || chosen.url;
    if (!rawUrl) {
      console.warn("[API] chosen format has no direct url");
      return res.status(422).json({ error: 'format has no direct url' });
    }

    // Log if ip= or ipbits= present in URL (possible IP-bound url)
    try {
      const u = new URL(rawUrl);
      if (u.searchParams.has('ip') || u.searchParams.has('ipbits')) {
        console.warn("[API] url contains ip/ipbits — may be IP-bound", { ip: u.searchParams.get('ip'), ipbits: u.searchParams.get('ipbits') });
      }
      if (u.searchParams.has('expire')) {
        console.info("[API] url expire at (epoch)", u.searchParams.get('expire'));
      }
    } catch (e) {
      // ignore
    }

    const chosenItag = chosen.itag || chosen.itagNo || "best";
    const cacheFile = path.join(base, `${chosenItag}.mp4`);

    // if fully cached already => serve from file
    if (fs.existsSync(cacheFile)) {
      console.info("[API] serving from cache", cacheFile);
      return streamFromCache(cacheFile, req, res);
    }

    // otherwise start streaming+cache; key for dedupe is id+itag
    const key = `${id}:${chosenItag}`;
    await streamAndCacheMp4(rawUrl, cacheFile, req, res, key);
  } catch (e) {
    console.error("[API] unexpected", e?.message || e);
    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => console.log(`Server listening on ${PORT}`));
