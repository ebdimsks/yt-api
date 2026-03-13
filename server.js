import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { PassThrough, Readable } from "stream";

if (!process.env.WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}

const app = express();
const port = process.env.PORT || 3000;
const CACHE_DIR = path.resolve(process.cwd(), "cache");
const MAX_CACHE_BYTES = 5 * 1024 * 1024 * 1024; // 5GB
const FORWARD_HEADER_KEYS = new Set([
  "content-type",
  "content-length",
  "accept-ranges",
  "content-range",
  "content-disposition",
  "cache-control",
  "etag",
  "last-modified"
]);
const ALLOWED_WINDOW = 300; // seconds
const WORKER_SECRET = process.env.WORKER_SECRET;
const UPSTREAM_TIMEOUT_MS = 5000; // upstream fetch timeout

if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

// ---------------- Instance lists ----------------
const INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://invidious.ducks.party",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.privacyredirect.com",
  "https://invidious.technicalvoid.dev",
  "https://invidious.darkness.services",
  "https://invidious.nikkosphere.com",
  "https://invidious.schenkel.eti.br",
  "https://invidious.tiekoetter.com",
  "https://invidious.perennialte.ch",
  "https://invidious.reallyaweso.me",
  "https://invidious.private.coffee",
  "https://invidious.privacydev.net",
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

// ---------------- Instance rotation & health ----------------
const badInstances = new Map(); // instance -> timestamp of failure
const INSTANCE_BAN_MS = 5 * 60 * 1000; // 5 minutes

function markBad(instance) { badInstances.set(instance, Date.now()); }
function isBad(instance) {
  const t = badInstances.get(instance);
  if (!t) return false;
  if (Date.now() - t > INSTANCE_BAN_MS) { badInstances.delete(instance); return false; }
  return true;
}
function getInstances(list) {
  const good = list.filter(i => !isBad(i));
  return good.length ? good : list;
}

// ---------------- youtubei.js (Innertube) ----------------
let ytPromise = Innertube.create({ client_type: "ANDROID", generate_session_locally: true });
let ytClient;
async function getYtClient() {
  if (!ytClient) ytClient = await ytPromise;
  return ytClient;
}

// ---------------- Utilities ----------------
function sha1Key(id, itag) {
  return crypto.createHash("sha1").update(`${id}:${itag}`).digest("hex");
}
function filePathForKey(key) { return path.join(CACHE_DIR, key); }
function tmpPathForKey(key) { return `${filePathForKey(key)}.tmp`; }

function parseSignatureUrl(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher || format.signatureCipher;
  if (!sc) return null;
  try {
    const params = new URLSearchParams(sc);
    const u = params.get("url") || params.get("u");
    return u || null;
  } catch (e) {
    return null;
  }
}

function parseRangeHeader(rangeHeader, size) {
  if (!rangeHeader) return null;
  const m = rangeHeader.match(/bytes=(\d*)-(\d*)/);
  if (!m) return null;
  let start = m[1] === "" ? null : parseInt(m[1], 10);
  let end = m[2] === "" ? null : parseInt(m[2], 10);
  if (start === null) {
    start = Math.max(0, size - end);
    end = size - 1;
  } else if (end === null) {
    end = size - 1;
  }
  if (Number.isNaN(start) || Number.isNaN(end) || start > end || start < 0 || end >= size) return null;
  return { start, end };
}

async function enforceCacheLimit() {
  try {
    const files = fs.readdirSync(CACHE_DIR)
      .filter(f => !f.endsWith(".tmp"))
      .map(f => {
        const p = path.join(CACHE_DIR, f);
        const s = fs.statSync(p);
        return { path: p, mtime: s.mtimeMs, size: s.size };
      })
      .sort((a,b) => a.mtime - b.mtime);
    let total = files.reduce((acc,f) => acc + f.size, 0);
    if (total <= MAX_CACHE_BYTES) return;
    for (const f of files) {
      try {
        fs.unlinkSync(f.path);
        total -= f.size;
        if (total <= MAX_CACHE_BYTES) break;
      } catch {}
    }
  } catch {}
}

function timingSafeEqualHex(aHex, bHex) {
  try {
    const a = Buffer.from(aHex, "hex");
    const b = Buffer.from(bHex, "hex");
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch { return false; }
}

// ---------------- Worker auth middleware ----------------
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

// ---------------- Providers ----------------

async function fetchFromInvidious(id) {
  const instances = getInstances(INVIDIOUS_INSTANCES);
  for (const base of instances) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
      const r = await fetch(`${base}/api/v1/videos/${id}`, {
        headers: { "User-Agent": "Mozilla/5.0" },
        signal: controller.signal
      });
      clearTimeout(timeout);
      if (!r.ok) throw new Error(`invidious ${base} status ${r.status}`);
      const data = await r.json();
      const formats = [];
      for (const f of data.formatStreams || []) formats.push({ ...f, mime_type: f.type, itag: f.itag, url: f.url });
      for (const f of data.adaptiveFormats || []) formats.push({ ...f, mime_type: f.type, itag: f.itag, url: f.url });
      if (formats.length) return { streaming_data: { formats, adaptive_formats: [] } };
    } catch (e) {
      markBad(base);
    }
  }
  throw new Error("invidious failed");
}

async function fetchFromPiped(id) {
  const instances = getInstances(PIPED_INSTANCES);
  for (const base of instances) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
      const r = await fetch(`${base}/streams/${id}`, { signal: controller.signal });
      clearTimeout(timeout);
      if (!r.ok) throw new Error(`piped ${base} status ${r.status}`);
      const data = await r.json();
      const formats = [];
      for (const v of data.videoStreams || []) formats.push({ ...v, mime_type: v.mimeType, itag: v.itag, url: v.url });
      for (const a of data.audioStreams || []) formats.push({ ...a, mime_type: a.mimeType, itag: a.itag, url: a.url });
      if (formats.length) return { streaming_data: { formats, adaptive_formats: [] } };
    } catch (e) {
      markBad(base);
    }
  }
  throw new Error("piped failed");
}

async function fetchFromInnertube(id) {
  const client = await getYtClient();
  const info = await client.getInfo(id);
  if (!info?.streaming_data) throw new Error("innertube failed");
  return { streaming_data: info.streaming_data };
}

// Choose best progressive (combined audio+video) format. If none, fallback to best video-only.
function selectBestProgressive(formats) {
  if (!Array.isArray(formats) || formats.length === 0) return null;
  // Normalize keys for different providers
  const norm = formats.map(f => ({
    original: f,
    itag: f.itag,
    url: parseSignatureUrl(f) || f.url || f.urlSimple || null,
    mime: (f.mime_type || f.mimeType || "").toLowerCase(),
    has_audio: Boolean(f.has_audio || f.audioBitrate || f.audioQuality || /mp4a|aac|vorbis|opus|audio/.test((f.mime_type||"") + (f.codecs||""))),
    height: Number(f.height || f.qualityLabel && parseInt((f.qualityLabel||"").replace(/[^0-9]/g,""),10) || f.resolution || 0) || 0,
    bitrate: Number(f.bitrate || f.audioBitrate || f.bitrateKbps || 0) || 0
  })).filter(Boolean);

  // Filter combined
  const combined = norm.filter(f => f.url && f.has_audio && /video/.test(f.mime || "video"));
  if (combined.length) {
    // sort by height desc, then bitrate desc
    combined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate));
    return combined[0].original;
  }

  // Fallback: choose best single file that likely contains both audio (by codecs) or highest height
  const codecsCombined = norm.filter(f => f.url && /mp4a|aac|opus|vorbis/.test(f.mime));
  if (codecsCombined.length) {
    codecsCombined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate));
    return codecsCombined[0].original;
  }

  // Final fallback: best video-only
  const videos = norm.filter(f => f.url && /video/.test(f.mime || "") );
  if (videos.length) {
    videos.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate));
    return videos[0].original;
  }

  // Very last resort: first format that has a url
  const any = norm.find(f => f.url);
  return any ? any.original : null;
}

async function fetchStreamingInfo(id) {
  try { return await fetchFromInvidious(id); } catch (e) { /* continue */ }
  try { return await fetchFromPiped(id); } catch (e) { /* continue */ }
  try { return await fetchFromInnertube(id); } catch (e) { /* continue */ }
  throw new Error("all providers failed");
}

// ---------------- Shared in-progress downloads ----------------
const inProgress = new Map();

async function startBackgroundDownload(key, url) {
  // If already in progress, return existing entry
  if (inProgress.has(key)) return inProgress.get(key);

  const tmpPath = tmpPathForKey(key);
  const finalPath = filePathForKey(key);

  const entry = {
    pass: new PassThrough(),
    tmpPath,
    finalPath,
    headers: null,
    status: null,
    clients: new Set(),
    ready: null,
    resolve: null,
    reject: null,
    headersReady: null,
    _resolveHeaders: null
  };

  entry.ready = new Promise((res, rej) => { entry.resolve = res; entry.reject = rej; });
  entry.headersReady = new Promise((res) => { entry._resolveHeaders = res; });

  inProgress.set(key, entry);

  (async () => {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
      const resp = await fetch(url, { signal: controller.signal });
      clearTimeout(timeout);

      if (!resp.ok) throw new Error(`upstream status ${resp.status}`);

      // create headers object to forward to clients
      const headersObj = {};
      for (const [k,v] of resp.headers) {
        try {
          const lk = k.toLowerCase();
          if (FORWARD_HEADER_KEYS.has(lk)) headersObj[lk] = v;
        } catch {}
      }
      entry.headers = headersObj;
      entry.status = resp.status;
      // notify headers ready
      try { entry._resolveHeaders(); } catch {}

      if (!resp.body) throw new Error("no body");
      const ws = fs.createWriteStream(tmpPath);

      const stream = typeof resp.body.pipe === "function" ? resp.body : Readable.fromWeb(resp.body);
      stream.on("error", (e) => { try { ws.destroy(e); } catch(_){} entry.reject(e); });
      ws.on("error", (e) => { try { stream.destroy(e); } catch(_){} entry.reject(e); });

      // stream to shared pass and file
      stream.pipe(entry.pass);
      stream.pipe(ws);

      await Promise.all([
        new Promise((r,j) => { stream.on("end", r); stream.on("error", j); }),
        new Promise((r,j) => { ws.on("finish", r); ws.on("error", j); })
      ]);

      try { ws.end(); } catch(_) {}
      if (fs.existsSync(tmpPath)) {
        try { fs.renameSync(tmpPath, finalPath); } catch (e) {}
        entry.resolve();
        inProgress.delete(key);
        await enforceCacheLimit();
      } else {
        entry.reject(new Error("tmp missing"));
        inProgress.delete(key);
      }
    } catch (e) {
      entry.reject(e);
      inProgress.delete(key);
    }
  })();

  return entry;
}

// Attach res to existing entry (non-range safe)
function attachClientToEntry(entry, res) {
  try {
    // set status and headers if available; otherwise wait for headersReady
    if (entry.headers) {
      try {
        res.status(entry.status || 200);
        for (const [k,v] of Object.entries(entry.headers)) res.setHeader(k, v);
        // ensure Accept-Ranges and Content-Type exist
        if (!res.getHeader("accept-ranges")) res.setHeader("Accept-Ranges", "bytes");
        if (!res.getHeader("content-type")) res.setHeader("Content-Type", "application/octet-stream");
      } catch {}
    } else {
      // wait until headers are ready (non-blocking)
      entry.headersReady.then(() => {
        try {
          res.status(entry.status || 200);
          for (const [k,v] of Object.entries(entry.headers || {})) res.setHeader(k, v);
          if (!res.getHeader("accept-ranges")) res.setHeader("Accept-Ranges", "bytes");
          if (!res.getHeader("content-type")) res.setHeader("Content-Type", "application/octet-stream");
        } catch {}
      }).catch(() => {});
    }

    entry.clients.add(res);
    entry.pass.pipe(res);

    const onClose = () => {
      entry.clients.delete(res);
      try { entry.pass.unpipe(res); } catch (_) {}
    };
    res.once("close", onClose);
    res.once("finish", onClose);
  } catch (e) {
    try { res.destroy(); } catch {}
  }
}

// ---------------- API: /api/stream ----------------
app.get("/api/stream", verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: "id required" });

    // get streaming info (providers chain)
    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    if (!formats.length) return res.status(404).json({ error: "no formats" });

    // choose best progressive (combined) format
    const chosenFormat = selectBestProgressive(formats);
    if (!chosenFormat) return res.status(404).json({ error: "no usable format" });

    // allow overriding itag via query param (optional)
    let itag = req.query.itag ? Number(req.query.itag) : chosenFormat.itag;
    const format = formats.find(f => f.itag === itag) || chosenFormat;
    const url = parseSignatureUrl(format) || format.url;
    if (!url) return res.status(422).json({ error: "format has no direct url" });

    const key = sha1Key(id, format.itag);
    const finalPath = filePathForKey(key);
    const range = req.headers.range;

    // If file cached -> serve from disk (support range)
    if (fs.existsSync(finalPath)) {
      const stat = fs.statSync(finalPath);
      const size = stat.size;
      res.setHeader("Accept-Ranges", "bytes");
      const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
      res.setHeader("Content-Type", contentType);

      if (range) {
        const r = parseRangeHeader(range, size);
        if (!r) return res.status(416).setHeader("Content-Range", `bytes */${size}`).end();
        const { start, end } = r;
        res.status(206);
        res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
        res.setHeader("Content-Length", String(end - start + 1));
        const stream = fs.createReadStream(finalPath, { start, end });
        stream.on("error", () => { try { res.destroy(); } catch (e) {} });
        stream.pipe(res);
        return;
      } else {
        res.setHeader("Content-Length", String(size));
        const stream = fs.createReadStream(finalPath);
        stream.on("error", () => { try { res.destroy(); } catch (e) {} });
        stream.pipe(res);
        return;
      }
    }

    // Not cached
    const ongoing = inProgress.get(key);

    // If there's an ongoing shared download
    if (ongoing) {
      // Range requests: serve separately (can't satisfy arbitrary range from shared pass)
      if (range) {
        try {
          const upstream = await fetch(url, { headers: { Range: range } });
          res.status(upstream.status);
          for (const [k,v] of upstream.headers) {
            try { if (FORWARD_HEADER_KEYS.has(k.toLowerCase())) res.setHeader(k, v); } catch {}
          }
          if (!upstream.body) return res.status(502).json({ error: "no upstream body" });
          const upstreamStream = typeof upstream.body.pipe === "function" ? upstream.body : Readable.fromWeb(upstream.body);
          upstreamStream.on("error", () => { try { res.destroy(); } catch (e) {} });
          upstreamStream.pipe(res);
          // ensure background download exists
          startBackgroundDownload(key, url).catch(()=>{});
          return;
        } catch (e) {
          return res.status(502).json({ error: "upstream error" });
        }
      } else {
        // non-range — attach to shared stream
        attachClientToEntry(ongoing, res);
        return;
      }
    }

    // No ongoing inProgress
    if (range) {
      // Start background full download (so subsequent clients benefit), but serve this range immediately separately
      startBackgroundDownload(key, url).catch(()=>{});
      try {
        const upstream = await fetch(url, { headers: { Range: range } });
        res.status(upstream.status);
        for (const [k,v] of upstream.headers) {
          try { if (FORWARD_HEADER_KEYS.has(k.toLowerCase())) res.setHeader(k, v); } catch {}
        }
        if (!upstream.body) return res.status(502).json({ error: "no upstream body" });
        const upstreamStream = typeof upstream.body.pipe === "function" ? upstream.body : Readable.fromWeb(upstream.body);
        upstreamStream.on("error", () => { try { res.destroy(); } catch (e) {} });
        upstreamStream.pipe(res);
        return;
      } catch (e) {
        return res.status(502).json({ error: "upstream error" });
      }
    } else {
      // No range, start a shared download and attach this client
      const entry = await startBackgroundDownload(key, url);
      attachClientToEntry(entry, res);
      return;
    }
  } catch (e) {
    console.error("stream handler error:", e?.message || e);
    try { res.status(500).json({ error: String(e?.message || e) }); } catch {}
  }
});

// ---------------- Start server ----------------
app.listen(port, () => {
  console.log(`Server listening on ${port}`);
});
