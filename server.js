import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { PassThrough } from "stream";

const app = express();
const port = process.env.PORT || 3000;

const CACHE_DIR = process.env.CACHE_DIR || path.resolve(process.cwd(), "cache");
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 1000 * 60 * 60);
const MAX_CACHE_BYTES = Number(process.env.MAX_CACHE_BYTES || 5 * 1024 * 1024 * 1024);
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

if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

const infoCache = new Map();
const inProgress = new Map();

function cacheKeyForMedia(id, itag) {
  return crypto.createHash("sha1").update(`${id}:${itag}`).digest("hex");
}

function cacheFilePath(key) {
  return path.join(CACHE_DIR, key);
}

function tmpFilePath(key) {
  return cacheFilePath(key) + ".tmp";
}

function parseRange(rangeHeader, size) {
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

async function cleanCacheIfNeeded() {
  try {
    const files = fs.readdirSync(CACHE_DIR)
      .filter(f => !f.endsWith(".tmp"))
      .map(f => {
        const p = path.join(CACHE_DIR, f);
        const stat = fs.statSync(p);
        return { p, mtime: stat.mtimeMs, size: stat.size };
      })
      .sort((a, b) => a.mtime - b.mtime);
    let total = files.reduce((s, f) => s + f.size, 0);
    if (total <= MAX_CACHE_BYTES) return;
    for (const f of files) {
      try {
        fs.unlinkSync(f.p);
        total -= f.size;
        if (total <= MAX_CACHE_BYTES) break;
      } catch (e) {}
    }
  } catch (e) {}
}

let yt;
(async () => {
  try {
    yt = await Innertube.create();
  } catch (e) {
    console.error("failed to initialize Innertube", e);
    process.exit(1);
  }
})();

function extractUrlFromFormat(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher || format.signature_cipher;
  if (!sc) return null;
  try {
    const params = new URLSearchParams(sc);
    const u = params.get("url") || params.get("u");
    if (!u) return null;
    return decodeURIComponent(u);
  } catch (e) {
    return null;
  }
}

async function getInfoCached(id) {
  const now = Date.now();
  const entry = infoCache.get(id);
  if (entry && entry.expires > now) return entry.value;
  try {
    const info = await yt.getInfo(id);
    infoCache.set(id, { value: info, expires: now + CACHE_TTL_MS });
    return info;
  } catch (err) {
    console.error("[youtubei] getInfo error", err && err.message ? err.message : err);
    try {
      const sd = await yt.getStreamingData(id);
      if (!sd) throw new Error("getStreamingData returned empty");
      let streaming_data = null;
      if (Array.isArray(sd)) streaming_data = { formats: sd, adaptive_formats: [] };
      else if (sd.formats || sd.adaptive_formats) streaming_data = sd;
      else streaming_data = { formats: Array.isArray(sd) ? sd : [sd], adaptive_formats: [] };
      const fallbackInfo = { streaming_data };
      infoCache.set(id, { value: fallbackInfo, expires: now + CACHE_TTL_MS });
      console.info("[youtubei] getStreamingData fallback succeeded for", id);
      return fallbackInfo;
    } catch (err2) {
      console.error("[youtubei] getStreamingData failed", err2 && err2.message ? err2.message : err2);
      throw err;
    }
  }
}

app.get("/api/stream", async (req, res) => {
  try {
    const id = req.query.id;
    let itag = req.query.itag ? Number(req.query.itag) : null;
    if (!id) return res.status(400).json({ error: "id required" });
    const info = await getInfoCached(id);
    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    if (!formats.length) return res.status(404).json({ error: "no formats" });
    if (!itag) {
      const nonDash = formats.find(f => f.itag === 22) || formats.find(f => f.itag === 18);
      if (nonDash) itag = nonDash.itag;
      else {
        const dashVideo = formats.find(f => f.mime_type && f.mime_type.includes("video")) || formats[0];
        itag = dashVideo.itag;
      }
    }
    const format = formats.find(f => f.itag === itag);
    if (!format) return res.status(404).json({ error: "itag not found" });
    const url = extractUrlFromFormat(format);
    const range = req.headers.range;
    const key = cacheKeyForMedia(id, format.itag);
    const finalPath = cacheFilePath(key);
    const tmpPathFile = tmpFilePath(key);
    if (fs.existsSync(finalPath)) {
      const stat = fs.statSync(finalPath);
      const size = stat.size;
      res.setHeader("Accept-Ranges", "bytes");
      const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
      res.setHeader("Content-Type", contentType);
      if (range) {
        const r = parseRange(range, size);
        if (!r) return res.status(416).setHeader("Content-Range", `bytes */${size}`).end();
        const { start, end } = r;
        res.status(206);
        res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
        res.setHeader("Content-Length", String(end - start + 1));
        const stream = fs.createReadStream(finalPath, { start, end });
        stream.pipe(res);
        return;
      } else {
        res.setHeader("Content-Length", String(size));
        const stream = fs.createReadStream(finalPath);
        stream.pipe(res);
        return;
      }
    }
    if (!url) return res.status(422).json({ error: "format has no direct url" });
    const existing = inProgress.get(key);
    if (existing && !range) {
      try {
        await existing.ready;
      } catch (e) {
        return res.status(502).json({ error: "upstream error" });
      }
      res.status(existing.status || 200);
      for (const [k, v] of Object.entries(existing.headers || {})) {
        try { res.setHeader(k, v); } catch (e) {}
      }
      existing.clients.add(res);
      existing.pass.pipe(res);
      const onClose = () => {
        existing.clients.delete(res);
        try { existing.pass.unpipe(res); } catch (e) {}
      };
      res.once("close", onClose);
      res.once("finish", onClose);
      return;
    }
    if (range) {
      const upstreamHeaders = { Range: range };
      const gvRes = await fetch(url, { headers: upstreamHeaders });
      res.status(gvRes.status);
      for (const [k, v] of gvRes.headers) {
        try { res.setHeader(k, v); } catch (e) {}
      }
      gvRes.body.pipe(res);
      gvRes.body.on("error", () => { try { res.destroy(); } catch (e) {} });
      return;
    }
    let resolveReady;
    let rejectReady;
    const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
    const pass = new PassThrough();
    const entry = {
      pass,
      tmpPath: tmpPathFile,
      finalPath,
      headers: null,
      status: null,
      ready,
      resolveReady,
      rejectReady,
      clients: new Set(),
      aborted: false
    };
    inProgress.set(key, entry);
    const gvRes = await fetch(url);
    const headersObj = {};
    for (const [k, v] of gvRes.headers) {
      const lk = k.toLowerCase();
      if (FORWARD_HEADER_KEYS.has(lk)) headersObj[lk] = v;
    }
    entry.headers = headersObj;
    entry.status = gvRes.status;
    entry.resolveReady();
    const writeStream = fs.createWriteStream(tmpPathFile);
    res.status(gvRes.status);
    for (const [k, v] of Object.entries(headersObj)) {
      try { res.setHeader(k, v); } catch (e) {}
    }
    entry.clients.add(res);
    gvRes.body.pipe(pass);
    pass.pipe(res);
    pass.pipe(writeStream);
    const onClientClose = () => {
      entry.clients.delete(res);
      try { pass.unpipe(res); } catch (e) {}
    };
    res.once("close", onClientClose);
    res.once("finish", onClientClose);
    gvRes.body.on("end", async () => {
      try {
        try { writeStream.end(); } catch (e) {}
        if (fs.existsSync(tmpPathFile)) {
          try { fs.renameSync(tmpPathFile, finalPath); } catch (e) {}
          await cleanCacheIfNeeded();
        }
      } finally {
        inProgress.delete(key);
      }
    });
    gvRes.body.on("error", err => {
      try { writeStream.destroy(); } catch (e) {}
      try { if (fs.existsSync(tmpPathFile)) fs.unlinkSync(tmpPathFile); } catch (e) {}
      entry.rejectReady(err);
      for (const clientRes of entry.clients) {
        try {
          if (!clientRes.headersSent) clientRes.status(502).json({ error: "upstream error" });
          else clientRes.destroy();
        } catch (e) {}
      }
      inProgress.delete(key);
    });
    return;
  } catch (e) {
    try { res.status(500).json({ error: "proxy error" }); } catch (e2) {}
  }
});

app.listen(port, () => {
  console.log(`Server listening on ${port}`);
});