import express from "express";
import { Innertube } from "youtubei.js";
import crypto from "crypto";

/* -------------------------------------------------- */
/* Config */
/* -------------------------------------------------- */

const PORT = process.env.PORT || 3000;

if (!process.env.WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}

const WORKER_SECRET = process.env.WORKER_SECRET;

const ALLOWED_WINDOW = 300;
const INSTANCE_BAN_MS = 5 * 60 * 1000;

/* -------------------------------------------------- */
/* Instances */
/* -------------------------------------------------- */

const INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.tiekoetter.com",
  "https://yewtu.be"
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
  "https://pipedapi.orangenet.cc"
];

/* -------------------------------------------------- */
/* Express */
/* -------------------------------------------------- */

const app = express();

/* -------------------------------------------------- */
/* Innertube */
/* -------------------------------------------------- */

let ytClient;

async function getYtClient() {
  if (!ytClient) {
    ytClient = await Innertube.create({
      client_type: "ANDROID",
      generate_session_locally: true
    });
  }
  return ytClient;
}

/* -------------------------------------------------- */
/* Instance Health */
/* -------------------------------------------------- */

const badInstances = new Map();
const nextIndex = { invidious: 0, piped: 0 };

function markBad(instance) {
  badInstances.set(instance, Date.now());
}

function isBad(instance) {
  const t = badInstances.get(instance);
  if (!t) return false;

  if (Date.now() - t > INSTANCE_BAN_MS) {
    badInstances.delete(instance);
    return false;
  }

  return true;
}

function rotateInstances(list, key) {

  const idx = nextIndex[key] % list.length;
  nextIndex[key] = (idx + 1) % list.length;

  const rotated = [...list.slice(idx), ...list.slice(0, idx)];
  const healthy = rotated.filter(i => !isBad(i));

  return healthy.length ? healthy : rotated;
}

/* -------------------------------------------------- */
/* Utilities */
/* -------------------------------------------------- */

function safeEqual(a,b){
  const A = Buffer.from(a,"hex");
  const B = Buffer.from(b,"hex");

  if (A.length !== B.length) return false;

  return crypto.timingSafeEqual(A,B);
}

function parseUrl(format){

  if (format.url) return format.url;

  const cipher =
    format.signatureCipher ||
    format.signature_cipher ||
    format.cipher;

  if (!cipher) return null;

  try {
    return new URLSearchParams(cipher).get("url");
  } catch {
    return null;
  }

}

function normalizeFormats(sd){

  return [
    ...(sd.formats || []),
    ...(sd.adaptive_formats || [])
  ].map(f => ({
    ...f,
    mime: (f.mimeType || f.mime_type || "").toLowerCase()
  }));

}

/* -------------------------------------------------- */
/* Format Selection */
/* -------------------------------------------------- */

function selectBestVideo(formats){

  return formats
    .filter(f => f.mime.includes("video"))
    .sort((a,b)=>
      (b.height || 0) - (a.height || 0) ||
      (b.bitrate || 0) - (a.bitrate || 0)
    )[0] || null;

}

function selectBestAudio(formats){

  return formats
    .filter(f => f.mime.includes("audio"))
    .sort((a,b)=>
      (b.bitrate || 0) - (a.bitrate || 0)
    )[0] || null;

}

function selectBestProgressive(formats){

  return formats
    .filter(f =>
      f.mime.includes("video") &&
      /mp4a|aac|opus/.test(f.mime)
    )
    .sort((a,b)=>
      (b.height || 0) - (a.height || 0)
    )[0] || null;

}

/* -------------------------------------------------- */
/* フォーマット判定 */
/* -------------------------------------------------- */

function isSupportedFormat(f) {

  const url = f.url || parseUrl(f);
  if (!url) return false;

  const mime = (f.mimeType || f.mime_type || "").toLowerCase();

  // -----------------------------
  // DASH / Progressive 判定
  // -----------------------------
  if (
    mime.includes("video/mp4") ||
    mime.includes("audio/mp4") ||
    mime.includes("video/webm") ||
    mime.includes("audio/webm") ||
    /audio\/opus|audio\/aac|audio\/mp3/.test(mime)
  ) {
    return true;
  }

  // -----------------------------
  // その他は非対応
  // -----------------------------
  return false;
}

/* -------------------------------------------------- */
/* Parallel Fetch */
/* -------------------------------------------------- */

async function fastestFetch(instances, buildUrl, parser){

  const controllers = [];

  const tasks = instances.map(async base => {

    const controller = new AbortController();
    controllers.push(controller);

    try{

      const res = await fetch(buildUrl(base), {
        signal: controller.signal
      });

      if (!res.ok) throw new Error();

      const data = await res.json();
      const parsed = parser(data);

      if (!parsed) throw new Error();

      return parsed;

    }catch{

      markBad(base);
      throw new Error();

    }

  });

  const result = await Promise.any(tasks);

  controllers.forEach(c => c.abort());

  return result;

}

/* -------------------------------------------------- */
/* Providers */
/* -------------------------------------------------- */

async function fetchFromInvidious(id){

  const instances = rotateInstances(
    INVIDIOUS_INSTANCES,
    "invidious"
  );

  return fastestFetch(
    instances,
    base => `${base}/api/v1/videos/${id}`,
    data => {

      const formats = [];

      data.formatStreams?.forEach(f =>
        formats.push({ ...f, mimeType: f.type })
      );

      data.adaptiveFormats?.forEach(f =>
        formats.push({ ...f, mimeType: f.type })
      );

      if (!formats.length) return null;

      return {
        provider: "invidious",
        streaming_data: { formats }
      };

    }
  );

}

async function fetchFromPiped(id){

  const instances = rotateInstances(
    PIPED_INSTANCES,
    "piped"
  );

  return fastestFetch(
    instances,
    base => `${base}/streams/${id}`,
    data => {

      const formats = [];

      data.videoStreams?.forEach(v => formats.push(v));
      data.audioStreams?.forEach(a => formats.push(a));

      if (!formats.length) return null;

      return {
        provider: "piped",
        streaming_data: { formats }
      };

    }
  );

}

async function fetchFromInnertube(id){

  const client = await getYtClient();
  const info = await client.getInfo(id);

  if (!info?.streaming_data)
    throw new Error("No streaming data");

  return {
    provider: "innertube",
    streaming_data: info.streaming_data
  };

}

async function fetchStreamingInfo(id){

  try { return await fetchFromInvidious(id); } catch {}
  try { return await fetchFromPiped(id); } catch {}

  return fetchFromInnertube(id);

}

/* -------------------------------------------------- */
/* Worker Auth */
/* -------------------------------------------------- */

function verifyWorkerAuth(req,res,next){

  const ts = req.header("x-proxy-timestamp");
  const sig = req.header("x-proxy-signature");

  if (!ts || !sig)
    return res.status(401).json({error:"unauthorized"});

  const now = Math.floor(Date.now()/1000);

  if (Math.abs(now - Number(ts)) > ALLOWED_WINDOW)
    return res.status(401).json({error:"unauthorized"});

  const payload = `${ts}:${req.originalUrl}`;

  const expected = crypto
    .createHmac("sha256", WORKER_SECRET)
    .update(payload)
    .digest("hex");

  if (!safeEqual(expected, sig))
    return res.status(401).json({error:"unauthorized"});

  next();

}

/* -------------------------------------------------- */
/* API */
/* -------------------------------------------------- */

app.get("/api/stream", verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: "id required" });

    // -----------------------
    // 1. Streaming info 取得
    // -----------------------
    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data;

    // -----------------------
    // 2. フォーマットをサニタイズ
    // DASH/Progressiveのみ残す
    // -----------------------
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])]
      .filter(isSupportedFormat)
      .map(f => ({ ...f, mime: (f.mimeType || f.mime_type || "").toLowerCase() }));

    if (!formats.length) {
      return res.status(415).json({
        error: "No supported stream formats available (DASH/Progressive only, HLS rejected)"
      });
    }

    // -----------------------
    // 3. 最高画質の選択
    // -----------------------
    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);

    if (video && audio) {
      return res.json({
        type: "dash",
        quality: video.height || null,
        video_url: parseUrl(video),
        audio_url: parseUrl(audio),
        video_itag: video.itag,
        audio_itag: audio.itag,
        provider: info.provider
      });
    }

    const progressive = selectBestProgressive(formats);

    if (progressive) {
      return res.json({
        type: "progressive",
        quality: progressive.height || null,
        url: parseUrl(progressive),
        itag: progressive.itag,
        provider: info.provider
      });
    }

    return res.status(404).json({ error: "no stream available" });

  } catch (e) {
    return res.status(e.status || 500).json({ error: e.message });
  }
});

/* -------------------------------------------------- */

app.listen(PORT,()=>{
  console.log(`Server running on ${PORT}`);
});
