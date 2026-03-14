import express from "express";
import { Innertube } from "youtubei.js";
import crypto from "crypto";

if (!process.env.WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}

const app = express();
const port = process.env.PORT || 3000;

const WORKER_SECRET = process.env.WORKER_SECRET;
const ALLOWED_WINDOW = 300;
const INSTANCE_BAN_MS = 5 * 60 * 1000;

/* ---------------- Instances ---------------- */

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

/* ---------------- Innertube ---------------- */

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

/* ---------------- Instance Health ---------------- */

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
  const good = rotated.filter(i => !isBad(i));

  return good.length ? good : rotated;
}

/* ---------------- HLS Utilities ---------------- */

async function resolveHlsVariant(manifestUrl) {

  try {

    const res = await fetch(manifestUrl);
    if (!res.ok) return manifestUrl;

    const text = await res.text();
    const lines = text.split("\n");

    const variants = [];

    for (let i = 0; i < lines.length; i++) {

      const line = lines[i];

      if (line.startsWith("#EXT-X-STREAM-INF")) {

        const next = lines[i + 1];

        const match = line.match(/RESOLUTION=(\d+)x(\d+)/);
        const height = match ? Number(match[2]) : 0;

        if (next && !next.startsWith("#")) {

          const url = new URL(next, manifestUrl).href;

          variants.push({
            url,
            height
          });

        }

      }

    }

    if (!variants.length) return manifestUrl;

    variants.sort((a,b)=>b.height-a.height);

    return variants[0].url;

  } catch {
    return manifestUrl;
  }

}

/* ---------------- Format Utilities ---------------- */

function parseUrl(format) {

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

function normalizeFormats(sd) {

  return [
    ...(sd.formats || []),
    ...(sd.adaptive_formats || [])
  ].map(f => ({
    ...f,
    mime: (f.mimeType || f.mime_type || "").toLowerCase()
  }));

}

function selectBestVideo(formats) {

  return formats
    .filter(f => f.mime.includes("video"))
    .sort((a,b)=>
      (b.height || 0) - (a.height || 0) ||
      (b.bitrate || 0) - (a.bitrate || 0)
    )[0] || null;

}

function selectBestAudio(formats) {

  return formats
    .filter(f => f.mime.includes("audio"))
    .sort((a,b)=>
      (b.bitrate || 0) - (a.bitrate || 0)
    )[0] || null;

}

function selectBestProgressive(formats) {

  return formats
    .filter(f =>
      f.mime.includes("video") &&
      /mp4a|aac|opus/.test(f.mime)
    )
    .sort((a,b)=>
      (b.height || 0) - (a.height || 0)
    )[0] || null;

}

/* ---------------- Parallel Fetch ---------------- */

async function fastestFetch(instances, buildUrl, parser) {

  const controllers = [];

  const tasks = instances.map(async base => {

    const controller = new AbortController();
    controllers.push(controller);

    try {

      const res = await fetch(buildUrl(base), {
        signal: controller.signal
      });

      if (!res.ok) {
        markBad(base);
        throw new Error();
      }

      const data = await res.json();
      const parsed = parser(data);

      if (!parsed) throw new Error();

      return parsed;

    } catch {

      markBad(base);
      throw new Error();

    }

  });

  const result = await Promise.any(tasks);

  controllers.forEach(c => c.abort());

  return result;

}

/* ---------------- Providers ---------------- */

async function fetchFromInvidious(id) {

  const instances = rotateInstances(
    INVIDIOUS_INSTANCES,
    "invidious"
  );

  return fastestFetch(
    instances,
    base => `${base}/api/v1/videos/${id}`,
    data => {

      if (data.hlsUrl) {

        const url = data.hlsUrl.startsWith("http")
          ? data.hlsUrl
          : base + data.hlsUrl;

        return {
          provider: "invidious",
          streaming_data: {
            hlsManifestUrl: url
          }
        };

      }

      const formats = [];

      if (data.formatStreams)
        data.formatStreams.forEach(f =>
          formats.push({ ...f, mimeType: f.type })
        );

      if (data.adaptiveFormats)
        data.adaptiveFormats.forEach(f =>
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

async function fetchFromPiped(id) {

  const instances = rotateInstances(
    PIPED_INSTANCES,
    "piped"
  );

  return fastestFetch(
    instances,
    base => `${base}/streams/${id}`,
    data => {

      if (data.hls) {

        return {
          provider: "piped",
          streaming_data: { hlsManifestUrl: data.hls }
        };

      }

      const formats = [];

      if (data.videoStreams)
        data.videoStreams.forEach(v => formats.push(v));

      if (data.audioStreams)
        data.audioStreams.forEach(a => formats.push(a));

      if (!formats.length) return null;

      return {
        provider: "piped",
        streaming_data: { formats }
      };

    }
  );

}

async function fetchFromInnertube(id) {

  const client = await getYtClient();
  const info = await client.getInfo(id);

  if (!info?.streaming_data)
    throw new Error("No streaming data");

  return {
    provider: "innertube",
    streaming_data: info.streaming_data
  };

}

async function fetchStreamingInfo(id) {

  try { return await fetchFromInvidious(id); } catch {}
  try { return await fetchFromPiped(id); } catch {}

  return fetchFromInnertube(id);

}

/* ---------------- Auth ---------------- */

function safeEqual(a,b){

  const A = Buffer.from(a,"hex");
  const B = Buffer.from(b,"hex");

  if (A.length !== B.length) return false;

  return crypto.timingSafeEqual(A,B);

}

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

/* ---------------- API ---------------- */

app.get("/api/stream", verifyWorkerAuth, async (req,res)=>{

  try{

    const id = req.query.id;

    if (!id)
      return res.status(400).json({error:"id required"});

    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data;

    /* HLSを明示的に拒否 */
    const hls =
      sd.hlsManifestUrl ||
      sd.hls_manifest_url ||
      sd.hlsUrl ||
      sd.hls;

    if (hls) {
      return res.status(403).json({ error: "HLS streams are not supported" });
    }

    const formats = normalizeFormats(sd);

    /* DASH */

    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);

    if (video && audio) {

      return res.json({
        type:"dash",
        quality: video.height || null,
        video_url: parseUrl(video),
        audio_url: parseUrl(audio),
        provider: info.provider
      });

    }

    /* Progressive */

    const progressive = selectBestProgressive(formats);

    if (progressive) {

      return res.json({
        type:"progressive",
        quality: progressive.height || null,
        url: parseUrl(progressive),
        provider: info.provider
      });

    }

    return res.status(404).json({error:"no stream"});

  } catch(e){

    return res.status(500).json({
      error: e.message
    });

  }

});

app.listen(port,()=>{
  console.log(`Server running on ${port}`);
});
