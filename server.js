import express from 'express';
import { Innertube } from 'youtubei.js';
import crypto from 'crypto';

// Required env
const { WORKER_SECRET, PORT } = process.env;
if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = PORT || 3000;

const ALLOWED_WINDOW = 300; // seconds
const INSTANCE_BAN_MS = 5 * 60 * 1000; // 5 minutes

/* ---------------- Invidious instances ---------------- */
const INVIDIOUS_INSTANCES = [
  'https://inv.nadeko.net',
  'https://invidious.f5.si',
  'https://invidious.lunivers.trade',
  'https://iv.melmac.space',
  'https://yt.omada.cafe',
  'https://invidious.nerdvpn.de',
  'https://invidious.tiekoetter.com',
  'https://yewtu.be',
];

/* ---------------- Innertube client (cached) ---------------- */
let ytClient = null;
const getYtClient = async () => {
  if (!ytClient) {
    ytClient = await Innertube.create({
      client_type: 'ANDROID',
      generate_session_locally: true,
    });
  }
  return ytClient;
};

/* ---------------- Instance health / rotation ---------------- */
const badInstances = new Map();
let rrIndex = 0; // simple round-robin index

const markBad = (instance) => badInstances.set(instance, Date.now());

const rotateInstances = (list) => {
  if (!Array.isArray(list) || list.length === 0) return [];

  const start = rrIndex % list.length;
  rrIndex = (start + 1) % list.length;

  const rotated = [...list.slice(start), ...list.slice(0, start)];

  // prefer healthy instances
  const good = rotated.filter((i) => {
    const t = badInstances.get(i);
    if (!t) return true;
    if (Date.now() - t > INSTANCE_BAN_MS) {
      badInstances.delete(i);
      return true;
    }
    return false;
  });

  return good.length ? good : rotated;
};

/* ---------------- Format helpers ---------------- */
const parseUrl = (format) => {
  if (!format) return null;
  if (format.url) return format.url;

  const cipher = format.signatureCipher || format.signature_cipher || format.cipher;
  if (!cipher) return null;

  try {
    return new URLSearchParams(cipher).get('url');
  } catch {
    return null;
  }
};

const normalizeFormats = (sd = {}) => [
  ...(sd.formats || []),
  ...(sd.adaptive_formats || []),
].map((f) => ({
  ...f,
  mime: (f.mimeType || f.mime_type || f.type || '').toLowerCase(),
}));

const selectBestVideo = (formats) =>
  formats
    .filter((f) => f.mime.includes('video'))
    .sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestAudio = (formats) =>
  formats
    .filter((f) => f.mime.includes('audio'))
    .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestProgressive = (formats) =>
  formats
    .filter((f) => f.mime.includes('video') && /mp4a|aac|opus/.test(f.mime))
    .sort((a, b) => (b.height || 0) - (a.height || 0))[0] || null;

/* ---------------- Parallel fetch to multiple Invidious instances ---------------- */
const fastestFetch = async (instances, buildUrl, parser) => {
  const controllers = [];

  const tasks = instances.map(async (base) => {
    const controller = new AbortController();
    controllers.push(controller);

    try {
      const res = await fetch(buildUrl(base), { signal: controller.signal });

      if (!res.ok) {
        markBad(base);
        throw new Error('bad response');
      }

      const data = await res.json();
      const parsed = parser(data);

      if (!parsed) {
        markBad(base);
        throw new Error('parse failed');
      }

      return parsed;
    } catch (err) {
      markBad(base);
      throw err;
    }
  });

  const result = await Promise.any(tasks);

  // cancel remaining requests
  controllers.forEach((c) => c.abort());

  return result;
};

/* ---------------- Providers ---------------- */
const fetchFromInvidious = async (id) => {
  const instances = rotateInstances(INVIDIOUS_INSTANCES);

  return fastestFetch(
    instances,
    (base) => `${base}/api/v1/videos/${id}`,
    (data) => {
      const formats = [];

      (data.formatStreams || []).forEach((f) => formats.push({ ...f, mimeType: f.type }));
      (data.adaptiveFormats || []).forEach((f) => formats.push({ ...f, mimeType: f.type }));

      if (!formats.length) return null;

      return {
        provider: 'invidious',
        streaming_data: { formats },
      };
    }
  );
};

const fetchFromInnertube = async (id) => {
  const client = await getYtClient();
  const info = await client.getInfo(id);

  if (!info?.streaming_data) throw new Error('No streaming data');

  return {
    provider: 'innertube',
    streaming_data: info.streaming_data,
  };
};

const fetchStreamingInfo = async (id) => {
  try {
    return await fetchFromInvidious(id);
  } catch (e) {
    return fetchFromInnertube(id);
  }
};

/* ---------------- Auth helpers ---------------- */
const safeEqual = (a, b) => {
  try {
    const A = Buffer.from(a, 'hex');
    const B = Buffer.from(b, 'hex');
    if (A.length !== B.length) return false;
    return crypto.timingSafeEqual(A, B);
  } catch {
    return false;
  }
};

const verifyWorkerAuth = (req, res, next) => {
  const ts = req.header('x-proxy-timestamp');
  const sig = req.header('x-proxy-signature');

  if (!ts || !sig) return res.status(401).json({ error: 'unauthorized' });

  const now = Math.floor(Date.now() / 1000);
  const t = Number(ts);

  if (!Number.isFinite(t) || Math.abs(now - t) > ALLOWED_WINDOW)
    return res.status(401).json({ error: 'unauthorized' });

  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');

  if (!safeEqual(expected, sig)) return res.status(401).json({ error: 'unauthorized' });

  next();
};

/* ---------------- API ---------------- */
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'id required' });

    const info = await fetchStreamingInfo(String(id));
    const sd = info.streaming_data || {};

    // HLS explicitly disallowed
    const hls = sd.hlsManifestUrl || sd.hls_manifest_url || sd.hlsUrl || sd.hls;
    if (hls) return res.status(403).json({ error: 'HLS streams are not supported' });

    const formats = normalizeFormats(sd);

    // DASH (separate video+audio)
    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);

    if (video && audio) {
      return res.json({
        type: 'dash',
        video_url: parseUrl(video),
        audio_url: parseUrl(audio),
        provider: info.provider,
      });
    }

    // Progressive (muxed)
    const progressive = selectBestProgressive(formats);
    if (progressive) {
      return res.json({ type: 'progressive', url: parseUrl(progressive), provider: info.provider });
    }

    return res.status(404).json({ error: 'no stream' });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'internal error' });
  }
});

app.listen(port, () => console.log(`Server running on ${port}`));
