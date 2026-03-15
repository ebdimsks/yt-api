import express from 'express';
import { Innertube } from 'youtubei.js';
import crypto from 'crypto';

const { WORKER_SECRET, PORT } = process.env;
if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = PORT || 3000;
const ALLOWED_WINDOW = 300;
const INSTANCE_BAN_MS = 5 * 60 * 1000;
const YT_ID_REGEX = /^[a-zA-Z0-9_-]{11}$/;

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

let ytClient = null;
const getYtClient = async () => {
  if (ytClient) return ytClient;
  ytClient = await Innertube.create({ client_type: 'ANDROID', generate_session_locally: true });
  return ytClient;
};

const badInstances = new Map();
let rrIndex = 0;
const rotateInstances = (list) => {
  if (!Array.isArray(list) || list.length === 0) return [];
  const start = rrIndex % list.length;
  rrIndex = (start + 1) % list.length;
  const rotated = [...list.slice(start), ...list.slice(0, start)];
  const healthy = rotated.filter((i) => {
    const t = badInstances.get(i);
    if (!t) return true;
    if (Date.now() - t > INSTANCE_BAN_MS) {
      badInstances.delete(i);
      return true;
    }
    return false;
  });
  return healthy.length ? healthy : rotated;
};
const markBad = (instance) => badInstances.set(instance, Date.now());

const parseUrl = (fmt) => {
  if (!fmt) return null;
  if (typeof fmt === 'string') return fmt;
  if (fmt.url) return fmt.url;
  const cipher = fmt.signatureCipher || fmt.signature_cipher || fmt.cipher;
  if (!cipher) return null;
  try {
    return new URLSearchParams(cipher).get('url');
  } catch {
    return null;
  }
};

const normalizeFormats = (sd = {}) => {
  const raw = [
    ...(sd.formats || []),
    ...(sd.adaptive_formats || []),
    ...(sd.adaptiveFormats || []),
    ...(sd.formatStreams || []),
  ];
  return raw.map((f) => ({ ...f, mime: (f.mimeType || f.mime_type || f.type || '').toLowerCase() }));
};

const pick = (formats, predicate, compare) => {
  return formats.filter(predicate).sort(compare)[0] || null;
};

const selectBestVideo = (fmts) =>
  pick(fmts, (f) => (f.mime || '').includes('video'), (a, b) => (b.height || 0) - (a.height || 0) || (b.bitrate || 0) - (a.bitrate || 0));

const selectBestAudio = (fmts) =>
  pick(fmts, (f) => (f.mime || '').includes('audio'), (a, b) => (b.bitrate || 0) - (a.bitrate || 0));

const selectBestProgressive = (fmts) =>
  pick(
    fmts,
    (f) => (f.mime || '').includes('video') && /mp4a|aac|opus/.test(f.mime),
    (a, b) => (b.height || 0) - (a.height || 0)
  );

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
      return { instance: base, data: parsed };
    } catch (err) {
      markBad(base);
      throw err;
    }
  });
  const result = await Promise.any(tasks);
  controllers.forEach((c) => c.abort());
  return result;
};

const fetchFromInvidious = async (id) => {
  const instances = rotateInstances(INVIDIOUS_INSTANCES);
  const { instance, data } = await fastestFetch(
    instances,
    (base) => `${base}/api/v1/videos/${id}`,
    (d) => {
      const fmts = [];
      (d.formatStreams || []).forEach((f) => fmts.push({ ...f, mimeType: f.type }));
      (d.adaptiveFormats || []).forEach((f) => fmts.push({ ...f, mimeType: f.type }));
      (d.formatStreams_v2 || []).forEach((f) => fmts.push({ ...f, mimeType: f.type }));
      const streaming = {};
      if (d.hls || d.hlsUrl || d.hls_manifest_url || d.hlsManifestUrl) streaming.hls = d.hls || d.hlsUrl || d.hls_manifest_url || d.hlsManifestUrl;
      if (!fmts.length && !streaming.hls) return null;
      return { streaming_data: { formats: fmts, ...streaming } };
    }
  );
  return { provider: 'invidious', instance, streaming_data: data.streaming_data };
};

const fetchFromInnertube = async (id) => {
  const client = await getYtClient();
  const info = await client.getInfo(id);
  if (!info) throw new Error('no info');
  return { provider: 'innertube', streaming_data: info.streaming_data || info.streamingData || {} };
};

const fetchStreamingInfo = async (id) => {
  try {
    return await fetchFromInvidious(id);
  } catch (e) {
    return fetchFromInnertube(id);
  }
};

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
  if (!Number.isFinite(t) || Math.abs(now - t) > ALLOWED_WINDOW) return res.status(401).json({ error: 'unauthorized' });
  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');
  if (!safeEqual(expected, sig)) return res.status(401).json({ error: 'unauthorized' });
  next();
};

const isValidVideoId = (id) => typeof id === 'string' && YT_ID_REGEX.test(id);

app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!isValidVideoId(id)) return res.status(400).json({ error: 'invalid video id' });
    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data || {};
    const hls = sd.hlsManifestUrl || sd.hls_manifest_url || sd.hlsUrl || sd.hls || sd.hls_url || sd.hls_manifest;
    if (hls) return res.json({ type: 'hls', url: hls, provider: info.provider, instance: info.instance || null });
    const fmts = normalizeFormats(sd);
    const video = selectBestVideo(fmts);
    const audio = selectBestAudio(fmts);
    if (video && audio) return res.json({ type: 'dash', video_url: parseUrl(video), audio_url: parseUrl(audio), provider: info.provider, instance: info.instance || null });
    const progressive = selectBestProgressive(fmts);
    if (progressive) return res.json({ type: 'progressive', url: parseUrl(progressive), provider: info.provider, instance: info.instance || null });
    return res.status(404).json({ error: 'no stream' });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'internal error' });
  }
});

app.listen(port, () => console.log(`Server running on ${port}`));
