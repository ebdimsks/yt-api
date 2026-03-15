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
  if (!ytClient) {
    ytClient = await Innertube.create({
      client_type: 'ANDROID',
      generate_session_locally: true,
    });
  }
  return ytClient;
};

const badInstances = new Map();
let rrIndex = 0;

const markBad = (instance) => badInstances.set(instance, Date.now());

const rotateInstances = (list) => {
  if (!Array.isArray(list) || list.length === 0) return [];
  const start = rrIndex % list.length;
  rrIndex = (start + 1) % list.length;
  const rotated = [...list.slice(start), ...list.slice(0, start)];
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

const parseUrl = (format) => {
  if (!format) return null;
  if (typeof format === 'string') return format;
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
  ...(sd.adaptiveFormats || []),
  ...(sd.adaptive_formats || []),
].map((f) => ({
  ...f,
  mime: (f.mimeType || f.mime_type || f.type || '').toLowerCase(),
}));

const selectBestVideo = (formats) =>
  formats
    .filter((f) => f.mime && f.mime.includes('video'))
    .sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestAudio = (formats) =>
  formats
    .filter((f) => f.mime && f.mime.includes('audio'))
    .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestProgressive = (formats) =>
  formats
    .filter((f) => f.mime && f.mime.includes('video') && /mp4a|aac|opus/.test(f.mime))
    .sort((a, b) => (b.height || 0) - (a.height || 0))[0] || null;

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
  const result = await fastestFetch(
    instances,
    (base) => `${base}/api/v1/videos/${id}`,
    (data) => {
      const formats = [];
      if (Array.isArray(data.formatStreams)) {
        data.formatStreams.forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
      }
      if (Array.isArray(data.adaptiveFormats)) {
        data.adaptiveFormats.forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
      }
      if (Array.isArray(data.streamingData?.formats)) {
        data.streamingData.formats.forEach((f) => formats.push(f));
      }
      const sd = { formats };
      const is_live = Boolean(
        data.liveNow || data.isLive || data.is_live || data.live || data.streamingData?.isLive
      );
      return { streaming_data: sd, is_live, raw: data };
    }
  );
  return {
    provider: 'invidious',
    instance: result.instance,
    streaming_data: result.data.streaming_data,
    is_live: result.data.is_live,
    raw: result.data.raw,
  };
};

const fetchFromInnertube = async (id) => {
  const client = await getYtClient();
  const info = await client.getInfo(id);
  if (!info?.streaming_data && !info?.player_response) throw new Error('No streaming data');
  const sd = info.streaming_data || info.player_response?.streamingData || {};
  const is_live = Boolean(
    info?.video_details?.isLive ||
    info?.basic_info?.is_live ||
    info?.microformat?.isLive ||
    info?.player_response?.playabilityStatus?.liveStreamability ||
    info?.player_response?.videoDetails?.isLive ||
    info?.playability_status?.status === 'LIVE'
  );
  return {
    provider: 'innertube',
    streaming_data: sd,
    is_live,
    raw: info,
  };
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
    const A = Buffer.from(String(a), 'hex');
    const B = Buffer.from(String(b), 'hex');
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

function isValidVideoId(id) {
  return typeof id === 'string' && YT_ID_REGEX.test(id);
}

app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!isValidVideoId(String(id))) return res.status(400).json({ error: 'invalid video id' });
    const info = await fetchStreamingInfo(String(id));
    if (info.is_live) return res.status(403).json({ error: 'live streams are not supported' });
    const sd = info.streaming_data || {};
    const hlsCandidates = [
      sd.hlsManifestUrl,
      sd.hls_manifest_url,
      sd.hlsUrl,
      sd.hls,
      sd.streamingData?.hlsManifestUrl,
      sd.streamingData?.hls_manifest_url,
    ].filter(Boolean);
    if (hlsCandidates.length) return res.status(403).json({ error: 'live streams are not supported' });
    const dashCandidates = [
      sd.dashManifestUrl,
      sd.dash_manifest_url,
      sd.streamingData?.dashManifestUrl,
      sd.streamingData?.dash_manifest_url,
    ].filter(Boolean);
    if (dashCandidates.length) return res.status(403).json({ error: 'live streams are not supported' });
    const formats = normalizeFormats(sd);
    if (!formats.length) return res.status(404).json({ error: 'no stream' });
    const containsHlsFormat = formats.some((f) => {
      const url = parseUrl(f) || '';
      return (f.mime && f.mime.includes('mpegurl')) || url.includes('.m3u8') || /application\/vnd\.apple\.mpegurl/.test(f.mime || '');
    });
    if (containsHlsFormat) return res.status(403).json({ error: 'live streams are not supported' });
    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);
    if (video && audio) {
      return res.json({
        type: 'dash',
        video_url: parseUrl(video),
        audio_url: parseUrl(audio),
        provider: info.provider,
        instance: info.instance || null,
      });
    }
    const progressive = selectBestProgressive(formats);
    if (progressive) {
      return res.json({
        type: 'progressive',
        url: parseUrl(progressive),
        provider: info.provider,
        instance: info.instance || null,
      });
    }
    return res.status(404).json({ error: 'no stream' });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'internal error' });
  }
});

app.listen(port, () => console.log(`Server running on ${port}`));
