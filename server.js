import express from 'express';
import { Innertube, Platform } from 'youtubei.js';
import crypto from 'crypto';
import { ProxyAgent } from 'undici';

// Configuration
const { WORKER_SECRET, PORT = 3000, PROXY_URL } = process.env;
if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = Number(PORT) || 3000;

// Constants
const ALLOWED_WINDOW_SECONDS = 300; // 5 minutes
const INSTANCE_BAN_MS = 5 * 60 * 1000; // 5 minutes
const REQUEST_TIMEOUT_MS = 5_000; // per-instance request timeout
const YT_ID_REGEX = /^[a-zA-Z0-9_-]{11}$/;

// Invidious instances (rotated)
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

// Innertube client singletons (direct and proxied)
let ytClient = null;
let proxiedYtClient = null;
const getYtClient = async (useProxy = false) => {
  if (useProxy) {
    if (proxiedYtClient) return proxiedYtClient;
    if (!PROXY_URL) throw new Error('PROXY_URL not configured');
    const proxyAgent = new ProxyAgent(PROXY_URL);
    proxiedYtClient = await Innertube.create({
      client_type: 'ANDROID',
      generate_session_locally: true,
      // override fetch to use undici ProxyAgent via youtubei.js Platform shim
      fetch(input, init) {
        return Platform.shim.fetch(input, {
          ...init,
          dispatcher: proxyAgent,
        });
      },
    });
    return proxiedYtClient;
  } else {
    if (ytClient) return ytClient;
    ytClient = await Innertube.create({
      client_type: 'ANDROID',
      generate_session_locally: true,
    });
    return ytClient;
  }
};

// Instance health tracking (simple in-memory)
const badInstances = new Map();
let rrIndex = 0;

const markInstanceBad = (instance) => badInstances.set(instance, Date.now());

const rotateInstances = (list) => {
  if (!Array.isArray(list) || list.length === 0) return [];
  const start = rrIndex % list.length;
  rrIndex = (start + 1) % list.length;
  const rotated = [...list.slice(start), ...list.slice(0, start)];

  // Filter out recently-banned instances
  const now = Date.now();
  const available = rotated.filter((inst) => {
    const t = badInstances.get(inst);
    if (!t) return true;
    if (now - t > INSTANCE_BAN_MS) {
      badInstances.delete(inst);
      return true;
    }
    return false;
  });

  return available.length ? available : rotated;
};

// Helpers for parsing/normalizing streaming formats returned by different providers
const parseUrlFromFormat = (format) => {
  if (!format) return null;
  if (typeof format === 'string') return format;
  if (format.url) return format.url;

  const cipher = format.signatureCipher || format.signature_cipher || format.cipher;
  if (!cipher) return null;

  try {
    return new URLSearchParams(cipher).get('url');
  } catch (err) {
    return null;
  }
};

const normalizeFormats = (sd = {}) => {
  const list = [
    ...(sd.formats || []),
    ...(sd.adaptiveFormats || []),
    ...(sd.adaptive_formats || []),
    ...(sd.streamingData?.formats || []),
  ];
  return list.map((f) => ({
    ...f,
    mime: (f.mimeType || f.mime_type || f.type || '').toLowerCase(),
  }));
};

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

// Fetch fastest successful JSON from multiple instances: cancels the rest once one succeeds
const fastestFetch = async (instances, buildUrl, parser) => {
  if (!instances || !instances.length) throw new Error('no instances');

  const controllers = [];

  const wrappedFetch = async (base) => {
    const controller = new AbortController();
    controllers.push(controller);

    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
      const res = await fetch(buildUrl(base), { signal: controller.signal });
      clearTimeout(timeout);

      if (!res.ok) {
        markInstanceBad(base);
        throw new Error(`bad response ${res.status}`);
      }

      const json = await res.json();
      const parsed = parser(json);
      if (!parsed) {
        markInstanceBad(base);
        throw new Error('parse failed');
      }

      return { instance: base, data: parsed };
    } catch (err) {
      markInstanceBad(base);
      throw err;
    }
  };

  try {
    const tasks = instances.map((base) => wrappedFetch(base));
    const result = await Promise.any(tasks);
    // abort remaining requests
    controllers.forEach((c) => c.abort());
    return result;
  } catch (aggregateErr) {
    // If all failed, rethrow the first error for visibility
    throw new Error('All instances failed');
  }
};

// Provider-specific fetchers
const fetchFromInvidious = async (id) => {
  const instances = rotateInstances(INVIDIOUS_INSTANCES);
  const result = await fastestFetch(
    instances,
    (base) => `${base.replace(/\/$/, '')}/api/v1/videos/${id}`,
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

const fetchFromInnertube = async (id, { useProxy = false } = {}) => {
  const client = await getYtClient(useProxy);
  const info = await client.getInfo(id);
  if (!info) throw new Error('No info from innertube');

  // streaming data might be at different paths
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
    instance: useProxy ? PROXY_URL : null,
    streaming_data: sd,
    is_live,
    raw: info,
  };
};

// Top-level fetcher: order = proxied Innertube (if configured) -> Invidious -> direct Innertube
const fetchStreamingInfo = async (id) => {
  // 1) proxied innertube (if PROXY_URL configured)
  if (PROXY_URL) {
    try {
      return await fetchFromInnertube(id, { useProxy: true });
    } catch (e) {
      // proceed to invidious fallback
      console.warn('proxied innertube failed, falling back:', e.message || e);
    }
  }

  // 2) invidious
  try {
    return await fetchFromInvidious(id);
  } catch (e) {
    console.warn('invidious failed, falling back to direct innertube:', e.message || e);
  }

  // 3) direct innertube (final fallback)
  return fetchFromInnertube(id, { useProxy: false });
};

// Constant-time compare for signatures (accepts raw hex strings)
const safeEqualHex = (a, b) => {
  try {
    const A = Buffer.from(String(a), 'hex');
    const B = Buffer.from(String(b), 'hex');
    if (A.length !== B.length) return false;
    return crypto.timingSafeEqual(A, B);
  } catch {
    // If values are not hex or another error occurs, fall back to utf8 compare
    try {
      const A = Buffer.from(String(a), 'utf8');
      const B = Buffer.from(String(b), 'utf8');
      if (A.length !== B.length) return false;
      return crypto.timingSafeEqual(A, B);
    } catch {
      return false;
    }
  }
};

// Middleware: verify worker authorization headers
const verifyWorkerAuth = (req, res, next) => {
  const ts = req.header('x-proxy-timestamp');
  const sig = req.header('x-proxy-signature');
  if (!ts || !sig) return res.status(401).json({ error: 'unauthorized' });

  const now = Math.floor(Date.now() / 1000);
  const t = Number(ts);
  if (!Number.isFinite(t) || Math.abs(now - t) > ALLOWED_WINDOW_SECONDS)
    return res.status(401).json({ error: 'unauthorized' });

  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');
  if (!safeEqualHex(expected, sig)) return res.status(401).json({ error: 'unauthorized' });

  next();
};

// Utility: validate YouTube id
const isValidVideoId = (id) => typeof id === 'string' && YT_ID_REGEX.test(id);

// Utility: extract title from various provider responses
const extractTitle = (info) => {
  if (!info || !info.raw) return null;
  const r = info.raw;
  return (
    r.title ||
    r.videoDetails?.title ||
    r.video_details?.title ||
    r.player_response?.videoDetails?.title ||
    r.basic_info?.title ||
    r.microformat?.title?.simpleText ||
    r.titleText?.runs?.map?.((x) => x.text).join('') ||
    r.video?.title ||
    null
  );
};

// API endpoint
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!isValidVideoId(id)) return res.status(400).json({ error: 'invalid video id' });

    const info = await fetchStreamingInfo(id);

    const sd = info.streaming_data || {};

    // If provider is not innertube, keep rejecting live streams as before.
    if (info.is_live && info.provider !== 'innertube') {
      return res.status(403).json({ error: 'live streams are not supported' });
    }

    // quick check for HLS/DASH manifests (used for live variants)
    const hasManifest = [
      sd.hlsManifestUrl,
      sd.hls_manifest_url,
      sd.hlsUrl,
      sd.hls,
      sd.streamingData?.hlsManifestUrl,
      sd.streamingData?.hls_manifest_url,
      sd.dashManifestUrl,
      sd.dash_manifest_url,
      sd.streamingData?.dashManifestUrl,
      sd.streamingData?.dash_manifest_url,
    ].some(Boolean);

    // If manifest exists and provider is not innertube, reject (keep previous behavior).
    if (hasManifest && info.provider !== 'innertube') {
      return res.status(403).json({ error: 'live streams are not supported' });
    }

    const formats = normalizeFormats(sd);
    if (!formats.length && !hasManifest) return res.status(404).json({ error: 'no stream' });

    // HLS-format in formats detection (e.g. m3u8). If found and not innertube, block.
    const containsHlsFormat = formats.some((f) => {
      const url = parseUrlFromFormat(f) || '';
      return (f.mime && f.mime.includes('mpegurl')) || url.includes('.m3u8') || /application\/vnd\.apple\.mpegurl/.test(f.mime || '');
    });
    if (containsHlsFormat && info.provider !== 'innertube') {
      return res.status(403).json({ error: 'live streams are not supported' });
    }

    const title = extractTitle(info) || '';

    const providerObj = {
      name: info.provider || null,
      url: info.instance || null,
    };

    // If provider is innertube and it's live or has manifest, prefer returning a manifest (HLS) when possible
    if (info.provider === 'innertube' && (info.is_live || hasManifest || containsHlsFormat)) {
      // prefer explicit manifest urls from streaming data
      const manifestUrl =
        sd.hlsManifestUrl ||
        sd.hls_manifest_url ||
        sd.hlsUrl ||
        sd.hls ||
        sd.streamingData?.hlsManifestUrl ||
        sd.streamingData?.hls_manifest_url ||
        sd.streamingData?.hlsUrl ||
        sd.streamingData?.hls;

      if (manifestUrl) {
        return res.json({
          resourcetype: 'hls',
          title,
          url: manifestUrl,
          provider: providerObj,
        });
      }

      // fallback: if formats contain direct m3u8 urls, return one
      const hlsFormat = formats.find((f) => {
        const url = parseUrlFromFormat(f) || '';
        return url.includes('.m3u8') || (f.mime && f.mime.includes('mpegurl')) || /application\/vnd\.apple\.mpegurl/.test(f.mime || '');
      });
      if (hlsFormat) {
        return res.json({
          resourcetype: 'hls',
          title,
          url: parseUrlFromFormat(hlsFormat),
          provider: providerObj,
        });
      }

      // as a last resort for innertube live, attempt to return the best available dash/audio/video info
      const videoLive = selectBestVideo(formats);
      const audioLive = selectBestAudio(formats);
      if (videoLive && audioLive) {
        return res.json({
          resourcetype: 'dash',
          title,
          videourl: parseUrlFromFormat(videoLive),
          audiourl: parseUrlFromFormat(audioLive),
          provider: providerObj,
        });
      }
      const progressiveLive = selectBestProgressive(formats);
      if (progressiveLive) {
        return res.json({
          resourcetype: 'progressive',
          title,
          url: parseUrlFromFormat(progressiveLive),
          provider: providerObj,
        });
      }

      // nothing usable found for innertube live
      return res.status(404).json({ error: 'no stream' });
    }

    // Non-live / normal handling (same as before)
    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);

    if (video && audio) {
      return res.json({
        resourcetype: 'dash',
        title,
        videourl: parseUrlFromFormat(video),
        audiourl: parseUrlFromFormat(audio),
        provider: providerObj,
      });
    }

    const progressive = selectBestProgressive(formats);
    if (progressive) {
      return res.json({
        resourcetype: 'progressive',
        title,
        url: parseUrlFromFormat(progressive),
        provider: providerObj,
      });
    }

    return res.status(404).json({ error: 'no stream' });
  } catch (err) {
    console.error('Unexpected error in /api/stream', err);
    return res.status(500).json({ error: err?.message || 'internal error' });
  }
});

app.listen(port, () => console.log(`Server running on ${port}`));
