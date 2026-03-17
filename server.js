import express from 'express';
import { Innertube, Platform } from 'youtubei.js';
import crypto from 'crypto';
import { ProxyAgent } from 'undici';

// ------------------------
// Configuration / Constants
// ------------------------
const { WORKER_SECRET, PORT = 3000, PROXY_URL } = process.env;
if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = Number(PORT) || 3000;

const ALLOWED_WINDOW_SECONDS = 300; // 5 minutes
const INSTANCE_BAN_MS = 5 * 60 * 1000; // 5 minutes
const REQUEST_TIMEOUT_MS = 5_000; // per-instance request timeout
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

// keys that may contain HLS/DASH manifest urls in provider responses
const MANIFEST_KEYS = [
  'hlsManifestUrl', 'hls_manifest_url', 'hlsUrl', 'hls',
  'dashManifestUrl', 'dash_manifest_url',
];

// ------------------------
// Innertube client singletons (with race protection)
// ------------------------
let ytClient = null;
let ytClientPromise = null;
let proxiedYtClient = null;
let proxiedYtClientPromise = null;

const createInnertubeClient = async ({ useProxy = false } = {}) => {
  if (useProxy) {
    if (proxiedYtClient) return proxiedYtClient;
    if (proxiedYtClientPromise) return proxiedYtClientPromise;
    if (!PROXY_URL) throw new Error('PROXY_URL not configured');

    proxiedYtClientPromise = (async () => {
      const proxyAgent = new ProxyAgent(PROXY_URL);
      proxiedYtClient = await Innertube.create({
        client_type: 'ANDROID',
        generate_session_locally: true,
        fetch(input, init) {
          return Platform.shim.fetch(input, {
            ...init,
            dispatcher: proxyAgent,
          });
        },
      });
      proxiedYtClientPromise = null;
      return proxiedYtClient;
    })();

    return proxiedYtClientPromise;
  } else {
    if (ytClient) return ytClient;
    if (ytClientPromise) return ytClientPromise;

    ytClientPromise = (async () => {
      ytClient = await Innertube.create({
        client_type: 'ANDROID',
        generate_session_locally: true,
      });
      ytClientPromise = null;
      return ytClient;
    })();

    return ytClientPromise;
  }
};

const getYtClient = (useProxy = false) => createInnertubeClient({ useProxy });

// ------------------------
// Instance health & rotation
// ------------------------
const badInstances = new Map();
let rrIndex = 0;

const markInstanceBad = (instance) => {
  badInstances.set(instance, Date.now());
};

const rotateInstances = (list = []) => {
  if (!Array.isArray(list) || list.length === 0) return [];
  const start = rrIndex % list.length;
  rrIndex = (rrIndex + 1) % list.length; // advance for next call
  const rotated = [...list.slice(start), ...list.slice(0, start)];

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

// ------------------------
// Utilities: parsing & normalization
// ------------------------
const parseUrlFromFormat = (format) => {
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

const selectBestVideo = (formats = []) =>
  formats
    .filter((f) => f.mime && f.mime.includes('video'))
    .sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestAudio = (formats = []) =>
  formats
    .filter((f) => f.mime && f.mime.includes('audio'))
    .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestProgressive = (formats = []) =>
  formats
    .filter((f) => f.mime && f.mime.includes('video') && /mp4a|aac|opus/.test(f.mime))
    .sort((a, b) => (b.height || 0) - (a.height || 0))[0] || null;

const containsHlsFormat = (formats = []) =>
  formats.some((f) => {
    const url = parseUrlFromFormat(f) || '';
    return url.includes('.m3u8') || (f.mime && f.mime.includes('mpegurl')) || /application\/vnd\.apple\.mpegurl/.test(f.mime || '');
  });

const hasManifestInSd = (sd = {}) =>
  MANIFEST_KEYS.some((k) => Boolean(sd[k])) ||
  MANIFEST_KEYS.some((k) => Boolean(sd.streamingData?.[k]));

// ------------------------
// Fastest fetch across instances (cancellable)
// ------------------------
const fastestFetch = async (instances, buildUrl, parser) => {
  if (!instances || !instances.length) throw new Error('no instances');

  const controllers = [];
  const tasks = instances.map((base) => {
    return (async () => {
      const controller = new AbortController();
      controllers.push(controller);

      const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
      try {
        const res = await fetch(buildUrl(base), { signal: controller.signal });
        clearTimeout(timeout);

        if (!res.ok) {
          markInstanceBad(base);
          throw new Error(`bad response ${res.status} from ${base}`);
        }

        const json = await res.json();
        const parsed = parser(json);
        if (!parsed) {
          markInstanceBad(base);
          throw new Error(`parse failed from ${base}`);
        }

        return { instance: base, data: parsed };
      } catch (err) {
        markInstanceBad(base);
        throw err;
      }
    })();
  });

  try {
    const result = await Promise.any(tasks);
    // cancel any remaining controllers
    controllers.forEach((c) => c.abort());
    return result;
  } catch (aggregateErr) {
    // ensure all requests aborted
    controllers.forEach((c) => c.abort());
    throw new Error('All instances failed');
  }
};

// ------------------------
// Provider-specific fetchers
// ------------------------
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

// ------------------------
// Helpers: detect live/manifest and misc
// ------------------------
const responseLooksLikeLiveOrManifest = (info) => {
  if (!info || !info.streaming_data) return false;
  const sd = info.streaming_data || {};

  if (hasManifestInSd(sd)) return true;

  const formats = normalizeFormats(sd);
  if (containsHlsFormat(formats)) return true;

  if (info.is_live) return true;

  return false;
};

const safeEqualHex = (a, b) => {
  try {
    const A = Buffer.from(String(a), 'hex');
    const B = Buffer.from(String(b), 'hex');
    if (A.length !== B.length) return false;
    return crypto.timingSafeEqual(A, B);
  } catch {
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

const isValidVideoId = (id) => typeof id === 'string' && YT_ID_REGEX.test(id);

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

// ------------------------
// Top-level fetcher
// order: proxied Innertube (if configured) -> Invidious -> direct Innertube
// ------------------------
const fetchStreamingInfo = async (id) => {
  if (PROXY_URL) {
    try {
      return await fetchFromInnertube(id, { useProxy: true });
    } catch (e) {
      console.warn('proxied innertube failed, falling back:', e.message || e);
    }
  }

  try {
    const invInfo = await fetchFromInvidious(id);
    // If invidious looks like live/manifest, skip (no ban)
    if (!responseLooksLikeLiveOrManifest(invInfo)) {
      return invInfo;
    }
    console.warn('Skipping Invidious result because it looks like live/manifest');
  } catch (e) {
    console.warn('invidious failed, falling back to direct innertube:', e.message || e);
  }

  // final fallback
  return fetchFromInnertube(id, { useProxy: false });
};

// ------------------------
// Middleware: verify worker authorization headers
// ------------------------
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

// ------------------------
// API endpoint
// ------------------------
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
    const hasManifest = hasManifestInSd(sd);

    if (hasManifest && info.provider !== 'innertube') {
      return res.status(403).json({ error: 'live streams are not supported' });
    }

    const formats = normalizeFormats(sd);

    if (!formats.length && !hasManifest) return res.status(404).json({ error: 'no stream' });

    const hasHls = containsHlsFormat(formats);
    if (hasHls && info.provider !== 'innertube') {
      return res.status(403).json({ error: 'live streams are not supported' });
    }

    const title = extractTitle(info) || '';

    const providerObj = {
      name: info.provider || null,
      url: info.instance || null,
    };

    // Innertube live / manifest handling
    if (info.provider === 'innertube' && (info.is_live || hasManifest || hasHls)) {
      // prefer explicit manifest urls from streaming data
      const manifestUrl = MANIFEST_KEYS.reduce((acc, k) => acc || sd[k] || sd.streamingData?.[k], null);

      if (manifestUrl) {
        return res.json({
          resourcetype: 'hls',
          title,
          url: manifestUrl,
          provider: providerObj,
        });
      }

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

      return res.status(404).json({ error: 'no stream' });
    }

    // Non-live handling (normal videos)
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

// ------------------------
// Server start
// ------------------------
app.listen(port, () => console.log(`Server running on ${port}`));
