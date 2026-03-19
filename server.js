import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';
import { spawn } from 'node:child_process';

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

const ALLOWED_WINDOW_SECONDS = 300;
const REQUEST_TIMEOUT_MS = 5_000;
const INSTANCE_BAN_MS = 5 * 60 * 1000;
const YT_DLP_TIMEOUT_MS = 10_000;
const YT_DLP_BIN = 'yt-dlp';
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

const MANIFEST_KEYS = [
  'hlsManifestUrl',
  'hls_manifest_url',
  'hlsUrl',
  'hls',
  'dashManifestUrl',
  'dash_manifest_url',
  'manifest_url',
  'manifestUrl',
];

// ------------------------
// Auth
// ------------------------
const safeEqualHex = (a, b) => {
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

  if (!Number.isFinite(t) || Math.abs(now - t) > ALLOWED_WINDOW_SECONDS) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');

  if (!safeEqualHex(expected, sig)) return res.status(401).json({ error: 'unauthorized' });

  next();
};

// ------------------------
// yt-dlp
// ------------------------
const runYtDlp = async (videoId, { useProxy = false } = {}) => {
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  const args = [
    '--dump-single-json',
    '--skip-download',
    '--no-playlist',
    '--no-warnings',
    '--no-progress',
  ];

  if (useProxy && PROXY_URL) {
    args.push('--proxy', PROXY_URL);
  }

  args.push(url);

  return new Promise((resolve, reject) => {
    const child = spawn(YT_DLP_BIN, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    const timer = setTimeout(() => {
      child.kill('SIGKILL');
    }, YT_DLP_TIMEOUT_MS);

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });

    child.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    child.on('close', (code, signal) => {
      clearTimeout(timer);

      if (code !== 0) {
        return reject(
          new Error(
            `yt-dlp failed (${code ?? signal ?? 'unknown'}): ${stderr.trim() || 'no stderr output'}`
          )
        );
      }

      try {
        resolve(JSON.parse(stdout));
      } catch {
        reject(new Error('yt-dlp returned invalid JSON'));
      }
    });
  });
};

// ------------------------
// Invidious
// ------------------------
const badInstances = new Map();
let rrIndex = 0;

const markInstanceBad = (instance) => {
  badInstances.set(instance, Date.now());
};

const rotateInstances = (list = []) => {
  if (!Array.isArray(list) || list.length === 0) return [];

  const start = rrIndex % list.length;
  rrIndex = (rrIndex + 1) % list.length;

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
    controllers.forEach((c) => c.abort());
    return result;
  } catch {
    controllers.forEach((c) => c.abort());
    throw new Error('All instances failed');
  }
};

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

// ------------------------
// Normalization / selection
// ------------------------
const isValidVideoId = (id) => typeof id === 'string' && YT_ID_REGEX.test(id);

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
    mime: String(f.mimeType || f.mime_type || f.type || '').toLowerCase(),
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
    return (
      url.includes('.m3u8') ||
      (f.mime && f.mime.includes('mpegurl')) ||
      /application\/vnd\.apple\.mpegurl/.test(f.mime || '')
    );
  });

const hasManifestInSd = (sd = {}) =>
  MANIFEST_KEYS.some((k) => Boolean(sd[k])) ||
  MANIFEST_KEYS.some((k) => Boolean(sd.streamingData?.[k]));

const extractTitle = (raw = {}) => {
  return (
    raw.title ||
    raw.videoDetails?.title ||
    raw.video_details?.title ||
    raw.player_response?.videoDetails?.title ||
    raw.basic_info?.title ||
    raw.microformat?.title?.simpleText ||
    raw.titleText?.runs?.map?.((x) => x.text).join('') ||
    raw.video?.title ||
    null
  );
};

const isLiveLike = (raw = {}, sd = {}, formats = []) =>
  Boolean(
    raw.is_live ||
    raw.live_status === 'is_live' ||
    raw.liveNow ||
    raw.isLive ||
    raw.live ||
    sd.isLive ||
    hasManifestInSd(sd) ||
    containsHlsFormat(formats)
  );

const buildSdFromRaw = (raw = {}) => {
  const sd = {
    formats: Array.isArray(raw.formats) ? raw.formats : [],
  };

  for (const key of MANIFEST_KEYS) {
    if (raw[key]) sd[key] = raw[key];
  }

  if (raw.streamingData && typeof raw.streamingData === 'object') {
    sd.streamingData = raw.streamingData;
  }

  return sd;
};

// ------------------------
// Fetch order
// yt-dlp(proxy) -> Invidious(non-live only) -> yt-dlp(direct)
// ------------------------
const fetchFromYtDlp = async (id, { useProxy = false } = {}) => {
  const raw = await runYtDlp(id, { useProxy });
  const sd = buildSdFromRaw(raw);
  const is_live = isLiveLike(raw, sd, normalizeFormats(sd));

  return {
    provider: 'yt-dlp',
    instance: useProxy ? (PROXY_URL || null) : null,
    streaming_data: sd,
    is_live,
    raw,
  };
};

const fetchStreamingInfo = async (id) => {
  if (PROXY_URL) {
    try {
      return await fetchFromYtDlp(id, { useProxy: true });
    } catch (e) {
      console.warn('proxied yt-dlp failed, falling back to invidious:', e.message || e);
    }
  }

  try {
    const invInfo = await fetchFromInvidious(id);

    // Invidious は非ライブのみ採用
    if (!invInfo.is_live && !isLiveLike(invInfo.raw, invInfo.streaming_data, normalizeFormats(invInfo.streaming_data))) {
      return invInfo;
    }

    console.warn('Skipping Invidious result because it looks like live/manifest');
  } catch (e) {
    console.warn('invidious failed, falling back to direct yt-dlp:', e.message || e);
  }

  return fetchFromYtDlp(id, { useProxy: false });
};

// ------------------------
// API
// ------------------------
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!isValidVideoId(id)) return res.status(400).json({ error: 'invalid video id' });

    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data || {};
    const formats = normalizeFormats(sd);

    if (!formats.length && !hasManifestInSd(sd)) {
      return res.status(404).json({ error: 'no stream' });
    }

    const title = extractTitle(info.raw) || '';
    const provider = {
      name: info.provider || null,
      url: info.instance || null,
    };

    const manifestUrl = MANIFEST_KEYS.reduce((acc, k) => acc || sd[k] || sd.streamingData?.[k], null);
    if (manifestUrl) {
      return res.json({
        resourcetype: 'hls',
        title,
        url: manifestUrl,
        provider,
      });
    }

    const hlsFormat = formats.find((f) => {
      const url = parseUrlFromFormat(f) || '';
      return (
        url.includes('.m3u8') ||
        (f.mime && f.mime.includes('mpegurl')) ||
        /application\/vnd\.apple\.mpegurl/.test(f.mime || '')
      );
    });

    if (hlsFormat) {
      return res.json({
        resourcetype: 'hls',
        title,
        url: parseUrlFromFormat(hlsFormat),
        provider,
      });
    }

    const progressive = selectBestProgressive(formats);
    if (progressive) {
      return res.json({
        resourcetype: 'progressive',
        title,
        url: parseUrlFromFormat(progressive),
        provider,
      });
    }

    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);

    if (video && audio) {
      return res.json({
        resourcetype: 'dash',
        title,
        videourl: parseUrlFromFormat(video),
        audiourl: parseUrlFromFormat(audio),
        provider,
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
