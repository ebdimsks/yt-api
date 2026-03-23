import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';
import { spawn } from 'node:child_process';

// ------------------------
// Environment / Constants
// ------------------------
const PROXY_URL = process.env.PROXY_URL;
const YT_DLP_BIN = '/opt/venv/bin/yt-dlp';
const PORT = Number(process.env.PORT || 3000);

const app = express();
app.disable('x-powered-by');

const REQUEST_TIMEOUT_MS = 5_000;
const INSTANCE_BAN_MS = 5 * 60 * 1000;
const YT_DLP_TIMEOUT_MS = 10_000;

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
  'dashUrl',
  'manifest_url',
  'manifestUrl',
];

// ------------------------
// Small utils
// ------------------------
const toNumber = (value, fallback = 0) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
};

const isNonEmptyString = (v) => typeof v === 'string' && v.trim().length > 0;

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

const isPlainObject = (value) =>
  Boolean(value) &&
  typeof value === 'object' &&
  !Array.isArray(value) &&
  Object.prototype.toString.call(value) === '[object Object]';

const snippet = (text, len = 220) => {
  const s = String(text ?? '').replace(/\s+/g, ' ').trim();
  return s.length > len ? `${s.slice(0, len)}…` : s;
};

const parseStrictJsonObject = (text, context = 'json') => {
  const trimmed = String(text ?? '').trim();

  if (!trimmed) {
    throw new Error(`${context}: empty output`);
  }

  let parsed;
  try {
    parsed = JSON.parse(trimmed);
  } catch (err) {
    throw new Error(`${context}: invalid JSON (${snippet(trimmed)})`);
  }

  if (!isPlainObject(parsed)) {
    throw new Error(`${context}: JSON must be an object`);
  }

  return parsed;
};

const isHlsUrl = (url) =>
  /(^|[/?#&])[^?#]*\.m3u8(\?|#|$)/i.test(url) || /mpegurl/i.test(url);

const isDashUrl = (url) =>
  /(^|[/?#&])[^?#]*\.mpd(\?|#|$)/i.test(url) || /dash/i.test(url);

const parseHeightFromLabel = (label) => {
  const m = String(label || '').match(/(\d{3,4})p/i);
  return m ? Number(m[1]) : 0;
};

const parseUrlFromFormat = (format) => {
  if (!format) return null;
  if (typeof format === 'string') return format;
  if (isNonEmptyString(format.url)) return format.url;

  const cipher = format.signatureCipher || format.signature_cipher || format.cipher;
  if (!cipher) return null;

  try {
    const params = new URLSearchParams(cipher);
    return params.get('url');
  } catch {
    return null;
  }
};

const uniqByKey = (items, keyFn) => {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = keyFn(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
};

const flattenArrays = (...values) =>
  values.flatMap((v) => (Array.isArray(v) ? v : [])).filter(Boolean);

const pickRequestedFormats = (raw = {}, sd = {}) =>
  flattenArrays(
    raw.requested_formats,
    sd.requested_formats,
    sd.streamingData?.requested_formats
  ).filter((v) => v && typeof v === 'object');

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
    '--format',
    'bestvideo*+bestaudio/best',
    url,
  ];

  if (useProxy && PROXY_URL) {
    args.splice(args.length - 1, 0, '--proxy', PROXY_URL);
  }

  return new Promise((resolve, reject) => {
    const child = spawn(YT_DLP_BIN, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';
    let stdoutBytes = 0;
    let settled = false;

    const done = (err, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (err) reject(err);
      else resolve(value);
    };

    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      done(new Error('yt-dlp timed out'));
    }, YT_DLP_TIMEOUT_MS);

    child.stdout.on('data', (chunk) => {
      stdoutBytes += chunk.length;
      stdout += chunk.toString('utf8');
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });

    child.on('error', (err) => {
      done(err);
    });

    child.on('close', (code, signal) => {
      if (settled) return;

      if (code !== 0) {
        done(
          new Error(
            `yt-dlp failed (${code ?? signal ?? 'unknown'}): ${snippet(stderr) || 'no stderr output'}`
          )
        );
        return;
      }

      try {
        const parsed = parseStrictJsonObject(stdout, 'yt-dlp stdout');
        done(null, parsed);
      } catch (err) {
        done(err);
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

  const tasks = instances.map((base) => (async () => {
    const controller = new AbortController();
    controllers.push(controller);

    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, REQUEST_TIMEOUT_MS);

    try {
      const res = await fetch(buildUrl(base), {
        signal: controller.signal,
        headers: { accept: 'application/json' },
      });

      if (!res.ok) {
        markInstanceBad(base);
        throw new Error(`bad response ${res.status} from ${base}`);
      }

      const json = await res.json();
      if (!isPlainObject(json)) {
        markInstanceBad(base);
        throw new Error(`non-object JSON from ${base}`);
      }

      const parsed = parser(json);
      if (!parsed) {
        markInstanceBad(base);
        throw new Error(`parse failed from ${base}`);
      }

      return { instance: base, data: parsed };
    } catch (err) {
      const aborted = err?.name === 'AbortError';
      if (timedOut || !aborted) markInstanceBad(base);
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  })());

  try {
    const result = await Promise.any(tasks);
    controllers.forEach((c) => c.abort());
    return result;
  } catch {
    controllers.forEach((c) => c.abort());
    throw new Error('All instances failed');
  }
};

// ------------------------
// Format normalization
// ------------------------
const collectFormats = (raw = {}, sd = {}) => {
  const sources = flattenArrays(
    raw.formats,
    raw.requested_formats,
    raw.adaptiveFormats,
    raw.adaptive_formats,
    raw.formatStreams,
    sd.formats,
    sd.requested_formats,
    sd.adaptiveFormats,
    sd.adaptive_formats,
    sd.formatStreams,
    sd.streamingData?.formats,
    sd.streamingData?.requested_formats,
    sd.streamingData?.adaptiveFormats,
    sd.streamingData?.adaptive_formats,
    sd.streamingData?.formatStreams
  );

  const normalized = sources
    .map((f) => {
      if (!isPlainObject(f)) return null;

      const url = parseUrlFromFormat(f);
      const mime = String(f.mimeType || f.mime_type || f.type || '').toLowerCase();
      const vcodec = String(f.vcodec || '').toLowerCase();
      const acodec = String(f.acodec || '').toLowerCase();

      return {
        ...f,
        url,
        mime,
        vcodec,
        acodec,
        width: toNumber(f.width, 0),
        height: toNumber(f.height || parseHeightFromLabel(f.qualityLabel || f.resolution), 0),
        fps: toNumber(f.fps, 0),
        tbr: toNumber(f.tbr || f.bitrate || f.total_bitrate, 0),
        abr: toNumber(f.abr || f.audioBitrate, 0),
        vbr: toNumber(f.vbr, 0),
        filesize: toNumber(f.filesize, 0),
        filesize_approx: toNumber(f.filesize_approx, 0),
        qualityLabel: String(f.qualityLabel || f.quality_label || f.resolution || ''),
      };
    })
    .filter(Boolean);

  return uniqByKey(normalized, (f) => {
    return (
      f.url ||
      `${String(f.itag || '')}|${String(f.format_id || '')}|${String(f.height || '')}|${String(f.width || '')}|${String(f.mime || '')}`
    );
  });
};

const extractTitle = (raw = {}) =>
  raw.title ||
  raw.videoDetails?.title ||
  raw.video_details?.title ||
  raw.player_response?.videoDetails?.title ||
  raw.basic_info?.title ||
  raw.microformat?.title?.simpleText ||
  (raw.titleText?.runs?.map?.((x) => x.text) || []).join('') ||
  raw.video?.title ||
  null;

const isLiveLike = (raw = {}, sd = {}) =>
  Boolean(
    raw.is_live ||
    raw.live_status === 'is_live' ||
    raw.liveNow ||
    raw.isLive ||
    raw.live ||
    sd.isLive
  );

const extractManifest = (raw = {}, sd = {}, isLive = false) => {
  const candidates = [
    { kind: 'dash', url: sd.dashManifestUrl },
    { kind: 'dash', url: sd.dash_manifest_url },
    { kind: 'dash', url: sd.dashUrl },
    { kind: 'hls', url: sd.hlsManifestUrl },
    { kind: 'hls', url: sd.hls_manifest_url },
    { kind: 'hls', url: sd.hlsUrl },

    { kind: 'dash', url: raw.dashManifestUrl },
    { kind: 'dash', url: raw.dash_manifest_url },
    { kind: 'dash', url: raw.dashUrl },
    { kind: 'hls', url: raw.hlsManifestUrl },
    { kind: 'hls', url: raw.hls_manifest_url },
    { kind: 'hls', url: raw.hlsUrl },

    { kind: null, url: sd.manifestUrl },
    { kind: null, url: sd.manifest_url },
    { kind: null, url: raw.manifestUrl },
    { kind: null, url: raw.manifest_url },
  ].filter((x) => isNonEmptyString(x.url));

  if (!candidates.length) return null;

  const inferKind = (item) => {
    if (item.kind) return item.kind;
    if (isHlsUrl(item.url)) return 'hls';
    if (isDashUrl(item.url)) return 'dash';
    return 'hls';
  };

  const ordered = candidates.map((item) => ({
    url: item.url,
    kind: inferKind(item),
  }));

  const preferredKinds = isLive ? ['hls', 'dash'] : ['dash', 'hls'];

  for (const kind of preferredKinds) {
    const hit = ordered.find((x) => x.kind === kind);
    if (hit) return hit;
  }

  return ordered[0];
};

const isMuxedFormat = (f) =>
  Boolean(
    f &&
    ((f.vcodec && f.vcodec !== 'none' && f.acodec && f.acodec !== 'none') ||
      (f.mime.includes('video') && f.mime.includes('audio')))
  );

const isVideoOnly = (f) =>
  Boolean(
    f &&
    ((f.vcodec && f.vcodec !== 'none' && (!f.acodec || f.acodec === 'none')) ||
      (f.mime.includes('video') && !f.mime.includes('audio')))
  );

const isAudioOnly = (f) =>
  Boolean(
    f &&
    ((f.acodec && f.acodec !== 'none' && (!f.vcodec || f.vcodec === 'none')) ||
      (f.mime.includes('audio') && !f.mime.includes('video')))
  );

const scoreVideoFormat = (f) => [
  toNumber(f.height, 0),
  toNumber(f.width, 0),
  toNumber(f.fps, 0),
  toNumber(f.tbr || f.vbr || f.abr, 0),
  toNumber(f.filesize_approx || f.filesize, 0),
];

const scoreAudioFormat = (f) => [
  toNumber(f.abr, 0),
  toNumber(f.tbr || f.vbr, 0),
  toNumber(f.filesize_approx || f.filesize, 0),
];

const compareVideoQuality = (a, b) => {
  const A = scoreVideoFormat(a);
  const B = scoreVideoFormat(b);
  for (let i = 0; i < A.length; i++) {
    if (B[i] !== A[i]) return B[i] - A[i];
  }
  return 0;
};

const compareAudioQuality = (a, b) => {
  const A = scoreAudioFormat(a);
  const B = scoreAudioFormat(b);
  for (let i = 0; i < A.length; i++) {
    if (B[i] !== A[i]) return B[i] - A[i];
  }
  return 0;
};

const selectBestMuxed = (formats = []) =>
  [...formats].filter(isMuxedFormat).sort(compareVideoQuality)[0] || null;

const selectBestVideo = (formats = []) =>
  [...formats].filter(isVideoOnly).sort(compareVideoQuality)[0] ||
  [...formats].filter((f) => f.mime.includes('video')).sort(compareVideoQuality)[0] ||
  null;

const selectBestAudio = (formats = []) =>
  [...formats].filter(isAudioOnly).sort(compareAudioQuality)[0] ||
  [...formats].filter((f) => f.mime.includes('audio')).sort(compareAudioQuality)[0] ||
  null;

const buildSdFromRaw = (raw = {}) => {
  const sd = {
    formats: Array.isArray(raw.formats) ? raw.formats : [],
    requested_formats: Array.isArray(raw.requested_formats) ? raw.requested_formats : [],
  };

  for (const key of MANIFEST_KEYS) {
    if (raw[key]) sd[key] = raw[key];
  }

  if (raw.streamingData && isPlainObject(raw.streamingData)) {
    sd.streamingData = raw.streamingData;
  }

  if (Array.isArray(raw.adaptiveFormats)) sd.adaptiveFormats = raw.adaptiveFormats;
  if (Array.isArray(raw.adaptive_formats)) sd.adaptive_formats = raw.adaptive_formats;
  if (Array.isArray(raw.formatStreams)) sd.formatStreams = raw.formatStreams;

  return sd;
};

// ------------------------
// Resource selection
// ------------------------
const selectDashFromRequested = (raw = {}, sd = {}) => {
  const requested = pickRequestedFormats(raw, sd);
  if (requested.length < 2) return null;

  const normalized = collectFormats(
    {
      requested_formats: requested,
    },
    {}
  );

  const video = normalized.find(isVideoOnly);
  const audio = normalized.find(isAudioOnly);

  if (video && audio) {
    return {
      kind: 'dash',
      videourl: parseUrlFromFormat(video),
      audiourl: parseUrlFromFormat(audio),
      source: 'requested_formats',
    };
  }

  return null;
};

const normalizeResourceChoice = (raw = {}, sd = {}) => {
  const formats = collectFormats(raw, sd);
  const live = isLiveLike(raw, sd);

  if (live) {
    const manifest = extractManifest(raw, sd, true);
    if (!manifest) return null;
    return {
      kind: manifest.kind,
      url: manifest.url,
      source: 'manifest',
    };
  }

  const requestedDash = selectDashFromRequested(raw, sd);
  if (requestedDash?.videourl && requestedDash?.audiourl) {
    return requestedDash;
  }

  const video = selectBestVideo(formats);
  const audio = selectBestAudio(formats);

  if (video && audio) {
    return {
      kind: 'dash',
      videourl: parseUrlFromFormat(video),
      audiourl: parseUrlFromFormat(audio),
      source: 'adaptive',
    };
  }

  const muxed = selectBestMuxed(formats);
  if (muxed) {
    return {
      kind: 'progressive',
      url: parseUrlFromFormat(muxed),
      source: 'muxed',
    };
  }

  return null;
};

// ------------------------
// Providers
// ------------------------
const fetchFromYtDlp = async (id, { useProxy = false } = {}) => {
  const raw = await runYtDlp(id, { useProxy });
  if (!isPlainObject(raw)) throw new Error('yt-dlp returned non-object JSON');

  const sd = buildSdFromRaw(raw);
  const formats = collectFormats(raw, sd);
  const live = isLiveLike(raw, sd);

  return {
    provider: useProxy ? 'yt-dlp (proxy)' : 'yt-dlp (direct)',
    streaming_data: sd,
    is_live: live,
    raw,
    formats,
  };
};

const parseInvidiousVideo = (data) => {
  if (!isPlainObject(data)) return null;

  const sd = buildSdFromRaw(data);
  const live = Boolean(
    data.liveNow ||
    data.isLive ||
    data.is_live ||
    data.live ||
    data.live_status === 'is_live' ||
    sd.streamingData?.isLive
  );

  if (live) {
    throw new Error('skip live on invidious');
  }

  return {
    streaming_data: sd,
    is_live: live,
    raw: data,
  };
};

const fetchFromInvidious = async (id) => {
  const instances = rotateInstances(INVIDIOUS_INSTANCES);

  const result = await fastestFetch(
    instances,
    (base) => `${base.replace(/\/$/, '')}/api/v1/videos/${id}`,
    parseInvidiousVideo
  );

  return {
    provider: result.instance,
    streaming_data: result.data.streaming_data,
    is_live: result.data.is_live,
    raw: result.data.raw,
  };
};

const fetchStreamingInfo = async (id) => {
  if (PROXY_URL) {
    try {
      return await fetchFromYtDlp(id, { useProxy: true });
    } catch (e) {
      console.warn('proxied yt-dlp failed, falling back to invidious:', e?.message || e);
    }
  }

  try {
    return await fetchFromInvidious(id);
  } catch (e) {
    console.warn('invidious failed, falling back to direct yt-dlp:', e?.message || e);
  }

  return await fetchFromYtDlp(id, { useProxy: false });
};

const buildStreamResponse = ({ info, raw, sd, title, res }) => {
  const formats = collectFormats(raw, sd);
  const live = isLiveLike(raw, sd);

  if (live) {
    const manifest = extractManifest(raw, sd, true);
    if (!manifest) {
      return res.status(404).json({ error: 'no stream' });
    }

    return res.json({
      resourcetype: manifest.kind || 'hls',
      title,
      url: manifest.url,
      provider: info.provider,
    });
  }

  if (!formats.length) {
    return res.status(404).json({ error: 'no stream' });
  }

  const choice = normalizeResourceChoice(raw, sd);
  if (!choice) {
    return res.status(404).json({ error: 'no stream' });
  }

  if (choice.kind === 'dash') {
    if (choice.videourl && choice.audiourl) {
      return res.json({
        resourcetype: 'dash',
        title,
        videourl: choice.videourl,
        audiourl: choice.audiourl,
        provider: info.provider,
      });
    }
    return res.status(404).json({ error: 'no stream' });
  }

  if (choice.kind === 'progressive') {
    return res.json({
      resourcetype: 'progressive',
      title,
      url: choice.url,
      provider: info.provider,
    });
  }

  return res.status(404).json({ error: 'no stream' });
};

// ------------------------
// API
// ------------------------
app.get('/api/stream', async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!YT_ID_REGEX.test(id)) return res.status(400).json({ error: 'invalid video id' });

    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data || {};
    const raw = info.raw || {};
    const title = extractTitle(raw) || '';

    return buildStreamResponse({ info, raw, sd, title, res });
  } catch (err) {
    console.error('Unexpected error in /api/stream', err);
    return res.status(500).json({ error: err?.message || 'internal error' });
  }
});

// ------------------------
// Server start
// ------------------------
app.listen(PORT, () => console.log(`Server running on ${PORT}`));
