import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';
import { spawn } from 'node:child_process';

// ------------------------
// Environment / Constants
// ------------------------
const WORKER_SECRET = process.env.WORKER_SECRET;
const PROXY_URL = process.env.PROXY_URL;
const YT_DLP_BIN = '/opt/venv/bin/yt-dlp';
const PORT = Number(process.env.PORT || 3000);

if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
app.disable('x-powered-by');

const ALLOWED_WINDOW_SECONDS = 300;
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

const isNonEmptyString = (v) => typeof v === 'string' && v.trim().length > 0;

const isHlsUrl = (url) => /(^|[/?#&])[^?#]*\.m3u8(\?|#|$)/i.test(url) || /mpegurl/i.test(url);
const isDashUrl = (url) => /(^|[/?#&])[^?#]*\.mpd(\?|#|$)/i.test(url) || /dash/i.test(url);

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
// Auth
// ------------------------
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
    '--extractor-args',
    'youtube:player_client=android',
    '--format',
    'bv*+ba/b',
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

      const text = stdout.trim();
      if (!text) {
        return reject(new Error('yt-dlp returned empty output'));
      }

      try {
        resolve(JSON.parse(text));
        return;
      } catch {}

      const firstBrace = text.indexOf('{');
      const lastBrace = text.lastIndexOf('}');
      if (firstBrace >= 0 && lastBrace > firstBrace) {
        try {
          resolve(JSON.parse(text.slice(firstBrace, lastBrace + 1)));
          return;
        } catch {}
      }

      reject(new Error('yt-dlp returned invalid JSON'));
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

      let timedOut = false;
      const timeout = setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, REQUEST_TIMEOUT_MS);

      try {
        const res = await fetch(buildUrl(base), {
          signal: controller.signal,
          headers: {
            accept: 'application/json',
          },
        });

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
        const aborted = err?.name === 'AbortError';
        if (timedOut || !aborted) {
          markInstanceBad(base);
        }
        throw err;
      } finally {
        clearTimeout(timeout);
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
      if (!f || typeof f !== 'object') return null;

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
    const base =
      f.url ||
      String(f.itag || '') +
        '|' +
        String(f.format_id || '') +
        '|' +
        String(f.height || '') +
        '|' +
        String(f.width || '') +
        '|' +
        String(f.mime || '');
    return base;
  });
};

const hasManifestInSd = (sd = {}) =>
  MANIFEST_KEYS.some((k) => Boolean(sd?.[k])) ||
  MANIFEST_KEYS.some((k) => Boolean(sd?.streamingData?.[k]));

const containsManifestLikeFormat = (formats = []) =>
  formats.some((f) => {
    const url = parseUrlFromFormat(f) || '';
    return (
      isHlsUrl(url) ||
      isDashUrl(url) ||
      /application\/vnd\.apple\.mpegurl/i.test(f.mime || '') ||
      /application\/dash\+xml/i.test(f.mime || '')
    );
  });

const extractTitle = (raw = {}) => {
  return (
    raw.title ||
    raw.videoDetails?.title ||
    raw.video_details?.title ||
    raw.player_response?.videoDetails?.title ||
    raw.basic_info?.title ||
    raw.microformat?.title?.simpleText ||
    (raw.titleText?.runs?.map?.((x) => x.text) || []).join('') ||
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
      containsManifestLikeFormat(formats)
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

  if (raw.streamingData && typeof raw.streamingData === 'object') {
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
  const is_live = isLiveLike(raw, sd, formats);

  if (is_live) {
    const manifest = extractManifest(raw, sd, true);
    if (manifest) {
      return {
        kind: manifest.kind,
        url: manifest.url,
        source: 'manifest',
      };
    }
    return null;
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
  const sd = buildSdFromRaw(raw);
  const formats = collectFormats(raw, sd);
  const is_live = isLiveLike(raw, sd, formats);

  return {
    provider: useProxy ? 'yt-dlp (proxy)' : 'yt-dlp (direct)',
    streaming_data: sd,
    is_live,
    raw,
  };
};

const parseInvidiousVideo = (data) => {
  if (!data || typeof data !== 'object') return null;

  const sd = buildSdFromRaw(data);
  const formats = collectFormats(data, sd);

  const is_live = Boolean(
    data.liveNow ||
      data.isLive ||
      data.is_live ||
      data.live ||
      data.live_status === 'is_live' ||
      sd.streamingData?.isLive ||
      containsManifestLikeFormat(formats) ||
      hasManifestInSd(sd)
  );

  return {
    streaming_data: sd,
    is_live,
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

  try {
    return await fetchFromYtDlp(id, { useProxy: false });
  } catch (e) {
    console.warn('direct yt-dlp failed:', e?.message || e);
    throw new Error('all providers failed');
  }
};

// ------------------------
// API
// ------------------------
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!YT_ID_REGEX.test(id)) return res.status(400).json({ error: 'invalid video id' });

    const info = await fetchStreamingInfo(id);
    const sd = info.streaming_data || {};
    const raw = info.raw || {};
    const formats = collectFormats(raw, sd);
    const is_live = isLiveLike(raw, sd, formats);

    const title = extractTitle(raw) || '';

    if (is_live) {
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
  } catch (err) {
    console.error('Unexpected error in /api/stream', err);
    return res.status(500).json({ error: err?.message || 'internal error' });
  }
});

// ------------------------
// Server start
// ------------------------
app.listen(PORT, () => console.log(`Server running on ${PORT}`));
