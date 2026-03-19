import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';
import { spawn } from 'node:child_process';

// =====================================================
// Config
// =====================================================
const {
  WORKER_SECRET,
  PORT = 3000,
  PROXY_URL,
} = process.env;

if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = Number(PORT) || 3000;

const CONFIG = {
  ytDlpBin: '/opt/venv/bin/yt-dlp',
  ytTimeout: 15000,
  requestTimeout: 5000,
  instanceBanMs: 5 * 60 * 1000,
};

const INVIDIOUS = [
   'https://inv.nadeko.net',
   'https://invidious.f5.si',
   'https://invidious.lunivers.trade',
   'https://iv.melmac.space',
   'https://yt.omada.cafe',
   'https://invidious.nerdvpn.de',
   'https://invidious.tiekoetter.com',
   'https://yewtu.be',
 ];

// =====================================================
// Auth
// =====================================================
const verifyAuth = (req, res, next) => {
  const ts = req.header('x-proxy-timestamp');
  const sig = req.header('x-proxy-signature');

  if (!ts || !sig) return res.status(401).json({ error: 'unauthorized' });

  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');

  if (expected !== sig) return res.status(401).json({ error: 'unauthorized' });

  next();
};

// =====================================================
// Cache
// =====================================================
const CACHE = new Map();
const TTL_SHORT = 10 * 60 * 1000;
const TTL_LONG = 4 * 60 * 60 * 1000;

const getCache = (id) => {
  const c = CACHE.get(id);
  if (!c) return null;
  if (Date.now() - c.ts > c.ttl) {
    CACHE.delete(id);
    return null;
  }
  return c.data;
};

const setCache = (id, data) => {
  const ttl = data.formats.length >= 12 ? TTL_LONG : TTL_SHORT;
  CACHE.set(id, { ts: Date.now(), ttl, data });
};

// =====================================================
// yt-dlp
// =====================================================
const runYtDlp = (id, { proxy = false } = {}) => {
  const url = `https://www.youtube.com/watch?v=${id}`;

  const args = [
    '--print-json',
    '--no-playlist',
    '--no-warnings',
    '--no-progress',
    '--simulate',
    '-f', 'best',
    '--extractor-args',
    'youtube:player_client=android,web,ios,tv_embedded',
    '--no-call-home',
  ];

  if (proxy && PROXY_URL) {
    args.push('--proxy', PROXY_URL);
  }

  args.push(url);

  return new Promise((resolve, reject) => {
    const p = spawn(CONFIG.ytDlpBin, args);

    let out = '';
    let err = '';

    const timer = setTimeout(() => p.kill('SIGKILL'), CONFIG.ytTimeout);

    p.stdout.on('data', (d) => (out += d.toString()));
    p.stderr.on('data', (d) => (err += d.toString()));

    p.on('close', (code) => {
      clearTimeout(timer);

      if (code !== 0) {
        return reject(new Error(err || 'yt-dlp failed'));
      }

      try {
        resolve(JSON.parse(out));
      } catch {
        reject(new Error('invalid json'));
      }
    });
  });
};

// =====================================================
// Invidious
// =====================================================
const bad = new Map();

const markBad = (url) => bad.set(url, Date.now());

const validInstances = () => {
  const now = Date.now();
  return INVIDIOUS.filter((i) => {
    const t = bad.get(i);
    return !t || now - t > CONFIG.instanceBanMs;
  });
};

const fetchInvidious = async (id) => {
  const list = validInstances();

  for (const base of list) {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), CONFIG.requestTimeout);

      const res = await fetch(`${base}/api/v1/videos/${id}`, {
        signal: ctrl.signal,
      });

      clearTimeout(t);

      if (!res.ok) {
        markBad(base);
        continue;
      }

      const data = await res.json();

      const formats = [
        ...(data.formatStreams || []),
        ...(data.adaptiveFormats || []),
      ];

      return {
        provider: 'invidious',
        instance: base,
        formats,
        raw: data,
      };
    } catch {
      markBad(base);
    }
  }

  throw new Error('invidious failed');
};

// =====================================================
// Format normalization
// =====================================================
const isPlayable = (f) => {
  if (!f) return false;
  if (f.ext === 'mhtml') return false;
  if (f.protocol === 'mhtml') return false;

  const hasVideo = f.vcodec && f.vcodec !== 'none';
  const hasAudio = f.acodec && f.acodec !== 'none';

  return f.url && (hasVideo || hasAudio);
};

const normalizeFormats = (list = []) =>
  list.filter(isPlayable).map((f) => ({
    itag: f.format_id,
    ext: f.ext,
    resolution: f.resolution,
    fps: f.fps,
    acodec: f.acodec,
    vcodec: f.vcodec,
    url: f.url,
  }));

// =====================================================
// Fetch orchestration
// =====================================================
const fetchStreaming = async (id) => {
  if (PROXY_URL) {
    try {
      const r = await runYtDlp(id, { proxy: true });
      return { source: 'yt-dlp-proxy', data: r };
    } catch {}
  }

  try {
    const r = await fetchInvidious(id);
    return { source: 'invidious', data: r };
  } catch {}

  const r = await runYtDlp(id, { proxy: false });
  return { source: 'yt-dlp-direct', data: r };
};

// =====================================================
// Response builder
// =====================================================
const buildResponse = (id, result) => {
  if (result.source.startsWith('yt-dlp')) {
    const formats = normalizeFormats(result.data.formats);

    if (!formats.length) {
      throw new Error('no playable formats');
    }

    return {
      id,
      title: result.data.title,
      formats,
      provider: result.source,
    };
  }

  if (result.source === 'invidious') {
    const formats = normalizeFormats(result.data.formats);

    if (!formats.length) {
      throw new Error('no playable formats');
    }

    return {
      id,
      title: result.data.raw.title,
      formats,
      provider: result.data.instance,
    };
  }

  throw new Error('unknown source');
};

// =====================================================
// API
// =====================================================
app.get('/api/stream', verifyAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json({ error: 'id required' });

    const cached = getCache(id);
    if (cached) return res.json(cached);

    const result = await fetchStreaming(id);
    const response = buildResponse(id, result);

    setCache(id, response);

    return res.json(response);

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      error: err.message || 'internal error',
    });
  }
});

// =====================================================
app.listen(port, () => {
  console.log(`Server running on ${port}`);
});
