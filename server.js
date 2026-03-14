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
const YT_ID_REGEX = /^[a-zA-Z0-9_-]{11}$/;

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

/* ---------------- Network helpers (fetch with timeout) ---------------- */
const timeout = (ms) => new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), ms));

const fetchWithTimeout = async (url, opts = {}, ms = 5000) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, { signal: controller.signal, ...opts });
    clearTimeout(timer);
    return res;
  } catch (e) {
    clearTimeout(timer);
    throw e;
  }
};

/* ---------------- CDN verification ---------------- */
const verifyCdn = async (url, timeoutMs = 4000) => {
  if (!url) return false;

  try {
    // try HEAD first
    const headRes = await fetchWithTimeout(url, { method: 'HEAD', redirect: 'follow' }, timeoutMs);
    if (headRes && headRes.status === 200) return true;

    // Some CDNs reject HEAD. Fall back to small-range GET
    if (headRes && [405, 501].includes(headRes.status)) {
      // fall through to range GET
    } else if (headRes && headRes.status >= 400) {
      // non-200 HEAD (e.g., 403) — still try range GET as a fallback
    }
  } catch (e) {
    // network error or aborted -> fall back to range
  }

  try {
    const rangeRes = await fetchWithTimeout(url, {
      method: 'GET',
      headers: { Range: 'bytes=0-0' },
      redirect: 'follow',
    }, timeoutMs);
    if (rangeRes && (rangeRes.status === 206 || rangeRes.status === 200)) return true;
  } catch (e) {
    // failed
  }

  return false;
};

/* ---------------- Parallel fetch-to-instances but process in order-of-arrival ---------------- */
const fastestFetch = async (instances, buildUrl, parser) => {
  if (!instances || instances.length === 0) throw new Error('no instances');

  // create per-instance tasks that always resolve to an object (no rejection)
  const wrappers = instances.map((base) => {
    const controller = new AbortController();

    const taskPromise = (async () => {
      try {
        const res = await fetch(buildUrl(base), { signal: controller.signal });
        if (!res.ok) {
          // markBad later after we settle — but early mark is fine
          return { ok: false, instance: base, error: new Error(`status ${res.status}`) };
        }

        const data = await res.json();
        const parsed = parser(data);

        if (!parsed) {
          return { ok: false, instance: base, error: new Error('parse failed') };
        }

        return { ok: true, instance: base, streaming_data: parsed.streaming_data };
      } catch (err) {
        return { ok: false, instance: base, error: err };
      }
    })();

    return { base, controller, promise: taskPromise };
  });

  // We'll handle results as they resolve (in completion order)
  const remaining = new Set(wrappers.map((w) => w.base));

  while (remaining.size > 0) {
    // build array of promises that resolve to the wrapper result (instance included)
    const races = [...wrappers]
      .filter((w) => remaining.has(w.base))
      .map((w) =>
        w.promise.then((v) => ({ ...v })) // copy to avoid accidental mutation
      );

    // Wait for the next promise to settle (fulfilled) — since our promises never reject,
    // the next resolved item is the next finished HTTP response.
    const result = await Promise.race(races);

    // remove this instance from remaining
    remaining.delete(result.instance);

    if (!result.ok) {
      // mark instance bad and continue
      markBad(result.instance);
      continue;
    }

    const sd = result.streaming_data || {};
    const formats = normalizeFormats(sd);

    // Build candidate URL sets to check.
    // Priority: DASH (video+audio) -> progressive
    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);
    const progressive = selectBestProgressive(formats);

    let ok = false;

    // Helper to verify one or multiple URLs
    const verifyUrls = async (urls) => {
      for (const u of urls) {
        if (!u) return false;
      }
      // verify all individually
      for (const u of urls) {
        const parsed = parseUrl(u);
        if (!parsed) return false;
        const yes = await verifyCdn(parsed);
        if (!yes) return false;
      }
      return true;
    };

    try {
      if (video && audio) {
        const vUrl = parseUrl(video);
        const aUrl = parseUrl(audio);
        if (vUrl && aUrl) {
          const bothOk = await verifyUrls([vUrl, aUrl]);
          if (bothOk) ok = true;
        }
      }

      if (!ok && progressive) {
        const pUrl = parseUrl(progressive);
        if (pUrl) {
          const pOk = await verifyUrls([pUrl]);
          if (pOk) ok = true;
        }
      }
    } catch (e) {
      // treat as failure for this instance
      ok = false;
    }

    if (ok) {
      // Abort all other in-flight requests
      wrappers.forEach((w) => {
        try {
          w.controller.abort();
        } catch {}
      });

      return {
        instance: result.instance,
        streaming_data: sd,
      };
    }

    // CDN verification failed for this instance -> mark bad and continue loop to next finished instance
    markBad(result.instance);
  }

  // none worked
  throw new Error('no valid instance');
};

/* ---------------- Providers ---------------- */
const fetchFromInvidious = async (id) => {
  const instances = rotateInstances(INVIDIOUS_INSTANCES);

  const result = await fastestFetch(
    instances,
    (base) => `${base}/api/v1/videos/${id}`,
    (data) => {
      const formats = [];

      (data.formatStreams || []).forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
      (data.adaptiveFormats || []).forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));

      if (!formats.length) return null;

      return {
        streaming_data: { formats },
      };
    }
  );

  return {
    provider: 'invidious',
    instance: result.instance,
    streaming_data: result.streaming_data,
  };
};

const fetchFromInnertube = async (id) => {
  const client = await getYtClient();
  const info = await client.getInfo(id);

  if (!info?.streaming_data) throw new Error('No streaming data');

  // normalize somewhat to match our other code
  return {
    provider: 'innertube',
    streaming_data: info.streaming_data,
  };
};

const fetchStreamingInfo = async (id) => {
  try {
    return await fetchFromInvidious(id);
  } catch (e) {
    // fallback to innertube. Because innertube may also produce CDN URLs that are inaccessible,
    // we will still return it and let the route handler do final verification before responding.
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

function isValidVideoId(id) {
  return typeof id === "string" && YT_ID_REGEX.test(id);
}

/* ---------------- API ---------------- */
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'id required' });

    if (!isValidVideoId(id))
      return res.status(400).json({ error: 'invalid video id' });

    const info = await fetchStreamingInfo(String(id));
    const sd = info.streaming_data || {};

    // HLS explicitly disallowed
    const hls = sd.hlsManifestUrl || sd.hls_manifest_url || sd.hlsUrl || sd.hls;
    if (hls) return res.status(403).json({ error: 'HLS streams are not supported' });

    const formats = normalizeFormats(sd);

    // DASH (separate video+audio)
    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);

    // Progressive (muxed)
    const progressive = selectBestProgressive(formats);

    // Before returning, perform final CDN verification (important for Innertube fallback)
    const ensureCdnOk = async () => {
      if (video && audio) {
        const v = parseUrl(video);
        const a = parseUrl(audio);
        if (!v || !a) return false;
        return (await verifyCdn(v)) && (await verifyCdn(a));
      }

      if (progressive) {
        const p = parseUrl(progressive);
        if (!p) return false;
        return await verifyCdn(p);
      }

      return false;
    };

    const cdnOk = await ensureCdnOk();
    if (!cdnOk) {
      // If this came from an Invidious instance, it should have been validated inside fastestFetch.
      // If we ended up here via Innertube fallback and CDN check fails, return 502 (bad gateway).
      return res.status(502).json({ error: 'cdn unreachable' });
    }

    if (video && audio) {
      return res.json({
        type: 'dash',
        video_url: parseUrl(video),
        audio_url: parseUrl(audio),
        provider: info.provider,
        instance: info.instance || null
      });
    }

    if (progressive) {
      return res.json({
        type: 'progressive',
        url: parseUrl(progressive),
        provider: info.provider,
        instance: info.instance || null
      });
    }

    return res.status(404).json({ error: 'no stream' });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'internal error' });
  }
});

app.listen(port, () => console.log(`Server running on ${port}`));
