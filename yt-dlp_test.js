import 'dotenv/config';
import express from 'express';
import { spawn } from 'node:child_process';

// =============================
// Config
// =============================
const {
  PORT = 3000,
  PROXY_URL,
} = process.env;

const app = express();
const port = Number(PORT) || 3000;

const YT_DLP_BIN = '/opt/venv/bin/yt-dlp';
const YT_DLP_TIMEOUT_MS = 15_000;

// =============================
// yt-dlp runner (raw)
// =============================
function runYtDlpRaw(videoId) {
  const url = `https://www.youtube.com/watch?v=${videoId}`;

  const args = [
    '--print-json',
    '--no-playlist',
    '--no-warnings',
    '--no-progress',
    '--simulate',

    // できるだけ多くの client を試す
    '--extractor-args',
    'youtube:player_client=android,web,ios,tv_embedded',

    '-f', 'best',

    '--no-call-home',
  ];

  if (PROXY_URL) {
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

      // 👇 デバッグ用にそのまま返す
      resolve({
        ok: code === 0,
        code,
        signal,
        stderr,
        stdout,
      });
    });
  });
}

// =============================
// API
// =============================
app.get('/test', async (req, res) => {
  try {
    const id = String(req.query.id || '');

    if (!id) {
      return res.status(400).json({ error: 'id required' });
    }

    const result = await runYtDlpRaw(id);

    // 👇 完全にそのまま返す（JSONパースしない）
    return res.json(result);

  } catch (err) {
    return res.status(500).json({
      error: err?.message || 'internal error',
    });
  }
});

// =============================
// Start
// =============================
app.listen(port, () => {
  console.log(`yt-dlp test server running on ${port}`);
});
