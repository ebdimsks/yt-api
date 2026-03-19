import 'dotenv/config';
import express from 'express';
import { spawn } from 'node:child_process';

const YT_DLP_BIN = '/opt/venv/bin/yt-dlp';
const PORT = Number(process.env.PORT || 3001);
const PROXY_URL = process.env.PROXY_URL; // 例: http://127.0.0.1:8080

const app = express();
app.disable('x-powered-by');

const runYtDlp = (videoId, { useProxy = false } = {}) => {
  const url = `https://www.youtube.com/watch?v=${videoId}`;

  const args = [
    '--dump-single-json',
    '--skip-download',
    '--no-playlist',
    '--no-warnings',
    '--no-progress',
    '--extractor-args',
    'youtube:player_client=android',
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

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });

    child.on('error', reject);

    child.on('close', (code, signal) => {
      if (code !== 0) {
        return reject(
          new Error(
            `yt-dlp failed (${code ?? signal ?? 'unknown'}): ${stderr.trim() || 'no stderr'}`
          )
        );
      }

      try {
        resolve(JSON.parse(stdout));
      } catch (err) {
        reject(new Error(`JSON parse failed: ${err.message}\n${stdout.slice(0, 500)}`));
      }
    });
  });
};

// ------------------------
// API
// ------------------------

// プロキシあり
app.get('/test/proxy/:id', async (req, res) => {
  const id = req.params.id;

  if (!/^[a-zA-Z0-9_-]{11}$/.test(id)) {
    return res.status(400).json({ error: 'invalid video id' });
  }

  try {
    const data = await runYtDlp(id, { useProxy: true });
    return res.json({
      mode: 'proxy',
      proxy: PROXY_URL || null,
      data,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
});

// プロキシなし（比較用）
app.get('/test/direct/:id', async (req, res) => {
  const id = req.params.id;

  if (!/^[a-zA-Z0-9_-]{11}$/.test(id)) {
    return res.status(400).json({ error: 'invalid video id' });
  }

  try {
    const data = await runYtDlp(id, { useProxy: false });
    return res.json({
      mode: 'direct',
      data,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`yt-dlp test server running on ${PORT}`);
});
