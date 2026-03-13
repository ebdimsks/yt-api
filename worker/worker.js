let cachedKey = null;

async function getHmacKey(secret) {
  if (cachedKey) return cachedKey;

  const enc = new TextEncoder();
  cachedKey = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  return cachedKey;
}

async function hmacHex(secret, msg) {
  const key = await getHmacKey(secret);

  const sig = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(msg)
  );

  const bytes = new Uint8Array(sig);

  let hex = "";
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, "0");
  }

  return hex;
}

export default {
  async fetch(request, env) {

    if (!env.WORKER_SECRET || !env.SERVER_URL) {
      return new Response("Server not configured", { status: 500 });
    }

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders()
      });
    }

    const url = new URL(request.url);
    const target = `${env.SERVER_URL}/api/stream${url.search}`;

    const ts = Math.floor(Date.now() / 1000);
    const payload = `${ts}:/api/stream${url.search}`;

    const signature = await hmacHex(env.WORKER_SECRET, payload);

    const nodeRes = await fetch(target, {
      headers: {
        "x-proxy-timestamp": ts.toString(),
        "x-proxy-signature": signature
      }
    });

    return new Response(nodeRes.body, {
      status: nodeRes.status,
      headers: {
        ...corsHeaders(),
        "content-type": nodeRes.headers.get("content-type") || "application/json"
      }
    });
  }
};

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS"
  };
}
