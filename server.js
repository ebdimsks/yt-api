import express from "express";
import { Innertube } from "youtubei.js";
import crypto from "crypto";

const app = express();
const port = process.env.PORT || 3000;

const WORKER_SECRET = process.env.WORKER_SECRET;

if (!WORKER_SECRET) {
  console.error("WORKER_SECRET required");
  process.exit(1);
}

const ALLOWED_WINDOW = 300;
const INSTANCE_BAN_MS = 300000;

/* ---------------- Instances ---------------- */

const INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.tiekoetter.com",
  "https://yewtu.be",
];

const PIPED_INSTANCES = [
  "https://pipedapi.kavin.rocks",
  "https://pipedapi.leptons.xyz",
  "https://pipedapi.nosebs.ru",
  "https://pipedapi-libre.kavin.rocks",
  "https://piped-api.privacy.com.de",
  "https://pipedapi.adminforge.de",
  "https://api.piped.yt",
  "https://pipedapi.drgns.space",
  "https://pipedapi.owo.si",
  "https://pipedapi.ducks.party",
  "https://piped-api.codespace.cz",
  "https://pipedapi.reallyaweso.me",
  "https://api.piped.private.coffee",
  "https://pipedapi.darkness.services",
  "https://pipedapi.orangenet.cc",
];

/* ---------------- Health ---------------- */

const bad = new Map();

function markBad(url){
  bad.set(url,Date.now());
}

function isBad(url){

  const t = bad.get(url);

  if(!t) return false;

  if(Date.now()-t > INSTANCE_BAN_MS){
    bad.delete(url);
    return false;
  }

  return true;

}

/* ---------------- Utils ---------------- */

function normalizeUrl(base,url){

  if(!url) return null;

  if(url.startsWith("http"))
    return url;

  return base + url;

}

/* ---------------- HLS ---------------- */

async function resolveHls(master){

  try{

    const r = await fetch(master);
    if(!r.ok) return master;

    const text = await r.text();
    const lines = text.split("\n");

    const variants=[];

    for(let i=0;i<lines.length;i++){

      if(lines[i].startsWith("#EXT-X-STREAM-INF")){

        const next = lines[i+1];

        const m = lines[i].match(/RESOLUTION=(\d+)x(\d+)/);

        const h = m ? Number(m[2]) : 0;

        if(next && !next.startsWith("#")){

          variants.push({
            height:h,
            url:new URL(next,master).href
          });

        }

      }

    }

    if(!variants.length) return master;

    variants.sort((a,b)=>b.height-a.height);

    return variants[0].url;

  }catch{

    return master;

  }

}

/* ---------------- Format Helpers ---------------- */

function parseUrl(f){

  if(f.url) return f.url;

  const cipher =
    f.signatureCipher ||
    f.signature_cipher ||
    f.cipher;

  if(!cipher) return null;

  return new URLSearchParams(cipher).get("url");

}

function normalizeFormats(sd){

  return [
    ...(sd.formats||[]),
    ...(sd.adaptive_formats||[])
  ].map(f=>({
    ...f,
    mime:(f.mimeType||f.mime_type||"").toLowerCase()
  }));

}

function bestVideo(formats){

  return formats
    .filter(f=>f.mime.includes("video"))
    .sort((a,b)=>(b.height||0)-(a.height||0))[0];

}

function bestAudio(formats){

  return formats
    .filter(f=>f.mime.includes("audio"))
    .sort((a,b)=>(b.bitrate||0)-(a.bitrate||0))[0];

}

function bestProgressive(formats){

  return formats
    .filter(f=>f.mime.includes("video") && /mp4a|aac|opus/.test(f.mime))
    .sort((a,b)=>(b.height||0)-(a.height||0))[0];

}

/* ---------------- Instance Racing ---------------- */

async function fastest(list,build,parser){

  const controllers=[];

  const tasks=list
    .filter(i=>!isBad(i))
    .map(async base=>{

      const c=new AbortController();
      controllers.push(c);

      try{

        const r = await fetch(build(base),{signal:c.signal});

        if(!r.ok) throw 0;

        const j = await r.json();

        const parsed = parser(j,base);

        if(!parsed) throw 0;

        return parsed;

      }catch{

        markBad(base);
        throw 0;

      }

    });

  const res = await Promise.any(tasks);

  controllers.forEach(c=>c.abort());

  return res;

}

/* ---------------- Providers ---------------- */

async function fromInvidious(id){

  return fastest(
    INVIDIOUS,
    b=>`${b}/api/v1/videos/${id}`,
    (d,b)=>{

      if(d.hlsUrl){

        return{
          provider:"invidious",
          streaming_data:{
            hlsManifestUrl:normalizeUrl(b,d.hlsUrl)
          }
        };

      }

      const formats=[];

      if(d.formatStreams)
        d.formatStreams.forEach(f=>formats.push({...f,mimeType:f.type}));

      if(d.adaptiveFormats)
        d.adaptiveFormats.forEach(f=>formats.push({...f,mimeType:f.type}));

      if(!formats.length) return null;

      return{
        provider:"invidious",
        streaming_data:{formats}
      };

    }
  );

}

async function fromPiped(id){

  return fastest(
    PIPED,
    b=>`${b}/streams/${id}`,
    (d,b)=>{

      if(d.hls){

        return{
          provider:"piped",
          streaming_data:{
            hlsManifestUrl:normalizeUrl(b,d.hls)
          }
        };

      }

      const formats=[
        ...(d.videoStreams||[]),
        ...(d.audioStreams||[])
      ];

      if(!formats.length) return null;

      return{
        provider:"piped",
        streaming_data:{formats}
      };

    }
  );

}

let yt;

async function fromInnertube(id){

  if(!yt){

    yt = await Innertube.create({
      client_type:"ANDROID",
      generate_session_locally:true
    });

  }

  const info = await yt.getInfo(id);

  return{
    provider:"innertube",
    streaming_data:info.streaming_data
  };

}

/* ---------------- Fetch Router ---------------- */

async function getStreaming(id){

  try{ return await fromInvidious(id); }catch{}
  try{ return await fromPiped(id); }catch{}

  return fromInnertube(id);

}

/* ---------------- Auth ---------------- */

function safeEqual(a,b){

  const A=Buffer.from(a,"hex");
  const B=Buffer.from(b,"hex");

  if(A.length!==B.length) return false;

  return crypto.timingSafeEqual(A,B);

}

function auth(req,res,next){

  const ts=req.header("x-proxy-timestamp");
  const sig=req.header("x-proxy-signature");

  if(!ts||!sig)
    return res.status(401).json({error:"unauthorized"});

  const now=Math.floor(Date.now()/1000);

  if(Math.abs(now-Number(ts))>ALLOWED_WINDOW)
    return res.status(401).json({error:"unauthorized"});

  const payload=`${ts}:${req.originalUrl}`;

  const expected=crypto
    .createHmac("sha256",WORKER_SECRET)
    .update(payload)
    .digest("hex");

  if(!safeEqual(expected,sig))
    return res.status(401).json({error:"unauthorized"});

  next();

}

/* ---------------- API ---------------- */

app.get("/api/stream",auth,async(req,res)=>{

  try{

    const id=req.query.id;

    if(!id)
      return res.status(400).json({error:"id required"});

    const info = await getStreaming(id);
    const sd = info.streaming_data;

    const hls =
      sd.hlsManifestUrl ||
      sd.hls_manifest_url ||
      sd.hlsUrl ||
      sd.hls;

    if(hls){

      const url = await resolveHls(hls);

      return res.json({
        type:"hls",
        url,
        manifest:hls,
        provider:info.provider
      });

    }

    const formats = normalizeFormats(sd);

    const v = bestVideo(formats);
    const a = bestAudio(formats);

    if(v && a){

      return res.json({
        type:"dash",
        quality:v.height,
        video_url:parseUrl(v),
        audio_url:parseUrl(a),
        provider:info.provider
      });

    }

    const p = bestProgressive(formats);

    if(p){

      return res.json({
        type:"progressive",
        quality:p.height,
        url:parseUrl(p),
        provider:info.provider
      });

    }

    res.status(404).json({error:"no stream"});

  }catch(e){

    res.status(500).json({error:e.message});

  }

});

app.listen(port,()=>{
  console.log("server running",port);
});
