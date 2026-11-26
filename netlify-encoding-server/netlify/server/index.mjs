import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import ffmpegPath from 'ffmpeg-static';

const PRESETS = Object.freeze({
  'h264-1080p-crf-0': { id: 'h264-1080p-crf-0', name: 'H.264 1080p Lossless', codec: 'libx264', crf: 0, height: 1080, preset: 'medium', audioBitrate: '192k' },
  'h264-1080p-crf-1': { id: 'h264-1080p-crf-1', name: 'H.264 1080p CRF 1', codec: 'libx264', crf: 1, height: 1080, preset: 'medium', audioBitrate: '192k' },
  'h264-1080p-crf-2': { id: 'h264-1080p-crf-2', name: 'H.264 1080p CRF 2', codec: 'libx264', crf: 2, height: 1080, preset: 'medium', audioBitrate: '192k' }
});

// One encoder at a time, with at most eight additional requests waiting.
const MAX_WAITING = 8;
const LEASE_MS = 5 * 60 * 1000;
const RENEW_EVERY_MS = 30 * 1000;
const MAX_JOB_MS = Number(process.env.MAX_JOB_MS || 25 * 60 * 1000);
const API_KEY = process.env.ENCODING_API_KEY || '';
const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const KEY_PREFIX = process.env.QUEUE_KEY_PREFIX || '';
const ACTIVE_KEY = `${KEY_PREFIX}:active`;
const WAITING_KEY = `${KEY_PREFIX}:waiting`;

const ENQUEUE_SCRIPT = `
local active = redis.call('GET', KEYS[1])
local waiting = redis.call('LLEN', KEYS[2])
if (not active) and waiting == 0 then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return {'ACTIVE', '0'}
end
if waiting >= tonumber(ARGV[3]) then return {'FULL', tostring(waiting)} end
redis.call('RPUSH', KEYS[2], ARGV[1])
return {'WAIT', tostring(redis.call('LLEN', KEYS[2]))}
`;
const CLAIM_SCRIPT = `
local active = redis.call('GET', KEYS[1])
if active == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not active and redis.call('LINDEX', KEYS[2], 0) == ARGV[1] then
  redis.call('LPOP', KEYS[2])
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`;
const RELEASE_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end
redis.call('LREM', KEYS[2], 0, ARGV[1])
return 1
`;
const RENEW_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0
`;
const STATUS_SCRIPT = `
local active = redis.call('GET', KEYS[1])
local waiting = redis.call('LLEN', KEYS[2])
return {active and 1 or 0, waiting}
`;

function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...extraHeaders }
  });
}

async function redisCommand(...command) {
  if (!REDIS_URL || !REDIS_TOKEN) throw new Error('Upstash Redis is not configured. Set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.');
  const response = await fetch(REDIS_URL, {
    method: 'POST',
    headers: { authorization: `Bearer ${REDIS_TOKEN}`, 'content-type': 'application/json' },
    body: JSON.stringify(command)
  });
  if (!response.ok) throw new Error(`Redis HTTP ${response.status}`);
  const payload = await response.json();
  if (payload.error) throw new Error(payload.error);
  return payload.result;
}

async function evalRedis(script, keys, args = []) {
  return redisCommand('EVAL', script, String(keys.length), ...keys, ...args.map(String));
}

async function queueStatus(jobToken = '') {
  const result = await evalRedis(STATUS_SCRIPT, [ACTIVE_KEY, WAITING_KEY]);
  return { active: Number(result[0]) === 1, waiting: Number(result[1]) };
}

function isPublicHttpsUrl(value) {
  try {
    const u = new URL(value);
    if (u.protocol !== 'https:') return false;
    const host = u.hostname.toLowerCase();
    if (!host || host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local')) return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.|0\.)/.test(host) || /^172\.(1[6-9]|2\d|3[01])\./.test(host)) return false;
    if (host === '::1' || host.startsWith('fc') || host.startsWith('fd') || host.startsWith('fe80:')) return false;
    const allow = (process.env.ALLOWED_INPUT_HOSTS || '').split(',').map(x => x.trim().toLowerCase()).filter(Boolean);
    if (allow.length && !allow.some(domain => host === domain || host.endsWith(`.${domain}`))) return false;
    return true;
  } catch { return false; }
}

function authOk(req) {
  if (!API_KEY) return process.env.NODE_ENV !== 'production';
  return req.headers.get('authorization') === `Bearer ${API_KEY}` || req.headers.get('x-api-key') === API_KEY;
}

function ffmpegArgs(inputUrl, p) {
  return [
    '-hide_banner', '-loglevel', 'error', '-nostdin',
    '-i', inputUrl,
    '-map', '0:v:0', '-map', '0:a:0?',
    '-vf', `scale=-2:${p.height}:flags=lanczos`,
    '-c:v', p.codec, '-preset', p.preset, '-crf', String(p.crf),
    '-pix_fmt', 'yuv420p',
    '-c:a', 'aac', '-b:a', p.audioBitrate,
    '-movflags', '+frag_keyframe+empty_moov+default_base_moof',
    '-f', 'mp4', 'pipe:1'
  ];
}

async function admitQueue(jobToken) {
  const result = await evalRedis(ENQUEUE_SCRIPT, [ACTIVE_KEY, WAITING_KEY], [jobToken, LEASE_MS, MAX_WAITING]);
  const state = result[0];
  if (state === 'FULL') return { state: 'FULL', waiting: Number(result[1]) };
  if (state === 'ACTIVE') return { state: 'ACTIVE', position: 0 };
  return { state: 'WAIT', position: Number(result[1]) };
}

async function waitForTurn(jobToken, signal, deadline) {
  while (Date.now() < deadline) {
    if (signal?.aborted) throw new Error('REQUEST_CANCELLED');
    const claimed = await evalRedis(CLAIM_SCRIPT, [ACTIVE_KEY, WAITING_KEY], [jobToken, LEASE_MS]);
    if (Number(claimed) === 1) return;
    await new Promise((resolve, reject) => {
      let settled = false;
      const finish = (fn, value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
        fn(value);
      };
      const onAbort = () => finish(reject, new Error('REQUEST_CANCELLED'));
      const timer = setTimeout(() => finish(resolve), 1500);
      if (signal) {
        if (signal.aborted) onAbort();
        else signal.addEventListener('abort', onAbort, { once: true });
      }
      timer.unref?.();
    });
  }
  throw new Error('QUEUE_WAIT_TIMEOUT');
}

function createEncodingStream(inputUrl, preset, jobToken, admissionState, requestSignal) {
  let child = null;
  let stderr = '';
  let finished = false;
  let cancelled = false;
  let heartbeat = null;
  let jobTimer = null;
  const requestDeadline = Date.now() + MAX_JOB_MS;
  let cleanupPromise;
  let resolveCleanup;
  cleanupPromise = new Promise(resolve => { resolveCleanup = resolve; });

  const cleanup = async () => {
    if (finished) return;
    finished = true;
    clearInterval(heartbeat);
    clearTimeout(jobTimer);
    requestSignal?.removeEventListener('abort', abortJob);
    try { await evalRedis(RELEASE_SCRIPT, [ACTIVE_KEY, WAITING_KEY], [jobToken]); }
    catch (error) { console.error('Queue release failed:', error.message); }
    resolveCleanup();
  };

  const abortJob = () => {
    cancelled = true;
    if (child && !finished) child.kill('SIGTERM');
    else void cleanup();
  };

  return new ReadableStream({
    start(controller) {
      requestSignal?.addEventListener('abort', abortJob, { once: true });
      if (requestSignal?.aborted) abortJob();

      // Do not await queue position here: return response headers immediately.
      // The response body remains open while this request waits for its FIFO turn.
      void (async () => {
        try {
          if (cancelled) return;
          if (admissionState === 'WAIT') await waitForTurn(jobToken, requestSignal, requestDeadline);
          if (cancelled) return;

          heartbeat = setInterval(async () => {
            try {
              const renewed = await evalRedis(RENEW_SCRIPT, [ACTIVE_KEY], [jobToken, LEASE_MS]);
              if (Number(renewed) !== 1) abortJob();
            } catch (error) {
              console.error('Queue lease renewal failed:', error.message);
              abortJob();
            }
          }, RENEW_EVERY_MS);
          heartbeat.unref?.();
          jobTimer = setTimeout(abortJob, Math.max(1, requestDeadline - Date.now()));

          child = spawn(ffmpegPath, ffmpegArgs(inputUrl, preset), { stdio: ['ignore', 'pipe', 'pipe'] });
          child.stderr.setEncoding('utf8');
          child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-12000); });
          child.stdout.on('data', chunk => {
            try { controller.enqueue(new Uint8Array(chunk)); }
            catch { abortJob(); }
          });
          child.on('error', async error => {
            await cleanup();
            try { controller.error(error); } catch {}
          });
          child.on('close', async code => {
            await cleanup();
            if (code === 0 && !cancelled) controller.close();
            else {
              try { controller.error(new Error(`FFmpeg exited with code ${code}: ${stderr.slice(-3000)}`)); } catch {}
            }
          });
        } catch (error) {
          await cleanup();
          try { controller.error(error); } catch {}
        }
      })();
    },
    async cancel() {
      abortJob();
      if (child && !finished) child.kill('SIGTERM');
      await cleanupPromise;
    }
  });
}

export default async function handler(req) {
  const url = new URL(req.url);
  if (req.method === 'GET' && url.pathname === '/health') {
    return json({ ok: true, service: 'reusable-encoding-server', ffmpeg: Boolean(ffmpegPath), queueConfigured: Boolean(REDIS_URL && REDIS_TOKEN), maxActiveJobs: 1, maxWaitingJobs: MAX_WAITING });
  }
  if (req.method === 'GET' && url.pathname === '/v1/presets') {
    return json({ presets: Object.values(PRESETS).map(({ id, name, codec, crf, height, audioBitrate }) => ({ id, name, codec, crf, height, audioBitrate })) });
  }
  if (!authOk(req)) return json({ error: 'Unauthorized. Configure ENCODING_API_KEY and send it as a Bearer token.' }, 401);

  if (req.method === 'GET' && url.pathname === '/v1/queue') {
    try { return json({ ...await queueStatus(), maxActive: 1, maxWaiting: MAX_WAITING, maxTotal: MAX_WAITING + 1 }); }
    catch (error) { return json({ error: error.message }, 503); }
  }
  if (url.pathname !== '/v1/encode' || req.method !== 'POST') return json({ error: 'Not found' }, 404);
  if (!ffmpegPath) return json({ error: 'FFmpeg binary is unavailable in this deployment.' }, 500);

  let body;
  try { body = await req.json(); } catch { return json({ error: 'Request body must be valid JSON.' }, 400); }
  const { inputUrl, presetId } = body || {};
  if (typeof inputUrl !== 'string' || !isPublicHttpsUrl(inputUrl)) return json({ error: 'inputUrl must be a public HTTPS URL.' }, 400);
  const preset = PRESETS[presetId];
  if (!preset) return json({ error: 'Unknown presetId.', availablePresets: Object.keys(PRESETS) }, 400);

  const jobToken = randomUUID();
  let admission;
  try { admission = await admitQueue(jobToken); }
  catch (error) {
    console.error('Queue admission failed:', error);
    return json({ error: 'Queue service unavailable.', detail: error.message }, 503);
  }
  if (admission.state === 'FULL') return json({ error: 'Encoding queue is full. One job is active and eight users are already waiting.', code: 'QUEUE_FULL', maxActive: 1, maxWaiting: MAX_WAITING }, 429, { 'retry-after': '30' });

  const stream = createEncodingStream(inputUrl, preset, jobToken, admission.state, req.signal);

  return new Response(stream, {
    status: 200,
    headers: {
      'content-type': 'video/mp4',
      'content-disposition': `attachment; filename="${preset.id}.mp4"`,
      'cache-control': 'no-store, no-transform',
      'x-encoding-job-id': jobToken,
      'x-encoding-preset': preset.id,
      'x-video-codec': 'h264',
      'x-video-crf': String(preset.crf),
      'x-video-height': String(preset.height),
      'x-queue-policy': '1-active-8-waiting',
      'x-queue-status': admission.state.toLowerCase(),
      'x-queue-position': String(admission.position)
    }
  });
    }
