#!/usr/bin/env node
import { access, mkdir, rename, rm, stat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { basename, dirname, isAbsolute, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';

const videoRoot = dirname(fileURLToPath(import.meta.url));

export function parseArgs(args) {
  const options = { url: 'http://127.0.0.1:8088/', out: resolve(videoRoot, 'work/film.mp4'), from: 0, fps: 30, animatic: false, preflight: false };
  const values = new Set(['url', 'out', 'from', 'to', 'fps', 'audio', 'ffmpeg', 'playwright']);
  const flags = new Set(['help', 'preflight', 'animatic']);
  const seen = new Set();
  for (let index = 0; index < args.length; index += 1) {
    let token = args[index];
    if (token === '-h') token = '--help';
    const compact = token.match(/^(--(?:from|to|fps))(-?\d.*)$/);
    if (compact) token = `${compact[1]}=${compact[2]}`;
    const equal = token.indexOf('=');
    const name = (equal < 0 ? token : token.slice(0, equal)).replace(/^--/, '');
    if (!token.startsWith('--') || (!values.has(name) && !flags.has(name))) throw new Error(`Unknown option: ${token}. Use --help.`);
    if (seen.has(name)) throw new Error(`Option --${name} was supplied more than once.`);
    seen.add(name);
    if (flags.has(name)) {
      if (equal >= 0) throw new Error(`--${name} takes no value.`);
      options[name] = true;
      continue;
    }
    const value = equal >= 0 ? token.slice(equal + 1) : args[++index];
    if (!value || value.startsWith('--')) throw new Error(`--${name} needs a value.`);
    options[name] = ['from', 'to', 'fps'].includes(name) ? Number(value) : value;
  }
  if (options.help) return options;
  const url = new URL(options.url);
  if (!['http:', 'https:'].includes(url.protocol) || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) throw new Error('--url must use HTTP(S) on localhost or a loopback address.');
  if (url.username || url.password) throw new Error('--url must not contain credentials.');
  options.url = url.href;
  finite(options.from, '--from');
  positive(options.fps, '--fps');
  if (options.from < 0) throw new Error('--from must be nonnegative.');
  if (options.to !== undefined) {
    finite(options.to, '--to');
    if (options.to <= options.from) throw new Error('--to must be greater than --from.');
  }
  return options;
}

export function renderPlan(options, duration, missingFootage, archiveCues) {
  positive(duration, 'Film duration');
  const from = options.from;
  const to = options.to ?? duration;
  finite(from, '--from');
  finite(to, '--to');
  positive(options.fps, '--fps');
  if (from < 0 || from >= to || to > duration) throw new Error(`Capture range must satisfy 0 <= from < to <= ${duration}.`);
  if (!Array.isArray(missingFootage)) throw new Error('gdlFilm.getState().missingFootage must be an array.');
  const missing = missingFootage.filter((clip) => {
    if (!clip || !Number.isFinite(clip.start) || !Number.isFinite(clip.end) || clip.start < 0 || clip.end <= clip.start) throw new Error('Each missingFootage item must have finite start/end seconds with start < end.');
    return from < clip.end && to > clip.start;
  });
  if (missing.length && !options.animatic) throw new Error(`Recorded footage is missing for: ${missing.map((clip) => clip.id ?? `${clip.start}–${clip.end}s`).join(', ')}. Attach clips or use --animatic for an explicitly incomplete preview.`);
  if (!Array.isArray(archiveCues)) throw new Error('gdlFilm.getState().archiveCues must be an array.');
  const archive = archiveCues.filter((cue) => {
    if (!cue || typeof cue.id !== 'string' || !cue.id.trim() || !Number.isFinite(cue.start) || !Number.isFinite(cue.end) || cue.start < 0 || cue.end <= cue.start || cue.end > duration) throw new Error('Each archiveCues item must have an id and finite start/end seconds within the film, with start < end.');
    return from < cue.end && to > cue.start;
  });
  if (archive.length && !options.audio && !options.animatic) throw new Error(`Archive speech intersects this range: ${archive.map((cue) => cue.id).join(', ')}. Supply --audio with a full-film mix aligned to time zero, or use --animatic for a silent preview. Manifest voice takes are not embedded automatically.`);
  const frames = Math.max(1, Math.ceil((to - from) * options.fps - 1e-9));
  if (!Number.isSafeInteger(frames)) throw new Error('Capture range and frame rate produce too many frames.');
  return { from, to, fps: options.fps, frames, encodedDuration: frames / options.fps, missing };
}

export function ffmpegArgs(options, plan, output) {
  const args = ['-hide_banner', '-loglevel', 'error', '-y', '-f', 'image2pipe', '-framerate', String(plan.fps), '-i', 'pipe:0'];
  // The optional track starts at film time zero, so clip exports seek the same offset.
  if (options.audio) args.push('-ss', String(plan.from), '-i', options.audio);
  args.push('-map', '0:v:0');
  if (options.audio) args.push('-map', '1:a:0', '-af', 'apad', '-c:a', 'aac', '-b:a', '192k');
  args.push('-c:v', 'libx264', '-preset', 'medium', '-crf', '18', '-pix_fmt', 'yuv420p', '-frames:v', String(plan.frames), '-t', String(plan.encodedDuration), '-movflags', '+faststart', '-f', 'mp4', output);
  return args;
}

async function run(options) {
  if (options.audio) {
    options.audio = resolve(options.audio);
    try {
      await access(options.audio, constants.R_OK);
      if (!(await stat(options.audio)).isFile()) throw new Error('not a file');
    } catch {
      throw new Error(`Audio track is not a readable file: ${options.audio}. Supply a full-film track aligned to time zero.`);
    }
  }
  const ffmpeg = await findFfmpeg(options.ffmpeg);
  const chromium = await findChromium(options.playwright);
  const controller = new AbortController();
  const interrupt = () => controller.abort(new Error('Render interrupted.'));
  process.once('SIGINT', interrupt);
  process.once('SIGTERM', interrupt);
  let browser;
  let encoder;
  let partial;
  try {
    try {
      browser = await guard(chromium.launch({ channel: 'chrome', headless: true }), 30000, 'Launching installed Google Chrome', controller.signal);
    } catch (error) {
      throw new Error(`Cannot launch installed Google Chrome: ${error.message}. Install Chrome or check the selected Playwright module.`);
    }
    const context = await browser.newContext({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1, reducedMotion: 'reduce' });
    const page = await context.newPage();
    page.setDefaultTimeout(15000);
    const response = await guard(page.goto(options.url, { waitUntil: 'load', timeout: 30000 }), 35000, 'Loading local film stage', controller.signal);
    if (!response?.ok()) throw new Error(`Film stage returned HTTP ${response?.status() ?? 'no response'} at ${options.url}. Start the local video server first.`);
    const setup = await guard(page.evaluate(async () => {
      const film = window.gdlFilm;
      if (!film) throw new Error('window.gdlFilm is missing.');
      await film.ready;
      for (const method of ['seek', 'pause', 'setCapture', 'setFootageMode', 'getState']) {
        if (typeof film[method] !== 'function') throw new Error(`gdlFilm.${method} is missing.`);
      }
      await film.pause();
      await film.setCapture(true);
      await film.setFootageMode('recorded');
      await document.fonts.ready;
      return { duration: film.duration, state: film.getState() };
    }), 30000, 'Waiting for gdlFilm.ready', controller.signal);
    const plan = renderPlan(options, setup.duration, setup.state?.missingFootage, setup.state?.archiveCues);
    const stage = page.locator('#film-stage');
    await stage.waitFor({ state: 'visible', timeout: 15000 });
    const box = await stage.boundingBox();
    if (!box || Math.abs(box.width - 1920) > 0.1 || Math.abs(box.height - 1080) > 0.1) throw new Error(`Capture stage must be 1920×1080 after setCapture(true); got ${box ? `${box.width}×${box.height}` : 'no visible stage'}.`);
    console.log(`Preflight OK: Chrome, ${ffmpeg.version}; stage 1920×1080; ${plan.frames} frames at ${plan.fps} fps; ${plan.from}–${plan.to}s.`);
    console.log(options.audio ? `Audio: full-film track, seek ${plan.from}s; short tracks are padded with silence.` : 'Audio: none. Supply --audio with a full-film track aligned to film time zero.');
    if (plan.missing.length) console.warn(`ANIMATIC: ${plan.missing.length} missing recorded demo clip(s) will remain placeholders.`);
    if (options.preflight) return;
    const output = resolve(options.out);
    await mkdir(dirname(output), { recursive: true });
    partial = resolve(dirname(output), `.${basename(output)}.${randomUUID()}.partial.mp4`);
    encoder = startEncoder(ffmpeg.path, ffmpegArgs(options, plan, partial));
    const progressEvery = Math.max(1, Math.round(plan.fps * 5));
    for (let frame = 0; frame < plan.frames; frame += 1) {
      if (controller.signal.aborted) throw controller.signal.reason;
      if (encoder.status) throw new Error(`ffmpeg exited before frame ${frame}: ${encoder.status.error?.message ?? encoder.status.code}.`);
      const time = plan.from + frame / plan.fps;
      await guard(page.evaluate(async (seconds) => { await window.gdlFilm.seek(seconds); }, time), 15000, `Seeking frame ${frame}`, controller.signal);
      const png = await guard(stage.screenshot({ type: 'png', animations: 'disabled', caret: 'hide', timeout: 15000 }), 20000, `Capturing frame ${frame}`, controller.signal);
      await guard(new Promise((accept, reject) => {
        encoder.child.stdin.write(png, (error) => error ? reject(error) : accept());
      }), 30000, `Streaming frame ${frame} to ffmpeg`, controller.signal);
      if (frame % progressEvery === 0) console.log(`Frame ${frame + 1}/${plan.frames} (${time.toFixed(2)}s)`);
    }
    encoder.child.stdin.end();
    const status = await guard(encoder.done, 120000, 'Finishing MP4 encoding', controller.signal);
    if (status.error || status.code !== 0) throw new Error(`ffmpeg failed: ${status.error?.message ?? `exit ${status.code}, signal ${status.signal ?? 'none'}`}.`);
    await rename(partial, output);
    console.log(`Saved ${output} (${plan.encodedDuration.toFixed(3)}s, ${plan.frames} frames).`);
  } catch (error) {
    const detail = encoder?.stderr.trim();
    throw new Error(detail ? `${error.message}\nffmpeg: ${detail}` : error.message);
  } finally {
    if (encoder && !encoder.status) {
      encoder.child.kill('SIGTERM');
      try {
        await guard(encoder.done, 5000, 'Stopping ffmpeg');
      } catch {
        encoder.child.kill('SIGKILL');
        await guard(encoder.done, 5000, 'Killing ffmpeg').catch(() => {});
      }
    }
    if (browser) await guard(browser.close(), 15000, 'Closing Chrome').catch((error) => console.error(error.message));
    if (partial) await rm(partial, { force: true });
    process.removeListener('SIGINT', interrupt);
    process.removeListener('SIGTERM', interrupt);
  }
}

async function findFfmpeg(override) {
  let path = override ?? process.env.FFMPEG_PATH;
  if (!path) {
    try { path = (await import('ffmpeg-static')).default; } catch { /* A PATH executable is also supported. */ }
  }
  path ||= 'ffmpeg';
  try {
    const { stdout } = await promisify(execFile)(path, ['-version'], { timeout: 10000, maxBuffer: 131072 });
    return { path, version: stdout.split('\n')[0] };
  } catch (error) {
    throw new Error(`ffmpeg is unavailable (${path}): ${error.message}. Add ffmpeg-static to tools/video dependencies, put ffmpeg on PATH, or set --ffmpeg /absolute/path (FFMPEG_PATH).`);
  }
}

async function findChromium(override) {
  const modulePath = override ?? process.env.PLAYWRIGHT_MODULE_PATH;
  if (modulePath && !isAbsolute(modulePath)) throw new Error('--playwright / PLAYWRIGHT_MODULE_PATH must be an absolute module path.');
  const candidates = modulePath ? [pathToFileURL(modulePath).href] : ['playwright', 'playwright-core'];
  for (const candidate of candidates) {
    try {
      const module = await import(candidate);
      const chromium = module.chromium ?? module.default?.chromium;
      if (chromium) return chromium;
    } catch { /* Try the next local package before reporting prerequisites. */ }
  }
  throw new Error('Playwright is unavailable. Add playwright to tools/video dependencies or set --playwright /absolute/path/to/playwright/index.js (PLAYWRIGHT_MODULE_PATH). The renderer uses installed Google Chrome; browser downloads are unnecessary.');
}

function startEncoder(path, args) {
  const child = spawn(path, args, { stdio: ['pipe', 'ignore', 'pipe'] });
  const encoder = { child, status: null, stderr: '', done: null };
  let spawnError;
  child.stderr.on('data', (data) => { encoder.stderr = (encoder.stderr + data.toString()).slice(-16384); });
  child.stdin.on('error', () => {});
  child.on('error', (error) => { spawnError = error; });
  encoder.done = new Promise((accept) => child.once('close', (code, signal) => {
    encoder.status = { code, signal, error: spawnError };
    accept(encoder.status);
  }));
  return encoder;
}

function guard(promise, milliseconds, label, signal) {
  return new Promise((accept, reject) => {
    const timer = setTimeout(() => finish(new Error(`${label} timed out after ${milliseconds / 1000}s.`)), milliseconds);
    const abort = () => finish(signal.reason ?? new Error('Render interrupted.'));
    const finish = (error, value) => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      if (error) reject(error); else accept(value);
    };
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    Promise.resolve(promise).then((value) => finish(null, value), (error) => finish(error));
  });
}

function finite(value, label) {
  if (!Number.isFinite(value)) throw new Error(`${label} must be finite.`);
}

function positive(value, label) {
  finite(value, label);
  if (value <= 0) throw new Error(`${label} must be positive.`);
}

const help = `Render a deterministic local GDL film stage to MP4.

Usage: node tools/video/render.mjs [options]
  --url URL          Local film stage (default http://127.0.0.1:8088/)
  --out PATH         MP4 output (default module-local tools/video/work/film.mp4)
  --from SECONDS     First film time (default 0)
  --to SECONDS       Exclusive end time (default film duration)
  --fps NUMBER       Frames per second (default 30)
  --audio PATH       Full-film audio track aligned to time zero; clip offset is applied
  --ffmpeg PATH      ffmpeg executable override (also FFMPEG_PATH)
  --playwright PATH  Absolute Playwright module path (also PLAYWRIGHT_MODULE_PATH)
  --animatic         Allow missing demo clips and silent archive speech
  --preflight        Check dependencies, local stage and clip coverage without rendering
  --help             Show this help

Frames stream directly to ffmpeg; no frame directory is written. Rendering uses
one headless installed Google Chrome at 1920×1080, recorded footage mode, exact
frame seeks, H.264/yuv420p and MP4 faststart. Existing output is replaced only
after a complete encode. The audio track is trimmed to the same film range,
padded with silence if short, and encoded as AAC; it is not an audio generator.
Live composer iframes are never exported. Attach demo clips before final export.
Archive speech requires --audio; manifest voice takes support browser preview.

Example:
  node tools/video/render.mjs --from 0 --to 55 --fps 30 --out tools/video/work/opening.mp4
`;

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const options = parseArgs(process.argv.slice(2));
    if (options.help) console.log(help); else await run(options);
  } catch (error) {
    console.error(`render: ${error.message}`);
    process.exitCode = 1;
  }
}
