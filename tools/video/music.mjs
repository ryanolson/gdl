#!/usr/bin/env node
import { access, mkdir, rename, rm, stat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { dirname, resolve, basename } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import ffmpeg from 'ffmpeg-static';

const videoRoot = dirname(fileURLToPath(import.meta.url));
const numeric = new Map([['duration', 'duration'], ['gain', 'gain'], ['voice-at', 'voiceAt'], ['music-start', 'musicStart']]);

export function parseArgs(args) {
  const options = { duration: 80, gain: 1, musicStart: 0, voiceAt: 0, out: resolve(videoRoot, 'work/opening.wav') };
  const seen = new Set();
  for (let index = 0; index < args.length; index++) {
    const token = args[index] === '-h' ? '--help' : args[index];
    if (token === '--help') return { help: true };
    const name = token.replace(/^--/, '');
    if (!token.startsWith('--') || !['music', 'voice', 'out', ...numeric.keys()].includes(name)) throw new Error(`Unknown option: ${token}. Use --help.`);
    if (seen.has(name)) throw new Error(`Duplicate option: ${token}.`);
    seen.add(name);
    const value = args[++index];
    if (!value || value.startsWith('--')) throw new Error(`${token} needs a value.`);
    options[numeric.get(name) ?? name] = numeric.has(name) ? Number(value) : value;
  }
  if (!options.music) throw new Error('--music needs a local audio file.');
  for (const name of ['duration', 'gain', 'musicStart', 'voiceAt']) if (!Number.isFinite(options[name])) throw new Error(`${name} must be finite.`);
  if (options.duration <= 0 || !Number.isSafeInteger(Math.round(options.duration * 48000)) || Math.round(options.duration * 48000) < 1) throw new Error('duration must contain a positive, safe number of audio samples.');
  if (options.gain < 0 || options.gain > 1) throw new Error('gain must be between 0 and 1.');
  if (options.musicStart < 0 || options.voiceAt < 0) throw new Error('music-start and voice-at must be nonnegative.');
  if (!options.voice && seen.has('voice-at')) throw new Error('--voice-at requires --voice.');
  if (options.voice && options.voiceAt >= options.duration) throw new Error('voice-at must be before the output ends.');
  if (!options.out.toLowerCase().endsWith('.wav')) throw new Error('--out must name a WAV file.');
  return options;
}

export function mixArgs(options, output) {
  const args = ['-hide_banner', '-loglevel', 'error', '-nostdin', '-y', '-ss', String(options.musicStart), '-i', options.music];
  if (options.voice) args.push('-i', options.voice);
  const format = 'asetpts=PTS-STARTPTS,aresample=48000,aformat=channel_layouts=stereo';
  const length = `apad=whole_dur=${options.duration},atrim=end=${options.duration}`;
  const graph = [`[0:a]${format},volume=${options.gain},afade=t=out:st=1:d=1:silence=0.12:unity=1,afade=t=out:st=23.995:d=0.005,atrim=end=24,${length}[music]`];
  let source = '[music]';
  if (options.voice) {
    graph.push(`[1:a]${format},adelay=${Math.round(options.voiceAt * 48000)}S:all=1,${length}[voice]`);
    graph.push('[music][voice]amix=inputs=2:duration=longest:normalize=0[mix]'); source = '[mix]';
  }
  graph.push(`${source}alimiter=limit=0.95:level=0:latency=1,${length}[out]`);
  args.push('-filter_complex', graph.join(';'), '-map', '[out]', '-ar', '48000', '-ac', '2', '-c:a', 'pcm_s16le', '-f', 'wav', output);
  return args;
}

async function run(options) {
  const inputs = [options.music, ...(options.voice ? [options.voice] : [])].map(path => resolve(path));
  const inputStats = await Promise.all(inputs.map(async path => {
    try {
      await access(path, constants.R_OK);
      const info = await stat(path);
      if (!info.isFile()) throw new Error('not a file');
      return info;
    } catch { throw new Error(`Input must be a readable local file: ${path}`); }
  }));
  const output = resolve(options.out);
  const current = await stat(output).catch(error => { if (error.code === 'ENOENT') return null; throw error; });
  if (current && inputStats.some(info => info.dev === current.dev && info.ino === current.ino)) throw new Error('Output must not overwrite an input, including an alias.');
  if (current && !current.isFile()) throw new Error('Output must name a WAV file, not a directory.');
  options.music = inputs[0]; if (options.voice) options.voice = inputs[1];
  await mkdir(dirname(output), { recursive: true });
  const partial = resolve(dirname(output), `.${basename(output)}.${randomUUID()}.partial.wav`);
  const controller = new AbortController();
  const interrupt = () => controller.abort();
  process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);
  try {
    if (!ffmpeg) throw new Error('ffmpeg-static has no executable for this platform.');
    await promisify(execFile)(ffmpeg, mixArgs(options, partial), { timeout: 300000, maxBuffer: 262144, killSignal: 'SIGKILL', signal: controller.signal });
    await rename(partial, output);
    console.log(`Saved ${output}: ${options.duration}s, 48 kHz stereo PCM. Music cuts at 24s; voice timing is independent.`);
  } catch (error) {
    throw new Error(error.stderr?.trim() || error.message);
  } finally {
    await rm(partial, { force: true });
    process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
  }
}

const help = `Mix local music and an optional voice stem into a film-aligned WAV.

Usage: node tools/video/music.mjs --music FILE [options]
  --out FILE          Output WAV (default tools/video/work/opening.wav)
  --duration SECONDS  Output length (default 80; use 505 or 512 for a full film)
  --music-start SEC   Skip an input preface (default 0)
  --gain NUMBER       Music source gain, 0 through 1 (default 1)
  --voice FILE        Optional local voice stem
  --voice-at SEC      Voice offset in film time (default 0; opening narrator starts at 2)
  --help              Show this help

The music holds source level for 0–1s, fades to 12% during 1–2s, then stays
under narration. A 5ms declick fade ends exactly at Kyle's 24s interruption.
The rest is silence unless a voice stem is supplied. Short inputs are padded,
never looped. The final limiter caps peaks at 0.95 without automatic boosting.
Output is 48kHz stereo 16-bit PCM, aligned to film time zero for render --audio.
The mixer reads local files only. It does not download music or generate voices.
`;

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { const options = parseArgs(process.argv.slice(2)); if (options.help) console.log(help); else await run(options); }
  catch (error) { console.error(`music: ${error.message}`); process.exitCode = 1; }
}
