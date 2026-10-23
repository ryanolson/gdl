#!/usr/bin/env node
import { copyFile, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { constants } from 'node:fs';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import ffmpeg from 'ffmpeg-static';
import { parseScript } from './dist/cues.mjs';

const root = dirname(fileURLToPath(import.meta.url));
const execute = promisify(execFile);
const endpoint = 'https://api.elevenlabs.io';
const tailAllowance = .12;

export function openingTurns(markdown) {
  return parseScript(markdown).filter(scene => scene.end <= 80).flatMap(scene => scene.turns);
}

export function buildRequest(turns, cast, model = 'eleven_v4') {
  if (!cast || typeof cast !== 'object' || Array.isArray(cast)) throw new Error('cast must map speaker names to voice IDs.');
  const inputs = turns.map(turn => {
    const voice = cast[turn.speaker];
    if (typeof voice !== 'string' || !/^[\w-]+$/.test(voice)) throw new Error(`Missing or invalid voice ID for ${turn.speaker}.`);
    return { text: turn.text, voice_id: voice };
  });
  if (new Set(inputs.map(input => input.voice_id)).size > 10) throw new Error('Dialogue supports at most ten voices per request.');
  return { inputs, model_id: model };
}

export function planCuts(turns, cast, response, audioDuration = Infinity) {
  buildRequest(turns, cast);
  if (!Array.isArray(response?.voice_segments)) throw new Error('Response needs voice_segments.');
  const groups = turns.map(() => []);
  const ordered = [...response.voice_segments].sort((a, b) => a.start_time_seconds - b.start_time_seconds);
  ordered.forEach((segment, index) => {
    const input = segment.dialogue_input_index, start = segment.start_time_seconds, end = segment.end_time_seconds;
    if (!Number.isInteger(input) || !turns[input]) throw new Error(`Unknown dialogue input index: ${input}.`);
    if (segment.voice_id !== cast[turns[input].speaker]) throw new Error(`Voice mismatch for input ${input} (${turns[input].id}).`);
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end <= start) throw new Error(`Invalid timestamps for input ${input}.`);
    if (end > audioDuration + tailAllowance) throw new Error(`Input ${input} ends at ${end.toFixed(3)}s beyond decoded audio ${audioDuration.toFixed(3)}s.`);
    if (index && start < ordered[index - 1].end_time_seconds - 1e-6) throw new Error('Overlapping voice segments cannot be isolated from the mixed source.');
    if (groups[input].length && ordered[index - 1].dialogue_input_index !== input) throw new Error(`Interleaved segments for input ${input} require separate source stems.`);
    groups[input].push({ start, end });
  });
  return turns.map((turn, index) => {
    const parts = groups[index];
    if (!parts.length) throw new Error(`Missing segments for input ${index} (${turn.id}).`);
    const first = parts[0].start, duration = parts.at(-1).end - first, window = turn.end - turn.start;
    if (duration > window + 1e-6) throw new Error(`${turn.id}: take ${duration.toFixed(3)}s exceeds cue window ${window.toFixed(3)}s; retime or regenerate.`);
    return { id: turn.id, start: turn.start, end: turn.end, duration, parts: parts.map(part => ({ ...part, offset: part.start - first })) };
  });
}

export function parseArgs(args) {
  if (args.includes('--help') || args.includes('-h')) return { help: true };
  const command = args[0], options = { command, model: 'eleven_v4', out: resolve(root, 'work/eleven-opening') };
  if (!['plan', 'preflight', 'list', 'generate', 'assemble'].includes(command)) throw new Error('Choose plan, preflight, list, generate, or assemble. Use --help.');
  const seen = new Set();
  for (let index = 1; index < args.length; index++) {
    const flag = args[index], name = flag.replace(/^--/, ''), value = args[++index];
    if (!flag.startsWith('--') || !['cast', 'out', 'model', 'response', 'audio'].includes(name) || seen.has(name)) throw new Error(`Invalid or duplicate option: ${flag}.`);
    if (!value || value.startsWith('--')) throw new Error(`${flag} needs a value.`);
    seen.add(name); options[name] = value;
  }
  if (['plan', 'generate', 'assemble'].includes(command) && !options.cast) throw new Error('--cast must name a speaker-to-voice JSON map.');
  if (command === 'assemble' && !options.response) throw new Error('--response is required for offline assembly.');
  return options;
}

async function api(path, body) {
  const key = process.env.ELEVENLABS_API_KEY;
  if (!key) throw new Error('Inject ELEVENLABS_API_KEY with openv before an authenticated command.');
  const response = await fetch(`${endpoint}${path}`, {
    method: body ? 'POST' : 'GET', headers: { 'xi-api-key': key, ...(body ? { 'content-type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(180000),
  });
  if (!response.ok) throw new Error(`ElevenLabs returned HTTP ${response.status}; request was not retried.`);
  return response.json();
}

async function saveJson(path, value) { await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, { flag: 'wx' }); }
async function ff(args) {
  if (!ffmpeg) throw new Error('ffmpeg-static has no executable for this platform.');
  try { await execute(ffmpeg, ['-hide_banner', '-loglevel', 'error', '-nostdin', '-n', ...args], { timeout: 60000, maxBuffer: 262144 }); }
  catch (error) { throw new Error(error.stderr?.trim() || error.message); }
}

function wavDuration(bytes) {
  for (let position = 12; position + 8 <= bytes.length;) {
    const size = bytes.readUInt32LE(position + 4);
    if (bytes.toString('ascii', position, position + 4) === 'data') return size / (48000 * 4);
    position += 8 + size + size % 2;
  }
  throw new Error('Decoded WAV has no PCM data.');
}

export async function assemble({ turns, cast, response, audio, out }) {
  // Validate before writing; no line is shortened to make the editorial windows fit.
  planCuts(turns, cast, response);
  await mkdir(out, { recursive: true });
  const original = resolve(out, `source${audio ? extname(audio) || '.audio' : '.mp3'}`);
  if (audio) await copyFile(audio, original, constants.COPYFILE_EXCL);
  else {
    if (typeof response.audio_base64 !== 'string' || !response.audio_base64.length) throw new Error('Response has no audio_base64; supply --audio FILE.');
    await writeFile(original, Buffer.from(response.audio_base64, 'base64'), { flag: 'wx' });
  }
  const { audio_base64, ...metadata } = response;
  await saveJson(resolve(out, 'response.json'), metadata);
  await saveJson(resolve(out, 'turns.json'), turns);
  const decoded = resolve(out, 'decoded.wav');
  await ff(['-i', original, '-ar', '48000', '-ac', '2', '-c:a', 'pcm_s16le', decoded]);
  const sourceDuration = wavDuration(await readFile(decoded)), cuts = planCuts(turns, cast, response, sourceDuration);
  const last = Math.max(...cuts.flatMap(cut => cut.parts.map(part => part.end)));
  for (const cut of cuts) {
    const graph = cut.parts.map((part, i) => `[0:a]apad=whole_dur=${last},atrim=start=${part.start}:end=${part.end},asetpts=PTS-STARTPTS,adelay=${Math.round(part.offset * 48000)}S:all=1[p${i}]`);
    graph.push(`${cut.parts.map((_, i) => `[p${i}]`).join('')}amix=inputs=${cut.parts.length}:duration=longest:normalize=0[out]`);
    await ff(['-i', decoded, '-filter_complex', graph.join(';'), '-map', '[out]', '-ar', '48000', '-ac', '2', '-c:a', 'pcm_s16le', resolve(out, `${cut.id}.wav`)]);
  }
  const args = cuts.flatMap(cut => ['-i', resolve(out, `${cut.id}.wav`)]);
  const graph = cuts.map((cut, i) => `[${i}:a]adelay=${Math.round(cut.start * 48000)}S:all=1[t${i}]`);
  graph.push(`${cuts.map((_, i) => `[t${i}]`).join('')}amix=inputs=${cuts.length}:duration=longest:normalize=0,apad=whole_dur=80[out]`);
  await ff([...args, '-filter_complex', graph.join(';'), '-map', '[out]', '-ar', '48000', '-ac', '2', '-c:a', 'pcm_s16le', resolve(out, 'opening-voice.wav')]);
  await saveJson(resolve(out, 'timeline.json'), { sourceDuration, metadataEnd: last, tailSilence: Math.max(0, last - sourceDuration), timeStretch: false, cuts });
  await saveJson(resolve(out, 'media.json'), { voices: Object.fromEntries(cuts.map(cut => [cut.id, `${cut.id}.wav`])) });
  return cuts;
}

async function run(options) {
  if (options.command === 'preflight') { console.log(JSON.stringify(await api('/v1/models'), null, 2)); return; }
  if (options.command === 'list') {
    const voices = []; let token;
    do {
      const page = await api(`/v2/voices?page_size=100${token ? `&next_page_token=${encodeURIComponent(token)}` : ''}`);
      voices.push(...page.voices.map(({ voice_id, name, labels }) => ({ voice_id, name, labels })));
      token = page.has_more ? page.next_page_token : null;
      if (page.has_more && !token) throw new Error('Voice pagination did not provide its next token.');
    } while (token);
    console.log(JSON.stringify(voices, null, 2)); return;
  }
  const turns = openingTurns(await readFile(resolve(root, '../../agent-docs/video/script.md'), 'utf8'));
  const cast = JSON.parse(await readFile(options.cast, 'utf8')), request = buildRequest(turns, cast, options.model);
  if (options.command === 'plan') { console.log(JSON.stringify({ turns, request }, null, 2)); return; }
  let response;
  if (options.command === 'generate') {
    await mkdir(options.out, { recursive: true });
    if ((await readdir(options.out)).length) throw new Error('Generation output directory must be empty before using credits.');
    await saveJson(resolve(options.out, 'request.json'), request);
    response = await api('/v1/text-to-dialogue/with-timestamps?output_format=mp3_44100_128', request);
    const { audio_base64, ...metadata } = response;
    await saveJson(resolve(options.out, 'response.json'), metadata);
    await writeFile(resolve(options.out, 'dialogue.mp3'), Buffer.from(audio_base64, 'base64'), { flag: 'wx' });
    console.log(`Saved raw dialogue and timestamps to ${resolve(options.out)}. Run assemble with a separate --out directory.`); return;
  }
  response = JSON.parse(await readFile(options.response, 'utf8'));
  const cuts = await assemble({ turns, cast, response, audio: options.audio, out: resolve(options.out) });
  console.log(`Saved ${cuts.length} un-stretched takes and an 80s opening-voice.wav to ${resolve(options.out)}.`);
}

const help = `Produce the canonical nine-turn opening with ElevenLabs timestamped dialogue.
Usage: node tools/video/voices.mjs COMMAND [options]
  plan       Print the tagged inputs and cue mapping; no network
  preflight  GET the model list; requires models_read permission
  list       List available voices; requires voices_read permission
  generate   Use credits for one request; never retry automatically
  assemble   Split and align a saved response locally; no authentication
Options: --cast FILE --out DIR --model ID --response FILE --audio FILE
Default model: eleven_v4. Default output: tools/video/work/eleven-opening.
Cast JSON maps exact speaker names to chosen voice IDs. Inject credentials with
openv; only authenticated commands read ELEVENLABS_API_KEY. No key is saved.
Generate preserves raw MP3 and response metadata. Assemble accepts that MP3 via
--audio, or audio_base64 in a response. Output files are never overwritten.
Split segments retain their relative pauses; overlapping/interleaved voices fail.
Cue overflow fails, never truncates or stretches. Up to 120ms of timestamp tail
beyond decoded MP3 EOF is explicitly padded with silence in timeline.json.
`;

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { const options = parseArgs(process.argv.slice(2)); if (options.help) console.log(help); else await run(options); }
  catch (error) { console.error(`voices: ${error.message}`); process.exitCode = 1; }
}
