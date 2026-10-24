import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { assemble, buildRequest, openingTurns, parseArgs, planCuts } from '../voices.mjs';

const turns = [
  { id: 'opening-1', speaker: 'Narrator', text: '[measured] Begin.', start: 2, end: 5 },
  { id: 'interruption-1', speaker: 'Kyle', text: '[interrupting] Wait.', start: 6, end: 8 },
];
const cast = { Narrator: 'narrator-voice', Kyle: 'kyle-voice' };
const segment = (index, start, end, voice = cast[turns[index]?.speaker]) => ({ dialogue_input_index: index, start_time_seconds: start, end_time_seconds: end, voice_id: voice });

test('canonical opening selects nine indexed inputs and preserves emotion tags', async () => {
  const selected = openingTurns(await readFile(new URL('../../../agent-docs/video/script.md', import.meta.url), 'utf8'));
  assert.equal(selected.length, 9); assert.equal(new Set(selected.map(turn => turn.speaker)).size, 7);
  assert.equal(selected[0].id, 'opening-1'); assert.equal(selected.at(-1).id, 'interruption-8');
  assert.equal(selected.reduce((sum, turn) => sum + turn.text.length, 0), 936);
  const request = buildRequest(turns, cast);
  assert.equal(request.model_id, 'eleven_v4');
  assert.deepEqual(request.inputs, [{ text: turns[0].text, voice_id: cast.Narrator }, { text: turns[1].text, voice_id: cast.Kyle }]);
  assert.throws(() => buildRequest(turns, { Narrator: cast.Narrator }), /Kyle/);
});

test('cuts use input index despite reordered and split segments, preserving internal pauses', () => {
  const cuts = planCuts(turns, cast, { voice_segments: [segment(1, 4, 5), segment(0, 1.5, 2), segment(0, 0, 1)] }, 5);
  assert.deepEqual(cuts[0], { id: 'opening-1', start: 2, end: 5, duration: 2, parts: [{ start: 0, end: 1, offset: 0 }, { start: 1.5, end: 2, offset: 1.5 }] });
  assert.equal(cuts[1].id, 'interruption-1'); assert.equal(cuts[1].duration, 1);
});

test('missing, wrong-index and wrong-voice metadata cannot map a take to the wrong person', () => {
  for (const segments of [[segment(0, 0, 1)], [segment(0, 0, 1), segment(2, 1, 2, 'other')], [segment(0, 0, 1), segment(1, 1, 2, cast.Narrator)], [segment(0, 0, 1), { ...segment(1, 1, 2), dialogue_input_index: 1.5 }]]) {
    assert.throws(() => planCuts(turns, cast, { voice_segments: segments }), /input|voice|missing/i);
  }
});

test('nonfinite, reversed, overlapping and beyond-audio timestamps fail explicitly', () => {
  for (const [start, end] of [[NaN, 1], [0, Infinity], [-1, 1], [2, 1], [1, 1]]) {
    assert.throws(() => planCuts(turns, cast, { voice_segments: [segment(0, start, end), segment(1, 3, 4)] }), /time/i);
  }
  assert.throws(() => planCuts(turns, cast, { voice_segments: [segment(0, 0, 2), segment(1, 1, 3)] }), /overlap/i);
  assert.throws(() => planCuts(turns, cast, { voice_segments: [segment(0, 0, 1), segment(1, 1, 2)] }, 1), /audio/i);
});

test('interleaved inputs are rejected instead of including foreign speech in a line span', () => {
  assert.throws(() => planCuts(turns, cast, { voice_segments: [segment(0, 0, 1), segment(1, 1, 2), segment(0, 2, 3)] }), /interleav/i);
});

test('a line that exceeds its cue window is reported rather than truncated or sped up', () => {
  assert.throws(() => planCuts(turns, cast, { voice_segments: [segment(0, 0, 3.01), segment(1, 4, 5)] }), /opening-1.*3\.010.*3\.000/i);
});

test('assembly is local by default and authenticated commands require explicit selection', () => {
  assert.equal(parseArgs(['--help']).help, true);
  assert.equal(parseArgs(['assemble', '--cast', 'cast.json', '--response', 'response.json', '--audio', 'take.mp3']).command, 'assemble');
  assert.equal(parseArgs(['generate', '--cast', 'cast.json']).model, 'eleven_v4');
  for (const args of [[], ['generate'], ['assemble', '--cast', 'cast.json'], ['assemble', '--cast', 'x', '--response', 'r', '--unknown', 'x'], ['generate', '--cast', 'x', '--cast', 'y']]) assert.throws(() => parseArgs(args));
});

test('generation rejects a nonempty output directory before writing a request or authenticating', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'gdl-voices-'));
  try {
    const out = join(directory, 'output'), castPath = join(directory, 'cast.json');
    await mkdir(out); await writeFile(join(out, 'response.json'), 'preserve this response');
    const selected = openingTurns(await readFile(new URL('../../../agent-docs/video/script.md', import.meta.url), 'utf8'));
    await writeFile(castPath, JSON.stringify(Object.fromEntries(selected.map(turn => [turn.speaker, 'test-voice']))));
    await assert.rejects(promisify(execFile)(process.execPath, [fileURLToPath(new URL('../voices.mjs', import.meta.url)), 'generate', '--cast', castPath, '--out', out], { env: {}, timeout: 5000 }), error => /output directory must be empty/i.test(error.stderr));
    await assert.rejects(stat(join(out, 'request.json')), { code: 'ENOENT' });
    assert.equal(await readFile(join(out, 'response.json'), 'utf8'), 'preserve this response');
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('actual PCM preserves split-line pauses and aligns takes without trimming or stretching', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'gdl-voices-'));
  try {
    const audio = join(directory, 'raw.wav'), out = join(directory, 'aligned');
    const frames = 2 * 48000, bytes = Buffer.alloc(44 + frames * 4);
    bytes.write('RIFF'); bytes.writeUInt32LE(bytes.length - 8, 4); bytes.write('WAVEfmt ', 8);
    bytes.writeUInt32LE(16, 16); bytes.writeUInt16LE(1, 20); bytes.writeUInt16LE(2, 22);
    bytes.writeUInt32LE(48000, 24); bytes.writeUInt32LE(192000, 28); bytes.writeUInt16LE(4, 32); bytes.writeUInt16LE(16, 34);
    bytes.write('data', 36); bytes.writeUInt32LE(frames * 4, 40);
    for (let index = 44; index < bytes.length; index += 2) bytes.writeInt16LE(8192, index);
    await writeFile(audio, bytes);
    await assemble({ turns, cast, audio, out, response: { voice_segments: [segment(1, 1, 1.5), segment(0, .5, .75), segment(0, 0, .25)] } });
    const take = await samples(join(out, 'opening-1.wav')), full = await samples(join(out, 'opening-voice.wav'));
    assert.equal(take.frames, .75 * 48000); assert.equal(full.frames, 80 * 48000);
    for (const time of [2.1, 2.6, 6.1]) assert.equal(full.at(time), .25);
    for (const time of [1.9, 2.4, 2.8, 5.9, 6.6, 79.9]) assert.equal(full.at(time), 0);
    assert.deepEqual(await readFile(join(out, 'source.wav')), bytes);
    const timeline = JSON.parse(await readFile(join(out, 'timeline.json'), 'utf8'));
    assert.equal(timeline.timeStretch, false); assert.equal(timeline.tailSilence, 0);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

async function samples(path) {
  const bytes = await readFile(path);
  for (let position = 12; position + 8 <= bytes.length;) {
    const size = bytes.readUInt32LE(position + 4);
    if (bytes.toString('ascii', position, position + 4) === 'data') {
      const data = bytes.subarray(position + 8, position + 8 + size);
      return { frames: data.length / 4, at: time => data.readInt16LE(Math.round(time * 48000) * 4) / 32768 };
    }
    position += 8 + size + size % 2;
  }
  throw new Error('Missing PCM data.');
}
