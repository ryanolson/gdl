import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs, mixArgs } from '../music.mjs';

const execute = promisify(execFile);
const cli = fileURLToPath(new URL('../music.mjs', import.meta.url));
let directory, music, voice;

before(async () => {
  directory = await mkdtemp(join(tmpdir(), 'gdl-music-'));
  music = join(directory, 'music with preface.wav'); voice = join(directory, 'voice.wav');
  await writeFile(music, wave(26, time => time < 1 ? .25 : .5));
  await writeFile(voice, wave(26, () => .1));
});
after(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

test('music arguments preserve local paths and reject invalid envelope inputs', () => {
  const defaults = parseArgs(['--music', 'local score.wav']);
  assert.equal(defaults.duration, 80); assert.equal(defaults.gain, 1); assert.equal(defaults.musicStart, 0);
  assert.equal(defaults.out, fileURLToPath(new URL('../work/opening.wav', import.meta.url)));
  const options = parseArgs(['--music', 'local score.wav', '--music-start', '1', '--voice', 'voice.wav', '--voice-at', '2', '--duration', '505', '--gain', '.5', '--out', 'mix.wav']);
  assert.equal(options.musicStart, 1); assert.equal(options.voiceAt, 2); assert.equal(options.duration, 505);
  assert.equal(parseArgs(['--help']).help, true);
  for (const args of [[], ['--music'], ['--music', 'x', '--gain', '2'], ['--music', 'x', '--gain', '-1'], ['--music', 'x', '--duration', 'NaN'], ['--music', 'x', '--duration', '0'], ['--music', 'x', '--music-start', '-1'], ['--music', 'x', '--voice-at', '2'], ['--music', 'x', '--voice', 'y', '--voice-at', '-1'], ['--music', 'x', '--out', 'mix.mp3'], ['--music', 'x', '--music', 'y']]) {
    assert.throws(() => parseArgs(args));
  }
});

test('ffmpeg receives separate music and voice inputs without normalization or shell quoting', () => {
  const args = mixArgs({ music: 'music with spaces.wav', musicStart: 3, voice: 'voice.wav', voiceAt: 2, duration: 80, gain: .8 }, 'partial.wav');
  assert.deepEqual(args.slice(args.indexOf('-ss'), args.indexOf('-ss') + 4), ['-ss', '3', '-i', 'music with spaces.wav']);
  assert.ok(args.includes('voice.wav')); assert.equal(args.at(-1), 'partial.wav');
  const graph = args[args.indexOf('-filter_complex') + 1];
  assert.match(graph, /adelay=96000S:all=1/); assert.match(graph, /normalize=0/);
  assert.ok(args.includes('pcm_s16le')); assert.equal(args[args.indexOf('-ar') + 1], '48000');
});

test('actual samples hold the first notes, duck by narration, and cut exactly at Kyle', async () => {
  const out = join(directory, 'music-bed.wav');
  await execute(process.execPath, [cli, '--music', music, '--music-start', '1', '--duration', '25', '--out', out], { timeout: 15000 });
  const pcm = await samples(out);
  assert.equal(pcm.frames, 25 * 48000);
  near(pcm.at(.5), .5); near(pcm.at(1), .5); near(pcm.at(1.5), .28);
  near(pcm.at(2.5), .06); near(pcm.at(23.99), .06); near(pcm.at(23.999), .012);
  near(pcm.at(24), 0); near(pcm.at(24.5), 0);
});

test('voice begins at its exact offset and continues after the music cut', async () => {
  const out = join(directory, 'voiced.wav');
  await execute(process.execPath, [cli, '--music', music, '--music-start', '1', '--voice', voice, '--voice-at', '2', '--duration', '26', '--out', out], { timeout: 15000 });
  const pcm = await samples(out);
  assert.equal(pcm.frames, 26 * 48000);
  near(pcm.at(2 - 1 / 48000), .06); near(pcm.at(2), .16);
  near(pcm.at(2.5), .16); near(pcm.at(24.5), .1); near(pcm.at(25.5), .1);
});

test('output cannot overwrite an input or its symlink', async () => {
  const before = await readFile(music);
  const alias = join(directory, 'alias.wav'); await symlink(music, alias);
  for (const out of [music, alias]) {
    await assert.rejects(execute(process.execPath, [cli, '--music', music, '--out', out], { timeout: 5000 }), error => /must not overwrite an input/.test(error.stderr));
    assert.deepEqual(await readFile(music), before);
  }
});

test('failed decoding preserves an existing output and removes the partial file', async () => {
  const bad = join(directory, 'bad.wav'), out = join(directory, 'existing.wav');
  await writeFile(bad, 'not audio'); await writeFile(out, 'preserve this output');
  await assert.rejects(execute(process.execPath, [cli, '--music', bad, '--out', out], { timeout: 10000 }));
  assert.equal(await readFile(out, 'utf8'), 'preserve this output');
  assert.ok(!(await readdir(directory)).some(name => name.endsWith('.partial.wav')));
});

function wave(duration, value) {
  const frames = duration * 48000, bytes = Buffer.alloc(44 + frames * 4);
  bytes.write('RIFF'); bytes.writeUInt32LE(bytes.length - 8, 4); bytes.write('WAVEfmt ', 8);
  bytes.writeUInt32LE(16, 16); bytes.writeUInt16LE(1, 20); bytes.writeUInt16LE(2, 22);
  bytes.writeUInt32LE(48000, 24); bytes.writeUInt32LE(192000, 28); bytes.writeUInt16LE(4, 32); bytes.writeUInt16LE(16, 34);
  bytes.write('data', 36); bytes.writeUInt32LE(frames * 4, 40);
  for (let frame = 0; frame < frames; frame++) {
    const sample = Math.round(value(frame / 48000) * 32768);
    bytes.writeInt16LE(sample, 44 + frame * 4); bytes.writeInt16LE(sample, 46 + frame * 4);
  }
  return bytes;
}

async function samples(path) {
  const bytes = await readFile(path);
  assert.equal(bytes.toString('ascii', 0, 4), 'RIFF'); assert.equal(bytes.toString('ascii', 8, 12), 'WAVE');
  let data;
  for (let position = 12; position + 8 <= bytes.length;) {
    const id = bytes.toString('ascii', position, position + 4), size = bytes.readUInt32LE(position + 4), start = position + 8;
    if (id === 'fmt ') { assert.equal(bytes.readUInt16LE(start), 1); assert.equal(bytes.readUInt16LE(start + 2), 2); assert.equal(bytes.readUInt32LE(start + 4), 48000); assert.equal(bytes.readUInt16LE(start + 14), 16); }
    if (id === 'data') data = bytes.subarray(start, start + size);
    position = start + size + size % 2;
  }
  assert.ok(data);
  return { frames: data.length / 4, at: time => data.readInt16LE(Math.round(time * 48000) * 4) / 32768 };
}

function near(actual, expected) { assert.ok(Math.abs(actual - expected) < .0001, `${actual} differs from ${expected}`); }
