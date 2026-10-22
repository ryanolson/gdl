import assert from 'node:assert/strict';
import test from 'node:test';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parseArgs, renderPlan, ffmpegArgs } from '../render.mjs';

test('default output belongs to the video tool regardless of caller directory', () => {
  const expected = fileURLToPath(new URL('../work/film.mp4', import.meta.url));
  assert.equal(parseArgs([]).out, expected);
  const moduleUrl = new URL('../render.mjs', import.meta.url).href;
  const result = spawnSync(process.execPath, ['--input-type=module', '-e', `import { parseArgs } from ${JSON.stringify(moduleUrl)}; console.log(parseArgs([]).out);`], {
    cwd: fileURLToPath(new URL('.', import.meta.url)), encoding: 'utf8', timeout: 5000,
  });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.trim(), expected);
  assert.equal(parseArgs(['--out', 'chosen.mp4']).out, 'chosen.mp4');
});

test('renderer parses capture options and module overrides', () => {
  const options = parseArgs(['--url', 'http://127.0.0.1:8088/', '--out', 'opening.mp4', '--from', '0', '--to=55', '--fps30', '--audio', 'full-film.wav', '--ffmpeg', '/opt/ffmpeg', '--playwright', '/opt/playwright/index.js', '--animatic']);
  assert.equal(options.url, 'http://127.0.0.1:8088/');
  assert.equal(options.out, 'opening.mp4');
  assert.equal(options.from, 0);
  assert.equal(options.to, 55);
  assert.equal(options.fps, 30);
  assert.equal(options.audio, 'full-film.wav');
  assert.equal(options.ffmpeg, '/opt/ffmpeg');
  assert.equal(options.playwright, '/opt/playwright/index.js');
  assert.equal(options.animatic, true);
  assert.equal(parseArgs(['--help']).help, true);
  assert.equal(parseArgs(['--preflight']).preflight, true);
});

test('renderer rejects unsafe URLs and invalid capture options', () => {
  for (const url of ['https://example.com/', 'file:///tmp/index.html', 'http://localhost.example.com/', 'http://user:pass@localhost/']) {
    assert.throws(() => parseArgs(['--url', url]), /localhost|loopback|credentials/);
  }
  for (const url of ['http://localhost:8088/', 'http://127.0.0.1:8088/', 'http://[::1]:8088/']) {
    assert.equal(parseArgs(['--url', url]).url, url);
  }
  for (const args of [['--from', '-1'], ['--to', 'NaN'], ['--fps', '0'], ['--fps', 'Infinity'], ['--from', '5', '--to', '4'], ['--audio'], ['--wat'], ['--from', '1', '--from', '2']]) {
    assert.throws(() => parseArgs(args));
  }
});

test('render preflight calculates exclusive-end frame samples', () => {
  const plan = renderPlan({ from: 2, to: 3, fps: 30 }, 480, []);
  assert.deepEqual(plan, { from: 2, to: 3, fps: 30, frames: 30, encodedDuration: 1, missing: [] });
  assert.equal(renderPlan({ from: 479, fps: 30 }, 480, []).to, 480);
  assert.equal(renderPlan({ from: 0, to: 0.11, fps: 30 }, 480, []).frames, 4);
  assert.throws(() => renderPlan({ from: 480, fps: 30 }, 480, []), /range/);
  assert.throws(() => renderPlan({ from: 0, to: 481, fps: 30 }, 480, []), /range/);
});

test('recorded demo coverage fails closed and uses half-open intervals', () => {
  const missing = [{ id: 'catalog', start: 60, end: 90 }];
  assert.equal(renderPlan({ from: 0, to: 60, fps: 30 }, 480, missing).missing.length, 0);
  assert.equal(renderPlan({ from: 90, to: 100, fps: 30 }, 480, missing).missing.length, 0);
  assert.throws(() => renderPlan({ from: 55, to: 65, fps: 30 }, 480, missing), /catalog.*--animatic/);
  assert.equal(renderPlan({ from: 55, to: 65, fps: 30, animatic: true }, 480, missing).missing.length, 1);
  assert.throws(() => renderPlan({ from: 0, to: 55, fps: 30 }, 480, [{ id: 'bad' }]), /missingFootage/);
});

test('ffmpeg receives video-only encoding and full-film audio offset without a shell', () => {
  const plan = renderPlan({ from: 12, to: 14, fps: 30 }, 480, []);
  const silent = ffmpegArgs({}, plan, '/output/partial.mp4');
  assert.equal(silent[silent.indexOf('-framerate') + 1], '30');
  assert.deepEqual(silent.slice(silent.indexOf('-map'), silent.indexOf('-map') + 2), ['-map', '0:v:0']);
  assert.ok(silent.includes('libx264'));
  assert.ok(silent.includes('yuv420p'));
  assert.ok(silent.includes('+faststart'));
  assert.equal(silent.at(-1), '/output/partial.mp4');
  assert.ok(!silent.includes('-af'));
  const voiced = ffmpegArgs({ audio: '/audio/full film.wav' }, plan, '/output/partial.mp4');
  const seekIndex = voiced.indexOf('-ss');
  assert.deepEqual(voiced.slice(seekIndex, seekIndex + 4), ['-ss', '12', '-i', '/audio/full film.wav']);
  assert.ok(voiced.includes('1:a:0'));
  assert.ok(voiced.includes('apad'));
  assert.equal(voiced[voiced.indexOf('-t') + 1], '2');
});
