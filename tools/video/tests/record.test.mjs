import assert from 'node:assert/strict';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { parseArgs, conversionArgs } from '../record.mjs';

test('capture defaults target the local composer and checkout-owned output', () => {
  const options = parseArgs([]);
  assert.equal(options.url, 'http://127.0.0.1:8089/');
  assert.equal(options.out, fileURLToPath(new URL('../work/composer.mp4', import.meta.url)));
  assert.equal(options.dryRun, false);
  assert.equal(options.fixture, 'api');
  assert.equal(parseArgs(['--dry-run']).dryRun, true);
  assert.equal(parseArgs(['--help']).help, true);
  assert.equal(parseArgs(['--url=http://localhost:8090/', '--out', 'chosen.mp4']).out, 'chosen.mp4');
});

test('film fixture is explicit and does not change the API default', () => {
  assert.equal(parseArgs(['--fixture', 'film']).fixture, 'film');
  assert.equal(parseArgs(['--fixture=api']).fixture, 'api');
  assert.throws(() => parseArgs(['--fixture', 'unknown']), /fixture/);
  assert.throws(() => parseArgs(['--fixture']), /value/);
  assert.throws(() => parseArgs(['--fixture', 'api', '--fixture', 'film']), /Duplicate/);
});

test('capture URL guard rejects remote hosts, credentials, and non-HTTP URLs', () => {
  for (const url of ['https://example.com/', 'http://localhost.example.com/', 'http://127.0.0.1.example.com/', 'http://user:pass@localhost/', 'file:///tmp/composer.html', 'javascript:alert(1)']) assert.throws(() => parseArgs(['--url', url]), /loopback|credentials|HTTP/);
  for (const url of ['http://localhost:8089/', 'http://127.0.0.1:8089/', 'http://[::1]:8089/']) assert.equal(parseArgs(['--url', url]).url, url);
});

test('capture rejects missing values, duplicate switches, and unknown arguments', () => {
  for (const args of [['--url'], ['--out'], ['--wat'], ['--out', 'a.mp4', '--out', 'b.mp4'], ['--dry-run=true'], ['--out', 'clip.webm']]) assert.throws(() => parseArgs(args));
});

test('conversion uses an argument array, 1080p H.264 and no fabricated audio', () => {
  const source = '/capture/actual.webm';
  const destination = '/capture/film $(touch unwanted).mp4';
  const args = conversionArgs(source, destination);
  assert.deepEqual(args.slice(args.indexOf('-i'), args.indexOf('-i') + 2), ['-i', source]);
  assert.equal(args.at(-1), destination);
  assert.ok(args.includes('libx264')); assert.ok(args.includes('yuv420p'));
  assert.ok(args.includes('scale=1920:1080:flags=lanczos')); assert.ok(args.includes('-an'));
  assert.ok(args.includes('+faststart'));
});
