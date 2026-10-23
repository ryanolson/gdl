import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { after, before, test } from 'node:test';
import { chromium } from 'playwright-core';
import { createFilmServer } from '../server.mjs';

let server;
let browser;
let baseUrl;

before(async () => {
  server = createFilmServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}/`;
  browser = await chromium.launch({ channel: 'chrome', headless: true, timeout: 15000 });
}, { timeout: 20000 });

after(async () => {
  await browser?.close();
  server?.closeAllConnections();
  if (server) await new Promise(resolve => server.close(resolve));
}, { timeout: 10000 });

async function openStage(query = '', script = null) {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  page.setDefaultTimeout(3000);
  if (script !== null) {
    await page.route('**/script.md', route => route.fulfill({ status: 200, contentType: 'text/markdown', body: script }));
  }
  await page.goto(baseUrl + query, { waitUntil: 'domcontentloaded' });
  await page.evaluate(() => window.gdlFilm.ready);
  return page;
}

test('entering capture while playing continues to paint advancing film time', { timeout: 15000 }, async () => {
  const page = await openStage();
  try {
    await page.evaluate(() => window.gdlFilm.seek(10));
    await page.locator('#play').click();
    await page.waitForFunction(() => Number(document.getElementById('timeline').value) > 10.05);
    await page.evaluate(() => {
      window.gdlFilm.setCapture(true);
      window.captureCheckpoint = {
        clock: window.gdlFilm.getState().time,
        timeline: Number(document.getElementById('timeline').value),
        crawl: document.querySelector('.film-crawl').style.transform,
      };
    });
    await page.waitForFunction(() => window.gdlFilm.getState().time > window.captureCheckpoint.clock + 0.15);
    const result = await page.evaluate(() => ({
      ...window.gdlFilm.getState(),
      painted: Number(document.getElementById('timeline').value),
      crawl: document.querySelector('.film-crawl').style.transform,
      checkpoint: window.captureCheckpoint,
    }));
    assert.equal(result.capture, true);
    assert.equal(result.playing, true);
    assert.ok(result.painted > result.checkpoint.timeline + 0.1, `clock reached ${result.time}, but painted time stayed at ${result.painted}`);
    assert.notEqual(result.crawl, result.checkpoint.crawl);
    await page.evaluate(() => window.gdlFilm.pause());
    assert.equal((await page.evaluate(() => window.gdlFilm.getState())).playing, false);
  } finally { await page.close(); }
});

test('script duration controls the API, transport, end replay and exported cue sheet', { timeout: 15000 }, async () => {
  const script = await readFile(new URL('../../../agent-docs/video/script.md', import.meta.url), 'utf8');
  const last = [...script.matchAll(/^## \d{2}:\d{2}–(\d{2}):(\d{2}) — /gm)].at(-1);
  const end = Number(last[1]) * 60 + Number(last[2]) + 12;
  const timecode = `${String(Math.floor(end / 60)).padStart(2, '0')}:${String(end % 60).padStart(2, '0')}`;
  const revised = script.slice(0, last.index) + script.slice(last.index).replace(`${last[1]}:${last[2]}`, timecode);
  const totalVoices = [...revised.matchAll(/^\*\*[^*]+:\*\* “/gm)].length;
  const page = await openStage('', revised);
  try {
    const initial = await page.evaluate(() => ({
      duration: window.gdlFilm.duration,
      state: window.gdlFilm.getState(),
      maximum: Number(document.getElementById('timeline').max),
      timecode: document.getElementById('timecode').textContent,
      sidebarDuration: document.getElementById('film-duration')?.textContent,
    }));
    assert.equal(initial.duration, end);
    assert.equal(initial.state.duration, end);
    assert.equal(initial.state.totalVoices, totalVoices);
    assert.equal(initial.maximum, end);
    assert.ok(initial.timecode.endsWith(`/ ${timecode}`));
    assert.equal(initial.sidebarDuration, timecode);
    await page.evaluate(() => window.gdlFilm.seek(10000));
    assert.equal((await page.evaluate(() => window.gdlFilm.getState())).time, end);
    await page.locator('#play').click();
    await page.waitForFunction(() => window.gdlFilm.getState().playing && window.gdlFilm.getState().time < 1);
    await page.evaluate(() => window.gdlFilm.pause());
    const [download] = await Promise.all([page.waitForEvent('download'), page.locator('#cue-export').click()]);
    const stream = await download.createReadStream();
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    const sheet = JSON.parse(Buffer.concat(chunks).toString());
    assert.equal(sheet.duration, end);
    assert.equal(sheet.scenes.at(-1).end, end);
    assert.equal(sheet.scenes.flatMap(scene => scene.turns).length, totalVoices);
  } finally { await page.close(); }
});

test('the default animation mode initializes the footage deck without a live iframe', { timeout: 15000 }, async () => {
  const page = await openStage('?t=80');
  try {
    const result = await page.evaluate(() => ({
      mode: window.gdlFilm.getState().footageMode,
      control: document.getElementById('footage-mode').value,
      overlayHidden: document.querySelector('.stage-footage').hidden,
      iframes: document.querySelectorAll('.stage-composer').length,
    }));
    assert.equal(result.mode, 'none');
    assert.equal(result.control, 'none');
    assert.equal(result.overlayHidden, true);
    assert.equal(result.iframes, 0);
  } finally { await page.close(); }
});

test('the composer link follows the selected media manifest', { timeout: 15000 }, async () => {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  try {
    await page.route('**/media.json', route => route.fulfill({
      status: 200, contentType: 'application/json',
      body: JSON.stringify({ composerUrl: 'http://127.0.0.1:8090/', voices: {}, footage: {} }),
    }));
    await page.goto(baseUrl);
    await page.evaluate(() => window.gdlFilm.ready);
    assert.equal(await page.getByRole('link', { name: 'Open gdl-web' }).getAttribute('href'), 'http://127.0.0.1:8090/');
  } finally { await page.close(); }
});

test('paused capture keeps an exact seek position across browser frames', { timeout: 15000 }, async () => {
  const page = await openStage();
  try {
    const result = await page.evaluate(async () => {
      window.gdlFilm.pause();
      window.gdlFilm.setCapture(true);
      await window.gdlFilm.seek(24.5);
      const transform = document.querySelector('[data-actor="kyle"] .film-actor-head').style.transform;
      await new Promise(requestAnimationFrame);
      await new Promise(requestAnimationFrame);
      return {
        ...window.gdlFilm.getState(),
        painted: Number(document.getElementById('timeline').value),
        unchanged: transform === document.querySelector('[data-actor="kyle"] .film-actor-head').style.transform,
      };
    });
    assert.equal(result.capture, true);
    assert.equal(result.playing, false);
    assert.equal(result.time, 24.5);
    assert.equal(result.painted, 24.5);
    assert.equal(result.unchanged, true);
    assert.deepEqual(await page.locator('#film-stage').boundingBox(), { x: 0, y: 0, width: 1920, height: 1080 });
  } finally { await page.close(); }
});

test('the optional tribute loads only on request and seeks reproducible guest and black frames', { timeout: 15000 }, async () => {
  const plain = await openStage();
  try {
    assert.equal(await plain.locator('.film-tribute').count(), 0);
    assert.equal((await plain.evaluate(() => window.gdlFilm.getState())).archiveCues.length, 0);
    assert.equal(await plain.locator('img[src*="/assets/casey/"]').count(), 0);
  } finally { await plain.close(); }
  const page = await openStage('?ending=hardfork&t=507');
  try {
    const state = await page.evaluate(() => window.gdlFilm.getState());
    assert.equal(state.duration, 512);
    assert.equal(state.totalVoices, 41);
    assert.deepEqual(state.archiveCues.map(turn => turn.id), ['hardfork-casey', 'hardfork-kevin']);
    assert.equal(await page.locator('.film-tribute').isVisible(), true);
    assert.equal(await page.locator('.film-root').isVisible(), false);
    assert.equal(await page.locator('.film-tribute').getAttribute('data-black'), 'false');
    assert.equal(await page.locator('.film-guest-speaking .film-guest-name').textContent(), 'Casey Newton');
    const transform = await page.locator('[data-guest="casey"] .film-guest-head').getAttribute('style');
    await page.evaluate(() => window.gdlFilm.seek(508.3));
    assert.equal(await page.locator('.film-guest-speaking .film-guest-name').textContent(), 'Kevin Roose');
    await page.evaluate(() => window.gdlFilm.seek(507));
    assert.equal(await page.locator('[data-guest="casey"] .film-guest-head').getAttribute('style'), transform);
    await page.evaluate(() => window.gdlFilm.seek(510));
    assert.equal(await page.locator('.film-tribute').getAttribute('data-black'), 'true');
    assert.equal(await page.locator('#caption-layer').textContent(), '');
    await page.evaluate(() => window.gdlFilm.seek(500));
    assert.equal(await page.locator('.film-tribute').isVisible(), false);
    assert.equal(await page.locator('.film-root').isVisible(), true);
  } finally { await page.close(); }
});

test('optional preview music follows real Audio seek, rate, play, pause and capture transport', { timeout: 15000 }, async () => {
  const page = await musicStage();
  try {
    const canonical = await readFile(new URL('../../../agent-docs/video/script.md', import.meta.url), 'utf8');
    assert.equal((await page.evaluate(() => window.gdlFilm.getState())).totalVoices, [...canonical.matchAll(/^\*\*[^*]+:\*\* “/gm)].length);
    assert.equal((await page.evaluate(() => window.gdlFilm.getState())).configuredVoices, 0);
    await page.waitForFunction(() => document.getElementById('background-audio')?.readyState >= 1);
    assert.equal(await page.locator('#background-audio').getAttribute('src'), `${baseUrl}media/music-fixture/bed.wav`);
    await page.evaluate(() => window.gdlFilm.seek(4));
    await page.waitForFunction(() => { const audio = document.getElementById('background-audio'); return audio.paused && Math.abs(audio.currentTime - 4) < .03; });
    await page.locator('#rate').selectOption('1.5');
    assert.equal(await page.locator('#background-audio').evaluate(audio => audio.playbackRate), 1.5);
    await page.locator('#play').click();
    await page.waitForFunction(() => { const audio = document.getElementById('background-audio'); return !audio.paused && audio.currentTime > 4.1; });
    const checkpoint = await page.evaluate(() => { window.gdlFilm.setCapture(true); return document.getElementById('background-audio').currentTime; });
    await page.waitForFunction(time => document.getElementById('background-audio').currentTime > time + .15, checkpoint);
    await page.evaluate(() => window.gdlFilm.pause());
    assert.equal(await page.locator('#background-audio').evaluate(audio => audio.paused), true);
    await page.evaluate(() => window.gdlFilm.seek(9));
    const held = await page.locator('#background-audio').evaluate(async audio => {
      await new Promise(requestAnimationFrame); await new Promise(requestAnimationFrame);
      return { time: audio.currentTime, paused: audio.paused, rate: audio.playbackRate, capture: window.gdlFilm.getState().capture };
    });
    assert.ok(Math.abs(held.time - 9) < .03); assert.equal(held.paused, true); assert.equal(held.rate, 1.5); assert.equal(held.capture, true);
  } finally { await page.close(); }
});

test('preview music source failures appear in the stage status', { timeout: 15000 }, async () => {
  const page = await musicStage(true);
  try { await page.locator('#status.error').filter({ hasText: /music/i }).waitFor({ state: 'visible', timeout: 3000 }); }
  finally { await page.close(); }
});

test('an unset music bed creates no Audio element or media request', { timeout: 15000 }, async () => {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  const media = [];
  page.on('request', request => { if (request.resourceType() === 'media') media.push(request.url()); });
  try {
    await page.goto(baseUrl); await page.evaluate(() => window.gdlFilm.ready);
    assert.equal(await page.locator('#background-audio').count(), 0); assert.deepEqual(media, []);
  } finally { await page.close(); }
});

async function musicStage(fail = false) {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  page.setDefaultTimeout(3000);
  await page.route(url => url.pathname === '/media/music-fixture/manifest.json', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ music: 'bed.wav' }) }));
  await page.route(url => url.pathname === '/media/music-fixture/bed.wav', route => {
    if (fail) return route.fulfill({ status: 404, body: 'Missing test audio' });
    const wave = silentWave();
    const range = /^bytes=(\d+)-(\d*)$/.exec(route.request().headers().range ?? '');
    const start = range ? Number(range[1]) : 0;
    const end = range?.[2] ? Math.min(Number(range[2]), wave.length - 1) : wave.length - 1;
    const body = wave.subarray(start, end + 1);
    return route.fulfill({ status: range ? 206 : 200, contentType: 'audio/wav', headers: { 'accept-ranges': 'bytes', 'content-length': String(body.length), ...(range ? { 'content-range': `bytes ${start}-${end}/${wave.length}` } : {}) }, body });
  });
  try {
    await page.goto(`${baseUrl}?manifest=/media/music-fixture/manifest.json`);
    await page.evaluate(() => window.gdlFilm.ready); return page;
  } catch (error) { await page.close(); throw error; }
}

function silentWave() {
  const rate = 8000; const bytes = rate * 20 * 2; const wave = Buffer.alloc(44 + bytes);
  wave.write('RIFF'); wave.writeUInt32LE(36 + bytes, 4); wave.write('WAVEfmt ', 8); wave.writeUInt32LE(16, 16);
  wave.writeUInt16LE(1, 20); wave.writeUInt16LE(1, 22); wave.writeUInt32LE(rate, 24); wave.writeUInt32LE(rate * 2, 28);
  wave.writeUInt16LE(2, 32); wave.writeUInt16LE(16, 34); wave.write('data', 36); wave.writeUInt32LE(bytes, 40);
  return wave;
}
