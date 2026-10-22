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
