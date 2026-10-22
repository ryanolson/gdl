import assert from 'node:assert/strict';
import test from 'node:test';
import { FootageDeck, validateManifest } from '../dist/media.mjs';

const scenes = [
  { id: 'opening', start: 0, end: 24, captureKind: 'animation', turns: [{ id: 'opening-1' }] },
  { id: 'catalog', start: 24, end: 55, captureKind: 'live-composer', turns: [{ id: 'catalog-1' }] },
  { id: 'types', start: 55, end: 95, captureKind: 'live-composer', turns: [{ id: 'types-1' }] },
];
const frame = (id, time) => ({ scene: scenes.find(scene => scene.id === id), time, turn: null });

test('manifest defaults and relative URLs are valid without a browser', () => {
  assert.deepEqual(validateManifest({}, scenes), { composerUrl: 'http://127.0.0.1:8089/', voices: {}, footage: {} });
  assert.deepEqual(validateManifest({ composerUrl: '/composer', voices: { 'opening-1': '/media/narrator.wav' }, footage: { catalog: 'media/catalog.webm' } }, scenes), { composerUrl: '/composer', voices: { 'opening-1': '/media/narrator.wav' }, footage: { catalog: 'media/catalog.webm' } });
});

test('manifest rejects malformed objects, unknown keys and unknown timeline IDs', () => {
  for (const value of [null, [], 'manifest', { mystery: {} }, { voices: [] }, { footage: null }, { voices: { missing: '/x.wav' } }, { footage: { missing: '/x.webm' } }]) assert.throws(() => validateManifest(value, scenes));
  assert.throws(() => validateManifest(JSON.parse('{"__proto__":{}}'), scenes), /unknown/i);
  assert.throws(() => validateManifest({}, [{ ...scenes[0], turns: [{ id: 'same' }, { id: 'same' }] }]), /unique/i);
});

test('manifest rejects non-string and unsafe media URLs', () => {
  for (const url of ['', ' ', 4, null, 'javascript:alert(1)', 'data:text/html,hello', 'file:///etc/passwd', 'a\nb.wav']) {
    assert.throws(() => validateManifest({ voices: { 'opening-1': url } }, scenes));
    assert.throws(() => validateManifest({ footage: { catalog: url } }, scenes));
  }
  assert.throws(() => validateManifest({ composerUrl: 'javascript:alert(1)' }, scenes));
});

test('live iframe is persistent, restricted to capture scenes, and none hides it', t => {
  const { stage, document } = browser(t);
  const deck = new FootageDeck({ stage, scenes, manifest: {} });
  deck.sync(frame('opening', 2), { playing: true, rate: 1 });
  assert.equal(stage.children[0].hidden, true);
  deck.sync(frame('catalog', 30), { playing: true, rate: 1 });
  const iframe = document.created.find(node => node.tagName === 'IFRAME');
  assert.equal(iframe.src, 'http://127.0.0.1:8089/');
  deck.sync(frame('types', 60), { playing: true, rate: 1 });
  assert.equal(document.created.filter(node => node.tagName === 'IFRAME').length, 1);
  deck.setMode('none'); assert.equal(stage.children[0].hidden, true);
  assert.throws(() => deck.setMode('fake'), /mode/);
  deck.dispose(); assert.equal(stage.children.length, 0);
});

test('recorded scenes use scene-relative time, pause on seek, and stop at clip end', async t => {
  const { document, stage } = browser(t);
  const deck = new FootageDeck({ stage, scenes, manifest: { footage: { catalog: '/media/catalog.webm' } } });
  deck.setMode('recorded');
  deck.sync(frame('catalog', 30), { playing: true, rate: 1.5 });
  const video = document.created.find(node => node.tagName === 'VIDEO');
  assert.equal(video.currentTime, 6); assert.equal(video.playbackRate, 1.5); assert.equal(video.paused, false);
  await deck.seek(frame('catalog', 35));
  assert.equal(video.currentTime, 11); assert.equal(video.paused, true);
  const seeks = video.seeks;
  await deck.seek(frame('catalog', 35)); assert.equal(video.seeks, seeks);
  video.duration = 12;
  deck.sync(frame('catalog', 50), { playing: true, rate: 1 });
  assert.equal(video.currentTime, 12); assert.equal(video.paused, true);
  deck.dispose(); assert.equal(video.paused, true);
});

test('seek waits for an observed seeked frame and reports errors', async t => {
  const { document, stage } = browser(t);
  const errors = [];
  const deck = new FootageDeck({ stage, scenes, manifest: { footage: { catalog: '/media/catalog.webm' } }, onError: value => errors.push(value) });
  deck.setMode('recorded'); deck.sync(frame('catalog', 24), { playing: false, rate: 1 });
  const video = document.created.find(node => node.tagName === 'VIDEO');
  video.autoSeek = false;
  let completed = false;
  const seek = deck.seek(frame('catalog', 32)).then(() => { completed = true; });
  await Promise.resolve(); assert.equal(completed, false);
  video.seeking = false; video.dispatchEvent(new Event('seeked'));
  await seek; assert.equal(completed, true);
  const broken = deck.seek(frame('catalog', 33));
  await Promise.resolve(); video.dispatchEvent(new Event('error'));
  await assert.rejects(broken, /footage|video/i);
  assert.equal(errors.at(-1).sceneId, 'catalog'); assert.equal(errors.length, 1); deck.dispose();
});

test('missing footage is explicitly labeled and lists only capture scenes', t => {
  const { stage } = browser(t);
  const deck = new FootageDeck({ stage, scenes, manifest: { footage: { types: '/media/types.mp4' } } });
  assert.deepEqual(deck.missing(), [{ id: 'catalog', start: 24, end: 55 }]);
  deck.setMode('recorded'); deck.sync(frame('catalog', 29), { playing: false, rate: 1 });
  const overlay = stage.children[0];
  assert.equal(overlay.dataset.state, 'missing');
  assert.equal(overlay.children.find(node => node.className === 'stage-footage-badge').textContent, 'Recording required');
  deck.dispose();
});

function browser(t) {
  const previous = globalThis.document;
  const created = [];
  class Element extends EventTarget {
    constructor(tag = 'div') { super(); this.tagName = tag.toUpperCase(); this.children = []; this.dataset = {}; this.hidden = false; this.className = ''; this.style = {}; this.textContent = ''; }
    append(...nodes) { for (const node of nodes) { this.children.push(node); node.parent = this; } }
    remove() { if (this.parent) this.parent.children.splice(this.parent.children.indexOf(this), 1); }
    setAttribute(name, value) { this[name] = value; }
  }
  class Video extends Element {
    constructor() { super('video'); this.readyState = 4; this.duration = 120; this.paused = true; this.seeking = false; this.seeks = 0; this.position = 0; this.autoSeek = true; }
    get currentTime() { return this.position; }
    set currentTime(value) { this.position = value; this.seeking = true; this.seeks++; if (this.autoSeek) queueMicrotask(() => { this.seeking = false; this.dispatchEvent(new Event('seeked')); }); }
    pause() { this.paused = true; }
    play() { this.paused = false; return Promise.resolve(); }
    load() {}
  }
  const document = { baseURI: 'http://127.0.0.1:8088/', created, createElement(tag) { const node = tag === 'video' ? new Video() : new Element(tag); created.push(node); return node; } };
  globalThis.document = document;
  t.after(() => { if (previous === undefined) delete globalThis.document; else globalThis.document = previous; });
  return { stage: new Element(), document };
}
