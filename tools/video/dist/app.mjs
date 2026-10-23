import { TimelineClock } from './clock.mjs';
import { parseScript, frameAt } from './cues.mjs';
import { createRenderer } from './scene.mjs';
import { VoiceScheduler } from './audio.mjs';
import { FootageDeck, validateManifest } from './media.mjs';
import { captionAt } from './captions.mjs';
import { appendTribute, createTributeRenderer } from './tribute.mjs';

const element = id => document.getElementById(id);
const stage = element('film-stage');
const query = new URLSearchParams(location.search);
let clock, scenes, renderer, tribute, voices, music, footage, manifest;
let capture = false;
let footageMode = 'none';
let animationFrame = 0;
let sidebarScene = '';
let lastCaption = '';
let resolveReady, rejectReady;
const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
ready.catch(() => {});

window.gdlFilm = {
  ready,
  get duration() { return scenes?.at(-1).end ?? 0; },
  seek,
  pause() { clock?.pause(); voices?.pause(); music?.pause(); footage?.pause(); if (clock) paint(); },
  setCapture,
  async setFootageMode(mode) {
    footage.setMode(mode); footageMode = mode; element('footage-mode').value = mode;
    await seek(clock.snapshot().time);
  },
  getState() {
    return {
      ...(clock?.snapshot() ?? { time: 0, playing: false }), capture, footageMode,
      missingFootage: footage?.missing() ?? [],
      configuredVoices: Object.keys(manifest?.voices ?? {}).length,
      totalVoices: scenes?.reduce((sum, scene) => sum + scene.turns.length, 0) ?? 0,
      archiveCues: scenes?.flatMap(scene => scene.turns.filter(turn => turn.source === 'archive').map(({ id, start, end }) => ({ id, start, end }))) ?? [],
    };
  },
};

initialize().catch(error => { status(error.message, true); rejectReady(error); });

async function initialize() {
  const manifestUrl = new URL(query.get('manifest') ?? 'media.json', document.baseURI);
  const [scriptResponse, mediaResponse] = await Promise.all([fetch('/script.md'), fetch(manifestUrl)]);
  if (!scriptResponse.ok) throw new Error(`Script request failed: HTTP ${scriptResponse.status}`);
  if (!mediaResponse.ok) throw new Error(`Media manifest request failed: HTTP ${mediaResponse.status}`);
  scenes = appendTribute(parseScript(await scriptResponse.text()), query.get('ending'));
  manifest = validateManifest(await mediaResponse.json(), scenes);
  manifest.composerUrl = new URL(manifest.composerUrl, manifestUrl).href;
  element('composer-link').href = manifest.composerUrl;
  for (const key of ['voices', 'footage']) {
    manifest[key] = Object.fromEntries(Object.entries(manifest[key]).map(([id, url]) => [id, new URL(url, manifestUrl).href]));
  }
  if (manifest.music) manifest.music = new URL(manifest.music, manifestUrl).href;
  clock = new TimelineClock({ duration: scenes.at(-1).end });
  element('timeline').max = String(clock.snapshot().duration);
  element('timeline').disabled = false;
  element('film-duration').textContent = timecode(clock.snapshot().duration);
  renderer = createRenderer(stage, { assetBase: '/assets/' });
  if (query.get('ending') === 'hardfork') tribute = createTributeRenderer(stage);
  voices = new VoiceScheduler({ scenes, onError: ({ turnId, error }) => status(`${turnId}: ${error.message}`, true) });
  footage = new FootageDeck({ stage, scenes, manifest, onError: ({ sceneId, error }) => status(`${sceneId}: ${error.message}`, true) });
  for (const [id, url] of Object.entries(manifest.voices)) {
    const audio = new Audio(url); audio.preload = 'auto'; voices.setTake(id, audio);
  }
  if (manifest.music) {
    const duration = clock.snapshot().duration;
    music = new VoiceScheduler({ scenes: [{ id: 'music', start: 0, end: duration, turns: [{ id: 'music-bed', start: 0, end: duration }] }], onError: ({ error }) => status(`Music: ${error.message}`, true) });
    const audio = new Audio(); audio.id = 'background-audio'; audio.hidden = true; audio.preload = 'auto';
    audio.addEventListener('error', () => status(`Music: ${audio.error?.message || 'The audio source could not be loaded.'}`, true));
    audio.addEventListener('loadedmetadata', () => music.sync(clock.snapshot()));
    music.setTake('music-bed', audio); audio.src = manifest.music; document.body.append(audio);
  }
  buildSceneList();
  bindControls();
  await Promise.all([renderer.ready, tribute?.ready, document.fonts.ready]);
  const initial = Number(query.get('t') ?? 0);
  if (!Number.isFinite(initial)) throw new Error('The t query parameter must be a finite number of seconds.');
  if (query.get('mode')) footageMode = query.get('mode');
  footage.setMode(footageMode); element('footage-mode').value = footageMode;
  await seek(initial);
  if (query.get('capture') === '1') setCapture(true);
  if (!element('status').classList.contains('error')) status(manifest.music ? 'Preview music configured. Add selected voice takes and real demo clips through the media manifest.' : 'Silent rehearsal. Add selected voice takes and real demo clips through a media manifest.');
  new ResizeObserver(fitStage).observe(element('preview-shell'));
  fitStage();
  resolveReady();
  loop();
}

async function seek(time) {
  if (!clock) throw new Error('Film stage is not ready.');
  clock.pause(); voices.pause(); music?.pause(); footage.pause(); clock.seek(time); voices.seek(clock.snapshot().time); music?.sync(clock.snapshot());
  const frame = draw();
  await footage.seek(frame);
  return window.gdlFilm.getState();
}

function draw() {
  const snapshot = clock.snapshot();
  const frame = { ...frameAt(scenes, snapshot.time), capture };
  renderer.render(frame);
  tribute?.render(frame);
  stage.dataset.footageActive = String(frame.scene.captureKind === 'live-composer' && (footageMode === 'live' || footageMode === 'recorded' && Boolean(manifest.footage[frame.scene.id])));
  updateCaption(frame);
  element('timeline').value = snapshot.time;
  element('timecode').textContent = `${timecode(snapshot.time, true)} / ${timecode(snapshot.duration)}`;
  element('play').textContent = snapshot.playing ? 'Pause' : 'Play';
  element('scene-kicker').textContent = frame.scene.title.toUpperCase();
  const allTurns = scenes.flatMap(scene => scene.turns).length;
  const needed = scenes.filter(scene => scene.captureKind === 'live-composer').length;
  element('readiness').textContent = `${Object.keys(manifest.voices).length}/${allTurns} voice takes · ${Object.keys(manifest.footage).length}/${needed} demo clips`;
  for (const button of element('scene-list').children) button.setAttribute('aria-current', String(button.dataset.scene === frame.scene.id));
  if (sidebarScene !== frame.scene.id) { sidebarScene = frame.scene.id; buildCueList(frame.scene); }
  for (const button of element('cue-list').children) button.setAttribute('aria-current', String(button.dataset.cue === frame.turn?.id));
  return frame;
}

function paint() {
  if (!clock) return;
  const frame = draw();
  const snapshot = clock.snapshot();
  footage.sync(frame, { playing: snapshot.playing, rate: snapshot.rate });
  voices.sync(snapshot);
  music?.sync(snapshot);
}

function loop() {
  if (capture) return;
  paint();
  animationFrame = requestAnimationFrame(loop);
}

function updateCaption(frame) {
  const caption = element('captions').checked ? captionAt(frame.turn, frame.time) : '';
  if (caption === lastCaption) return;
  lastCaption = caption;
  element('caption-layer').replaceChildren();
  if (caption) { const span = document.createElement('span'); span.textContent = caption; element('caption-layer').append(span); }
}

function setCapture(value) {
  capture = Boolean(value);
  document.body.classList.toggle('capturing', capture);
  element('capture-exit').hidden = !capture;
  element('capture-toggle').textContent = capture ? 'Exit capture view' : 'Capture view';
  cancelAnimationFrame(animationFrame);
  fitStage();
  if (clock) paint();
  if (clock) {
    if (!capture) loop();
    else if (clock.snapshot().playing) animationFrame = requestAnimationFrame(captureLoop);
  }
}

function fitStage() {
  stage.style.transform = capture ? 'none' : `scale(${element('preview-shell').clientWidth / 1920})`;
}

function buildSceneList() {
  for (const scene of scenes) {
    const button = document.createElement('button'); button.className = 'scene-button'; button.dataset.scene = scene.id;
    const time = document.createElement('time'); time.textContent = timecode(scene.start);
    const copy = document.createElement('span'); const title = document.createElement('strong'); title.textContent = scene.title;
    const note = document.createElement('small'); note.textContent = scene.captureKind === 'live-composer' ? 'Real demo footage' : 'Animation';
    copy.append(title, note); button.append(time, copy); button.addEventListener('click', () => act(() => seek(scene.start)));
    element('scene-list').append(button);
  }
}

function buildCueList(scene) {
  element('cue-list').replaceChildren(); element('cue-count').textContent = `${scene.turns.length} cues`;
  for (const turn of scene.turns) {
    const button = document.createElement('button'); button.className = 'cue-button'; button.dataset.cue = turn.id;
    const speaker = document.createElement('strong'); speaker.textContent = `${timecode(turn.start)} · ${turn.speaker}`;
    const copy = document.createElement('span'); copy.textContent = turn.caption;
    const id = document.createElement('small'); id.textContent = turn.id;
    button.append(speaker, copy, id); button.title = turn.text ?? 'Original archive audio. Do not synthesize this cue.';
    button.addEventListener('click', () => act(() => seek(turn.start))); element('cue-list').append(button);
  }
}

function bindControls() {
  element('play').addEventListener('click', togglePlay);
  element('timeline').addEventListener('input', event => act(() => seek(Number(event.target.value))));
  element('previous').addEventListener('click', () => changeScene(-1));
  element('next').addEventListener('click', () => changeScene(1));
  element('rate').addEventListener('change', event => { clock.setRate(Number(event.target.value)); paint(); });
  element('captions').addEventListener('change', paint);
  element('footage-mode').addEventListener('change', event => act(() => window.gdlFilm.setFootageMode(event.target.value)));
  element('capture-toggle').addEventListener('click', () => setCapture(!capture));
  element('capture-exit').addEventListener('click', () => setCapture(false));
  element('cue-export').addEventListener('click', exportCues);
  document.addEventListener('keydown', event => {
    if (/^(INPUT|SELECT|TEXTAREA|BUTTON)$/.test(event.target.tagName)) return;
    if (event.code === 'Space') { event.preventDefault(); togglePlay(); }
    if (event.code === 'ArrowRight' || event.code === 'ArrowLeft') { event.preventDefault(); act(() => seek(clock.snapshot().time + (event.code === 'ArrowRight' ? 1 : -1))); }
    if (event.key.toLowerCase() === 'f') setCapture(!capture);
    if (event.key === 'Escape' && capture) setCapture(false);
  });
}

function togglePlay() {
  const snapshot = clock.snapshot();
  if (snapshot.playing) { window.gdlFilm.pause(); return; }
  if (snapshot.time === snapshot.duration) clock.seek(0);
  clock.play(); paint();
  if (capture) setCapture(true);
}

function captureLoop() {
  if (!capture || !clock.snapshot().playing) return;
  paint(); animationFrame = requestAnimationFrame(captureLoop);
}

function changeScene(delta) {
  const current = frameAt(scenes, clock.snapshot().time).scene;
  const index = Math.max(0, Math.min(scenes.length - 1, scenes.indexOf(current) + delta));
  act(() => seek(scenes[index].start));
}

function exportCues() {
  const data = { title: 'CUT HERE', duration: clock.snapshot().duration, model_id: 'eleven_v4', timing: 'provisional', scenes };
  const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }));
  const link = document.createElement('a'); link.href = url; link.download = 'cut-here-cues.json'; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  status('Cue sheet exported. Archive cues have no TTS text. Stock voice IDs still need assignment.');
}

function timecode(time, precise = false) {
  const minutes = Math.floor(time / 60); const seconds = time - minutes * 60;
  return `${String(minutes).padStart(2, '0')}:${precise ? seconds.toFixed(2).padStart(5, '0') : String(Math.floor(seconds)).padStart(2, '0')}`;
}

function status(message, error = false) { element('status').textContent = message; element('status').classList.toggle('error', error); }
function act(action) { Promise.resolve().then(action).catch(error => status(error.message, true)); }
