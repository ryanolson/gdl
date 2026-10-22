export class FootageDeck {
  #scenes; #manifest; #overlay; #badge; #iframe = null; #onError;
  #videos = new Map(); #active = null; #mode = 'live'; #disposed = false;
  #lastFrame = null; #lastOptions = { playing: false, rate: 1 }; #seekController = null;

  constructor({ stage, scenes, manifest = {}, onError = () => {} }) {
    if (!stage || typeof stage.append !== 'function') throw new TypeError('stage must be a DOM element');
    if (typeof onError !== 'function') throw new TypeError('onError must be a function');
    this.#manifest = validateManifest(manifest, scenes); this.#scenes = scenes; this.#onError = onError;
    this.#overlay = document.createElement('div'); this.#overlay.className = 'stage-footage'; this.#overlay.hidden = true;
    this.#badge = document.createElement('div'); this.#badge.className = 'stage-footage-badge'; this.#badge.textContent = 'Recording required'; this.#badge.hidden = true;
    this.#overlay.append(this.#badge); stage.append(this.#overlay);
  }

  get mode() { return this.#mode; }

  setMode(mode) {
    if (!['live', 'recorded', 'none'].includes(mode)) throw new RangeError('mode must be live, recorded, or none');
    this.#seekController?.abort(); this.#mode = mode;
    if (this.#lastFrame) this.sync(this.#lastFrame, { ...this.#lastOptions, playing: false });
  }

  sync(frame, { playing, rate = 1 }) {
    if (this.#disposed) return;
    if (typeof playing !== 'boolean' || !Number.isFinite(rate) || rate <= 0) throw new TypeError('playing must be boolean and rate must be positive');
    this.#lastOptions = { playing, rate };
    const record = this.#select(frame);
    if (!record || record.failed || record.video.readyState < 1) return;
    const { video } = record;
    try {
      const position = positionAt(frame, video);
      video.playbackRate = rate;
      if (Math.abs(video.currentTime - position) > (playing && record.requested ? .15 : .001)) video.currentTime = position;
      const ended = frame.time >= frame.scene.end || (Number.isFinite(video.duration) && frame.time - frame.scene.start >= video.duration);
      if (!playing || ended) { video.pause(); record.requested = false; return; }
      if (!record.requested) {
        record.requested = true;
        Promise.resolve(video.play()).catch(error => { if (!this.#disposed) this.#report(record, error); });
      }
    } catch (error) { this.#report(record, error); }
  }

  async seek(frame) {
    if (this.#disposed) return;
    this.#seekController?.abort();
    const controller = new AbortController(); this.#seekController = controller;
    this.#lastOptions = { ...this.#lastOptions, playing: false };
    const record = this.#select(frame); this.#pauseVideos();
    if (!record) return;
    const { video } = record;
    const deadline = Date.now() + 3000;
    try {
      if (record.failed) throw record.error;
      if (video.readyState < 1) await observed(video, ['loadedmetadata'], () => video.readyState >= 1, deadline, controller.signal);
      if (video.readyState < 2) await observed(video, ['loadeddata', 'canplay'], () => video.readyState >= 2, deadline, controller.signal);
      const position = positionAt(frame, video);
      if (!video.seeking && Math.abs(video.currentTime - position) <= .001) return;
      const change = Math.abs(video.currentTime - position) > .001;
      await observed(video, ['seeked'], () => !video.seeking && Math.abs(video.currentTime - position) < .025, deadline, controller.signal, change ? () => { video.currentTime = position; } : undefined);
    } catch (error) {
      if (error.name !== 'AbortError') { this.#report(record, error); throw error; }
      throw error;
    } finally { if (this.#seekController === controller) this.#seekController = null; }
  }

  pause() { this.#seekController?.abort(); this.#lastOptions = { ...this.#lastOptions, playing: false }; this.#pauseVideos(); }

  missing() {
    return this.#scenes.filter(scene => scene.captureKind === 'live-composer' && !this.#manifest.footage[scene.id]).map(({ id, start, end }) => ({ id, start, end }));
  }

  dispose() {
    if (this.#disposed) return;
    this.pause(); this.#disposed = true;
    for (const { video } of this.#videos.values()) { video.removeAttribute?.('src'); video.load(); }
    this.#videos.clear(); this.#overlay.remove();
  }

  #select(frame) {
    if (!frame?.scene || !this.#scenes.some(scene => scene.id === frame.scene.id) || !Number.isFinite(frame.time)) throw new TypeError('frame must contain a known scene and finite time');
    this.#lastFrame = frame;
    const capture = frame.scene.captureKind === 'live-composer';
    this.#overlay.hidden = !capture || this.#mode === 'none';
    this.#badge.hidden = true;
    if (this.#iframe) this.#iframe.hidden = true;
    for (const { video } of this.#videos.values()) video.hidden = true;
    if (!capture || this.#mode === 'none') { this.#active = null; this.#pauseVideos(); return null; }
    if (this.#mode === 'live') {
      this.#active = null; this.#pauseVideos();
      if (!this.#iframe) {
        this.#iframe = document.createElement('iframe'); this.#iframe.className = 'stage-composer';
        this.#iframe.title = 'Live gdl-web composer'; this.#iframe.src = browserUrl(this.#manifest.composerUrl);
        this.#overlay.append(this.#iframe);
      }
      this.#iframe.hidden = false; this.#state('live'); return null;
    }
    const url = this.#manifest.footage[frame.scene.id];
    if (!url) { this.#active = null; this.#pauseVideos(); this.#state('missing'); return null; }
    let record = this.#videos.get(frame.scene.id);
    if (!record) {
      const video = document.createElement('video'); video.className = 'stage-recorded'; video.preload = 'auto';
      video.playsInline = true; video.muted = true; video.loop = false; video.controls = false;
      record = { sceneId: frame.scene.id, video, requested: false, failed: false };
      video.addEventListener('error', () => this.#report(record, new Error(`Footage video failed: ${frame.scene.id}${video.error?.message ? ` (${video.error.message})` : ''}`)));
      video.addEventListener('loadedmetadata', () => { if (!this.#disposed && this.#active === record && !this.#seekController) this.sync(this.#lastFrame, this.#lastOptions); });
      video.src = browserUrl(url); this.#videos.set(frame.scene.id, record); this.#overlay.append(video);
    }
    this.#pauseVideos(record); this.#active = record;
    videoVisibility(record, !record.failed); this.#state(record.failed ? 'missing' : 'recorded'); return record;
  }

  #state(state) {
    this.#overlay.dataset.state = state;
    this.#overlay.className = state === 'missing' ? 'stage-footage stage-footage-missing' : 'stage-footage';
    this.#badge.hidden = state !== 'missing';
  }

  #pauseVideos(except = null) { for (const record of this.#videos.values()) if (record !== except) { record.video.pause(); record.requested = false; } }
  #report(record, error) {
    if (this.#disposed || record.failed) return;
    record.video.pause(); record.requested = false; record.failed = true; record.error = error;
    if (this.#active === record) { videoVisibility(record, false); this.#state('missing'); }
    this.#onError({ sceneId: record.sceneId, error });
  }
}

export function validateManifest(value, scenes) {
  object(value, 'manifest');
  if (!Array.isArray(scenes) || !scenes.length) throw new TypeError('scenes must contain a timeline');
  const sceneIds = new Set(); const turnIds = new Set();
  for (const scene of scenes) {
    if (!scene || typeof scene.id !== 'string' || !scene.id || sceneIds.has(scene.id)) throw new TypeError('scene IDs must be unique strings');
    if (!Number.isFinite(scene.start) || !Number.isFinite(scene.end) || scene.end <= scene.start || !Array.isArray(scene.turns)) throw new TypeError('scenes must contain valid intervals and turns');
    sceneIds.add(scene.id);
    for (const turn of scene.turns) {
      if (!turn || typeof turn.id !== 'string' || !turn.id || turnIds.has(turn.id)) throw new TypeError('turn IDs must be unique strings');
      turnIds.add(turn.id);
    }
  }
  for (const key of Object.keys(value)) if (!['composerUrl', 'voices', 'footage'].includes(key)) throw new TypeError(`unknown manifest key: ${key}`);
  const composerUrl = Object.hasOwn(value, 'composerUrl') ? urlString(value.composerUrl, 'composerUrl', false) : 'http://127.0.0.1:8089/';
  return { composerUrl, voices: mappings(value, 'voices', turnIds), footage: mappings(value, 'footage', sceneIds) };
}

function mappings(manifest, key, known) {
  if (!Object.hasOwn(manifest, key)) return {};
  object(manifest[key], key);
  return Object.fromEntries(Object.entries(manifest[key]).map(([id, url]) => {
    if (!known.has(id)) throw new RangeError(`unknown ${key} ID: ${id}`);
    return [id, urlString(url, `${key}.${id}`, true)];
  }));
}
function object(value, label) { if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) throw new TypeError(`${label} must be an object`); }
function urlString(value, label, media) {
  if (typeof value !== 'string' || !value || value.trim() !== value || /[\u0000-\u001f\u007f]/.test(value)) throw new TypeError(`${label} must be a nonempty URL string`);
  let url;
  try { url = new URL(value, 'http://film.invalid/'); } catch { throw new TypeError(`${label} must be a valid URL`); }
  if (!(media ? ['http:', 'https:', 'blob:'] : ['http:', 'https:']).includes(url.protocol)) throw new TypeError(`${label} uses an unsupported URL protocol`);
  return value;
}
function browserUrl(value) { return new URL(value, document.baseURI).href; }
function videoVisibility(record, visible) { record.video.hidden = !visible; }
function positionAt(frame, video) { const elapsed = Math.max(0, frame.time - frame.scene.start); return Number.isFinite(video.duration) ? Math.min(elapsed, Math.max(0, video.duration)) : elapsed; }

function observed(video, events, predicate, deadline, signal, trigger) {
  return new Promise((resolve, reject) => {
    let timer;
    const cleanup = () => { clearTimeout(timer); for (const event of events) video.removeEventListener(event, check); video.removeEventListener('error', fail); signal.removeEventListener('abort', abort); };
    const finish = error => { cleanup(); error ? reject(error) : resolve(); };
    const check = () => { if (predicate()) finish(); };
    const fail = () => finish(new Error('Footage video failed while seeking.'));
    const abort = () => { const error = new Error('Footage seek superseded.'); error.name = 'AbortError'; finish(error); };
    if (signal.aborted) return abort();
    if (!trigger && predicate()) return resolve();
    for (const event of events) video.addEventListener(event, check);
    video.addEventListener('error', fail); signal.addEventListener('abort', abort, { once: true });
    timer = setTimeout(() => finish(new Error('Footage video did not seek within 3 seconds.')), Math.max(0, deadline - Date.now()));
    try { trigger?.(); } catch (error) { finish(error); }
  });
}
