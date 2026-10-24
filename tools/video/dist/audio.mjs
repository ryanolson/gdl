import { frameAt } from './cues.mjs';

export class VoiceScheduler {
  #scenes;
  #turnIds;
  #takes = new Map();
  #active = null;
  #onError;
  #epoch = 0;

  constructor({ scenes, onError = () => {} }) {
    if (!Array.isArray(scenes) || !scenes.length) throw new TypeError('scenes must contain the parsed timeline');
    if (typeof onError !== 'function') throw new TypeError('onError must be a function');
    const ids = scenes.flatMap(scene => scene.turns.map(turn => turn.id));
    if (ids.some(id => typeof id !== 'string') || new Set(ids).size !== ids.length) {
      throw new TypeError('turn IDs must be unique strings');
    }
    this.#scenes = scenes;
    this.#turnIds = new Set(ids);
    this.#onError = onError;
  }

  setTake(turnId, media) {
    if (!this.#turnIds.has(turnId)) throw new RangeError(`unknown turn ${turnId}`);
    if (!media || typeof media.play !== 'function' || typeof media.pause !== 'function' || !Number.isFinite(media.currentTime)) {
      throw new TypeError('media must provide play, pause and a finite currentTime');
    }
    this.removeTake(turnId);
    media.pause();
    this.#takes.set(turnId, media);
  }

  removeTake(turnId) {
    if (this.#active?.turnId === turnId) {
      this.pause();
      this.#active = null;
    }
    this.#takes.get(turnId)?.pause();
    this.#takes.delete(turnId);
  }

  clear() {
    this.pause();
    for (const media of this.#takes.values()) media.pause();
    this.#takes.clear();
    this.#active = null;
  }

  pause() {
    if (!this.#active) return;
    this.#active.media.pause();
    this.#active.requested = false;
    this.#active.blocked = false;
    this.#active.epoch = ++this.#epoch;
  }

  seek(time) {
    return this.sync({ time, playing: false });
  }

  sync({ time, playing, rate = 1 }) {
    if (!Number.isFinite(time)) throw new TypeError('time must be finite');
    if (typeof playing !== 'boolean') throw new TypeError('playing must be a boolean');
    if (!Number.isFinite(rate) || rate <= 0) throw new RangeError('rate must be finite and positive');
    const frame = frameAt(this.#scenes, time);
    const turnId = frame.turn?.id ?? null;
    const media = this.#takes.get(turnId);
    const offset = frame.turn ? frame.time - frame.turn.start : 0;
    if (this.#active?.turnId !== turnId || this.#active?.media !== media) {
      this.pause();
      this.#active = media ? { turnId, media, requested: false, blocked: false, epoch: ++this.#epoch } : null;
    }
    const result = status => ({ turnId, status, offset });
    if (!frame.turn) return result('silent');
    if (!media) return result('missing');
    const active = this.#active;
    try {
      const duration = Number.isFinite(media.duration) ? Math.max(0, media.duration) : Infinity;
      const position = Math.min(offset, duration);
      media.playbackRate = rate;
      // Small drift follows the media clock; a seek or stalled frame realigns it.
      if (!playing || !active.requested || Math.abs(media.currentTime - position) > 0.15) {
        media.currentTime = position;
      }
      if (offset >= duration) {
        this.pause();
        return result('ended');
      }
      if (!playing) {
        this.pause();
        return result('paused');
      }
      if (active.blocked) return result('blocked');
      if (!active.requested) {
        active.requested = true;
        const epoch = active.epoch;
        Promise.resolve(media.play()).catch(error => {
          if (this.#active === active && active.epoch === epoch) this.#block(active, error);
        });
      }
      return result('playing');
    } catch (error) {
      this.#block(active, error);
      return result('blocked');
    }
  }

  #block(active, error) {
    active.media.pause();
    active.requested = false;
    active.blocked = true;
    this.#onError({ turnId: active.turnId, error });
  }
}
