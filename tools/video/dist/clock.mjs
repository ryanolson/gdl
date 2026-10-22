export class TimelineClock {
  #duration;
  #now;
  #anchor;
  #time = 0;
  #playing = false;
  #rate = 1;

  constructor({ duration, now = () => performance.now() / 1000 }) {
    requirePositive(duration, 'duration');
    if (typeof now !== 'function') throw new TypeError('now must be a function');
    this.#duration = duration;
    this.#now = now;
    this.#anchor = this.#readNow();
  }

  play() {
    const now = this.#readNow();
    this.#reanchor(now);
    this.#playing = this.#time < this.#duration;
  }

  pause() {
    this.#reanchor(this.#readNow());
    this.#playing = false;
  }

  seek(seconds) {
    requireFinite(seconds, 'time');
    this.#reanchor(this.#readNow());
    this.#time = this.#clamp(seconds);
    if (this.#time === this.#duration) this.#playing = false;
  }

  setRate(rate) {
    requirePositive(rate, 'rate');
    this.#reanchor(this.#readNow());
    this.#rate = rate;
  }

  snapshot() {
    const time = this.#sample(this.#readNow());
    return { time, duration: this.#duration, playing: this.#playing, rate: this.#rate };
  }

  #readNow() {
    const now = this.#now();
    requireFinite(now, 'now');
    return now;
  }

  #clamp(time) {
    return Math.min(this.#duration, Math.max(0, time));
  }

  #sample(now) {
    if (!this.#playing) return this.#time;
    // Sampling from one anchor keeps frame frequency out of the timeline arithmetic.
    const time = this.#clamp(this.#time + (now - this.#anchor) * this.#rate);
    if (time === this.#duration) {
      this.#time = time;
      this.#anchor = now;
      this.#playing = false;
    }
    return time;
  }

  #reanchor(now) {
    this.#time = this.#sample(now);
    this.#anchor = now;
  }
}

function requireFinite(value, name) {
  if (!Number.isFinite(value)) throw new TypeError(`${name} must be finite`);
}

function requirePositive(value, name) {
  requireFinite(value, name);
  if (value <= 0) throw new RangeError(`${name} must be positive`);
}
