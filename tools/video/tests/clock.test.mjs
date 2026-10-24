import assert from 'node:assert/strict';
import test from 'node:test';
import { TimelineClock } from '../dist/clock.mjs';

function fixture(duration = 10) {
  let seconds = 100;
  const clock = new TimelineClock({ duration, now: () => seconds });
  return { clock, advance: (delta) => { seconds += delta; } };
}

test('a paused clock stays at its seek position', () => {
  const { clock, advance } = fixture();
  assert.deepEqual(clock.snapshot(), { time: 0, duration: 10, playing: false, rate: 1 });
  clock.seek(3);
  advance(5);
  assert.equal(clock.snapshot().time, 3);
  clock.play();
  advance(2);
  clock.pause();
  advance(20);
  assert.deepEqual(clock.snapshot(), { time: 5, duration: 10, playing: false, rate: 1 });
});

test('play, seek, and rate changes preserve elapsed time without drift', () => {
  const { clock, advance } = fixture(100);
  clock.play();
  advance(2);
  assert.equal(clock.snapshot().time, 2);
  clock.play();
  advance(1);
  assert.equal(clock.snapshot().time, 3);
  clock.setRate(2);
  advance(3);
  assert.deepEqual(clock.snapshot(), { time: 9, duration: 100, playing: true, rate: 2 });
  clock.seek(20);
  advance(0.5);
  assert.equal(clock.snapshot().time, 21);
  clock.setRate(0.5);
  advance(4);
  assert.equal(clock.snapshot().time, 23);
  clock.pause();
  clock.setRate(4);
  advance(2);
  assert.equal(clock.snapshot().time, 23);
  clock.play();
  advance(1);
  assert.equal(clock.snapshot().time, 27);
});

test('sampling does not move the anchor or accumulate drift', () => {
  const { clock, advance } = fixture(100);
  clock.setRate(1.5);
  clock.play();
  for (let i = 0; i < 100; i += 1) {
    advance(0.125);
    assert.equal(clock.snapshot().time, (i + 1) * 0.125 * 1.5);
  }
});

test('reaching the end stops playback until the clock is sought backward', () => {
  const { clock, advance } = fixture();
  clock.play();
  advance(12);
  assert.deepEqual(clock.snapshot(), { time: 10, duration: 10, playing: false, rate: 1 });
  advance(5);
  clock.play();
  assert.equal(clock.snapshot().playing, false);
  clock.seek(2);
  assert.equal(clock.snapshot().playing, false);
  clock.play();
  advance(1);
  assert.equal(clock.snapshot().time, 3);
});

test('seeks clamp to the film and a seek to the end stops playback', () => {
  const { clock, advance } = fixture();
  clock.seek(-4);
  assert.equal(clock.snapshot().time, 0);
  clock.seek(20);
  assert.equal(clock.snapshot().time, 10);
  clock.seek(4);
  clock.play();
  clock.seek(12);
  advance(1);
  assert.deepEqual(clock.snapshot(), { time: 10, duration: 10, playing: false, rate: 1 });
});

test('mutations observe completion before changing position or rate', () => {
  const { clock, advance } = fixture();
  clock.play();
  advance(11);
  clock.seek(2);
  assert.equal(clock.snapshot().playing, false);
  clock.play();
  advance(11);
  clock.setRate(2);
  assert.deepEqual(clock.snapshot(), { time: 10, duration: 10, playing: false, rate: 2 });
});

test('invalid duration, rate, seek time and clock source fail before mutation', () => {
  for (const duration of [0, -1, NaN, Infinity, -Infinity, '10', undefined]) {
    assert.throws(() => new TimelineClock({ duration }), /duration/);
  }
  assert.throws(() => new TimelineClock({ duration: 10, now: 2 }), /now/);
  assert.throws(() => new TimelineClock({ duration: 10, now: () => NaN }), /now/);
  const { clock } = fixture();
  clock.seek(3);
  for (const value of [NaN, Infinity, -Infinity, '2', undefined]) {
    assert.throws(() => clock.seek(value), /time/);
  }
  for (const value of [0, -1, NaN, Infinity, -Infinity, '2', undefined]) {
    assert.throws(() => clock.setRate(value), /rate/);
  }
  assert.deepEqual(clock.snapshot(), { time: 3, duration: 10, playing: false, rate: 1 });
});

test('non-finite now samples fail without corrupting the clock', () => {
  let seconds = 1;
  const clock = new TimelineClock({ duration: 10, now: () => seconds });
  clock.play();
  seconds = Infinity;
  for (const operation of [() => clock.snapshot(), () => clock.play(), () => clock.pause(), () => clock.seek(3), () => clock.setRate(2)]) {
    assert.throws(operation, /now/);
  }
  seconds = 3;
  assert.deepEqual(clock.snapshot(), { time: 2, duration: 10, playing: true, rate: 1 });
});
