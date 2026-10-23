import assert from 'node:assert/strict';
import test from 'node:test';
import { VoiceScheduler } from '../dist/audio.mjs';

const scenes = [{ start: 0, end: 10, turns: [
  { id: 'first', start: 1, end: 4 }, { id: 'second', start: 5, end: 8 },
] }];

function media(duration = 20) {
  return {
    duration, currentTime: 0, playbackRate: 1, paused: true, plays: 0, pauses: 0,
    play() { this.plays += 1; this.paused = false; return Promise.resolve(); },
    pause() { this.pauses += 1; this.paused = true; },
  };
}

test('registered takes follow the stage offset and playback rate without repeated play calls', () => {
  const scheduler = new VoiceScheduler({ scenes });
  const take = media();
  scheduler.setTake('first', take);
  assert.equal(take.paused, true);
  assert.deepEqual(scheduler.sync({ time: 1.5, playing: true, rate: 1.5 }), { turnId: 'first', status: 'playing', offset: 0.5 });
  assert.equal(take.currentTime, 0.5);
  assert.equal(take.playbackRate, 1.5);
  assert.equal(take.plays, 1);
  take.currentTime = 0.58;
  scheduler.sync({ time: 1.6, playing: true, rate: 1.5 });
  assert.equal(take.currentTime, 0.58);
  assert.equal(take.plays, 1);
  scheduler.sync({ time: 2.5, playing: true });
  assert.equal(take.currentTime, 1.5);
});

test('seeking pauses immediately and a paused stage never starts a take', () => {
  const scheduler = new VoiceScheduler({ scenes });
  const take = media();
  scheduler.setTake('first', take);
  scheduler.sync({ time: 2, playing: true });
  assert.equal(take.paused, false);
  assert.deepEqual(scheduler.seek(3), { turnId: 'first', status: 'paused', offset: 2 });
  assert.equal(take.currentTime, 2);
  assert.equal(take.paused, true);
  scheduler.sync({ time: 3.2, playing: false });
  assert.equal(take.currentTime, 2.2);
  assert.equal(take.plays, 1);
  scheduler.sync({ time: 3.2, playing: true });
  assert.equal(take.plays, 2);
  scheduler.pause();
  assert.equal(take.paused, true);
});

test('scene holds, missing takes and turn changes cannot leak the prior voice', () => {
  const scheduler = new VoiceScheduler({ scenes });
  const first = media();
  const second = media();
  scheduler.setTake('first', first);
  assert.deepEqual(scheduler.sync({ time: 0, playing: true }), { turnId: null, status: 'silent', offset: 0 });
  scheduler.sync({ time: 2, playing: true });
  assert.equal(scheduler.sync({ time: 4, playing: true }).status, 'silent');
  assert.equal(first.paused, true);
  assert.deepEqual(scheduler.sync({ time: 5.5, playing: true }), { turnId: 'second', status: 'missing', offset: 0.5 });
  scheduler.setTake('second', second);
  scheduler.sync({ time: 5.5, playing: true });
  assert.equal(second.currentTime, 0.5);
  assert.equal(second.paused, false);
  scheduler.sync({ time: 2, playing: true });
  assert.equal(second.paused, true);
  assert.equal(first.currentTime, 1);
  assert.equal(first.paused, false);
  scheduler.sync({ time: 10, playing: true });
  assert.equal(first.paused, true);
});

test('short takes stop at their actual duration and never loop to fill a provisional cue', () => {
  const scheduler = new VoiceScheduler({ scenes });
  const take = media(1);
  scheduler.setTake('first', take);
  assert.deepEqual(scheduler.sync({ time: 3, playing: true }), { turnId: 'first', status: 'ended', offset: 2 });
  assert.equal(take.currentTime, 1);
  assert.equal(take.plays, 0);
  assert.equal(take.paused, true);
});

test('autoplay rejection is reported once and can be retried after pausing', async () => {
  const errors = [];
  const scheduler = new VoiceScheduler({ scenes, onError: issue => errors.push(issue) });
  const take = media();
  take.play = function () { this.plays += 1; return Promise.reject(new Error('autoplay blocked')); };
  scheduler.setTake('first', take);
  scheduler.sync({ time: 2, playing: true });
  await Promise.resolve();
  assert.equal(scheduler.sync({ time: 2.1, playing: true }).status, 'blocked');
  assert.equal(take.plays, 1);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].turnId, 'first');
  scheduler.sync({ time: 2.1, playing: false });
  take.play = function () { this.plays += 1; this.paused = false; return Promise.resolve(); };
  assert.equal(scheduler.sync({ time: 2.1, playing: true }).status, 'playing');
  assert.equal(take.plays, 2);
});

test('late failures from replaced takes cannot affect the current voice', async () => {
  const errors = [];
  const scheduler = new VoiceScheduler({ scenes, onError: issue => errors.push(issue) });
  let reject;
  const first = media();
  first.play = () => new Promise((resolve, fail) => { reject = fail; });
  scheduler.setTake('first', first);
  scheduler.sync({ time: 2, playing: true });
  const replacement = media();
  scheduler.setTake('first', replacement);
  scheduler.sync({ time: 2, playing: true });
  reject(new Error('stale rejection'));
  await Promise.resolve();
  assert.equal(errors.length, 0);
  assert.equal(scheduler.sync({ time: 2.1, playing: true }).status, 'playing');
  assert.equal(replacement.paused, false);
  scheduler.removeTake('first');
  assert.equal(replacement.paused, true);
  assert.equal(scheduler.sync({ time: 2, playing: true }).status, 'missing');
});

test('invalid inputs fail before changing playback and clear pauses and removes every take', () => {
  const scheduler = new VoiceScheduler({ scenes });
  const take = media();
  scheduler.setTake('first', take);
  scheduler.sync({ time: 2, playing: true });
  assert.throws(() => scheduler.setTake('unknown', media()), /unknown turn/i);
  assert.throws(() => scheduler.setTake('first', 'take.wav'), /media/i);
  for (const snapshot of [
    { time: NaN, playing: true }, { time: 2, playing: 'yes' },
    { time: 2, playing: true, rate: 0 }, { time: 2, playing: true, rate: Infinity },
  ]) assert.throws(() => scheduler.sync(snapshot), /time|playing|rate/);
  assert.equal(take.paused, false);
  scheduler.clear();
  assert.equal(take.paused, true);
  assert.equal(scheduler.sync({ time: 2, playing: true }).status, 'missing');
});
