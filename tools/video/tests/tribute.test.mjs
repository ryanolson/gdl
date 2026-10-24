import assert from 'node:assert/strict';
import test from 'node:test';
import { appendTribute, tributeAt } from '../dist/tribute.mjs';
import { frameAt } from '../dist/cues.mjs';

const main = [{ id: 'reveal', start: 475, end: 505, turns: [] }];

test('the tribute is opt-in and preserves the main film timeline', () => {
  assert.equal(appendTribute(main, null), main);
  assert.throws(() => appendTribute(main, 'other'), /Unknown ending/);
  const scenes = appendTribute(main, 'hardfork');
  assert.equal(main.length, 1);
  assert.equal(scenes[0], main[0]);
  assert.equal(scenes.at(-1).start, 505);
  assert.equal(scenes.at(-1).end, 512);
  assert.equal(scenes.at(-1).captureKind, 'animation');
  assert.equal(frameAt(scenes, 504.9).scene.id, 'reveal');
  assert.equal(frameAt(scenes, 505).scene.id, 'hardfork');
});

test('archive cues have explicit source metadata and no TTS input', () => {
  const scene = appendTribute(main, 'hardfork').at(-1);
  assert.deepEqual(scene.turns.map(turn => turn.id), ['hardfork-casey', 'hardfork-kevin']);
  assert.deepEqual(scene.turns.map(turn => turn.speaker), ['Casey Newton', 'Kevin Roose']);
  for (const turn of scene.turns) {
    assert.equal(turn.source, 'archive');
    assert.equal(turn.model_id, null);
    assert.equal(turn.caption, 'Stop generating.');
    assert.equal(turn.text, undefined);
    assert.ok(turn.start >= scene.start && turn.end < scene.end);
    assert.equal(frameAt([scene], turn.start).turn.id, turn.id);
  }
});

test('guest entrances are staggered and every frame after the cut stays black', () => {
  const scene = appendTribute(main, 'hardfork').at(-1);
  const state = seconds => tributeAt(scene.start + seconds, scene);
  assert.equal(state(0).black, true);
  assert.equal(state(.5).casey.visible, true);
  assert.equal(state(.5).kevin.visible, false);
  assert.equal(state(1.5).kevin.visible, true);
  for (const time of [4.4, 5, 6.99, 7]) assert.equal(state(time).black, true);
  assert.deepEqual(state(2), state(2));
  assert.notDeepEqual(state(.5).casey, state(.55).casey);
  for (const actor of ['casey', 'kevin']) {
    assert.ok(Math.abs(state(4)[actor].rotation) < 1);
    assert.ok(Math.abs(state(4)[actor].offset) < 2);
  }
});
