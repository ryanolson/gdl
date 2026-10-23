import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { parseScript } from '../dist/cues.mjs';
import { introductionAt } from '../dist/introductions.mjs';

const scene = parseScript(readFileSync(new URL('../../../agent-docs/video/script.md', import.meta.url), 'utf8'))[1];

test('each face gets an introduction hold before its first line', () => {
  assert.equal(introductionAt(scene, 24).speaker, 'Kyle');
  for (const turn of scene.turns.slice(1, 6)) {
    assert.equal(introductionAt(scene, turn.start - 1).speaker, turn.speaker);
    assert.equal(introductionAt(scene, turn.start).speaker, turn.speaker);
    assert.equal(introductionAt(scene, turn.end + .5).speaker, turn.speaker);
  }
});

test('returning dialogue and narration do not repeat an introduction', () => {
  assert.equal(introductionAt(scene, 64), null);
  assert.equal(introductionAt(scene, 72), null);
  assert.equal(introductionAt({ ...scene, id: 'catalog' }, 30), null);
  assert.equal(introductionAt(scene, 80), null);
});
