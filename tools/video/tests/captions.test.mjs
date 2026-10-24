import test from 'node:test';
import assert from 'node:assert/strict';
import { captionAt } from '../dist/captions.mjs';

test('captions preserve words across compact timed chunks', () => {
  const caption = 'one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty';
  const turn = { caption, start: 10, end: 30 };
  const a = captionAt(turn, 10);
  const b = captionAt(turn, 29.99);
  assert.equal(`${a} ${b}`, caption);
  assert.ok(a.split(' ').length <= 12);
  assert.ok(b.split(' ').length <= 12);
});

test('caption edges and holds never retain the preceding line', () => {
  const turn = { caption: 'A short line.', start: 4, end: 7 };
  assert.equal(captionAt(null, 5), '');
  assert.equal(captionAt(turn, 3), '');
  assert.equal(captionAt(turn, 4), 'A short line.');
  assert.equal(captionAt(turn, 7), '');
});
