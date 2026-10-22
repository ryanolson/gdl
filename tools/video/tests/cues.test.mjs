import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { frameAt, parseScript } from '../dist/cues.mjs';

const markdown = await readFile(new URL('../../../agent-docs/video/script.md', import.meta.url), 'utf8');
const ids = ['opening', 'interruption', 'catalog', 'types', 'acme', 'deployment', 'nim', 'stargate', 'receipts', 'reveal'];

test('the production script yields ten scenes and preserves all tagged dialogue', () => {
  const scenes = parseScript(markdown);
  assert.deepEqual(scenes.map(scene => scene.id), ids);
  assert.equal(scenes.flatMap(scene => scene.turns).length, 39);
  assert.deepEqual(scenes.map(scene => [scene.start, scene.end]), [
    [0, 24], [24, 80], [80, 130], [130, 170], [170, 225],
    [225, 315], [315, 360], [360, 430], [430, 475], [475, 505],
  ]);
  assert.equal(scenes[0].title, 'The graph was already there');
  assert.match(scenes[1].turns[0].text, /^\[jumping in[^\]]*\] Wait\./);
  assert.equal(scenes[1].turns[0].caption, 'Wait. Who decides where it runs?');
  assert.equal(scenes[1].turns[0].speaker, 'Kyle');
  for (const scene of scenes) {
    assert.equal(scene.timing.status, 'provisional');
    scene.turns.forEach((turn, i) => {
      assert.equal(turn.id, `${scene.id}-${i + 1}`);
      assert.match(turn.text, /^\[/);
      assert.doesNotMatch(turn.caption, /[\[\]]/);
      assert.ok(turn.start >= scene.start && turn.end <= scene.end);
      assert.ok(turn.end > turn.start);
      if (i) assert.ok(turn.start >= scene.turns[i - 1].end);
    });
  }
});

test('opening cues retain the editorial interruption schedule and readable holds', () => {
  const [opening, interruption] = parseScript(markdown);
  assert.deepEqual(opening.turns.map(turn => [turn.start, turn.end]), [[2, 22]]);
  assert.deepEqual(interruption.turns.map(turn => [turn.speaker, turn.start, turn.end]), [
    ['Kyle', 24, 28], ['Ishan', 30, 34], ['Ryan', 36, 39], ['John T', 42, 46],
    ['Graham', 49, 53], ['Alec', 56, 60], ['Ryan', 63, 67], ['Narrator', 70, 80],
  ]);
  assert.equal(opening.timing.source, 'editorial');
  assert.equal(interruption.timing.source, 'editorial');
});

test('each character receives one introduction window before later speaking turns', () => {
  const [, interruption] = parseScript(markdown);
  assert.deepEqual(interruption.introductions, [
    { speaker: 'Kyle', start: 24, end: 28 },
    { speaker: 'Ishan', start: 29, end: 35 },
    { speaker: 'Ryan', start: 35, end: 40 },
    { speaker: 'John T', start: 41, end: 47 },
    { speaker: 'Graham', start: 48, end: 54 },
    { speaker: 'Alec', start: 55, end: 61 },
  ]);
});

test('other dialogue uses word counts within its scene and leaves result holds', () => {
  const scenes = parseScript(markdown);
  for (const scene of scenes.slice(2)) {
    assert.equal(scene.timing.source, 'word-count');
    assert.ok(scene.turns[0].start > scene.start);
    assert.ok(scene.turns.at(-1).end < scene.end);
    for (let i = 1; i < scene.turns.length; i += 1) {
      assert.ok(scene.turns[i].start > scene.turns[i - 1].end);
    }
  }
  const catalog = scenes[2];
  assert.ok(catalog.turns[1].end - catalog.turns[1].start > catalog.turns[0].end - catalog.turns[0].start);
  const expanded = parseScript(markdown.replace(catalog.turns[0].text, Array(12).fill(catalog.turns[0].text).join(' ')))[2];
  assert.ok(expanded.turns[0].end - expanded.turns[0].start > catalog.turns[0].end - catalog.turns[0].start);
  assert.ok(expanded.turns.at(-1).end < expanded.end);
});

test('technical scenes require real composer footage without claiming a recording exists', () => {
  const scenes = parseScript(markdown);
  for (const scene of scenes.slice(2, 9)) {
    assert.equal(scene.captureKind, 'live-composer');
    assert.equal(scene.captureStatus, 'required');
  }
  for (const scene of [scenes[0], scenes[1], scenes[9]]) {
    assert.equal(scene.captureKind, 'animation');
  }
});

test('frame lookup respects scene boundaries, silent holds, seeking and the final frame', () => {
  const scenes = parseScript(markdown);
  assert.equal(frameAt(scenes, -10).time, 0);
  assert.equal(frameAt(scenes, 0).scene.id, 'opening');
  assert.equal(frameAt(scenes, 0).turn, null);
  assert.equal(frameAt(scenes, 2).turn.id, 'opening-1');
  assert.equal(frameAt(scenes, 22).turn, null);
  assert.equal(frameAt(scenes, 24).turn.speaker, 'Kyle');
  assert.equal(frameAt(scenes, 28).turn, null);
  assert.equal(frameAt(scenes, 30).turn.speaker, 'Ishan');
  assert.equal(frameAt(scenes, 36).turn.speaker, 'Ryan');
  assert.equal(frameAt(scenes, 40).turn, null);
  assert.equal(frameAt(scenes, 80).scene.id, 'catalog');
  assert.equal(frameAt(scenes, 2).turn.id, 'opening-1');
  assert.equal(frameAt(scenes, 505).scene.id, 'reveal');
  assert.equal(frameAt(scenes, 505).turn, null);
  assert.equal(frameAt(scenes, 1000).time, 505);
  for (const time of [NaN, Infinity, -Infinity, '2', undefined]) {
    assert.throws(() => frameAt(scenes, time), /time must be finite/);
  }
  assert.throws(() => frameAt([], 0), /scenes/);
});

test('the final heading supplies the duration instead of a fixed film length', () => {
  const scenes = parseScript(markdown.replace('07:55–08:25', '07:55–08:37'));
  assert.equal(scenes.at(-1).end, 517);
  assert.equal(frameAt(scenes, 999).time, 517);
  assert.equal(frameAt(scenes, 517).turn, null);
});

test('malformed, overlapping, missing and noncontiguous scene headings fail explicitly', () => {
  assert.throws(() => parseScript(null), /markdown must be a string/);
  assert.throws(() => parseScript(markdown.replace('00:24–01:20', '00:24 to 01:20')), /malformed timed heading/i);
  assert.throws(() => parseScript(markdown.replace('01:20–02:10', '01:19–02:10')), /overlap/i);
  assert.throws(() => parseScript(markdown.replace('01:20–02:10', '01:21–02:10')), /gap/i);
  assert.throws(() => parseScript(markdown.replace('01:20–02:10', '01:20–02:60')), /seconds/i);
  assert.throws(() => parseScript(markdown.replace('01:20–02:10', '02:10–01:20')), /end.*after.*start/i);
  assert.throws(() => parseScript(markdown.replace(/^## 01:20.*\n/m, '')), /10 timed scenes/i);
  assert.throws(() => parseScript(markdown.replace('00:00–00:24', '00:01–00:24')), /start.*0|0.*start/i);
});

test('invalid or missing delivery cues cannot leak into captions silently', () => {
  const source = markdown.match(/^\*\*John T:\*\* “.*”$/m)[0];
  for (const [replacement, error] of [
    ['[curious What can we inspect before we build?', /unclosed.*cue/i],
    ['[[curious]] What can we inspect before we build?', /nested.*cue/i],
    ['[] What can we inspect before we build?', /empty.*cue/i],
    ['[curious] What can we inspect] before we build?', /unexpected.*\]/i],
    ['What can we inspect before we build?', /delivery cue/i],
  ]) {
    assert.throws(() => parseScript(markdown.replace(source, `**John T:** “${replacement}”`)), error);
  }
  assert.throws(() => parseScript(markdown.replace(source, source.replace('**John T:**', '**Nobody:**'))), /unknown speaker/i);
  assert.throws(() => parseScript(markdown.replace(source, source.slice(0, -1))), /malformed dialogue/i);
});

test('an altered opening cast order fails rather than applying cues to the wrong speaker', () => {
  const source = markdown.match(/^\*\*Kyle:\*\* “.*”$/m)[0];
  assert.throws(() => parseScript(markdown.replace(source, source.replace('**Kyle:**', '**Ryan:**'))), /interruption.*speaker/i);
});
