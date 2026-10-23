const SCENE_IDS = ['opening', 'interruption', 'catalog', 'types', 'acme', 'deployment', 'nim', 'stargate', 'receipts', 'reveal'];
const SPEAKERS = new Set(['Narrator', 'Ishan', 'Alec', 'John T', 'Kyle', 'Graham', 'Ryan']);
const INTERRUPTION = [
  ['Kyle', 24, 28], ['Ishan', 30, 34], ['Ryan', 36, 39], ['John T', 42, 46],
  ['Graham', 49, 53], ['Alec', 56, 60], ['Ryan', 63, 67], ['Narrator', 70, 80],
];

export function parseScript(markdown) {
  if (typeof markdown !== 'string') throw new TypeError('markdown must be a string');
  const scenes = [];
  let current = null;
  for (const [index, raw] of markdown.split(/\r?\n/).entries()) {
    const line = raw.trim();
    const context = `line ${index + 1}`;
    if (/^##\s+\d/.test(line)) {
      const match = /^## (\d{2}):(\d{2})–(\d{2}):(\d{2}) — (.+)$/.exec(line);
      if (!match) throw new Error(`${context}: malformed timed heading`);
      const start = timestamp(match[1], match[2], context);
      const end = timestamp(match[3], match[4], context);
      if (end <= start) throw new Error(`${context}: scene end must be after its start`);
      current = { title: match[5], start, end, turns: [] };
      scenes.push(current);
      continue;
    }
    if (/^##\s/.test(line)) current = null;
    if (!line.startsWith('**')) continue;
    const turn = /^\*\*([^*]+):\*\*\s+“(.*)”$/.exec(line);
    if (!turn) throw new Error(`${context}: malformed dialogue; expected a speaker and quoted speech`);
    if (!current) throw new Error(`${context}: dialogue is outside a timed scene`);
    if (!SPEAKERS.has(turn[1])) throw new Error(`${context}: unknown speaker ${turn[1]}`);
    const caption = captionFor(turn[2], context);
    current.turns.push({ speaker: turn[1], text: turn[2], caption });
  }
  if (scenes.length !== SCENE_IDS.length) throw new Error('script must contain exactly 10 timed scenes');
  if (scenes[0].start !== 0) throw new Error('script must start at 0 seconds');
  scenes.forEach((scene, i) => {
    scene.id = SCENE_IDS[i];
    const previous = scenes[i - 1];
    if (previous && scene.start < previous.end) throw new Error(`${scene.id}: scenes overlap`);
    if (previous && scene.start > previous.end) throw new Error(`${scene.id}: gap between scenes`);
    if (!scene.turns.length) throw new Error(`${scene.id}: scene contains no dialogue`);
    scene.captureKind = i >= 2 && i <= 8 ? 'live-composer' : 'animation';
    if (scene.captureKind === 'live-composer') scene.captureStatus = 'required';
    scene.timing = { status: 'provisional', source: i < 2 ? 'editorial' : 'word-count' };
    scene.turns.forEach((turn, turnIndex) => { turn.id = `${scene.id}-${turnIndex + 1}`; });
    if (i === 0) {
      if (scene.start !== 0 || scene.end !== 24 || scene.turns.length !== 1 || scene.turns[0].speaker !== 'Narrator') {
        throw new Error('opening: editorial timings require one Narrator turn in 0–24 seconds');
      }
      Object.assign(scene.turns[0], { start: 2, end: 22 });
    } else if (i === 1) {
      if (scene.start !== 24 || scene.end !== 80 || scene.turns.length !== INTERRUPTION.length) {
        throw new Error('interruption: editorial timings require eight turns in 24–80 seconds');
      }
      scene.turns.forEach((turn, turnIndex) => {
        const [speaker, start, end] = INTERRUPTION[turnIndex];
        if (turn.speaker !== speaker) throw new Error(`interruption: speaker ${turnIndex + 1} must be ${speaker}`);
        Object.assign(turn, { start, end });
      });
      const seen = new Set(['Narrator']);
      scene.introductions = scene.turns.filter(turn => {
        if (seen.has(turn.speaker)) return false;
        seen.add(turn.speaker);
        return true;
      }).map(turn => ({
        speaker: turn.speaker,
        start: turn.speaker === 'Kyle' ? turn.start : turn.start - 1,
        end: turn.speaker === 'Kyle' ? turn.end : turn.end + 1,
      }));
    } else {
      distributeTurns(scene);
    }
  });
  return scenes;
}

export function frameAt(scenes, time) {
  if (!Array.isArray(scenes) || !scenes.length) throw new TypeError('scenes must contain the parsed timeline');
  if (!Number.isFinite(time)) throw new TypeError('time must be finite');
  const finalScene = scenes.at(-1);
  const position = Math.max(scenes[0].start, Math.min(finalScene.end, time));
  const scene = position === finalScene.end ? finalScene : scenes.find(item => position >= item.start && position < item.end);
  if (!scene) throw new RangeError('time lies outside the scene intervals');
  const turn = scene.turns.find(item => position >= item.start && position < item.end) ?? null;
  return { time: position, scene, turn };
}

function timestamp(minutes, seconds, context) {
  if (Number(seconds) >= 60) throw new Error(`${context}: timestamp seconds must be less than 60`);
  return Number(minutes) * 60 + Number(seconds);
}

function captionFor(text, context) {
  let cueStart = -1;
  for (let i = 0; i < text.length; i += 1) {
    if (text[i] === '[') {
      if (cueStart !== -1) throw new Error(`${context}: nested delivery cue`);
      cueStart = i;
    } else if (text[i] === ']') {
      if (cueStart === -1) throw new Error(`${context}: unexpected ] outside a delivery cue`);
      if (!text.slice(cueStart + 1, i).trim()) throw new Error(`${context}: empty delivery cue`);
      cueStart = -1;
    }
  }
  if (cueStart !== -1) throw new Error(`${context}: unclosed delivery cue`);
  if (!text.startsWith('[')) throw new Error(`${context}: speech must start with a delivery cue`);
  const caption = text.replace(/\[[^\[\]]+\]\s*/g, '').replace(/\s+/g, ' ').trim();
  if (!caption) throw new Error(`${context}: dialogue contains no spoken words`);
  return caption;
}

function distributeTurns(scene) {
  // These holds reserve reading time until recorded takes supply actual durations.
  const lead = 1;
  const tail = 2.5;
  const gap = 0.7;
  const available = scene.end - scene.start - lead - tail - gap * (scene.turns.length - 1);
  if (available <= 0) throw new Error(`${scene.id}: scene has no room for provisional dialogue and holds`);
  const weights = scene.turns.map(turn => Math.max(6, turn.caption.split(/\s+/).length));
  const total = weights.reduce((sum, value) => sum + value, 0);
  let position = scene.start + lead;
  scene.turns.forEach((turn, i) => {
    turn.start = position;
    turn.end = position + available * weights[i] / total;
    position = turn.end + gap;
  });
}
