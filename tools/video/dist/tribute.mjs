export function appendTribute(scenes, ending) {
  if (!ending) return scenes;
  if (ending !== 'hardfork') throw new RangeError(`Unknown ending: ${ending}`);
  const start = scenes.at(-1).end;
  const turn = (id, speaker, from, to) => ({
    id, speaker, start: start + from, end: start + to,
    caption: 'Stop generating.', source: 'archive', model_id: null,
  });
  return [...scenes, {
    id: 'hardfork', title: 'A nod to Hard Fork', start, end: start + 7,
    captureKind: 'animation', timing: { status: 'provisional', source: 'editorial' },
    turns: [turn('hardfork-casey', 'Casey Newton', 1.9, 2.9), turn('hardfork-kevin', 'Kevin Roose', 3.1, 4.2)],
  }];
}

export function tributeAt(time, scene) {
  const elapsed = time - scene.start;
  const guest = (entrance, direction) => {
    const age = elapsed - entrance;
    return {
      visible: age >= 0,
      offset: age < 0 ? 900 : 900 * Math.exp(-5 * age) * Math.cos(9 * age),
      rotation: age < 0 ? 0 : direction * 18 * Math.exp(-2.2 * age) * Math.sin(12 * age),
    };
  };
  return { black: time < scene.start + .35 || time >= scene.start + 4.4, casey: guest(.35, -1), kevin: guest(.9, 1) };
}

export function createTributeRenderer(stage, { assetBase = '/assets/' } = {}) {
  const root = document.createElement('section');
  root.className = 'film-tribute'; root.hidden = true;
  const label = document.createElement('p'); label.className = 'film-tribute-label';
  label.textContent = 'A NOD TO HARD FORK'; root.append(label);
  const guests = new Map();
  const ready = [];
  for (const [id, name, shirt] of [['casey', 'Casey Newton', '#4c7a75'], ['kevin', 'Kevin Roose', '#6c7f9e']]) {
    const actor = document.createElement('div'); actor.className = `film-guest film-guest-${id}`;
    actor.dataset.guest = id; actor.style.setProperty('--guest-shirt', shirt);
    const body = document.createElement('div'); body.className = 'film-guest-body';
    const head = document.createElement('img'); head.className = 'film-guest-head'; head.alt = `${name}, cartoon bobblehead`;
    ready.push(new Promise((resolve, reject) => {
      head.onload = resolve; head.onerror = () => reject(new Error(`Unable to load ${name}'s portrait.`));
    }));
    head.src = new URL(`${id}/neutral.png`, new URL(assetBase, document.baseURI)).href;
    const tag = document.createElement('span'); tag.className = 'film-guest-name'; tag.textContent = name;
    actor.append(body, head, tag); root.append(actor); guests.set(id, { actor, head });
  }
  stage.append(root);
  return {
    ready: Promise.all(ready),
    render(frame) {
      root.hidden = frame.scene.id !== 'hardfork';
      if (root.hidden) return;
      const state = tributeAt(frame.time, frame.scene);
      root.dataset.black = String(state.black);
      for (const [id, { actor, head }] of guests) {
        actor.hidden = !state[id].visible;
        actor.style.transform = `translateY(${state[id].offset}px)`;
        head.style.transform = `rotate(${state[id].rotation}deg)`;
        actor.classList.toggle('film-guest-speaking', frame.turn?.id === `hardfork-${id}`);
      }
    },
    dispose() { root.remove(); },
  };
}
