import { introductionAt } from './introductions.mjs';

const CAST = [
  ['ishan', 'Ishan', '#27878b'], ['john', 'John T', '#a43d4a'],
  ['kyle', 'Kyle', '#415c76'], ['graham', 'Graham', '#444b4b'],
  ['alec', 'Alec', '#687f98'], ['ryan', 'Ryan', '#6c7664'],
];
const LABELS = {
  opening: ['THE ORIGINAL IDEA', 'The graph was already there'],
  interruption: ['THE ENGINEERING REALITY', 'Some assembly required.'],
  catalog: ['01 / COMPOSE THE CATALOG', 'Good ideas travel in crates.'],
  types: ['02 / CONNECT THE CONTRACTS', 'The types have to agree.'],
  acme: ['03 / CUSTOMIZE THE REFERENCE', 'Your graph. Your nodes.'],
  deployment: ['04 / CHOOSE THE BOUNDARIES', 'One graph. Different placements.'],
  nim: ['CONCEPT / SINGLE PROCESS', 'One serving process.'],
  stargate: ['CONCEPT / GLOBAL ROUTING', 'Move the boundary. Keep the contract.'],
  receipts: ['05 / SHOW THE RECEIPTS', 'From source to a running graph.'],
  reveal: ['GRAPH DEFINITION LIBRARY', 'CUT HERE'],
};

export function createRenderer(stageElement, { assetBase = '../../../agent-docs/video/assets/', states = {} } = {}) {
  const root = element('div', 'film-root');
  const backdrop = element('div', 'film-backdrop');
  const stars = element('div', 'film-stars');
  stars.innerHTML = starfield();
  const paper = element('div', 'film-paper');
  const panel = element('section', 'film-panel');
  const cast = element('div', 'film-cast');
  const actors = new Map();
  const loaded = new Set();
  const pending = [];
  let disposed = false;
  let activeId = '';
  let refs = {};

  const cssUrl = new URL('./scene.css', import.meta.url).href;
  let stylesheet = document.querySelector('link[data-gdl-film-style]');
  if (!stylesheet) {
    stylesheet = document.createElement('link');
    stylesheet.rel = 'stylesheet'; stylesheet.href = cssUrl; stylesheet.dataset.gdlFilmStyle = '';
    pending.push(new Promise((resolve, reject) => { stylesheet.onload = resolve; stylesheet.onerror = () => reject(new Error('Unable to load scene.css')); }));
    document.head.append(stylesheet);
  }
  for (const [name, label, color] of CAST) {
    const node = element('div', 'film-actor');
    node.dataset.actor = name; node.style.setProperty('--shirt', color);
    const body = element('div', 'film-actor-body');
    const head = element('img', 'film-actor-head');
    head.alt = `${label}, paper cartoon`; head.draggable = false;
    const tag = element('div', 'film-actor-name', label);
    const urls = { neutral: new URL(`${name}/neutral.png`, new URL(assetBase, document.baseURI)).href, ...states[name] };
    for (const url of new Set(Object.values(urls))) {
      const image = new Image();
      pending.push(new Promise((resolve, reject) => {
        image.onload = () => { loaded.add(url); resolve(); };
        image.onerror = () => reject(new Error(`Unable to load portrait: ${url}`));
        image.src = url;
      }));
    }
    head.src = urls.neutral; node.append(body, head, tag); cast.append(node);
    actors.set(name, { node, head, urls });
  }
  root.append(backdrop, stars, paper, panel, cast); stageElement.append(root);

  function render(frame) {
    if (disposed) return;
    const time = Number.isFinite(frame.time) ? frame.time : 0;
    const scene = frame.scene || { id: 'opening', start: 0, end: 24 };
    const elapsed = Math.max(0, time - scene.start);
    const progress = clamp(elapsed / Math.max(1, scene.end - scene.start));
    const speaker = speakerKey(frame.turn?.speaker);
    const introduction = introductionAt(scene, time);
    const introduced = introduction ? speakerKey(introduction.speaker) : null;
    if (activeId !== scene.id) { activeId = scene.id; refs = mountPanel(panel, scene.id); }
    root.dataset.scene = scene.id; root.dataset.capture = String(Boolean(frame.capture));
    root.dataset.breakIn = String(scene.id === 'interruption' && elapsed < 3);
    root.style.setProperty('--scene-progress', progress);
    stars.style.opacity = scene.id === 'opening' || (scene.id === 'interruption' && elapsed < 3) ? '1' : '.08';
    paper.style.opacity = scene.id === 'opening' || (scene.id === 'interruption' && elapsed < 3) ? '0' : '1';
    if (refs.crawl) {
      refs.copy.textContent = (scene.turns?.[0]?.caption || '').replace(' Define the logic.', '\n\nDefine the logic.').replace(' Typed links', '\n\nTyped links');
      refs.crawl.style.transform = `rotateX(25deg) translateY(${640 - progress * 1850}px)`;
      refs.crawl.style.opacity = String(Math.min(1, elapsed / 1.5, (1 - progress) * 12));
    }
    if (refs.room) {
      refs.room.style.opacity = elapsed >= 3 ? '1' : '0';
      const paperStart = scene.turns.find(turn => turn.speaker === 'Graham').end;
      const rollStart = scene.turns.filter(turn => turn.speaker === 'Ryan').at(-1).start;
      const receipt = clamp((time - paperStart) / 1.2) * (time <= rollStart ? 1 : clamp((rollStart + 2 - time) / 2));
      refs.receipt.style.height = `${receipt * 660}px`;
      refs.receipt.style.opacity = String(receipt > 0 ? 1 : 0);
    }
    for (const packet of refs.packets || []) {
      const amount = ((time * .28 + Number(packet.dataset.offset || 0)) % 1 + 1) % 1;
      packet.setAttribute('cx', Number(packet.dataset.x) + Number(packet.dataset.dx) * amount);
      packet.setAttribute('cy', Number(packet.dataset.y) + Number(packet.dataset.dy) * amount);
      packet.style.opacity = String(Math.sin(amount * Math.PI));
    }
    if (refs.merge) refs.merge.style.transform = `translateX(${(1 - clamp(elapsed / 4)) * -120}px)`;
    for (const [name, actor] of actors) {
      const all = scene.id === 'interruption' || scene.id === 'reveal';
      const opening = scene.id === 'opening';
      const breakIn = scene.id === 'interruption' && elapsed < 3;
      const featured = speaker && speaker !== 'narrator' ? speaker : 'ryan';
      const visible = !opening && (all || name === featured || name === (featured === 'kyle' ? 'ryan' : 'kyle'));
      actor.node.hidden = !visible || (breakIn && name !== 'kyle');
      const speaking = speaker === name;
      const index = CAST.findIndex(entry => entry[0] === name);
      const focus = introduced === name && !breakIn;
      const size = breakIn ? 530 : focus ? 420 : all ? 230 : 260;
      const x = breakIn ? 695 : focus ? 750 : all ? 105 + index * 294 : name === 'kyle' ? 1575 : 85;
      const y = breakIn ? 240 - Math.min(1, elapsed * 3) * 25 : focus ? 295 : all ? scene.id === 'reveal' ? 595 : 485 : 625;
      actor.node.style.setProperty('--head-size', `${size}px`);
      actor.node.style.width = `${size}px`; actor.node.style.left = `${x}px`; actor.node.style.top = `${y}px`;
      actor.node.classList.toggle('film-speaking', speaking); actor.node.classList.toggle('film-head-only', breakIn);
      actor.node.classList.toggle('film-introducing', focus);
      actor.node.style.opacity = introduced && !focus && !breakIn ? '.18' : '1';
      actor.node.style.zIndex = focus ? '2' : '1';
      const bob = speaking ? Math.sin(time * 13) * 2.2 : Math.sin(time * 1.3 + index) * .6;
      actor.head.style.transform = `translateY(${bob}px) rotate(${speaking ? Math.sin(time * 5) * .7 : 0}deg)`;
      const blink = (time + index * .7) % 5.3 < .12;
      const state = blink && actor.urls.blink ? 'blink' : speaking && actor.urls.speaking ? 'speaking' : 'neutral';
      const url = actor.urls[state];
      if (loaded.has(url) && actor.head.src !== url) actor.head.src = url;
    }
  }
  return { render, ready: Promise.all(pending), dispose() { disposed = true; root.remove(); } };
}

function mountPanel(panel, id) {
  panel.replaceChildren();
  const refs = {};
  if (id === 'opening') {
    const perspective = element('div', 'film-crawl-perspective');
    const crawl = element('div', 'film-crawl');
    crawl.append(element('div', 'film-crawl-episode', LABELS.opening[0]), element('h1', '', LABELS.opening[1].toUpperCase()));
    const copy = element('p', 'film-crawl-copy'); crawl.append(copy); perspective.append(crawl); panel.append(perspective);
    return { crawl, copy };
  }
  const [eyebrow, title] = LABELS[id] || ['', id];
  const heading = element('header', 'film-heading');
  heading.append(element('div', 'film-eyebrow', eyebrow), element('h1', 'film-title', title)); panel.append(heading);
  if (id === 'interruption') {
    const room = element('div', 'film-room');
    room.append(element('div', 'film-room-board', 'DEFINE  →  CUT  →  BUILD  →  DEPLOY'), element('div', 'film-desk'));
    const mug = element('div', 'film-mug', 'cargo'); room.append(mug);
    const receipt = element('div', 'film-receipt');
    receipt.append(element('small', '', 'COMPILER OUTPUT · DRAMATIZED'), element('strong', '', 'error[E0277]'), element('pre', '', 'the trait bound\n\nPipeline<\n  Stream<Request>,\n  Result<Response,\n    Error<...>>\n>\n\nis not satisfied\n\nrequired by a bound\nrequired by a bound\nrequired by a bound\nrequired by a bound\nrequired by a bound'));
    room.append(receipt); panel.append(room); return { room, receipt };
  }
  if (id === 'catalog' || id === 'receipts') {
    const strip = element('div', 'film-catalog-strip');
    for (const [name, detail] of id === 'catalog' ? [['Reference crates', 'Types + config + metrics'], ['Third-party crates', 'Your extensions'], ['Working catalog', 'One composable collection']] : [['Source', 'Graph + placement'], ['Build', 'Segment crates + images'], ['Run', 'Coordinated deployment']]) {
      const card = element('div', 'film-crate'); card.append(element('strong', '', name), element('span', '', detail)); strip.append(card);
    }
    panel.append(strip);
    const footage = element('div', 'gdl-live-footage'); footage.dataset.captureSlot = 'composer';
    footage.append(element('div', 'film-footage-label', 'GDL-WEB CAPTURE SLOT'), element('div', 'film-footage-center', id === 'catalog' ? 'Compose the real graph.' : 'Build it. Run it. Show it.'));
    panel.append(footage); refs.footage = footage;
  } else if (id === 'reveal') {
    panel.append(element('div', 'film-reveal-rule'), element('p', 'film-reveal-copy', 'Compose the logic. Choose the boundaries. Ship the graph.'));
    const delivery = element('div', 'film-delivery');
    delivery.append(element('span', '', 'TODAY · IMAGE BUNDLES + DOCKER COMPOSE'), element('span', '', 'INTEGRATION TARGETS · DGD + CONFIGMAP'));
    panel.append(delivery);
  } else {
    const diagram = element('div', 'film-diagram'); diagram.innerHTML = schematic(id); panel.append(diagram);
    refs.packets = [...diagram.querySelectorAll('[data-packet]')]; refs.merge = diagram.querySelector('[data-merge]');
    if (['acme', 'nim', 'stargate'].includes(id)) panel.append(element('div', 'film-concept', 'CONCEPT SCHEMATIC · MOCK DEMO NODES'));
  }
  return refs;
}

function schematic(id) {
  const card = (x, y, width, title, detail, kind = '') => `<g class="film-node ${kind}"><rect x="${x}" y="${y}" width="${width}" height="126" rx="5"/><text class="film-node-title" x="${x + 24}" y="${y + 47}">${title}</text><text class="film-node-detail" x="${x + 24}" y="${y + 84}">${detail}</text></g>`;
  const edge = (x, y, dx, dy = 0, offset = 0) => `<path class="film-edge" d="M${x} ${y} l${dx} ${dy}" marker-end="url(#film-arrow)"/><circle class="film-packet" data-packet data-x="${x}" data-y="${y}" data-dx="${dx}" data-dy="${dy}" data-offset="${offset}" r="7"/>`;
  const badge = (x, y, text) => `<text class="film-contract" x="${x}" y="${y}" text-anchor="middle">${text}</text>`;
  const segment = (x, width, title, body, kind = '') => `<g class="film-segment ${kind}"><rect x="${x}" y="205" width="${width}" height="310" rx="8"/><text class="film-segment-title" x="${x + 26}" y="252">${title}</text>${body}</g>`;
  let body = '';
  if (id === 'types') body = card(345, 270, 345, 'Frontend', 'out: Request') + edge(690, 333, 130) + card(820, 270, 345, 'Engine', 'in: Request') + badge(755, 305, 'Request') + `<text class="film-proof" x="960" y="500" text-anchor="middle">MATCHING TYPES → VALID CONNECTION</text><text class="film-small-note" x="960" y="552" text-anchor="middle">Marked re-exports retain message identity.</text>`;
  if (id === 'acme') body = card(310, 230, 280, 'Engine', 'Response stream') + edge(590, 293, 100) + card(690, 230, 380, 'Acme usage tap', 'Config + metrics', 'film-node-accent') + edge(1070, 293, 100) + card(1170, 230, 370, 'Response plane', 'Continue the stream') + edge(880, 356, 0, 130, .5) + card(690, 486, 380, 'Acme accounting', 'Usage events', 'film-node-accent') + badge(1130, 457, 'CUSTOMIZE THE REFERENCE GRAPH');
  if (id === 'deployment') body = segment(275, 400, 'SEGMENT A', card(307, 303, 336, 'Frontend + router', 'Scale at entry')) + segment(760, 400, 'SEGMENT B', card(792, 303, 336, 'Engine', 'Scale for compute')) + segment(1245, 400, 'SEGMENT C', card(1277, 303, 336, 'Response + usage', 'Scale independently')) + edge(675, 365, 85) + edge(1160, 365, 85, 0, .4) + `<text class="film-small-note" x="960" y="600" text-anchor="middle">Placement defines process boundaries. The logical graph stays recognizable.</text>`;
  if (id === 'nim') body = segment(405, 1110, 'ONE PROCESS · CONCEPT NIM PACKAGING', `<g data-merge>${card(445, 310, 290, 'API entry', 'Request')}${edge(735, 373, 80)}${card(815, 310, 290, 'Engine', 'Response')}${edge(1105, 373, 80, 0, .4)}${card(1185, 310, 290, 'Output', 'Stream')}</g>`, 'film-segment-accent') + `<text class="film-small-note" x="960" y="600" text-anchor="middle">A tailored graph and placement for a single-process product.</text>`;
  if (id === 'stargate') body = segment(250, 600, 'STARGATE + GLOBAL ROUTING', card(285, 310, 530, 'Public APIs + routing', 'Normalize the request')) + edge(850, 373, 220) + badge(960, 320, 'REQUEST IR') + segment(1070, 600, 'DYNAMO DEPLOYMENT', card(1105, 310, 530, 'IR entry → engine → response', 'Operate on the internal contract')) + `<text class="film-small-note" x="960" y="600" text-anchor="middle">Concept: move public API handling outward, preserve typed request and response contracts.</text>`;
  return `<svg viewBox="0 0 1920 780" aria-label="${LABELS[id]?.[1] || 'Graph schematic'}" role="img"><defs><marker id="film-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0 0 L8 4 L0 8" fill="#213d3a"/></marker></defs>${body}</svg>`;
}

function element(tag, className, text) { const node = document.createElement(tag); node.className = className; if (text !== undefined) node.textContent = text; return node; }
function speakerKey(value = '') { const key = value.toLowerCase().replace(/[^a-z]/g, ''); return key === 'johnt' ? 'john' : key; }
function clamp(value) { return Math.max(0, Math.min(1, value)); }
function starfield() {
  const stars = Array.from({ length: 125 }, (_, i) => `<circle cx="${(i * 761 + 113) % 1920}" cy="${(i * 433 + 37) % 1080}" r="${i % 7 === 0 ? 1.9 : 1}" opacity="${.25 + (i % 5) * .13}"/>`).join('');
  return `<svg viewBox="0 0 1920 1080" preserveAspectRatio="none" aria-hidden="true"><g fill="#fff">${stars}</g></svg>`;
}
