// The composer's page. It keeps a draft (the source, each hop's nodes,
// and the cut edges), offers at each slot only the catalogue nodes whose
// port carries the message that arrives there, and asks the server for
// every verdict. It decides nothing itself: one copy of each rule, on the
// server.

"use strict";

const state = {
  entries: [],
  hop: newHop(),
  cuts: new Set(),
  segmentNames: {},
};

function newHop() {
  return { forward: [], inner: null, nested: null, backward: [] };
}

const $ = (id) => document.getElementById(id);
const key = (origin) => origin ? `${origin.package}@${origin.version} ${origin.path}` : "()";
const same = (a, b) => key(a) === key(b);
const samePort = (a, b) => same(a.head, b.head) && same(a.item, b.item);

// Every message the catalogue offers, by origin, with a readable label.
function messages() {
  const found = new Map();
  for (const entry of state.entries) {
    for (const m of entry.messages || []) {
      const origin = { package: entry.package, version: entry.version, source: entry.source, path: m.path };
      found.set(key(origin), { origin, label: `${m.name} (${entry.package} ${entry.version})` });
    }
  }
  return [...found.values()];
}

// Every node the catalogue offers, with its entry.
function nodes() {
  return state.entries.flatMap((entry) =>
    (entry.nodes || []).map((node) => ({ entry: `${entry.package}@${entry.version}`, node })));
}

const port = (node, name) => node.ports.find((p) => p.port === name) || {};
const portOf = (p) => ({ head: p.head || null, item: p.item || null });

async function load() {
  const response = await fetch("/api/catalog");
  state.entries = (await response.json()).entries;
  $("entries").textContent = state.entries.length
    ? state.entries.map((e) => `${e.package} ${e.version}`).join(", ")
    : "empty: add a crate";
  // A streamed request may have no head, `()`; a request that is not
  // streamed has no item.
  fillMessages($("source-request"), "() (no head)");
  fillMessages($("source-item"), "nothing: not streamed");
  fillMessages($("source-response"));
  render();
}

function fillMessages(select, none) {
  const chosen = select.value;
  select.innerHTML = "";
  if (none) select.add(new Option(none, ""));
  for (const m of messages()) {
    select.add(new Option(m.label, key(m.origin)));
  }
  if (chosen) select.value = chosen;
}

const chosenMessage = (select) => messages().find((m) => key(m.origin) === select.value)?.origin || null;

// A select of the nodes that fit, and what choosing one does.
function slot(label, candidates, choose) {
  const div = document.createElement("div");
  div.className = "slot";
  if (!candidates.length) {
    div.innerHTML = `<span class="muted">${label}: no node in the catalogue fits here</span>`;
    return div;
  }
  const select = document.createElement("select");
  select.add(new Option(`${label}…`, ""));
  candidates.forEach((c, i) => select.add(new Option(`${c.node.role} ${c.node.name} (${c.entry})`, i)));
  select.onchange = () => { if (select.value !== "") { choose(candidates[select.value]); render(); } };
  div.append(select);
  return div;
}

function chip(text, remove) {
  const span = document.createElement("span");
  span.className = "edge";
  span.textContent = `${text} ✕`;
  span.title = "remove";
  span.onclick = () => { remove(); render(); };
  return span;
}

// A chosen node whose port no longer carries the message that arrives
// there (a node before it was removed, or the source's request changed)
// is dropped with everything after it: a slot keeps only what it would
// offer now.
function prune(hop, request) {
  let current = request;
  const fits = hop.forward.findIndex((n) => {
    const ok = samePort(portOf(port(n.node, "In")), current);
    if (ok) current = portOf(port(n.node, "Out"));
    return !ok;
  });
  if (fits !== -1) {
    hop.forward.splice(fits);
    hop.inner = null;
  }
  if (hop.inner) {
    const role = hop.inner.node.role;
    const takes = portOf(port(hop.inner.node, role === "operator" ? "UpReq" : "Req"));
    if (!samePort(takes, current)) hop.inner = null;
  }
  if (!hop.inner) {
    hop.nested = null;
    hop.backward = [];
    return;
  }
  let item = port(hop.inner.node, hop.inner.node.role === "operator" ? "UpItem" : "Item").head || null;
  const stale = hop.backward.findIndex((n) => {
    const ok = same(port(n.node, "In").head, item);
    if (ok) item = port(n.node, "Out").head || null;
    return !ok;
  });
  if (stale !== -1) hop.backward.splice(stale);
}

// One hop's editor. `request` is the message that arrives; returns the
// item the hop streams back, or null while it has no inner end.
function hopEditor(hop, request, into) {
  prune(hop, request);
  const all = nodes();
  let current = request;
  const forward = document.createElement("div");
  forward.append("forward: ");
  hop.forward.forEach((n, i) => {
    forward.append(chip(n.node.name, () => hop.forward.splice(i)));
    current = portOf(port(n.node, "Out"));
  });
  into.append(forward);
  into.append(slot("add a forward node",
    all.filter((c) => c.node.role === "forward" && samePort(portOf(port(c.node, "In")), current)),
    (c) => { hop.forward.push(c); hop.inner = null; hop.nested = null; hop.backward = []; }));
  if (!hop.inner) {
    into.append(slot("choose the inner end",
      all.filter((c) =>
        (c.node.role === "sink" && samePort(portOf(port(c.node, "Req")), current)) ||
        (c.node.role === "operator" && samePort(portOf(port(c.node, "UpReq")), current))),
      (c) => { hop.inner = c; hop.nested = c.node.role === "operator" ? newHop() : null; hop.backward = []; }));
    return null;
  }
  const inner = document.createElement("div");
  inner.append("inner: ", chip(`${hop.inner.node.role} ${hop.inner.node.name}`,
    () => { hop.inner = null; hop.nested = null; hop.backward = []; }));
  into.append(inner);
  let item;
  if (hop.inner.node.role === "operator") {
    const nested = document.createElement("section");
    nested.append(`${hop.inner.node.name}'s own hop`);
    into.append(nested);
    hopEditor(hop.nested, portOf(port(hop.inner.node, "DownReq")), nested);
    item = port(hop.inner.node, "UpItem").head || null;
  } else {
    item = port(hop.inner.node, "Item").head || null;
  }
  const backward = document.createElement("div");
  backward.append("backward: ");
  hop.backward.forEach((n, i) => {
    backward.append(chip(n.node.name, () => hop.backward.splice(i)));
    item = port(n.node, "Out").head || null;
  });
  into.append(backward);
  const arriving = item;
  into.append(slot("add a backward node",
    all.filter((c) => c.node.role === "backward" && same(port(c.node, "In").head, arriving)),
    (c) => hop.backward.push(c)));
  return item;
}

// Graph names: a node's own name, numbered when it repeats.
function instances() {
  const names = new Map();
  const taken = new Set([$("source-name").value]);
  const visit = (hop) => {
    for (const n of [...hop.forward, hop.inner, ...hop.backward].filter(Boolean)) {
      let name = n.node.name;
      for (let i = 2; taken.has(name); i++) name = `${n.node.name}${i}`;
      taken.add(name);
      names.set(n, name);
    }
    if (hop.nested) visit(hop.nested);
  };
  visit(state.hop);
  return names;
}

// Each edge: request edges outward in, response edges back out.
function edges(hop, outer, names) {
  const list = [];
  const chain = [outer, ...hop.forward.map((n) => names.get(n))];
  if (!hop.inner) return list;
  const inner = names.get(hop.inner);
  chain.push(inner);
  for (let i = 1; i < chain.length; i++) list.push([chain[i - 1], chain[i]]);
  const back = [inner, ...hop.backward.map((n) => names.get(n)), outer];
  for (let i = 1; i < back.length; i++) list.push([back[i - 1], back[i]]);
  if (hop.nested) list.push(...edges(hop.nested, inner, names));
  return list;
}

// Segments: the groups the uncut edges leave connected.
function segments(names) {
  const source = $("source-name").value;
  const members = [source, ...names.values()];
  const parent = new Map(members.map((m) => [m, m]));
  const find = (m) => parent.get(m) === m ? m : find(parent.get(m));
  for (const [a, b] of edges(state.hop, source, names)) {
    if (!state.cuts.has(`${a}->${b}`)) parent.set(find(a), find(b));
  }
  const groups = new Map();
  for (const m of members) {
    const root = find(m);
    if (!groups.has(root)) groups.set(root, []);
    groups.get(root).push(m);
  }
  const result = {};
  [...groups.values()].forEach((group, i) => {
    const name = state.segmentNames[group[0]] || `s${i + 1}`;
    result[name] = group;
  });
  return result;
}

function render() {
  const hop = $("hop");
  hop.innerHTML = "";
  const request = { head: chosenMessage($("source-request")), item: chosenMessage($("source-item")) };
  hopEditor(state.hop, request, hop);

  const names = instances();
  const edgeList = $("edges");
  edgeList.innerHTML = "";
  for (const [a, b] of edges(state.hop, $("source-name").value, names)) {
    const id = `${a}->${b}`;
    const span = document.createElement("span");
    span.className = state.cuts.has(id) ? "edge cut" : "edge";
    span.textContent = `${a} → ${b}`;
    span.onclick = () => { state.cuts.has(id) ? state.cuts.delete(id) : state.cuts.add(id); render(); };
    edgeList.append(span);
  }
  const groups = $("groups");
  groups.innerHTML = "";
  for (const [name, members] of Object.entries(segments(names))) {
    const div = document.createElement("div");
    const input = document.createElement("input");
    input.value = name;
    input.size = 12;
    input.onchange = () => { state.segmentNames[members[0]] = input.value; render(); };
    div.append(input, `: ${members.join(", ")}`);
    groups.append(div);
  }
}

function draft() {
  const names = instances();
  const toHop = (hop) => ({
    forward: hop.forward.map((n) => names.get(n)),
    inner: hop.inner ? names.get(hop.inner) : "",
    nested: hop.nested ? toHop(hop.nested) : null,
    backward: hop.backward.map((n) => names.get(n)),
  });
  return {
    name: $("name").value,
    source: {
      name: $("source-name").value,
      request: { head: chosenMessage($("source-request")), item: chosenMessage($("source-item")) },
      response: chosenMessage($("source-response")),
    },
    nodes: [...names.entries()].map(([n, name]) => ({ name, entry: n.entry, node: n.node.name })),
    hop: toHop(state.hop),
    segments: segments(names),
  };
}

function show(body) {
  const result = $("result");
  result.innerHTML = "";
  if (body.problems) {
    const div = document.createElement("div");
    div.className = "problems";
    div.textContent = body.problems.join("\n");
    result.append(div);
  }
  for (const [title, text] of [["crossings", body.crossings], ["graph file", body.graph], ["spec", body.spec], ["diagram (Mermaid)", body.mermaid]]) {
    if (!text) continue;
    const h = document.createElement("h2");
    h.textContent = title;
    const pre = document.createElement("pre");
    pre.textContent = text;
    result.append(h, pre);
  }
}

$("check").onclick = async () => {
  const response = await fetch("/api/check", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(draft()) });
  show(await response.json());
};

$("export").onclick = async () => {
  const response = await fetch("/api/export", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(draft()) });
  if (!response.ok) { show(await response.json()); return; }
  const link = document.createElement("a");
  link.href = URL.createObjectURL(await response.blob());
  link.download = `${$("name").value}.tar.gz`;
  link.click();
};

$("add").onsubmit = async (event) => {
  event.preventDefault();
  $("adding").textContent = "fetching and reading…";
  const response = await fetch("/api/catalog", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ crate: $("crate").value }) });
  const body = await response.json();
  $("adding").textContent = body.problems ? body.problems.join("; ") : `added ${body.added}`;
  await load();
};

for (const id of ["source-request", "source-item", "source-name"]) $(id).onchange = render;

load();
