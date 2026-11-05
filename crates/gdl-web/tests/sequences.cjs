// Seeded sequences of page edits. `editor.cjs` runs 500 of them against the
// page's copy of the server's rules; `api.rs` sends the last complete
// draft of each through the server's own check, so the copy cannot drift.
//
// The catalogue is shaped as the server's: `regproto` and `regnodes` from
// `gdl-gen`'s vendored registry, with a hosted frontend and unary nodes
// added to `regnodes`, and a second unary node `Tok` in `regrenamed`.

const registry = 'registry+https://github.com/rust-lang/crates.io-index';

// The draft rules the server holds a draft to, that page edits can break:
// each node names a catalogue entry the page has now; no two declarations
// share a name; each segment member is declared, and each declaration is
// in a segment, a topology node in one; each `calls` names a unary node;
// and a caller with no copy of a unary node in its segment finds one copy
// elsewhere, not several. Empty when the draft keeps them all.
function draftRuleBreaks(output, entries) {
  const breaks = [];
  const offered = new Set(entries.flatMap(e => (e.nodes || []).map(n => `${e.package}@${e.version} ${n.name}`)));
  for (const node of output.nodes) {
    if (!offered.has(`${node.entry} ${node.node}`)) breaks.push(`node ${node.name} names ${node.entry} ${node.node}, which the catalogue does not have`);
  }
  const declared = [output.source.name, ...output.pipelines.map(p => p.source.name), ...Object.keys(output.identities),
    ...output.nodes.filter(n => n.name !== output.source.name).map(n => n.name)];
  const seen = new Set();
  for (const name of declared) { if (seen.has(name)) breaks.push(`${name} is declared twice`); seen.add(name); }
  const unary = new Set(output.nodes.filter(n => entries.some(e => `${e.package}@${e.version}` === n.entry && (e.nodes || []).some(c => c.name === n.node && c.role === 'unary'))).map(n => n.name));
  const listed = Object.values(output.segments).flat();
  for (const member of listed) if (!seen.has(member)) breaks.push(`segment member ${member} is not declared`);
  for (const name of seen) {
    const count = listed.filter(m => m === name).length;
    if (count === 0) breaks.push(`${name} is in no segment`);
    if (count > 1 && !unary.has(name)) breaks.push(`${name} is in ${count} segments`);
  }
  for (const node of output.nodes) for (const called of node.calls || []) if (!unary.has(called)) breaks.push(`${node.name} calls ${called}, which is no unary node`);
  const segmentsOf = name => Object.entries(output.segments).filter(([, members]) => members.includes(name)).map(([segment]) => segment);
  for (const node of output.nodes) {
    for (const called of node.calls || []) {
      const copies = segmentsOf(called);
      for (const segment of segmentsOf(node.name)) {
        if (!copies.includes(segment) && copies.length > 1) breaks.push(`${CHOSEN}${node.name} in ${segment} calls ${called}, whose copies sit in ${copies.length} other segments`);
      }
    }
  }
  return breaks;
}

// A refusal of a placement the user chose, which the server explains by
// name: no page edit makes one behind the user's back.
const CHOSEN = 'chosen: ';
const chosen = (rule) => rule.startsWith(CHOSEN);

// A small seeded generator, so that a failing sequence can be replayed.
function seeded(seed) {
  return () => { seed = (seed + 0x6D2B79F5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}

// Stub the page's elements in `run`'s context, and give it the catalogue.
function prepare(run) {
  run(`document.createElement = tag => ({tag, children:[], dataset:{}, style:{setProperty(){}}, className:'',
      append(...children){this.children.push(...children);}, setAttribute(){}, setCustomValidity(){}, reportValidity(){}, classList:{add(){},toggle(){}}, add(){}, querySelector(){ return {}; }});
    Option = function(label,value){this.label=label;this.value=value;};
    setTimeout = () => 0; clearTimeout = () => {};
    // Every element the page writes to takes children and options.
    const element = document.getElementById;
    document.getElementById = id => { const e = element(id); e.append ||= () => {}; e.add ||= () => {}; e.querySelectorAll ||= undefined; return e; };
    for (const id of ['hop','service-nodes','sink-nodes','placements']) $(id).append=()=>{};
    $('groups').children = []; $('groups').append = (...children) => $('groups').children.push(...children);
    for (const id of ['source-request','source-header','source-response','source-frontend','identity-request','identity-header','identity-response']) $(id).add=()=>{};
    $('identity-role').append=()=>{}; $('identity-form').querySelector=()=>({});
    $('identity-dialog').showModal=()=>{}; $('identity-dialog').close=()=>{};
    $('identity-name').setCustomValidity=()=>{}; $('identity-name').reportValidity=()=>{};
    renderCatalog=()=>{};
    const registry=${JSON.stringify(registry)};
    const m=path=>({package:'regproto',version:'0.1.0',source:registry,path:'regproto::'+path});
    req={head:m('Words'),item:null}; reply=m('Token');
    // Each version of regnodes has its own default for \`limit\`, so a
    // draft that pairs one version's entry with another's defaults shows.
    const limit=()=>({name:'limit',description:'',default:version});
    const unary=(name, crate)=>({name, role:'unary', type:crate+'::'+name, config:[limit()], ports:[{port:'Req',...req},{port:'Resp',head:reply}]});
    // Router is an operator over Engine; in a \`'role'\` twin it is a sink.
    const router=()=>({name:'Router',role:'operator',type:'regnodes::Router',config:[limit()],ports:[{port:'UpReq',...req},{port:'UpItem',head:reply},{port:'DownReq',...req},{port:'DownItem',head:reply}]});
    const routerSink=()=>({name:'Router',role:'sink',type:'regnodes::Router',config:[limit()],ports:[{port:'Req',...req},{port:'Item',head:reply}]});
    const engine=()=>({name:'Engine',role:'sink',type:'regnodes::Engine',config:[limit()],ports:[{port:'Req',...req},{port:'Item',head:reply}]});
    const flag=name=>({name,description:'',default:true});
    const api=()=>({name:'Api',role:'source',type:'regnodes::Api',clients:'regnodes::ApiClients',ports:[{port:'Req',...req},{port:'Item',head:reply}],config:[flag('chat'),flag('responses'),limit()],
      entries:['chat','responses'].map(name=>({name,path:'/'+name,enabled:name,request:{port:'Req',...req},response:{port:'Item',head:reply}}))});
    const message=name=>({name,path:'regproto::'+name,wire:true,encoding:'msgpack',public:true});
    const offer=(package,version,nodes,messages=[])=>({added:'2026-11-04T00:00:00Z',package,version,source:registry,messages,exports:[],nodes});
    version=1; twin=false;
    catalogue=()=>[
      // While \`twin\` is on, the catalogue also holds the next version of
      // regnodes, so every node of it names two versions. In a \`'role'\`
      // twin, Router is a sink there.
      ...(twin?[offer('regnodes','0.'+(version+1)+'.0',[api(),engine(),twin==='role'?routerSink():router(),unary('Tok','regnodes'),unary('Source_entry_responses','regnodes'),unary('Next_entry_responses','regnodes')])]:[]),
      offer('regproto','0.1.0',[],[message('Token'),message('Words')]),
      offer('regnodes','0.'+version+'.0',[api(),engine(),router(),unary('Tok','regnodes'),unary('Source_entry_responses','regnodes'),unary('Next_entry_responses','regnodes')]),
      offer('regrenamed','0.1.0',[unary('Tok','regrenamed')]),
    ];
    fetch=async(url)=>({ok:true,json:async()=>url==='/api/catalog'?{entries:catalogue()}:{ok:true}});
    base=()=>{
      clearGraph();
      state.entries=catalogue();
      $('name').value='composed';$('source-name').value='Source';
      state.frontend={...nodes().find(n=>n.node.role==='source'),arms:{chat:newHop(),responses:newHop()}};
      connectSink(state.frontend.arms.chat,req,reply,{...nodes().find(n=>n.node.role==='sink'),calls:[]});
      const sink=[...instances().keys()].find(n=>n.node.role==='sink');
      connectSink(state.frontend.arms.responses,req,reply,sink);
      // The chat endpoint's pipeline runs through an operator over the
      // sink, so that a nested hop can be lost.
      const pipeline=state.pipelines[0];
      pipeline.hop.nested={...newHop(),inner:pipeline.hop.inner};
      pipeline.hop.inner=copyNode(nodes().find(n=>n.node.name==='Router'));
      // A unary node the sink calls, so that most drafts carry a call.
      const tok=unaryChoices().find(c=>c.entry.startsWith('regnodes@'));
      addService(tok.entry,tok.node);sink.calls=['Tok'];
      render();
    };
    shown=()=>{$('groups').children=[];render();};
    unaryChoices=()=>nodes().filter(n=>n.node.role==='unary');`);
}

// Each operation is what a user's action does, then the render the page
// runs after it. A third element marks an operation that reloads the
// catalogue.
const operations = [
  ['add a unary node', (r) => `{const c=unaryChoices()[${r}%unaryChoices().length];addService(c.entry,c.node);render();}`],
  ['remove a unary node', (r) => `{const s=state.services[${r}%Math.max(1,state.services.length)];if(s){removeService(s.instanceName);render();}}`],
  ['list a unary node in a segment', (r) => `{const s=state.services[0];if(s){const names=[...Object.keys(segments(instances())),'own${r}'];placeService(s.instanceName,names[${r}%names.length],true);render();}}`],
  ['move a unary node to a segment of its own', (r) => `{const s=state.services[${r}%Math.max(1,state.services.length)];if(s){const own='own${r}';placeService(s.instanceName,own,true);for(const segment of [...s.segments]) if(segment!==own) placeService(s.instanceName,segment,false);render();}}`],
  ['unlist a unary node', (r) => `{const s=state.services[0];if(s){placeService(s.instanceName,s.segments[${r}%s.segments.length],false);render();}}`],
  ['call a unary node', (r) => `{const s=state.services[${r}%Math.max(1,state.services.length)];const sink=[...instances().keys()].find(n=>n.node.role==='sink');if(s&&sink){sink.calls=[...new Set([...(sink.calls||[]),s.instanceName])];render();}}`],
  ['rename a segment', (r) => `{shown();const fields=$('groups').children;if(fields.length){const field=fields[${r}%fields.length].children[0];const names=Object.keys(segments(instances()));field.value=${r}%3?'seg${r}':names[${r}%names.length];field.oninput();render();}}`],
  ['re-import a package at a new version', () => `version++;`, true],
  ['add a second version of a package', () => `twin=true;`, true],
  ['add a version where a node has another role', () => `twin='role';`, true],
  ['remove the second version', () => `twin=false;`, true],
  ['refresh the catalogue', () => ``, true],
  ['undo', () => `undoEdit();`],
  ['redo', () => `redoEdit();`],
  ['clear the graph', () => `clearGraph();`],
  ['add an identity', (r) => `{const arm=state.frontend?.arms?.chat||state.hop;openIdentity(identityNode('forward',req),i=>arm.forward.push(i));$('identity-name').value=${r}%2?'Pass${r}':([...nodeHolders?.()||[]].map(n=>n.instanceName).filter(Boolean)[0]||'Tok');$('identity-form').onsubmit({preventDefault(){}});}`],
  // Only `responses`, through the frontend's settings, as the user toggles
  // it. Chat stays on: a pipeline no enabled endpoint reaches still has its
  // nodes declared, which the server refuses, and with no endpoint the page
  // places no source. Both are page defects apart from unary nodes.
  ['toggle an endpoint', (r) => `{if(state.frontend){const e='responses';const config=graphConfig(state.frontend);if(!config[e] || config.chat && config.responses){const controls=configControls(state.frontend);const index=state.frontend.node.config.findIndex(f=>f.name===e);if(index<0) throw new Error('the frontend lost its settings: its node is '+JSON.stringify(state.frontend.node));const input=controls.children[index].children.find(c=>c.type==='checkbox');input.checked=!config[e];input.onchange();}}}`],
  ['rename the source', (r) => `{$('source-name').value=${r}%2?'Next':'Source';$('source-name').oninput();}`],
  ['rebuild the graph', () => `base();`],
];

// Run each seed's sequence of `steps` operations from the base graph, and
// call `check` after each step with the seed, the operations so far, the
// draft, the catalogue, and whether the page takes the graph for complete.
async function runSequences(run, seeds, steps, check) {
  for (const seed of seeds) {
    const random = seeded(seed);
    run('version=1; twin=false; base();');
    const done = [];
    for (let step = 0; step < steps; step++) {
      const [name, code, loads] = operations[Math.floor(random() * operations.length)];
      const r = Math.floor(random() * 1000);
      done.push(name);
      try {
        run(code(r));
        if (loads) await run('load()');
      } catch (error) {
        throw new Error(`seed ${seed}, after: ${done.join(', ')}: ${error.message}`);
      }
      await check({
        seed, done,
        output: JSON.parse(run('JSON.stringify(draft())')),
        entries: JSON.parse(run('JSON.stringify(state.entries)')),
        complete: run('graphComplete()') === true,
        problems: JSON.parse(run("JSON.stringify(typeof draftProblems === 'function' ? draftProblems() : [])")),
        twin: !!run('twin'),
        // Each operator that has lost its nested hop's inner end. No
        // operation removes one, so a correct page never does.
        lost: JSON.parse(run(`JSON.stringify((()=>{const lost=[];const visit=hop=>{if(!hop) return;if(hop.inner && hop.inner.node.role==='operator' && !(hop.nested?.inner || hop.nested?.target!=null)) lost.push(hop.inner.instanceName);visit(hop.nested);};state.pipelines.forEach(p=>visit(p.hop));visit(state.hop);return lost;})())`)),
      });
    }
  }
}

// Each `limit` a draft node carries that is not its current entry's
// default. No operation sets `limit` (toggling an endpoint sets its flag),
// so every one must be the default.
function staleDefaults(output, entries) {
  const stale = [];
  for (const node of output.nodes) {
    const entry = entries.find(e => `${e.package}@${e.version}` === node.entry);
    const fields = entry?.nodes.find(n => n.name === node.node)?.config || [];
    for (const field of fields.filter(f => f.name === 'limit')) {
      if (node.config[field.name] !== field.default) stale.push(`${node.name}: ${field.name} is ${node.config[field.name]}, its default in ${node.entry} is ${field.default}`);
    }
  }
  return stale;
}

module.exports = {draftRuleBreaks, chosen, staleDefaults, prepare, runSequences};
