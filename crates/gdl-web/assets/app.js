// The composer's page. It keeps a draft (the source, each hop's nodes,
// and their segment placement), offers at each slot only the catalogue nodes whose
// port carries the message that arrives there, and asks the server for
// every verdict. It decides nothing itself: one copy of each rule, on the
// server.

"use strict";

const dropHandlers = new WeakMap();

const state = {
  entries: [],
  mode: "graph",
  validated: false,
  revision: 0,
  timer: null,
  hop: newHop(),
  pipelines: [],
  usedNames: [],
  frontend: null,
  placement: Object.create(null),
  routing: Object.create(null),
  // Keyed by names the user chooses: no prototype, so `constructor` is a name.
  segmentNames: Object.create(null),
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
    (entry.nodes || []).map((node) => ({ entry: `${entry.package}@${entry.version}`, source: entry.source, node })));
}

const port = (node, name) => node.ports.find((p) => p.port === name) || {};
const portOf = (p) => ({ head: p.head || null, item: p.item || null });

async function load() {
  const response = await fetch("/api/catalog");
  if (!response.ok) throw new Error(`Catalog could not be loaded (${response.status}).`);
  const catalog = await response.json();
  state.entries = catalog.entries;
  $("add-local").hidden = !catalog.local_enabled;
  renderCatalog();
  fillMessages($("source-request"), "Choose input message…");
  fillMessages($("source-header"), "No header");
  fillMessages($("source-response"), "Choose output message…");
  fillFrontends();
  if (state.frontend) selectFrontend(state.frontend);
  else render();
}

function fillFrontends() {
  const select = $("source-frontend");
  select.replaceChildren();
  select.add(new Option("Programmatic source · choose messages", ""));
  const available = nodes().filter(candidate => candidate.node.role === "source");
  for (const candidate of available) select.add(new Option(`${candidate.node.name} (${candidate.entry})`, `${candidate.entry} ${candidate.node.name}`));
  if (state.frontend) { const current = available.find(candidate => candidate.source === state.frontend.source && candidate.node.name === state.frontend.node.name); state.frontend = current ? {...current, config:state.frontend.config,arms:state.frontend.arms} : null; }
  select.value = state.frontend ? `${state.frontend.entry} ${state.frontend.node.name}` : "";
  select.onchange = () => selectFrontend(available.find(candidate => `${candidate.entry} ${candidate.node.name}` === select.value) || null);
}

function graphConfig(instance) {
  return Object.fromEntries((instance?.node.config || []).map(field => [field.name, instance.config?.[field.name] ?? field.default]));
}

function configControls(instance) {
  const container = document.createElement("div");
  container.className = "node-config";
  if (!instance) return container;
  for (const field of instance.node.config || []) {
    const label = textElement("label", "", "setting-field");
    const copy = textElement("span", field.label || field.name.replaceAll("_", " "), "setting-copy");
    if (field.subtext) copy.append(textElement("small", field.subtext, "muted"));
    label.append(copy);
    if (field.description) {
      const info = document.createElement("details"); info.className = "field-info";
      const summary = textElement("summary", "i"); summary.title = `About ${field.label || field.name}`;
      info.append(summary, textElement("p",field.description,"muted")); label.append(info);
    }
    label.title = field.description;
    const input = document.createElement("input");
    input.setAttribute("aria-label", field.label || field.name.replaceAll("_", " "));
    input.type = typeof field.default === "boolean" ? "checkbox" : typeof field.default === "number" ? "number" : "text";
    const value = graphConfig(instance)[field.name];
    if (input.type === "checkbox") input.checked = value; else input.value = value;
    input.disabled = state.mode === "segments";
    input.onchange = () => {
      const value = input.type === "checkbox" ? input.checked : input.type === "number" ? Number(input.value) : input.value;
      if (input.type === "number" && !Number.isSafeInteger(value)) {input.setCustomValidity("Enter a safe integer"); input.reportValidity(); return;}
      input.setCustomValidity("");
      instance.config = {...graphConfig(instance), [field.name]:value};
      render();
    };
    label.append(input);
    container.append(label);
  }
  return container;
}

function selectFrontend(frontend) {
  const changed = state.frontend?.node.name !== frontend?.node.name || state.frontend?.source !== frontend?.source;
  state.frontend = frontend;
  if(changed) { state.pipelines=[]; }
  if (frontend && !frontend.node.entries?.length) {
    const req = port(frontend.node, "Req");
    $("input-mode").value = req.item ? "stream" : "single";
    $("source-request").value = key(req.item || req.head);
    $("source-header").value = req.item && req.head ? key(req.head) : "";
    $("source-response").value = key(port(frontend.node, "Item").head);
    $("source-frontend").value = `${frontend.entry} ${frontend.node.name}`;
  }
  if (frontend) $("source-frontend").value = `${frontend.entry} ${frontend.node.name}`;
  render();
}

function enabledEntries(frontend = state.frontend) {
  const config = graphConfig(frontend);
  return (frontend?.node.entries || []).filter(entry => config[entry.enabled] === true);
}

function entryLabel(entry) {
  const field = state.frontend?.node.config?.find(f => f.name === entry.enabled);
  return field?.label || field?.name || entry.name;
}

function pipelineRoots() {
  if (!state.frontend?.node.entries?.length) return [{root:$('source-name').value,name:'Pipeline',hop:state.hop,request:sourcePort(),response:chosenMessage($('source-response'))}];
  return state.pipelines.map((p,i)=>({...p,root:i===0 ? $('source-name').value : `Pipeline${p.id}`}));
}
function deployedRoots() {
  if(!state.frontend?.node.entries?.length) return pipelineRoots();
  const expand=arm=>{
    if(arm.inner) return {...arm,nested:arm.nested?expand(arm.nested):null};
    const target=pipelineTarget(arm);
    if(!target) return {...arm,inner:null};
    return {...target.hop,forward:[...arm.forward,...target.hop.forward],backward:[...target.hop.backward,...arm.backward]};
  };
  return enabledEntries().map((entry,index)=>({root:index===0?$('source-name').value:`${$('source-name').value}_entry_${entry.name}`,
    name:entryLabel(entry),entry:entry.name,request:portOf(entry.request),response:entry.response.head,
    hop:expand(state.frontend.arms?.[entry.name] || newHop())}));
}
function pipelineTarget(hop) { return hop.target == null ? undefined : pipelineRoots().find(p=>p.id===hop.target); }
function armResponse(hop) {
  let item = hop.inner ? port(hop.inner.node,"UpItem").head : pipelineTarget(hop)?.response;
  for (const n of hop.backward) item = port(n.node,"Out").head;
  return item;
}
function armComplete(hop, request, response) {
  let current=request;
  for(const n of hop.forward) {
    if(!samePort(portOf(port(n.node,'In')),current)) return false;
    current=portOf(port(n.node,'Out'));
  }
  if(hop.inner) {
    if(hop.target || !samePort(current,portOf(port(hop.inner.node,'UpReq'))) || !hop.nested || !armComplete(hop.nested,portOf(port(hop.inner.node,'DownReq')),port(hop.inner.node,'DownItem').head)) return false;
  } else {
    const target=pipelineTarget(hop);
    if(!target || !samePort(current,target.request)) return false;
  }
  let item=hop.inner ? port(hop.inner.node,'UpItem').head : pipelineTarget(hop)?.response;
  for(const n of hop.backward) {
    if(!same(port(n.node,'In').head,item)) return false;
    item=port(n.node,'Out').head;
  }
  return same(item,response);
}
function graphComplete() {
  const roots=pipelineRoots();
  return roots.length>0 && roots.every(p=>flowComplete(p.hop,p.request,p.response))
    && enabledEntries().every(e=>armComplete(state.frontend.arms?.[e.name] || newHop(),portOf(e.request),e.response.head));
}

function directSink(pipeline) {
  const hop=pipeline.hop;
  return !hop.forward.length && !hop.backward.length && !hop.nested && hop.inner?.node.role==='sink' ? hop.inner : null;
}
function sinkChoices(request,response) {
  return [...nodes(),...[...instances().keys()].filter(n=>n.node.role==='sink').map(n=>({...n,existing:true}))]
    .filter(c=>c.node.role==='sink' && samePort(portOf(port(c.node,'Req')),request) && same(port(c.node,'Item').head,response));
}
function connectSink(arm,request,response,sink) {
  if(!samePort(portOf(port(sink.node,'Req')),request) || !same(port(sink.node,'Item').head,response))
    throw new Error('Sink request and response must match the connection.');
  connectDestination(arm,request,response,sink);
}
function connectDestination(arm,request,response,node) {
  const hop=newHop();
  if(node.node.role==='forward') hop.forward.push({...node});
  else {hop.inner={...node};if(node.node.role==='operator') hop.nested=newHop();}
  const id=Math.max(0,...state.pipelines.map(p=>p.id))+1;
  let name=`${node.instanceName || node.node.name} connection ${id}`;
  while(state.pipelines.some(p=>p.name===name)) name+=' connection';
  const pipeline={id,name,request,response,hop,automatic:true};
  state.pipelines.push(pipeline);arm.target=pipeline.id;
}
function openPipelineConnection(hop, request, response) {
  const list=$('pipeline-choices');
  const draw=existing=>{
    list.replaceChildren();
    $('destination-add').setAttribute('aria-pressed',String(!existing));
    $('destination-existing').setAttribute('aria-pressed',String(existing));
    const choices=existing?sinkChoices(request,response).filter(c=>c.existing):[
      identityNode('forward',request),identityNode('operator',request,response),
      ...nodes().filter(c=>c.node.role==='forward' && samePort(portOf(port(c.node,'In')),request) ||
        c.node.role==='operator' && samePort(portOf(port(c.node,'UpReq')),request) && same(port(c.node,'UpItem').head,response)),
      ...sinkChoices(request,response).filter(c=>!c.existing)];
    for(const candidate of selectorNodes(choices)) {
      const button=textElement('button',existing?`Connect ${candidate.instanceName}`:candidate.node.name,'pipeline-choice');
      button.append(textElement('small',candidate.specializations?'Choose node/operator kind and message types':`${candidate.node.role} · ${candidate.entry}`,'muted'));
      button.onclick=()=>{
        $('pipeline-connect-dialog').close();
        const choose=node=>{if(node.node.role==='sink') connectSink(hop,request,response,node);else connectDestination(hop,request,response,node);render();};
        if(candidate.specializations) openIdentity(candidate.specializations[0],choose,candidate.specializations);
        else choose(candidate);
      };
      list.append(button);
    }
    if(!choices.length) list.append(textElement('p','No existing sink matches both request and response types.','muted'));
  };
  $('destination-add').onclick=()=>draw(false);$('destination-existing').onclick=()=>draw(true);
  draw(false);$('pipeline-connect-dialog').showModal();
}
function openPipelineSettings(pipeline, request=sourcePort(), response=chosenMessage($('source-response')), arm=null) {
  $('pipeline-name').value=pipeline?.name || `processing-pipeline-${Math.max(0,...state.pipelines.map(p=>p.id))+1}`;
  for(const id of ['pipeline-request','pipeline-response','pipeline-header']) fillMessages($(id),id==='pipeline-header'?'No header':'Choose message…');
  request=pipeline?.request || request; response=pipeline?.response || response;
  $('pipeline-mode').value=request.item?'stream':'single';
  $('pipeline-request').value=key(request.item || request.head);
  $('pipeline-header').value=request.item && request.head ? key(request.head):'';
  $('pipeline-response').value=key(response);
  $('pipeline-form').onsubmit=event=>{
    event.preventDefault();
    const name=$('pipeline-name').value.trim();
    if(!name || state.pipelines.some(p=>p!==pipeline && p.name===name)) { $('pipeline-name').setCustomValidity('Choose a unique pipeline name.');$('pipeline-name').reportValidity();return; }
    const message=chosenMessage($('pipeline-request')), response=chosenMessage($('pipeline-response'));
    if(!message || !response) return;
    const request=$('pipeline-mode').value==='stream'?{head:chosenMessage($('pipeline-header')),item:message}:{head:message,item:null};
    if(pipeline) Object.assign(pipeline,{name,request,response});
    else { pipeline={id:Math.max(0,...state.pipelines.map(p=>p.id))+1,name,request,response,hop:newHop()};state.pipelines.push(pipeline); }
    if(arm) arm.target=pipeline.id;
    $('pipeline-dialog').close();render();
  };
  $('pipeline-name').oninput=()=> $('pipeline-name').setCustomValidity('');
  $('pipeline-dialog').showModal();
}
function renderPipelines(into,names) {
  into.replaceChildren();
  for(const pipeline of pipelineRoots()) {
    if(state.frontend?.node.entries?.length && directSink(pipeline)) continue;
    const card=textElement('section','','pipeline-card');card.dataset.root=pipeline.root;
    const heading=textElement('header','','box public-endpoint pipeline-heading');
    heading.append(textElement('strong',pipeline.name),textElement('small',`${carried(pipeline.request)} → stream ${short(pipeline.response)}`,'muted'));
    if(pipeline.id && state.mode==='graph') {
      const settings=textElement('button','','node-settings'); settings.innerHTML=$('source-settings').innerHTML; settings.setAttribute('aria-label',`Settings for ${pipeline.name}`);
      settings.onclick=()=>openPipelineSettings(state.pipelines.find(p=>p.id===pipeline.id));
      const remove=textElement('button','×','remove'); remove.setAttribute('aria-label',`Remove pipeline ${pipeline.name}`);
      remove.onclick=()=>{
        state.pipelines=state.pipelines.filter(p=>p.id!==pipeline.id);
        const disconnect=hop=>{if(hop.target===pipeline.id) delete hop.target;if(hop.nested) disconnect(hop.nested);};
        Object.values(state.frontend.arms || {}).forEach(disconnect);render();
      };
      heading.append(settings,remove);
    }
    heading.dataset.pipeline=pipeline.root;
    heading.hidden=state.mode==='segments' || pipeline.automatic===true;
    card.append(heading);
    if(pipeline.response) hopEditor(pipeline.hop,pipeline.request,card,pipeline.root,names,pipeline.response);
    else card.append(textElement('p','Choose the input and output message types in Source settings.','empty'));
    into.append(card);
  }
  const sinks=$('sink-nodes');sinks.replaceChildren();
  for(const [instance,name] of names) {
    if(instance.node.role!=='sink') continue;
    const remove=()=>{
      const disconnect=hop=>{if(hop.inner?.instanceName===name){hop.inner=null;hop.nested=null;hop.backward=[];}if(hop.nested)disconnect(hop.nested);};
      pipelineRoots().forEach(p=>disconnect(p.hop));
    };
    const sink=box(`sink ${name}`,remove,instance);sink.className+=' sink-node';sink.dataset.sink=name;
    sinks.append(sink);
  }

}

function responseInsertion(hop,response) {
  if(!hop.inner && !pipelineTarget(hop))
    return {port:'Out',message:hop.backward.length?port(hop.backward[0].node,'In').head:response,prepend:true};
  return {port:'In',message:armResponse(hop),prepend:false};
}

function armEditor(hop, request, response, outer=$("source-name").value) {
  const names=instances();
  let previous=outer;
  const arm = textElement("div","","arm-hop");
  const forward = textElement("div","","arm-forward");
  const backward = textElement("div","","arm-backward");
  let current=request;
  for (let i=0;i<hop.forward.length;i++) {
    const n=hop.forward[i];
    forward.append(state.mode==='segments'?arrow(previous,names.get(n),"↓",carried(current)):textElement("span",requestLabel(current),"edge-label"),box(n.node.name,()=>hop.forward.splice(i),n));
    previous=names.get(n);
    current=portOf(port(n.node,"Out"));
  }
  forward.append(textElement("span",requestLabel(current),"edge-label"));
  const candidates=nodes().filter(c => (c.node.role === "forward" && samePort(portOf(port(c.node,"In")),current)) || (!hop.inner && c.node.role === "operator" && samePort(portOf(port(c.node,"UpReq")),current)));
  const targetHop=pipelineTarget(hop)?.hop;
  const next=names.get(hop.inner || targetHop?.forward[0] || targetHop?.inner);
  forward.append(slot("Add request node",candidates,c=>{
    if(c.node.role === "forward") hop.forward.push(c);
    else {hop.inner=c;hop.nested=newHop();delete hop.target;}
  },next?[previous,next]:null));
  let downstream;
  if(hop.inner) {
    downstream=textElement("div","","arm-operator");
    downstream.append(box(hop.inner.node.name,()=>{hop.inner=null;hop.nested=null;},hop.inner));
    downstream.append(armEditor(hop.nested,portOf(port(hop.inner.node,"DownReq")),port(hop.inner.node,"DownItem").head,names.get(hop.inner)));
  } else {
    const target=pipelineTarget(hop);
    const targetName=target?(directSink(target)?.instanceName || target.name):'';
    downstream=textElement('div','','join-node'+(target?' joined':''));
    if(target) {downstream.dataset.root=target.root;downstream.dataset.from=previous;downstream.dataset.to=next;}
    const connection=textElement('button',target ? '↗' : '+','pipeline-link add-node');
    connection.setAttribute('aria-label',target ? `Change connection to ${targetName}` : 'Add node or connect existing');
    connection.title=target ? `Connected to ${targetName}` : 'Add node or connect existing';
    connection.disabled=state.mode==='segments';
    connection.onclick=()=>openPipelineConnection(hop,current,hop.backward.length?port(hop.backward[0].node,'In').head:response);
    downstream.append(connection);
    if(target && state.mode==='graph') {
      const detach=textElement('button','×','remove');detach.title='Disconnect node';detach.setAttribute('aria-label','Disconnect node');
      detach.onclick=()=>{delete hop.target;render();};downstream.append(detach);
    }
  }
  const insertion=responseInsertion(hop,response);
  let returned=hop.inner ? port(hop.inner.node,"UpItem").head : pipelineTarget(hop)?.response;
  if(!returned) returned=hop.backward.length?port(hop.backward[0].node,'In').head:response;

  let returning=names.get(hop.inner || targetHop?.backward.at(-1) || targetHop?.inner);
  for(let i=0;i<hop.backward.length;i++) {
    const n=hop.backward[i];
    backward.append(state.mode==='segments'?arrow(returning,names.get(n),"↑",`stream ${short(returned)}`):textElement("span",responseLabel(returned),"edge-label"),box(n.node.name,()=>hop.backward.splice(i),n));
    returning=names.get(n);
    returned=port(n.node,"Out").head;
  }
  const mismatch=!same(returned,response);
  const repair=slot("Add response node",nodes().filter(c=>insertion.message && c.node.role==="backward" && same(port(c.node,insertion.port).head,insertion.message)),c=>insertion.prepend?hop.backward.unshift(c):hop.backward.push(c),returning?[returning,outer]:null);
  if(mismatch) {repair.className+=" mismatch"; repair.append(textElement("span",`Needs stream ${short(response)}`,"response-error"));}
  backward.append(textElement("span",responseLabel(returned),"edge-label"),repair);
  arm.append(forward,downstream,backward);
  return arm;
}

function entryControls() {
  const container=textElement("div","","service-entries");
  if(!state.frontend?.node.entries?.length) return container;
  state.frontend.arms ||= {};
  for(const entry of enabledEntries()) {
    const row=textElement("section","","entry-pipeline");
    const endpoint=box(entryLabel(entry)); endpoint.className+=" public-endpoint";
    endpoint.title=entry.path;
    endpoint.append(textElement("small",entry.path,"muted"));
    const hop=state.frontend.arms[entry.name] ||= newHop();
    row.append(endpoint,armEditor(hop,portOf(entry.request),entry.response.head,deployedRoots().find(p=>p.entry===entry.name)?.root));
    container.append(row);
  }
  if(!enabledEntries().length) container.append(textElement("p","Enable an endpoint in Source settings.","empty"));
  return container;
}

function sourcePort() {
  const message = chosenMessage($("source-request"));
  return $("input-mode").value === "stream"
    ? { head: chosenMessage($("source-header")), item: message }
    : { head: message, item: null };
}

function flowComplete(hop, request, response) {
  if (!hop.inner || !response) return false;
  let current = request;
  for (const n of hop.forward) {
    if (!samePort(portOf(port(n.node, "In")), current)) return false;
    current = portOf(port(n.node, "Out"));
  }
  const operator = hop.inner.node.role === "operator";
  if (!samePort(portOf(port(hop.inner.node, operator ? "UpReq" : "Req")), current)) return false;
  if (operator && (!hop.nested || !flowComplete(hop.nested,
    portOf(port(hop.inner.node, "DownReq")), port(hop.inner.node, "DownItem").head))) return false;
  let item = port(hop.inner.node, operator ? "UpItem" : "Item").head;
  for (const n of hop.backward) {
    if (!same(port(n.node, "In").head, item)) return false;
    item = port(n.node, "Out").head;
  }
  return same(item, response);
}

function textElement(tag, text, className) {
  const el = document.createElement(tag);
  el.textContent = text;
  if (className) el.className = className;
  return el;
}

function catalogResources(entry, query, view = "all") {
  const packageMatch = entry.package.toLowerCase().includes(query);
  const matches = resource => packageMatch || [resource.name, resource.path, resource.ty].some(value => value?.toLowerCase().includes(query));
  return { packageMatch, messages:view === "nodes" ? [] : (entry.messages || []).filter(matches), nodes:view === "messages" ? [] : (entry.nodes || []).filter(matches) };
}

function catalogContracts() {
  const slots=[];
  const visit=(hop,request,arm=false,response=null)=>{
    let current=request;
    for(const n of hop.forward) current=portOf(port(n.node,'Out'));
    slots.push({roles:hop.inner ? ['forward'] : ['forward','operator','sink'],request:current,response:arm?(hop.backward.length?port(hop.backward[0].node,'In').head:response):null});
    if(hop.inner?.node.role==='operator' && hop.nested)
      visit(hop.nested,portOf(port(hop.inner.node,'DownReq')),arm,port(hop.inner.node,'DownItem').head);
    if(hop.inner || arm) {
      let item=hop.inner ? port(hop.inner.node,hop.inner.node.role==='operator'?'UpItem':'Item').head : pipelineTarget(hop)?.response;
      for(const n of hop.backward) item=port(n.node,'Out').head;
      const insertion=arm?responseInsertion(hop,response):{port:'In',message:item};
      slots.push({roles:['backward'],port:insertion.port,request:{head:insertion.message || null,item:null}});
    }
  };
  for(const p of pipelineRoots()) if(p.response) visit(p.hop,p.request);
  for(const entry of enabledEntries()) visit(state.frontend.arms?.[entry.name] || newHop(),portOf(entry.request),true,entry.response.head);
  return slots;
}

function catalogNodeCompatible(node, slots=catalogContracts()) {
  if(state.mode!=='graph') return false;
  if(node.role==='source') return !state.frontend;
  const input={forward:'In',backward:'In',operator:'UpReq',sink:'Req'}[node.role];
  return !!input && slots.some(slot=>slot.roles.includes(node.role) && samePort(slot.request,portOf(port(node,slot.port || input))) && (node.role!=='sink' || !slot.response || same(port(node,'Item').head,slot.response)));
}

let catalogView="nodes";

function renderCatalog() {
  const container = $("entries");
  const routerMode=state.mode==='segments';
  for(const selector of ['.catalog-views','.catalog-grouping','.catalog-filter','.import-actions']) {
    const element=document.querySelector(selector);if(element) element.hidden=routerMode;
  }
  $('search').parentElement.hidden=routerMode;
  $('catalog-description').textContent=routerMode?'Choose how requests reach destination replicas.':'Your building blocks for inference.';
  $('catalog-kind').textContent=routerMode?'ROUTER TYPES':'PACKAGES';
  $('catalog-hint').textContent=routerMode?'Cut a request connection with scissors, then select a router type and its boundary. Responses return through their stream anchors.':'Expand a package to inspect its messages and nodes. Drag a message to a source port or a node to a matching slot.';
  $('catalog-title').textContent=routerMode?'Routers':'Catalog';
  if(routerMode) {
    container.replaceChildren();
    for(const [policy,label,description] of [['round_robin','Round robin','Distribute requests across destination replicas in turn.'],['random','Random','Choose a destination replica randomly for each request.']]) {
      const button=textElement('button','','router-type');button.append(textElement('strong',label),textElement('small',description,'muted'));
      button.onclick=()=>{
        const list=$('router-targets');list.replaceChildren();
        for(const node of Object.keys(activeRouting())) {
          const target=textElement('button',`→ ${node}`,'pipeline-choice');
          target.onclick=()=>{state.routing[node]=policy;$('router-target-dialog').close();render();};list.append(target);
        }
        if(!list.children.length) list.append(textElement('p','Use scissors on a request connection to create a network boundary first.','muted'));
        $('router-target-title').textContent=`${label}: choose request boundary`;$('router-target-dialog').showModal();
      };
      container.append(button);
    }
    $('package-count').textContent='2';return;
  }
  const expanded = new Set([...container.querySelectorAll(".package[open]")].map(el => el.dataset.package));
  container.replaceChildren();
  $('catalog-nodes').setAttribute('aria-pressed',String(catalogView==='nodes'));
  $('catalog-messages').setAttribute('aria-pressed',String(catalogView==='messages'));
  $('catalog-compatible').parentElement.hidden=catalogView!=='nodes';
  const flat=$("catalog-grouping").value === "flat";
  const flatResources=[];
  let packageCount=0;
  const query = $("search").value.toLowerCase().trim();
  const contracts=catalogContracts();
  const onlyCompatible=$("catalog-compatible").checked;
  for (const entry of state.entries) {
    const allMessages = entry.messages || [];
    const allNodes = entry.nodes || [];
    const {packageMatch, messages:offeredMessages, nodes:matchedNodes} = catalogResources(entry, query, catalogView);
    const offeredNodes=matchedNodes.filter(node=>!onlyCompatible || catalogNodeCompatible(node,contracts));
    if(onlyCompatible && !offeredMessages.length && !offeredNodes.length) continue;
    if (!offeredMessages.length && !offeredNodes.length) continue;
    packageCount++;
    const group = document.createElement("details");
    group.className = "package";
    group.dataset.package = `${entry.package}@${entry.version}`;
    group.open = !!query || expanded.has(group.dataset.package);
    const summary = textElement("summary", entry.package);
    summary.append(textElement("small", entry.version));
    group.append(summary, textElement("p", `${allMessages.length} messages · ${allNodes.length} nodes`, "counts"));
    for (const [kind, resources] of [["message", offeredMessages], ["node", offeredNodes]]) {
      for (const resource of resources) {
        const detail = document.createElement("details");
        const compatible=kind==='message' || catalogNodeCompatible(resource,contracts);
        detail.className = `resource ${kind}${compatible?'':' incompatible'}`;
        if(!compatible) detail.title='No available insertion point accepts this node. Add or remove nodes, or enable a frontend endpoint to open a matching slot.';

        const title = document.createElement("summary");
        title.append(textElement("span", kind === "message" ? "MSG" : resource.role.toUpperCase(), "type"), document.createTextNode(resource.name));

        if(flat) title.append(textElement("small", `${entry.package}@${entry.version}`, "resource-package"));
        detail.append(title);
        if (kind === "message") {
          detail.append(textElement("p", resource.path), textElement("p", `${resource.wire ? "Network codec available" : "Local only · no network codec"} · ${resource.public ? "Public" : "Private"}`));
        } else {
          detail.append(textElement("p", resource.ty));
          for (const p of resource.ports) detail.append(textElement("p", `${p.port}: ${carried(portOf(p))}`));
          for (const endpoint of resource.entries || []) detail.append(textElement("p", `${endpoint.path}: ${carried(portOf(endpoint.request))} → stream ${short(endpoint.response.head)} · ${endpoint.enabled}`));
          for (const field of resource.config || []) detail.append(textElement("p", `${field.name}: ${JSON.stringify(field.default)} · ${field.description}`));
        }
        if (compatible && (kind === "node" || resource.public)) makeDraggable(detail, kind === "message"
          ? {kind, key: key({package:entry.package, version:entry.version, path:resource.path})}
          : {kind, entry:`${entry.package}@${entry.version}`, name:resource.name}, resource.name);
        if(flat) flatResources.push({name:resource.name,package:entry.package,detail});
        else group.append(detail);
      }
    }
    if (!allNodes.length && allMessages.length) group.append(textElement("p", "This package provides messages only.", "muted"));
    if(!flat) container.append(group);
  }
  if(flat) for(const item of flatResources.sort((a,b)=>a.name.localeCompare(b.name) || a.package.localeCompare(b.package))) container.append(item.detail);
  $("package-count").textContent=packageCount;
  if (!container.children.length) container.append(textElement("p", state.entries.length ? "No matching packages or types." : "Your catalog is empty. Add a package to explore its messages and nodes.", "empty"));
}

function dropTarget(element, accept) {
  element.dataset.drop = "";
  dropHandlers.set(element, data => state.mode === "graph" && accept(data));
}

let cancelDrag = null;
window.addEventListener("blur", () => cancelDrag?.());

function makeDraggable(element, data, label) {
  let start = null;
  let ghost = null;
  let target = null;
  let suppressClick = false;
  element.classList.add("draggable");
  const cleanup = () => {
    ghost?.remove();
    target?.classList.remove("drop-active");
    ghost = target = start = null;
    if (cancelDrag === cleanup) cancelDrag = null;
  };
  element.onpointerdown = event => {
    if (event.button !== 0 || state.mode !== "graph") return;
    cancelDrag?.();
    cancelDrag = cleanup;
    suppressClick = false;
    start = {x:event.clientX, y:event.clientY};

  };
  element.ondragstart = event => event.preventDefault();
  element.onpointermove = event => {
    if (event.buttons === 0) { cleanup(); return; }
    if (!start || Math.hypot(event.clientX - start.x, event.clientY - start.y) < 6) return;
    if (!ghost) {
      element.setPointerCapture(event.pointerId);
      ghost = textElement("div", label, "drag-ghost");
      document.body.append(ghost);
    }
    ghost.style.left = `${event.clientX + 12}px`;
    ghost.style.top = `${event.clientY + 12}px`;
    target?.classList.remove("drop-active");
    target = document.elementFromPoint(event.clientX, event.clientY)?.closest("[data-drop]");
    target?.classList.add("drop-active");
  };
  element.onpointerup = event => {
    if (ghost) {
      suppressClick = true;
      const destination = document.elementFromPoint(event.clientX, event.clientY)?.closest("[data-drop]");
      const accepted = destination && dropHandlers.get(destination)?.(data);
      if (!accepted) $("flow-status").textContent = "Drop this type on a matching source port or node slot.";
    }
    cleanup();
  };
  element.onpointercancel = cleanup;
  element.onlostpointercapture = cleanup;
  element.addEventListener("click", event => {
    if (!suppressClick) return;
    event.preventDefault();
    event.stopPropagation();
    suppressClick = false;
  }, true);
}

function fillMessages(select, none) {
  const chosen = select.value;
  select.innerHTML = "";
  if (none) select.add(new Option(none, ""));
  for (const m of messages().filter(m => state.entries.some(e => e.package === m.origin.package && e.version === m.origin.version && e.messages.some(item => item.path === m.origin.path && item.public)))) {
    select.add(new Option(m.label, key(m.origin)));
  }
  if (chosen) select.value = chosen;
}

const chosenMessage = (select) => messages().find((m) => key(m.origin) === select.value)?.origin || null;

function identityNode(role,request,response) {
  const ports=role==='operator'?[{port:'UpReq',...request},{port:'DownReq',...request},{port:'UpItem',head:response},{port:'DownItem',head:response}]
    :role==='forward'?[{port:'In',...request},{port:'Out',...request}]:[{port:'In',head:response},{port:'Out',head:response}];
  return {entry:'built-in',node:{name:role==='operator'?'IdentityOperator':'Identity',role,identity:true,ports}};
}
function identitySpec(instance) {
  const n=instance.node;
  return {role:n.role,...(n.role!=='backward'?{request:portOf(port(n,n.role==='operator'?'UpReq':'In'))}:{}),
    ...(n.role!=='forward'?{response:port(n,n.role==='operator'?'UpItem':'In').head}:{})};
}
function openIdentity(instance,choose,choices=[instance]) {
  const roles=$("identity-role");roles.replaceChildren();
  for(const choice of choices) {
    const option=textElement("option",{forward:"Single message · request node",backward:"Single message · response node",operator:"Request/response operator"}[choice.node.role]);
    option.value=choice.node.role;roles.append(option);
  }
  roles.value=instance.node.role;roles.disabled=choices.length===1 || state.mode==="segments";
  roles.onchange=()=>openIdentity(choices.find(c=>c.node.role===roles.value),choose,choices);
  const spec=identitySpec(instance), request=spec.request, response=spec.response;
  $('identity-title').textContent=spec.role==='operator'?'Identity operator':'Identity node';
  $('identity-kind').textContent=spec.role==='operator'?'Pass requests and responses through unchanged.':spec.role==='forward'?'Pass requests through unchanged. Responses bypass this node.':'Pass responses through unchanged. Requests bypass this node.';
  $('identity-name').value=instance.instanceName || instance.node.name;
  $('identity-name').disabled=!!instance.instanceName;
  $('identity-request-fields').hidden=spec.role==='backward';$('identity-response-fields').hidden=spec.role==='forward';
  for(const id of ['identity-request','identity-header','identity-response']) fillMessages($(id),id==='identity-header'?'No header':'Choose message…');
  $('identity-mode').value=request?.item?'stream':'single';
  $('identity-request').value=key(request?.item || request?.head);$('identity-header').value=key(request?.item && request.head);
  $('identity-response').value=key(response);
  for(const id of ['identity-mode','identity-request','identity-header','identity-response']) $(id).disabled=state.mode==='segments';
  $('identity-form').querySelector('button.primary').textContent=state.mode==='segments'?'Done':instance.instanceName?'Save identity':'Add identity';
  $('identity-form').onsubmit=event=>{
    event.preventDefault();
    if(state.mode==='segments') {$('identity-dialog').close();return;}
    const name=$('identity-name').value.trim();const used=[...instances()].some(([n,id])=>n!==instance && id===name);
    if(used || pipelineRoots().some(p=>p.root===name)) {$('identity-name').setCustomValidity('Choose a unique node name.');$('identity-name').reportValidity();return;}
    const selected=chosenMessage($('identity-request'));
    const req=$('identity-mode').value==='stream'?{head:chosenMessage($('identity-header')),item:selected}:{head:selected,item:null};
    const item=chosenMessage($('identity-response'));
    if((request && !samePort(req,request)) || (spec.role==='backward' && !same(item,response))) {
      $('identity-error').textContent='The message type must match the selected insertion point.';return;
    }
    if(spec.role!=='forward' && !item) {$('identity-error').textContent='Choose a response message type.';return;}
    choose({...identityNode(spec.role,req,item),instanceName:name});$('identity-dialog').close();render();
  };
  $('identity-name').oninput=()=> $('identity-name').setCustomValidity('');$('identity-error').textContent='';
  $('identity-dialog').showModal();
}

function selectorNodes(candidates) {
  const identities=candidates.filter(c=>c.node.identity);
  return identities.length ? [{...identities[0],node:{...identities[0].node,name:'Identity'},specializations:identities},...candidates.filter(c=>!c.node.identity)] : candidates;
}

// A select of the nodes that fit, and what choosing one does.
function slot(label, candidates, choose, edge) {
  const div = document.createElement("div");
  div.className = "slot";
  if(state.mode === 'segments') {
    let cut;
    if(edge) {
      cut=arrow(edge[0],edge[1],'','');
      cut.className=cut.className.replace('arrow','cut-node');
    } else {
      cut=document.createElement('button'); cut.className='cut-node local-edge'; cut.disabled=true;
      cut.title='Complete this typed connection before adding a network boundary.';
      cut.setAttribute('aria-label',cut.title);
    }
    cut.innerHTML=scissors;
    div.append(cut);
    if(edge && networkEdges().some(e=>e.request && e.from===edge[0] && e.to===edge[1]) && activeRouting()[edge[1]]) div.append(routerButton(edge[1]));
    return div;
  }
  const add = textElement("button", "+", "add-node");
  add.setAttribute("aria-label", label); add.title = label;
  add.disabled = state.mode === "segments";
  add.onclick = () => {
    $("node-title").textContent = label;
    const draw = () => {
      const query = $("node-search").value.trim().toLowerCase();
      const options = selectorNodes(candidates).filter(c=>c.node.name.toLowerCase().includes(query));
      $("node-options").replaceChildren();
      for(const candidate of options) {
        const button = textElement("button",candidate.existing ? `Connect ${candidate.instanceName}` : candidate.node.name,"node-choice");
        button.append(textElement("small",candidate.specializations ? "Choose message types and node/operator kind" : candidate.existing ? "existing sink · graph" : `${candidate.node.role} · ${candidate.entry}`,"muted"));
        button.onclick=()=>{$("node-dialog").close();if(candidate.node.identity) openIdentity(candidate.specializations?.[0] || candidate,choose,candidate.specializations || [candidate]);else {choose({...candidate});render();}};
        $("node-options").append(button);
      }
      if(!options.length) $("node-options").append(textElement("p","No matching nodes in the catalog.","muted"));
    };
    $("node-search").value=""; $("node-search").oninput=draw; draw();
    $("node-dialog").showModal(); $("node-search").focus();
  };
  dropTarget(div, data => {
    const candidate = candidates.find(c => data.kind === "node" && c.entry === data.entry && c.node.name === data.name);
    if (!candidate) return false;
    if(candidate.node.identity) openIdentity(candidate,choose);
    else {choose({...candidate});render();}
    return true;
  });
  div.append(add);
  return div;
}

// A message as an arrow names it: its type's name, and a streamed
// request's item.
const short = (origin) => origin ? origin.path.split("::").pop() : "()";
const carried = (p) => p.item ? `${p.head ? short(p.head) + " + " : ""}stream ${short(p.item)}` : short(p.head);

const scissors = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><circle cx="6" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="m8 8 12 12M8 16 20 4"/></svg>';

function networkEdges(hop, outer, request, names=instances()) {
  if(!hop) return deployedRoots().flatMap(p=>networkEdges(p.hop,p.root,p.request,names));
  const result=[];
  let from=outer, carried=request;
  for(const n of hop.forward) {
    const to=names.get(n); result.push({from,to,types:[carried.head,carried.item],request:true});
    from=to; carried=portOf(port(n.node,"Out"));
  }
  if(!hop.inner) return result;
  const inner=names.get(hop.inner), operator=hop.inner.node.role === 'operator';
  result.push({from,to:inner,types:[carried.head,carried.item],request:true});
  from=inner;
  let item=port(hop.inner.node,operator?'UpItem':'Item').head;
  for(const n of hop.backward) {
    const to=names.get(n); result.push({from,to,types:[item]});
    from=to; item=port(n.node,'Out').head;
  }
  result.push({from,to:outer,types:[item]});
  if(hop.nested) result.push(...networkEdges(hop.nested,inner,portOf(port(hop.inner.node,'DownReq')),names));
  return result;
}

function boundaryProblem(node,destination,onlyNode=false) {
  const groups=segments(instances());
  const placement=Object.fromEntries(Object.entries(groups).flatMap(([group,members])=>members.map(member=>[member,group])));
  for(const member of onlyNode?[node]:downstreamMembers(node)) placement[member]=destination;
  for(const edge of networkEdges()) {
    if(placement[edge.from]===placement[edge.to]) continue;
    for(const type of edge.types.filter(Boolean)) {
      const entry=state.entries.find(e=>e.package===type.package && e.version===type.version && (!type.source || e.source===type.source));
      if(!entry?.messages?.find(m=>m.path===type.path)?.wire)
        return `${short(type)} has no network codec (Serialize/Deserialize). This connection must stay local.`;
    }
  }
  return '';
}

function activeRouting() {
  const placement=Object.fromEntries(Object.entries(segments(instances())).flatMap(([group,members])=>members.map(member=>[member,group])));
  return Object.fromEntries(networkEdges().filter(edge=>edge.request && placement[edge.from]!==placement[edge.to]).map(edge=>[edge.to,state.routing[edge.to] || 'round_robin']));
}

function routerButton(node) {
  const policy=state.routing[node] || 'round_robin';
  const button=textElement('button',policy==='random'?'Random':'Round robin','request-router');
  button.setAttribute('aria-label',`Request router for ${node}`);
  button.title='Select the destination replica for each request. Responses return through their stream anchor.';
  button.onclick=()=>{
    $('router-title').textContent=`Request router → ${node}`;
    $('router-policy').value=state.routing[node] || 'round_robin';
    $('router-form').onsubmit=event=>{event.preventDefault();state.routing[node]=$('router-policy').value;$('router-dialog').close();render();};
    $('router-dialog').showModal();
  };
  return button;
}

function openBoundary(from,to) {
  const groups=segments(instances());
  const node=deployedRoots().some(p=>p.root===to)?from:to;
  $('boundary-title').textContent=`${from} → ${to}`;
  $('boundary-help').textContent=`Choose whether to move only ${node} or its downstream chain. Responses retain their own path.`;
  $('boundary-scope').value='node';
  const select=$('boundary-placement'); select.replaceChildren();
  for(const name of Object.keys(groups)) select.add(new Option(name,name));
  select.add(new Option('New segment…','')); select.value='';
  let name=`${node.toLowerCase()}-segment`;
  while(groups[name]) name+='-new';
  $('boundary-name').value=name;
  select.onchange=()=>{$('boundary-new').hidden=select.value!==''; $('boundary-name').required=select.value==='';};
  select.onchange();
  $('boundary-form').onsubmit=event=>{
    event.preventDefault();
    const destination=select.value || $('boundary-name').value.trim();
    const onlyNode=$("boundary-scope").value==='node';
    const problem=boundaryProblem(node,destination,onlyNode);
    if(problem) { $('boundary-help').textContent=problem; return; }
    if(onlyNode) placeNode(node,destination,instances());else placeDownstream(node,destination); $('boundary-dialog').close(); render();
  };
  $('boundary-dialog').showModal();
}

function arrow(from, to, direction, label) {
  const div=document.createElement('button');
  const groups=segments(instances());
  const segmentOf=node=>Object.keys(groups).find(group=>groups[group].includes(node));
  const cut=segmentOf(from)!==segmentOf(to);
  const moving=deployedRoots().some(p=>p.root===to)?from:to;
  const problem=boundaryProblem(moving,cut?segmentOf(from):'__new_segment',true);
  div.className=`arrow${cut?' cut':''}${problem?' local-edge':''}`;
  div.disabled=state.mode!=='segments';
  div.setAttribute('aria-disabled',String(!!problem));
  div.textContent=`${direction} ${label}${cut?` · ${segmentOf(from)} → ${segmentOf(to)}`:''}`;
  div.title=problem || `${from} → ${to}: ${cut?'edit':'add'} network boundary`;
  div.setAttribute('aria-label',div.title);
  div.onclick=()=>{if(!problem) openBoundary(from,to);};
  if(cut && !problem && direction==='↓' && state.mode==='segments') {
    const connection=textElement('div','','routed-connection');
    connection.append(div,routerButton(to)); return connection;
  }
  return div;
}

// A node, with a control that removes it when it has one.
function box(text, remove, instance) {
  const div = document.createElement("div");
  div.className = "box";
  div.textContent = text;
  if (instance) {
    div.append(settingsButton(instance));
    if(state.mode==='segments') {
      const name=instances().get(instance) || $('source-name').value;
      const group=Object.entries(segments(instances())).find(([,members])=>members.includes(name))?.[0];
      div.append(textElement('small',group,'segment-badge'));
      div.setAttribute('data-segment',group); div.setAttribute('data-member',name);
    }
  }
  if (remove && state.mode === "graph") {
    const x = document.createElement("button");
    x.className = "remove";
    x.textContent = " ✕";
    x.title = "Remove node";
    x.setAttribute("aria-label", `Remove ${text}`);
    x.onclick = () => { remove(); render(); };
    div.append(x);
  }
  return div;
}

function lane(title) {
  const div = document.createElement("div");
  div.className = "lane";
  const h = document.createElement("div");
  h.className = "muted";
  h.textContent = title;
  div.append(h);
  return div;
}

// Refresh implementations without discarding an unfinished type conversion chain.
function now(n) {
  if(n.node.identity) return n;
  const current = nodes().find((c) => c.source === n.source && c.node.name === n.node.name);
  return current ? Object.assign(n, current) : null;
}

function prune(hop, request) {
  // Removed catalog entries cannot be emitted into a deployment.
  const forward = hop.forward.map(now);
  const gone = forward.indexOf(null);
  hop.forward = gone === -1 ? forward : forward.slice(0, gone);
  hop.inner = gone === -1 && hop.inner ? now(hop.inner) : null;
  if (hop.inner) {
    const operator = hop.inner.node.role === "operator";
    if (operator && !hop.nested) hop.nested = newHop();
    if (!operator) hop.nested = null;
  }
  const backward = hop.backward.map(now);
  const lost = backward.indexOf(null);
  hop.backward = lost === -1 ? backward : backward.slice(0, lost);


}

// One hop, as two lanes: the request flows down from the outer end
// through the forward nodes to the inner end, and the response flows back
// up the lane beside it, through the backward nodes, to the outer end. An
// operator's own hop is drawn below it, indented. `request` is the message
// that arrives from `outer`.
function hopEditor(hop, request, into, outer, names, response = chosenMessage($("source-response"))) {
  prune(hop, request);
  const all = [...nodes(), ...[...names.keys()].filter(n=>n.node.role==='sink').map(n=>({...n,existing:true}))];
  const grid = document.createElement("div");
  grid.className = "hop";
  const down = lane("request ↓");
  const up = lane("response ↑");
  grid.append(down, up);
  into.append(grid);

  let current = request;
  let previous = outer;
  hop.forward.forEach((n, i) => {
    const name = names.get(n);
    down.append(arrow(previous, name, "↓", carried(current)));
    down.append(box(`forward ${name}`, () => hop.forward.splice(i), n));
    current = portOf(port(n.node, "Out"));
    previous = name;
  });
  if (!hop.inner) {
    down.append(textElement("span", `↓ ${carried(current)}`, "edge-label"));
    down.append(slot("Add pipeline node",
      [identityNode("forward",current),identityNode("operator",current,response),...all].filter(c => (c.node.role === "forward" && samePort(portOf(port(c.node,"In")),current)) ||
        (c.node.role === "sink" && samePort(portOf(port(c.node,"Req")),current)) ||
        (c.node.role === "operator" && samePort(portOf(port(c.node,"UpReq")),current))),
      c => {
        if(c.node.role === "forward") hop.forward.push(c);
        else {hop.inner=c;hop.nested=c.node.role === "operator" ? newHop() : null;hop.backward=[];}
      }));
    up.append(textElement("span", `stream ${short(response)} ↑`, "edge-label"));
    return;
  }
  const requestSlot=slot("add a forward node",
    [identityNode("forward",current),...all].filter(c => c.node.role === "forward" && samePort(portOf(port(c.node,"In")),current)),
    c => {hop.forward.push(c);}, [previous,names.get(hop.inner)]);
  const expected=portOf(port(hop.inner.node,hop.inner.node.role==='operator'?'UpReq':'Req'));
  if(!samePort(current,expected)) {requestSlot.className+=' mismatch';requestSlot.append(textElement('span',`Needs ${carried(expected)}`,'response-error'));}
  down.append(requestSlot);
  const inner = names.get(hop.inner);
  const operator = hop.inner.node.role === "operator";
  down.append(arrow(previous, inner, "↓", carried(current)));
  if(operator) {
    const endpoint=box(`operator ${inner}`,()=>{hop.inner=null;hop.nested=null;hop.backward=[];},hop.inner);
    endpoint.className+=' endpoint';grid.append(endpoint);
  } else {
    const connection=textElement('div','','sink-join endpoint');connection.dataset.sink=inner;
    if(state.mode==='graph') {
      const detach=textElement('button','×','disconnect-sink');detach.setAttribute('aria-label',`Disconnect ${inner} from pipeline`);detach.title=`Disconnect ${inner}`;
      detach.onclick=()=>{hop.inner=null;hop.backward=[];render();};connection.append(detach);
    }
    grid.append(connection);
  }
  if (operator) {
    const nested = document.createElement("div");
    nested.className = "nested";
    nested.append(`${inner}'s own hop`);
    grid.append(nested);
    hopEditor(hop.nested, portOf(port(hop.inner.node, "DownReq")), nested, inner, names, port(hop.inner.node, "DownItem").head);
  }

  // The response, from the inner end out: each backward node, and the
  // message on the edge into it. Drawn bottom to top, so the arrows point up.
  let item = port(hop.inner.node, operator ? "UpItem" : "Item").head || null;
  const steps = [];
  let from = inner;
  hop.backward.forEach((n, i) => {
    const name = names.get(n);
    steps.push({ from, to: name, item, n, i });
    item = port(n.node, "Out").head || null;
    from = name;
  });
  const arriving = item;
  const repair = slot("add a backward node",
    [identityNode("backward",null,arriving),...all].filter((c) => c.node.role === "backward" && same(port(c.node, "In").head, arriving)),
    (c) => hop.backward.push(c), [from,outer]);
  if(response && !same(arriving,response)) {
    repair.className += " mismatch";
    repair.append(textElement("span",`Needs stream ${short(response)}`,"response-error"));
  }
  up.append(repair);
  up.append(arrow(from, outer, "↑", `stream ${short(arriving)}`));
  for (const step of steps.reverse()) {
    up.append(box(`backward ${step.to}`, () => hop.backward.splice(step.i), step.n));
    up.append(arrow(step.from, step.to, "↑", `stream ${short(step.item)}`));
  }

}

// Graph names: a node's own name, numbered when it repeats.
function instances() {
  const names = new Map();
  const canonical = new Map();
  const taken = new Set([...pipelineRoots().map(p=>p.root),...state.usedNames]);
  const visit = (hop) => {
    if(hop.inner?.instanceName && canonical.has(hop.inner.instanceName)) hop.inner=canonical.get(hop.inner.instanceName);
    for (const n of [...hop.forward, hop.inner, ...hop.backward].filter(Boolean)) {
      if(!n.instanceName) {
        let name=n.node.name;
        for(let i=2;taken.has(name);i++) name=`${n.node.name}${i}`;
        n.instanceName=name;taken.add(name);state.usedNames.push(name);
      }
      canonical.set(n.instanceName,n);names.set(n,n.instanceName);
    }
    if (hop.nested) visit(hop.nested);
  };
  for(const entry of enabledEntries()) visit(state.frontend.arms?.[entry.name] || newHop());
  pipelineRoots().forEach(p=>visit(p.hop));
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

// Placement is explicit: moving a node updates both sides of its flow.
function segments(names) {
  const groups = new Map();
  for (const member of [...deployedRoots().map(p=>p.root), ...names.values()]) {
    const group = deployedRoots().some(p=>p.root===member) ? (state.placement[$("source-name").value] || "s1") : (state.placement[member] || "s1");
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group).push(member);
  }
  const result = Object.create(null);
  [...groups.values()].forEach((members, i) => {
    let name = state.segmentNames[members[0]] || state.placement[members[0]] || "s1";
    while (name in result) name = `${name}-${i + 1}`;
    result[name] = members;
  });
  return result;
}

function downstreamMembers(node) {
  const names=instances();
  const roles=Object.fromEntries([...names].map(([n,name])=>[name,n.node.role]));
  const edges=networkEdges(), groups=segments(names);
  const owner=Object.keys(groups).find(g=>groups[g].includes(node));
  const members=new Set([node]), pending=[node];
  while(pending.length) {
    const current=pending.pop();
    for(const edge of edges.filter(e=>e.from===current && (e.request || roles[e.to]==='backward'))) {
      if(groups[owner]?.includes(edge.to) && !members.has(edge.to)) {members.add(edge.to);pending.push(edge.to);}
    }
  }
  return members;
}
function placeDownstream(node,destination) {
  const members=downstreamMembers(node), names=instances();
  for(const member of members) placeNode(member,destination,names);
}
function placeNode(node, destination, names) {
  const placement = Object.create(null);
  for (const [name, members] of Object.entries(segments(names))) {
    for (const member of members) placement[member] = name;
  }
  placement[node] = destination;
  state.placement = placement;
  state.segmentNames = Object.create(null);

}

function placementControls(names) {
  const panel = $("placements");
  panel.replaceChildren();
  const groups = segments(names);
  for (const [name, members] of Object.entries(groups)) {
    for (const member of members) {
      if(deployedRoots().some(p=>p.root===member) && member!==$("source-name").value) continue;
      const row = textElement("label", member, "placement-row");
      const select = document.createElement("select");
      select.setAttribute("aria-label", `Place ${member} in segment`);
      for (const group of Object.keys(groups)) select.add(new Option(group, group));
      select.add(new Option("New segment…", ""));
      select.value = name;
      select.onchange = () => {
        let destination = select.value;
        if (!destination) {
          destination = member.toLowerCase() + "-segment";
          while (groups[destination]) destination += "-new";
        }
        const problem=boundaryProblem(member,destination);
        if(problem) { select.value=name; $('placement-summary').textContent=problem; return; }
        placeNode(member, destination, names);
        render();
      };
      row.append(select);
      panel.append(row);
    }
  }
  const segmentOf = member => Object.keys(groups).find(group => groups[group].includes(member));
  const crossings = networkEdges().filter(e => segmentOf(e.from) !== segmentOf(e.to));
  $("placement-summary").textContent = `${Object.keys(groups).length} segment${Object.keys(groups).length === 1 ? "" : "s"} · ${crossings.length} network crossings`;
}

function render(validate = true) {
  $("source-config").replaceChildren(configControls(state.frontend));
  $("source-ports").hidden=!!state.frontend?.node.entries?.length;
  $("source-title").textContent = $("source-name").value;
  $("source-subtitle").textContent = state.frontend?.node.name || "Choose a frontend";
  $("shared-contract").replaceChildren(textElement("strong", "Pipelines"), textElement("span", "Named request/response contracts · connect explicitly", "muted"));
  $("source-entries").replaceChildren(entryControls());
  $("source-help").textContent = state.frontend?.node.entries?.length
    ? "Wire each endpoint independently. Create a named pipeline or explicitly connect to an existing one. Each node can be placed independently in Segments."
    : "Choose or drop a source frontend to host a service. Its ports define the graph contract.";
  const hop = $("hop");
  hop.innerHTML = "";
  const names = instances();
  const source = $("source-name").value;
  const request = sourcePort();
  const ready = chosenMessage($("source-request")) && chosenMessage($("source-response"));
  renderPipelines(hop,names);
  const count = names.size + 1;
  $("node-count").textContent = `${count} node${count === 1 ? "" : "s"}`;

  const groups = $("groups");
  groups.innerHTML = "";
  const named = Object.entries(segments(names));
  for (const [name, members] of named) {
    const div = document.createElement("div");
    const input = document.createElement("input");
    input.value = name;
    input.size = 12;
    input.oninput = () => {
      // A name another segment has is refused: the two would be one.
      if (Object.entries(segments(names)).some(([other, them]) => other === input.value && them[0] !== members[0])) {
        input.value = state.segmentNames[members[0]] || name;
        input.title = "another segment has that name";
        return;
      }
      state.segmentNames[members[0]] = input.value;
      if (state.mode === "segments") {
        placementControls(names);
        hop.innerHTML = "";
        renderPipelines(hop,names);
      }
      paintSegments();
      if (typeof requestAnimationFrame === "function") requestAnimationFrame(drawWorkspaceEdges);
      scheduleValidation();
    };
    input.setAttribute("aria-label", `Segment for ${members.join(", ")}`);
    div.append(input, `: ${members.join(", ")}`);
    groups.append(div);
  }
  if (state.mode === "segments") placementControls(names);
  updateMode();
  if (validate) scheduleValidation();
  paintSegments();
  if($("entries").querySelectorAll) renderCatalog();
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(drawWorkspaceEdges);
}

function draft() {
  const names = instances();
  const toHop = (hop) => ({
    forward: hop.forward.map((n) => names.get(n)),
    inner: hop.inner ? names.get(hop.inner) : "",
    nested: hop.nested ? toHop(hop.nested) : null,
    backward: hop.backward.map((n) => names.get(n)),
  });
  const roots=deployedRoots();
  const first=roots[0];
  return {
    name: $("name").value,
    entries: Object.fromEntries(roots.filter(p=>p.entry).map(p=>[p.entry,{target:p.root,forward:[],backward:[]}])),
    source: {
      name: $("source-name").value,
      request: first?.request || sourcePort(),
      response: first?.response || chosenMessage($("source-response")),
    },
    nodes: [
      ...(state.frontend ? [{name:$("source-name").value,entry:state.frontend.entry,node:state.frontend.node.name,config:graphConfig(state.frontend)}] : []),
      ...[...names.entries()].filter(([n])=>!n.node.identity).map(([n, name]) => ({ name, entry: n.entry, node: n.node.name, config:graphConfig(n) })),
    ],
    hop: toHop(first?.hop || newHop()),
    pipeline_names: Object.fromEntries(roots.map(p=>[p.root,p.name])),
    identities: Object.fromEntries([...names].filter(([n])=>n.node.identity).map(([n,name])=>[name,identitySpec(n)])),
    pipelines: roots.slice(1).map(p=>({source:{name:p.root,request:p.request,response:p.response},hop:toHop(p.hop)})),
    segments: segments(names),
    routing: activeRouting(),
    deployment: deploymentDraft(segments(names)),
  };
}

let diagramSequence = 0;

async function show(body) {
  const result = $("result");
  result.innerHTML = "";
  if (body.problems) {
    const div = document.createElement("div");
    div.className = "problems";
    div.textContent = body.problems.join("\n");
    result.append(div);
  }
  let drawing = null;
  if (body.mermaid) {
    drawing = textElement("div", "Rendering diagram…", "diagram");
    result.append(textElement("h2", "Diagram"), drawing);
  }
  for (const [title, text] of [["crossings", body.crossings], ["graph file", body.graph], ["spec", body.spec], ["Mermaid source", body.mermaid]]) {
    if (!text) continue;
    const h = document.createElement("h2");
    h.textContent = title;
    const pre = document.createElement("pre");
    pre.textContent = text;
    result.append(h, pre);
  }
  if (drawing) {
    try {
      mermaid.initialize({startOnLoad:false, securityLevel:"strict", theme:"dark", suppressErrorRendering:true});
      const {svg} = await mermaid.render(`graph-diagram-${++diagramSequence}`, body.mermaid);
      if (drawing.isConnected) drawing.innerHTML = svg;
    } catch (error) {
      if (drawing.isConnected) drawing.textContent = `Diagram could not be rendered: ${error.message}`;
    }
  }
}

let deployOpen=false;
function updateMode() {
  const segmentMode = state.mode === "segments";
  $("graph-tab").setAttribute("aria-selected", String(!segmentMode && !deployOpen));
  $("segments-tab").setAttribute("aria-selected", String(segmentMode && !deployOpen));
  $("canvas").setAttribute("aria-labelledby", segmentMode ? "segments-tab" : "graph-tab");
  $("canvas").hidden=deployOpen;
  $("deploy-panel").hidden=!deployOpen;
  $("deploy-tab").setAttribute("aria-selected",String(deployOpen));
  $("deploy-tab").disabled=!state.validated && !deployOpen;
  $("export").hidden=deployOpen;
  $("segments").hidden = !segmentMode;
  $("segments-tab").disabled = !state.validated && !segmentMode;
  $("export").disabled = !state.validated;
  $("canvas-title").textContent = segmentMode ? "Place network boundaries" : "Build your request flow";
  $("canvas-help").textContent = segmentMode ? "Each box is one process. Move nodes to assign ownership; scissors split connections. Blue requests route forward, purple responses return to their named destination." : "Define the source, then connect nodes with matching message types.";
  for (const id of ["source-name", "source-frontend"]) $(id).disabled = segmentMode;
  for (const id of ["input-mode", "source-request", "source-header", "source-response"]) $(id).disabled = segmentMode || (!!state.frontend && !state.frontend.node.entries?.length);
  $("header-options").hidden = $("input-mode").value !== "stream";
}

function scheduleValidation() {
  recordHistory();
  clearTimeout(state.timer);
  state.revision++;
  state.validated = false;
  updateMode();
  $("validation-status").textContent = "";
  $("result").replaceChildren();
  if (!graphComplete()) {
    $("flow-status").textContent = "Complete each endpoint connection and pipeline to unlock Segments.";
    return;
  }
  $("flow-status").textContent = "Checking graph…";
  state.timer = setTimeout(() => validateGraph(), 250);
}

async function validateGraph() {
  clearTimeout(state.timer);
  const revision = ++state.revision;
  state.validated = false;
  updateMode();
  try {
    const payload = draft();
    if (state.mode === "graph") Object.assign(payload,{segments:{s1:[payload.source.name, ...(payload.pipelines || []).map(p=>p.source.name), ...Object.keys(payload.identities || {}), ...payload.nodes.map(n => n.name).filter(name => name !== payload.source.name)]},routing:{}});
    const response = await fetch("/api/check", {method:"POST", headers:{"content-type":"application/json"}, body:JSON.stringify(payload)});
    const body = await response.json();
    if (revision !== state.revision) return;
    state.validated = response.ok && body.ok === true;
    show(body);
    $("flow-status").textContent = state.validated ? (state.mode === "graph" ? "Flow complete · Segments is ready" : "Placement valid · Ready to download") : "Graph needs attention. See validation details.";
    $("validation-status").textContent = state.validated ? "Valid" : "Needs attention";
  } catch (error) {
    if (revision !== state.revision) return;
    $("flow-status").textContent = "Could not reach the server. Check the connection and retry.";
    show({problems:[error.message]});
  }
  updateMode();
}

$("check").onclick = validateGraph;
$("graph-tab").onclick = () => { deployOpen=false; state.mode = "graph"; setWorkspaceZoom(workspaceZooms.graph,true); render(); };
$("segments-tab").onclick = () => {
  deployOpen=false;
  if (!state.validated) return;
  state.mode = "segments";
  setWorkspaceZoom(workspaceZooms.segments,true);
  render();
};

$("export").onclick = async () => {
  $("export").disabled = true;
  try {
    const payload = draft();
    if (state.mode === "graph") Object.assign(payload,{segments:{s1:[payload.source.name, ...(payload.pipelines || []).map(p=>p.source.name), ...Object.keys(payload.identities || {}), ...payload.nodes.map(n => n.name).filter(name => name !== payload.source.name)]},routing:{}});
    const response = await fetch("/api/export", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload) });
    if (!response.ok) { show(await response.json()); return; }
    const link = document.createElement("a");
    link.href = URL.createObjectURL(await response.blob());
    link.download = `${payload.name}.tar.gz`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(link.href), 1000);
  } catch (error) { show({problems:[error.message]}); }
  finally { updateMode(); }
};

const importer = {source:"registry", path:"", revision:0, timer:null, busy:false};

function importMode(source) {
  importer.source = source;
  importer.revision++;
  clearTimeout(importer.timer);
  $("import-title").textContent = `Add from ${source === "registry" ? "crates.io" : source === "git" ? "Git" : "local"}`;
  for (const type of ["registry", "git", "local"]) $(type + "-fields").hidden = type !== source;
  for (const id of ["crate-name", "crate-version"]) $(id).required = source === "registry";
  for (const id of ["git-url", "git-revision"]) $(id).required = source === "git";
  $("import-status").textContent = "";
  $("import-submit").disabled = source === "local";
  $("import-dialog").showModal();
  if (source === "local") browseLocal("");
  else $(source === "git" ? "git-url" : "crate-query").focus();
}

function searchCrates() {
  clearTimeout(importer.timer);
  const revision = ++importer.revision;
  const query = $("crate-query").value.trim();
  $("crate-results").replaceChildren();
  if (query.length < 2) return;
  $("crate-results").textContent = "Searching…";
  importer.timer = setTimeout(async () => {
    try {
      const response = await fetch(`/api/crates?q=${encodeURIComponent(query)}`);
      const body = await response.json();
      if (revision !== importer.revision) return;
      if (!response.ok) throw new Error((body.problems || ["Search failed."]).join("; "));
      $("crate-results").replaceChildren();
      if (!body.crates.length) $("crate-results").textContent = "No packages found.";
      for (const crate of body.crates) {
        const button = textElement("button", `${crate.name} · ${crate.max_stable_version || crate.max_version}`, "search-result");
        button.type = "button";
        button.append(textElement("span", crate.description || "No description", "muted"));
        button.onclick = () => {
          $("crate-name").value = crate.name;
          $("crate-version").value = crate.max_stable_version || crate.max_version;
          $("import-status").textContent = `Selected ${crate.name}. You can change the exact version below.`;
        };
        $("crate-results").append(button);
      }
    } catch (error) {
      if (revision === importer.revision) $("crate-results").textContent = error.message;
    }
  }, 350);
}

async function browseLocal(path) {
  const revision = ++importer.revision;
  $("import-submit").disabled = true;
  $("local-package").textContent = "Loading directory…";
  try {
    const response = await fetch(`/api/local?path=${encodeURIComponent(path)}`);
    const body = await response.json();
    if (revision !== importer.revision) return;
    if (!response.ok) throw new Error((body.problems || ["Directory could not be read."]).join("; "));
    importer.path = body.path;
    $("local-path").textContent = "/" + body.path;
    $("local-up").disabled = body.parent === null;
    $("local-up").onclick = () => browseLocal(body.parent);
    $("local-directories").replaceChildren();
    for (const directory of body.directories) {
      const button = textElement("button", directory.name + "/", "directory");
      button.type = "button";
      button.onclick = () => browseLocal(directory.path);
      $("local-directories").append(button);
    }
    $("local-package").textContent = body.package ? `Ready to add ${body.package}` : "Choose a directory containing a package Cargo.toml.";
    $("import-submit").disabled = !body.package || importer.busy;
  } catch (error) {
    if (revision === importer.revision) $("local-package").textContent = error.message;
  }
}

$("add-local").onclick = () => importMode("local");
$("add-registry").onclick = () => importMode("registry");
$("add-git").onclick = () => importMode("git");
$("crate-query").oninput = searchCrates;
$("import-dialog").onclose = () => { importer.revision++; clearTimeout(importer.timer); };
$("import-dialog").oncancel = event => { if (importer.busy) event.preventDefault(); };
$("import-cancel").onclick = () => $("import-dialog").close();
$("search").oninput = renderCatalog;
$("catalog-compatible").onchange = renderCatalog;
$("catalog-grouping").onchange = renderCatalog;
$('catalog-nodes').onclick=()=>{catalogView='nodes';renderCatalog();};
$('catalog-messages').onclick=()=>{catalogView='messages';renderCatalog();};
$("add").onsubmit = async event => {
  event.preventDefault();
  if (importer.busy) return;
  const payload = importer.source === "local" ? {source:"local",path:importer.path} : importer.source === "git"
    ? {source:"git",url:$("git-url").value.trim(),revision:$("git-revision").value.trim(),path:$("git-path").value.trim()}
    : {source:"registry",name:$("crate-name").value.trim(),version:$("crate-version").value.trim()};
  importer.busy = true;
  $("import-submit").disabled = true;
  $("import-cancel").disabled = true;
  $("import-status").textContent = "Fetching and reading package…";
  try {
    const response = await fetch("/api/catalog", {method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(payload)});
    const body = await response.json();
    if (!response.ok || body.problems) throw new Error((body.problems || ["Package could not be added."]).join("; "));
    $("adding").textContent = `Added ${body.added}`;
    $("import-dialog").close();
    await load();
  } catch (error) { $("import-status").textContent = error.message; }
  finally {
    importer.busy = false;
    $("import-submit").disabled = false;
    $("import-cancel").disabled = false;
  }
};

for (const id of ["source-request", "source-header", "source-response", "input-mode"]) $(id).onchange = () => render();
$("source-name").oninput = () => render();
let nameBeforeEdit = null;
function finishNameEdit(save = true) {
  if (nameBeforeEdit === null) return;
  const previous = nameBeforeEdit;
  $("name").value = save ? ($("name").value.trim() || previous) : previous;
  nameBeforeEdit = null;
  $("name-display").textContent = $("name").value;
  $("name").hidden = $("save-name").hidden = true;
  $("name-display").hidden = $("edit-name").hidden = false;
  if ($("name").value !== previous) render();
}
$("edit-name").onclick = () => {
  nameBeforeEdit = $("name").value;
  $("name").hidden = $("save-name").hidden = false;
  $("name-display").hidden = $("edit-name").hidden = true;
  $("name").focus();
  $("name").select();
};
$("save-name").onmousedown = event => event.preventDefault();
$("save-name").onclick = () => finishNameEdit();
$("name").onblur = () => finishNameEdit();
$("name").onkeydown = event => {
  if (event.key === "Enter" || event.key === "Escape") {
    event.preventDefault();
    finishNameEdit(event.key === "Enter");
    $("edit-name").focus();
  }
};
for (const id of ["source-request", "source-response"]) {
  dropTarget(document.querySelector(`[data-message-target="${id}"]`), data => {
    if ((state.frontend && !state.frontend.node.entries?.length) || data.kind !== "message" || ![...$(id).options].some(option => option.value === data.key)) return false;
    $(id).value = data.key;
    render();
    return true;
  });
}
dropTarget(document.querySelector("#source-card"), data => {
  if (data.kind !== "node") return false;
  const frontend = nodes().find(candidate => candidate.node.role === "source" && candidate.entry === data.entry && candidate.node.name === data.name);
  if (!frontend) return false;
  selectFrontend(frontend);
  return true;
});
load().catch(error => { $("adding").textContent = error.message; });

function settingsButton(instance) {
  if(instance.node.identity) {
    const button=textElement('button','','node-settings');button.innerHTML=$('source-settings').innerHTML;
    button.setAttribute('aria-label',`Settings for ${instance.instanceName || instance.node.name}`);
    button.onclick=()=>openIdentity(instance,value=>Object.assign(instance,value));return button;
  }

  const button=textElement("button","","node-settings");
  button.innerHTML='<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M9 3h6l1 4 4 2v6l-4 2-1 4H9l-1-4-4-2V9l4-2z"/><circle cx="12" cy="12" r="3"/></svg>';
  button.setAttribute("aria-label",`Settings for ${instance.node.name}`);
  button.title=`Settings for ${instance.node.name}`;
  button.onclick=()=>{
    $("settings-title").textContent=instance.node.name;
    $("settings-fields").replaceChildren(configControls(instance));
    if(!instance.node.config?.length) $("settings-fields").append(textElement("p","No graph settings exposed by this node.","muted"));
    for(const p of instance.node.ports || []) $("settings-fields").append(textElement("p",`${p.port}: ${carried(p)}`,"port-detail"));
    $("settings-dialog").showModal();
  };
  return button;
}
function armDraft(hop) {
  const node=n=>({name:n.node.name,entry:n.entry,node:n.node.name,config:graphConfig(n)});
  return {target:pipelineTarget(hop)?.root || null,forward:hop.forward.map(node),operator:hop.inner ? node(hop.inner):null,nested:hop.nested?armDraft(hop.nested):null,backward:hop.backward.map(node)};
}
function armNodeCount() {
  const count=hop=>hop.forward.length+hop.backward.length+(hop.inner?1:0)+(hop.nested?count(hop.nested):0);
  return enabledEntries().reduce((n,e)=>n+count(state.frontend.arms?.[e.name] || newHop()),0);
}
$('source-settings').onclick=()=>$('source-dialog').showModal();
let workspaceZoom=1;
const workspaceZooms={graph:1,segments:1};
let workspaceLayout="vertical";
const requestLabel=p=>`${carried(p)} ${workspaceLayout === "vertical" ? "↓" : "→"}`;
const responseLabel=item=>`${workspaceLayout === "vertical" ? "↑" : "←"} stream ${short(item)}`;
$('workspace-layout').onchange=()=>{
  workspaceLayout=$('workspace-layout').value;
  $('graph-board').dataset.layout=workspaceLayout;
  render(false);
  $('graph-viewport').scrollLeft=0; $('graph-viewport').scrollTop=0;
};
function setWorkspaceZoom(value, fit=false) {
  workspaceZoom=Math.max(fit ? Number.EPSILON : .35,Math.min(1.5,value));
  workspaceZooms[state.mode]=workspaceZoom;
  $('graph-board').style.zoom=workspaceZoom;
  $('zoom-level').textContent=`${Math.round(workspaceZoom*100)}%`;
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(drawWorkspaceEdges);
}
$('zoom-in').onclick=()=>setWorkspaceZoom(workspaceZoom+.1);
$('zoom-out').onclick=()=>setWorkspaceZoom(workspaceZoom-.1);
$('zoom-fit').onclick=()=>{
  sizeWorkspace();
  const board=$('graph-board'), viewport=$('graph-viewport');
  board.querySelector('.workspace-edges')?.remove();
  board.querySelector('.segment-regions')?.remove();
  setWorkspaceZoom(1);
  const bounds=board.getBoundingClientRect();
  const scale=Math.min(1,Math.max(1,viewport.clientWidth-32)/bounds.width,Math.max(1,viewport.clientHeight-32)/bounds.height);
  setWorkspaceZoom(scale,true);
  viewport.scrollLeft=0; viewport.scrollTop=0;
};
$('graph-viewport').addEventListener('wheel',event=>{
  if(event.ctrlKey || event.metaKey) {event.preventDefault();setWorkspaceZoom(workspaceZoom+(event.deltaY<0?.05:-.05));}
},{passive:false});
let pan=null;
$('graph-viewport').onpointerdown=event=>{
  if(event.button!==0 || event.target.closest('button,[role=button],input,select,summary,.box,.slot,.source-tile')) return;
  pan={x:event.clientX,y:event.clientY,left:$('graph-viewport').scrollLeft,top:$('graph-viewport').scrollTop};
  $('graph-viewport').setPointerCapture(event.pointerId);
};
$('graph-viewport').onpointermove=event=>{
  if(!pan) return;
  $('graph-viewport').scrollLeft=pan.left+pan.x-event.clientX;
  $('graph-viewport').scrollTop=pan.top+pan.y-event.clientY;
};
$('graph-viewport').onpointerup=$('graph-viewport').onpointercancel=$('graph-viewport').onlostpointercapture=()=>{pan=null;};

function sizeWorkspace() {
  const viewport=$('graph-viewport');
  if(window.innerHeight && viewport.getBoundingClientRect)
    viewport.style.height=`${Math.max(280,window.innerHeight-viewport.getBoundingClientRect().top-24)}px`;
}
function drawWorkspaceEdges() {
  sizeWorkspace();
  const board=$('graph-board');
  board.querySelector('.workspace-edges')?.remove();
  board.querySelector('.segment-regions')?.remove();
  board.querySelector('.deployment-view')?.remove();
  if(state.mode==='segments') {renderDeployment(board);drawDeploymentEdges(board);return;}
  const target=board.querySelector('.pipeline-card');
  const vertical=workspaceLayout === 'vertical';
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
  svg.classList.add('workspace-edges');
  const bounds=board.getBoundingClientRect();
  svg.setAttribute('width',bounds.width/workspaceZoom);svg.setAttribute('height',bounds.height/workspaceZoom);
  const rect=element=>{
    const r=element.getBoundingClientRect();
    return {left:(r.left-bounds.left)/workspaceZoom,right:(r.right-bounds.left)/workspaceZoom,
      top:(r.top-bounds.top)/workspaceZoom,bottom:(r.bottom-bounds.top)/workspaceZoom,
      x:(r.left+r.width/2-bounds.left)/workspaceZoom,y:(r.top+r.height/2-bounds.top)/workspaceZoom};
  };
  const incoming=(r,offset=0)=>vertical?{x:r.x+offset,y:r.top}:{x:r.left,y:r.y+offset};
  const outgoing=(r,offset=0)=>vertical?{x:r.x+offset,y:r.bottom}:{x:r.right,y:r.y+offset};
  const connect=(a,b,kind,root=null)=>{
    const path=document.createElementNS('http://www.w3.org/2000/svg','path');
    const d=vertical ? `M ${a.x} ${a.y} V ${(a.y+b.y)/2} H ${b.x} V ${b.y}`
      : `M ${a.x} ${a.y} H ${(a.x+b.x)/2} V ${b.y} H ${b.x}`;
    path.setAttribute('d',d);path.setAttribute('class',kind);svg.append(path);
    if(state.mode==='segments') {
      const title=document.createElementNS('http://www.w3.org/2000/svg','title');
      const pipeline=pipelineRoots().find(p=>p.root===root);
      const name=typeof root==='object' && root?root.to:instances().get(pipeline?.hop.forward[0] || pipeline?.hop.inner);
      const from=typeof root==='object' && root?root.from:root;
      const problem=from && name ? boundaryProblem(name,'__new_segment',true) : 'Use the scissors on a typed connection to edit placement.';
      title.textContent=problem || 'Add network boundary'; path.append(title);
      path.classList.add('segment-wire');
      if(!problem) {
        path.setAttribute('role','button'); path.setAttribute('tabindex','0'); path.setAttribute('aria-label',`Edit network boundary to ${name}`);
        path.onclick=()=>openBoundary(from,name);
        path.onkeydown=e=>{if(e.key==='Enter' || e.key===' ') {e.preventDefault();path.onclick();}};
      } else path.classList.add('local-wire');
    }
  };
  const source=rect($('source-card'));
  const endpoints=[...board.querySelectorAll('.public-endpoint:not(.pipeline-heading)')];
  for(const endpoint of endpoints) connect(outgoing(source),incoming(rect(endpoint)),'source-wire');
  if(!endpoints.length && target) connect(outgoing(source),incoming(rect(target)),'source-wire',pipelineRoots()[0]?.root);
  for(const join of board.querySelectorAll('.join-node.joined')) {
    const destination=[...board.querySelectorAll('.pipeline-card')].find(card=>card.dataset.root===join.dataset.root);
    const root=pipelineRoots().find(p=>p.root===join.dataset.root);
    const sink=root && directSink(root);
    const heading=destination?.querySelector('.pipeline-heading');
    const anchor=sink?[...board.querySelectorAll('.sink-node')].find(n=>n.dataset.sink===sink.instanceName):heading?.hidden?destination.querySelector(':scope > .hop'):heading;
    if(!anchor) continue;
    const start=rect(join), end=rect(anchor);
    for(const offset of [-3,3]) connect(outgoing(start,offset),incoming(end,offset),offset<0?'request-wire':'response-wire',offset<0?{from:join.dataset.from,to:join.dataset.to}:null);
  }
  for(const join of board.querySelectorAll('.sink-join')) {
    const sink=[...board.querySelectorAll('.sink-node')].find(node=>node.dataset.sink===join.dataset.sink);
    if(!sink) continue;
    for(const offset of [-5,5]) connect(outgoing(rect(join),offset),incoming(rect(sink),offset),offset<0?'request-wire':'response-wire');
  }
  for(const pipeline of board.querySelectorAll('.pipeline-card')) {
    const heading=pipeline.querySelector('.pipeline-heading'), lanes=pipeline.querySelectorAll(':scope > .hop > .lane');
    if(heading && !heading.hidden && lanes.length===2) {
      connect(outgoing(rect(heading),-5),incoming(rect(lanes[0])),'request-wire');
      connect(incoming(rect(lanes[1])),outgoing(rect(heading),5),'response-wire');
    }
  }
  for(const arm of board.querySelectorAll('.arm-hop')) {
    const upstream=arm.parentElement.querySelector(':scope > .box');
    if(!upstream) continue;
    const start=rect(upstream), request=rect(arm.children[0]), inner=rect(arm.children[1]), response=rect(arm.children[2]);
    connect(outgoing(start,-5),incoming(request),'request-wire');
    connect(outgoing(request),incoming(inner,-5),'request-wire');
    connect(incoming(inner,5),outgoing(response),'response-wire');
    connect(incoming(response),outgoing(start,5),'response-wire');
  }
  board.prepend(svg);

}
window.addEventListener('resize',()=>{if(typeof requestAnimationFrame==='function') requestAnimationFrame(drawWorkspaceEdges);});


const historyFields=['name','source-name','source-request','source-header','source-response','input-mode'];
const editHistory=[];
let historyIndex=-1;
function recordHistory() {
  const snapshot=JSON.stringify({hop:state.hop,pipelines:state.pipelines,usedNames:state.usedNames,frontend:state.frontend,placement:state.placement,routing:state.routing,segmentNames:state.segmentNames,fields:Object.fromEntries(historyFields.map(id=>[id,$(id).value]))});
  if(editHistory[historyIndex]!==snapshot) {
    editHistory.splice(historyIndex+1); editHistory.push(snapshot);
    if(editHistory.length>100) editHistory.shift();
    historyIndex=editHistory.length-1;
  }
  updateHistoryButtons();
}
function updateHistoryButtons() {
  $('undo').disabled=historyIndex<=0; $('redo').disabled=historyIndex>=editHistory.length-1;
}
function restoreEdit(index) {
  if(index<0 || index>=editHistory.length) return;
  historyIndex=index;
  const snapshot=JSON.parse(editHistory[index]);
  Object.assign(state,{hop:snapshot.hop,pipelines:snapshot.pipelines || [],usedNames:snapshot.usedNames || [],frontend:snapshot.frontend,placement:snapshot.placement,routing:snapshot.routing,segmentNames:snapshot.segmentNames});
  for(const [id,value] of Object.entries(snapshot.fields)) $(id).value=value;
  $('name-display').textContent=$('name').value;
  $('source-frontend').value=state.frontend?`${state.frontend.entry} ${state.frontend.node.name}`:'';
  if(!graphComplete()) {state.mode='graph';setWorkspaceZoom(workspaceZooms.graph,true);}
  render(); updateHistoryButtons();
}
function clearGraph() {
  recordHistory();
  Object.assign(state,{mode:'graph',validated:false,hop:newHop(),pipelines:[],usedNames:[],frontend:null,
    placement:Object.create(null),routing:Object.create(null),segmentNames:Object.create(null)});
  for(const id of ['source-request','source-header','source-response','source-frontend']) $(id).value='';
  $('source-name').value='Source';$('input-mode').value='single';
  $('name').value='composed';$('name-display').textContent='composed';
  render();
}
$('clear-graph').onclick=clearGraph;
function undoEdit() {restoreEdit(historyIndex-1);}
function redoEdit() {restoreEdit(historyIndex+1);}
$('undo').onclick=undoEdit; $('redo').onclick=redoEdit;
window.addEventListener('keydown',event=>{
  if(!(event.ctrlKey || event.metaKey) || event.target.closest('input,textarea,select,[contenteditable]')) return;
  if(event.key.toLowerCase()==='z') {event.preventDefault(); if(event.shiftKey) redoEdit(); else undoEdit();}
});

const segmentColors=['#e9b44c','#ef806f','#c7c98b','#d9a67d','#e3c8ac','#bd9362'];
// Request dependencies order processes; response anchors do not reverse that order.
function deploymentView(groups, connections) {
  const owner=Object.fromEntries(Object.entries(groups).flatMap(([name,members])=>members.map(member=>[member,name])));
  const ranks=Object.fromEntries(Object.keys(groups).map(name=>[name,0]));
  const pending=new Set(Object.keys(groups)), done=new Set();
  while(pending.size) {
    const ready=[...pending].filter(name=>!connections.some(e=>e.request && owner[e.to]===name && owner[e.from]!==name && !done.has(owner[e.from])));
    if(!ready.length) break;
    for(const name of ready) {
      ranks[name]=Math.max(0,...connections.filter(e=>e.request && owner[e.to]===name && owner[e.from]!==name).map(e=>ranks[owner[e.from]]+1));
      pending.delete(name);done.add(name);
    }
  }
  return Object.entries(groups).map(([name,members])=>({name,rank:ranks[name],nodes:members.map(member=>({name:member,edges:connections.filter(e=>e.from===member).map(e=>({...e,destination:owner[e.to],network:owner[e.from]!==owner[e.to]}))}))}));
}

function deploymentNodeLayout(members, connections, roots) {
  const flow=connections.map(e=>e.request?{from:e.from,to:e.to}:{from:e.to,to:e.from});
  const lanes=new Map();
  roots.forEach((root,column)=>{
    const pending=[root], seen=new Set();
    while(pending.length) {
      const name=pending.shift();if(seen.has(name)) continue;seen.add(name);
      if(!lanes.has(name)) lanes.set(name,column);
      pending.push(...flow.filter(e=>e.from===name).map(e=>e.to));
    }
  });
  const local=flow.filter(e=>members.includes(e.from) && members.includes(e.to));
  const result={}, pending=new Set(members), occupied=new Set();
  const columns=[...new Set(members.map(name=>lanes.get(name) || 0))].sort((a,b)=>a-b);
  while(pending.size) {
    const ready=[...pending].filter(name=>!local.some(e=>e.to===name && pending.has(e.from)));
    if(!ready.length) ready.push(pending.values().next().value);
    for(const name of ready) {
      const column=columns.indexOf(lanes.get(name) || 0);
      let row=Math.max(0,...local.filter(e=>e.to===name && result[e.from]).map(e=>result[e.from].row+1));
      while(occupied.has(`${row}:${column}`)) row++;
      result[name]={row,column};occupied.add(`${row}:${column}`);pending.delete(name);
    }
  }
  return result;
}

function renderDeployment(board) {
  const names=instances(), roots=deployedRoots(), connections=networkEdges();
  const view=deploymentView(segments(names),connections);
  const instancesByName=new Map([...names].map(([instance,name])=>[name,instance]));
  const label=name=>roots.find(root=>root.root===name)?.name || name;
  const container=textElement('div','','deployment-view');
  container.dataset.layout=workspaceLayout;
  const rows=new Map();
  for(const segment of view) {
    if(!rows.has(segment.rank)) rows.set(segment.rank,textElement('div','','deployment-rank'));
    const card=textElement('section','','deployment-segment');
    card.style.setProperty('--segment-color',segmentColors[view.indexOf(segment)%segmentColors.length]);
    card.setAttribute('aria-label',`Segment ${segment.name}`);
    const heading=textElement('header','','deployment-heading');
    heading.append(textElement('strong',segment.name),textElement('span',`${segment.nodes.length} nodes · independent process`));
    card.append(heading);
    if(segment.nodes.some(member=>roots.some(root=>root.root===member.name))) {
      const source=textElement('div','','deployment-source');
      source.append(textElement('small','SOURCE'),textElement('strong',$('source-name').value || 'Source'),textElement('span',state.frontend?.node.name || 'Programmatic source'));
      const settings=textElement('button','Settings','deployment-move');
      settings.setAttribute('aria-label','Source settings in segment');
      settings.onclick=()=>$('source-dialog').showModal();source.append(settings);card.append(source);
    }
    const grid=textElement('div','','deployment-grid');card.append(grid);
    const layout=deploymentNodeLayout(segment.nodes.map(n=>n.name),connections,roots.map(r=>r.root));
    for(const member of segment.nodes) {
      const instance=instancesByName.get(member.name), root=roots.find(root=>root.root===member.name);
      const node=textElement('article','','deployment-node');node.dataset.deploymentMember=member.name;
      node.style.gridColumn=String(layout[member.name].column+1);node.style.gridRow=String(layout[member.name].row+1);
      const title=textElement('div','','deployment-node-title');
      title.append(textElement('strong',label(member.name)),textElement('small',root?'frontend endpoint':instance?.node.role || 'source'));
      if(instance) {
        title.append(settingsButton(instance));
        const move=textElement('button','Move','deployment-move');
        move.setAttribute('aria-label',`Move ${member.name} to segment`);
        move.onclick=()=>openBoundary(connections.find(e=>e.to===member.name)?.from || member.name,member.name);
        title.append(move);
      } else {
        const settings=textElement('button','Settings','deployment-move');settings.onclick=()=>$('source-dialog').showModal();title.append(settings);
      }
      node.append(title);
      const detail=textElement('details','','deployment-ports');
      detail.append(textElement('summary',`${member.edges.length} outgoing connection${member.edges.length===1?'':'s'}`));
      detail.ontoggle=()=>{if(typeof requestAnimationFrame==='function') requestAnimationFrame(()=>drawDeploymentEdges(board));};
      node.append(detail);
      for(const edge of member.edges) {
        const row=textElement('div','',`deployment-connection ${edge.request?'request':'response'}`);
        const message=edge.request?carried({head:edge.types[0],item:edge.types[1]}):`stream ${short(edge.types[0])}`;
        const text=textElement('span',`${edge.request?'Request':'Response'} · ${message}`,'deployment-message');
        const destination=textElement('button',`→ ${edge.network?edge.destination+' / ':''}${label(edge.to)}`,'deployment-destination');
        destination.title=`${edge.network?'Network':'Local'} connection to ${label(edge.to)}`;
        destination.onclick=()=>{
          const target=[...container.querySelectorAll('[data-deployment-member]')].find(el=>el.dataset.deploymentMember===edge.to);
          container.querySelectorAll('.connection-target').forEach(el=>el.classList.remove('connection-target'));
          target?.classList.add('connection-target');target?.scrollIntoView({block:'nearest',inline:'nearest',behavior:'smooth'});
        };
        const cut=textElement('button','','deployment-cut');cut.innerHTML=scissors;
        cut.setAttribute('aria-label',`${edge.from} → ${edge.to}: ${edge.network?'edit':'add'} network boundary`);
        const moving=roots.some(r=>r.root===edge.to)?edge.from:edge.to;
        const problem=boundaryProblem(moving,edge.network?segment.name:'__new_segment',true);
        cut.disabled=!!problem;cut.title=problem || (edge.network?'Edit network boundary':'Split into another process');
        cut.onclick=()=>openBoundary(edge.from,edge.to);
        row.append(text,destination,cut);
        if(edge.network && edge.request) row.append(routerButton(edge.to));
        detail.append(row);
      }
      grid.append(node);
    }
    rows.get(segment.rank).append(card);
  }
  for(const [,row] of [...rows].sort(([a],[b])=>a-b)) container.append(row);
  board.append(container);
}

function wireIntersections(points, obstacles) {
  let count=0;
  for(let i=1;i<points.length;i++) {
    const a=points[i-1],b=points[i];
    for(const r of obstacles) {
      if(a.x===b.x ? a.x>r.left && a.x<r.right && Math.max(a.y,b.y)>r.top && Math.min(a.y,b.y)<r.bottom
        : a.y>r.top && a.y<r.bottom && Math.max(a.x,b.x)>r.left && Math.min(a.x,b.x)<r.right) count++;
    }
  }
  return count;
}

const wireGap=24, wireLead=40;
function wireClearance(points, routed, gap=wireGap) {
  for(let i=1;i<points.length;i++) {
    const a=points[i-1],b=points[i],vertical=a.x===b.x;
    for(const previous of routed) for(let j=1;j<previous.length;j++) {
      const c=previous[j-1],d=previous[j];if(vertical!==(c.x===d.x)) continue;
      if((vertical?Math.abs(a.x-c.x):Math.abs(a.y-c.y))>=gap-1e-6) continue;
      const v=vertical?[a.y,b.y,c.y,d.y]:[a.x,b.x,c.x,d.x];
      if(Math.min(Math.max(v[0],v[1]),Math.max(v[2],v[3]))>Math.max(Math.min(v[0],v[1]),Math.min(v[2],v[3]))) return false;
    }
  }
  return true;
}

// Route on an orthogonal visibility grid. Clearance is a constraint, not a cost.
function routeDeploymentWire(start,end,obstacles,routed,direction=0) {
  const unique=values=>[...new Set(values)].sort((a,b)=>a-b);
  const xs=unique([start.x,end.x,start.x-wireGap,start.x+wireGap,end.x-wireGap,end.x+wireGap,...obstacles.flatMap(r=>[r.left-wireGap,r.right+wireGap]),...routed.flatMap(ps=>ps.flatMap(p=>[p.x-wireGap,p.x+wireGap]))]);
  const ys=unique([start.y,end.y,start.y-wireGap,start.y+wireGap,end.y-wireGap,end.y+wireGap,...obstacles.flatMap(r=>[r.top-wireGap,r.bottom+wireGap]),...routed.flatMap(ps=>ps.flatMap(p=>[p.y-wireGap,p.y+wireGap]))]);
  const distance=(a,b)=>Math.abs(a.x-b.x)+Math.abs(a.y-b.y);
  const clear=points=>!wireIntersections(points,obstacles) && wireClearance(points,routed);
  const score=points=>{
    let cost=(points.length-2)*24;
    for(let i=1;i<points.length;i++) {
      const a=points[i-1],b=points[i];cost+=distance(a,b);
      for(const previous of routed) for(let j=1;j<previous.length;j++) {
        const c=previous[j-1],d=previous[j];if((a.x===b.x)===(c.x===d.x)) continue;
        const v=a.x===b.x?[a,b]:[c,d],h=a.x===b.x?[c,d]:[a,b];
        if(v[0].x>Math.min(h[0].x,h[1].x) && v[0].x<Math.max(h[0].x,h[1].x) && h[0].y>Math.min(v[0].y,v[1].y) && h[0].y<Math.max(v[0].y,v[1].y)) cost+=100;
      }
    }
    return cost;
  };
  const candidates=[...xs.map(x=>[start,{x,y:start.y},{x,y:end.y},end]),...ys.map(y=>[start,{x:start.x,y},{x:end.x,y},end])]
    .map(ps=>ps.filter((p,i)=>!i || distance(p,ps[i-1])>0))
    .filter(ps=>(!direction || ((ps[1].y-start.y)*direction>=0 && (end.y-ps.at(-2).y)*direction>=0)) && clear(ps));
  if(candidates.length) return candidates.sort((a,b)=>score(a)-score(b))[0];
  const queue=[{point:start,dir:'',cost:0,priority:distance(start,end),path:[start]}],best=new Map();
  const push=item=>{queue.push(item);let i=queue.length-1;while(i) {const parent=(i-1)>>1;if(queue[parent].priority<=item.priority) break;queue[i]=queue[parent];i=parent;}queue[i]=item;};
  const pop=()=>{const item=queue[0],last=queue.pop();if(queue.length) {let i=0;while(i*2+1<queue.length) {let child=i*2+1;if(child+1<queue.length && queue[child+1].priority<queue[child].priority) child++;if(queue[child].priority>=last.priority) break;queue[i]=queue[child];i=child;}queue[i]=last;}return item;};
  while(queue.length) {
    const current=pop(),p=current.point,key=`${p.x}:${p.y}:${current.dir}`;
    if(current.cost>(best.get(key) ?? Infinity)) continue;
    if(p.x===end.x && p.y===end.y) return current.path;
    const options=[...xs.map(x=>({x,y:p.y})),...ys.map(y=>({x:p.x,y}))];
    for(const next of options) {
      if(distance(p,next)<1e-6 || !clear([p,next])) continue;
      if(direction && ((current.path.length===1 && (next.y-p.y)*direction<0) || (next.x===end.x && next.y===end.y && (next.y-p.y)*direction<0))) continue;
      const dir=next.x===p.x?'v':'h';
      if(dir===current.dir) continue;
      const cost=current.cost+score([p,next])+24+(current.dir && current.dir!==dir?24:0);
      const key=`${next.x}:${next.y}:${dir}`;
      if(cost>=(best.get(key) ?? Infinity)) continue;
      best.set(key,cost);push({point:next,dir,cost,priority:cost+distance(next,end),path:[...current.path,next]});
    }
  }
  return null;
}

function terminalReservations(start,end) {
  const direction=end.y>start.y?1:-1;
  return [[start,direction],[end,-direction]].map(([p,sign])=>({left:p.x-wireGap,right:p.x+wireGap,top:Math.min(p.y,p.y+sign*(wireLead+wireGap)),bottom:Math.max(p.y,p.y+sign*(wireLead+wireGap))}));
}

function terminalWire(start,end,obstacles,routed) {
  const direction=end.y>start.y?1:-1;
  const exit={x:start.x,y:start.y+direction*wireLead},entry={x:end.x,y:end.y-direction*wireLead};
  const reserved=routed.flatMap(ps=>[[ps[0],ps[1]],[ps.at(-2),ps.at(-1)]].map(([a,b])=>({left:Math.min(a.x,b.x)-wireGap/2,right:Math.max(a.x,b.x)+wireGap/2,top:Math.min(a.y,b.y),bottom:Math.max(a.y,b.y)})));
  const middle=routeDeploymentWire(exit,entry,[...obstacles,...reserved],routed,direction);
  const points=middle?[start,...middle,end]:null;
  return points && wireClearance(points,routed)?points:null;
}

function drawDeploymentEdges(board) {
  board.querySelector('.deployment-wires')?.remove();
  const container=board.querySelector('.deployment-view');if(!container) return;
  const bounds=container.getBoundingClientRect();
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.classList.add('deployment-wires');
  svg.setAttribute('width',bounds.width/workspaceZoom);svg.setAttribute('height',bounds.height/workspaceZoom);
  const create=(tag,attrs)=>{const el=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [key,value] of Object.entries(attrs)) el.setAttribute(key,value);return el;};
  const defs=create('defs',{});
  for(const kind of ['request','response']) {
    const marker=create('marker',{id:`deployment-${kind}`,viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:6,markerHeight:6,orient:'auto-start-reverse'});
    marker.append(create('path',{d:'M 0 0 L 10 5 L 0 10 z',fill:kind==='request'?'#6ba8ff':'#b792ff'}));defs.append(marker);
  }
  svg.append(defs);
  const rect=el=>{const r=el.getBoundingClientRect();return {left:(r.left-bounds.left)/workspaceZoom,right:(r.right-bounds.left)/workspaceZoom,top:(r.top-bounds.top)/workspaceZoom,bottom:(r.bottom-bounds.top)/workspaceZoom,x:(r.left+r.width/2-bounds.left)/workspaceZoom,y:(r.top+r.height/2-bounds.top)/workspaceZoom};};
  const nodes=new Map([...container.querySelectorAll('[data-deployment-member]')].map(el=>[el.dataset.deploymentMember,rect(el)]));
  const groups=segments(instances()), owner=Object.fromEntries(Object.entries(groups).flatMap(([group,members])=>members.map(name=>[name,group])));
  const headings=[...container.querySelectorAll('.deployment-heading,.deployment-source')].map(rect);
  for(const source of container.querySelectorAll('.deployment-source')) {
    const start=rect(source);
    for(const root of deployedRoots()) {
      const end=nodes.get(root.root);if(!end) continue;
      const y=(start.bottom+end.top)/2;
      svg.append(create('path',{d:`M ${start.x} ${start.bottom} V ${y} H ${end.x} V ${end.top}`,class:'deployment-host-wire'}));
    }
  }
  const routed=[], connections=networkEdges();
  const socket=(name,edge,outgoing)=>{
    const rect=nodes.get(name), other=nodes.get(outgoing?edge.to:edge.from);
    const peers=connections.filter(e=>{
      if(e.from!==name && e.to!==name) return false;
      const neighbor=nodes.get(e.from===name?e.to:e.from);
      return neighbor && (neighbor.y>rect.y)===(other.y>rect.y);
    }).sort((a,b)=>{
      const x=e=>nodes.get(e.from===name?e.to:e.from)?.x || 0;
      return x(a)-x(b) || Number(!!b.request)-Number(!!a.request);
    });
    const offset=(peers.indexOf(edge)-(peers.length-1)/2)*wireGap;
    return {x:rect.x+offset,y:other.y>rect.y?rect.bottom:rect.top};
  };
  const approaches=new Map(connections.map(edge=>[edge,terminalReservations(socket(edge.from,edge,true),socket(edge.to,edge,false))]));
  for(const edge of connections) {
    const from=nodes.get(edge.from),to=nodes.get(edge.to);if(!from || !to) continue;
    const request=!!edge.request,kind=request?'request':'response',crossing=owner[edge.from]!==owner[edge.to];
    const start=socket(edge.from,edge,true),end=socket(edge.to,edge,false);
    const obstacles=[...nodes].map(([,r])=>({left:r.left-6,right:r.right+6,top:r.top-6,bottom:r.bottom+6}));
    obstacles.push(...headings,...connections.filter(other=>other!==edge).flatMap(other=>approaches.get(other)));
    const points=terminalWire(start,end,obstacles,routed);
    if(!points) {const warning=create('text',{x:start.x,y:start.y+18,class:'wire-conflict'});warning.textContent='Route needs more space';svg.append(warning);continue;}
    routed.push(points);
    const d=points.map((p,i)=>`${i?'L':'M'} ${p.x} ${p.y}`).join(' ');
    const path=create('path',{d,class:`deployment-wire ${kind}${crossing?' network':''}`,'marker-end':`url(#deployment-${kind})`,tabindex:0,role:'button'});
    const message=request?carried({head:edge.types[0],item:edge.types[1]}):`stream ${short(edge.types[0])}`;
    const description=`${edge.from} → ${edge.to}: ${message}${crossing?' · network boundary':' · local connection'}`;
    path.setAttribute('aria-label',description);const title=create('title',{});title.textContent=description;path.append(title);
    const moving=deployedRoots().some(r=>r.root===edge.to)?edge.from:edge.to;
    const problem=boundaryProblem(moving,crossing?owner[edge.from]:'__new_segment',true);
    path.setAttribute('aria-disabled',String(!!problem));
    if(problem) title.textContent+=` · ${problem}`;
    path.onclick=()=>{if(!problem) openBoundary(edge.from,edge.to);};
    path.onkeydown=e=>{if(e.key==='Enter' || e.key===' ') {e.preventDefault();path.onclick();}};
    svg.append(path);
  }
  container.prepend(svg);
}

function paintSegments() {
  const board=$('graph-board');
  if(!board.querySelectorAll) return;
  board.classList.toggle('segment-mode',state.mode==='segments');
  board.querySelectorAll('.source-segment').forEach(el=>el.remove());
  const groups=Object.keys(segments(instances()));
  const colors=segmentColors;
  const sourceGroup=Object.entries(segments(instances())).find(([,members])=>members.includes($('source-name').value))?.[0];
  for(const element of board.querySelectorAll('[data-segment]')) {
    const group=Object.entries(segments(instances())).find(([,members])=>members.includes(element.dataset.member))?.[0];
    element.dataset.segment=group;
    const badge=element.querySelector('.segment-badge');if(badge) badge.textContent=group;
    element.style.setProperty('--segment-color',colors[groups.indexOf(group)%colors.length]);
  }
  for(const element of [$('source-card'), ...board.querySelectorAll('.entry-pipeline > .public-endpoint')]) {
    element.style.setProperty('--segment-color',colors[groups.indexOf(sourceGroup)%colors.length]);
    if(state.mode==='segments') {
      element.dataset.segment=sourceGroup;element.dataset.member=$('source-name').value;
      element.append(textElement('small',sourceGroup,'segment-badge source-segment'));
    } else {delete element.dataset.segment;delete element.dataset.member;}
  }
}

function actionTooltip(element) {
  return element.getAttribute('title') || element.getAttribute('aria-label') || '';
}
let tooltipTimer=null;
function hideActionTooltip() {
  clearTimeout(tooltipTimer);
  $('action-tooltip').hidden=true;
}
$('canvas').addEventListener('pointerover',event=>{
  const action=event.target.closest('button,[role="button"]');
  if(!action || action.contains(event.relatedTarget)) return;
  hideActionTooltip();
  const text=actionTooltip(action);
  if(!text) return;
  tooltipTimer=setTimeout(()=>{
    if(!action.isConnected) return;
    const tip=$('action-tooltip'), bounds=action.getBoundingClientRect();
    tip.textContent=text; tip.hidden=false;
    tip.style.left=`${Math.max(8,Math.min(bounds.left,window.innerWidth-tip.offsetWidth-8))}px`;
    tip.style.top=`${Math.max(8,Math.min(bounds.bottom+8,window.innerHeight-tip.offsetHeight-8))}px`;
  },600);
});
$('canvas').addEventListener('pointerout',event=>{
  const action=event.target.closest('button,[role="button"]');
  if(action && !action.contains(event.relatedTarget)) hideActionTooltip();
});
$('canvas').addEventListener('pointerdown',hideActionTooltip);
$('graph-viewport').addEventListener('scroll',hideActionTooltip);

let deploymentSettings={base_image:'debian:bookworm-slim',build_image:'rust:1.93-slim-bookworm',segment:Object.create(null)};
function deploymentDraft(groups) {
  const {segment,...defaults}=deploymentSettings;
  return {...defaults,segment:Object.fromEntries(Object.keys(groups).filter(name=>segment[name]).map(name=>[name,segment[name]]))};
}
function renderDeploy() {
  const groups=segments(instances());
  $('deploy-base').value=deploymentSettings.base_image;
  $('deploy-build').value=deploymentSettings.build_image;
  $('deploy-hub').value=deploymentSettings.hub_image || '';
  const cards=Object.entries(groups).map(([name,nodes])=>{
    const settings=deploymentSettings.segment[name] ||= {};
    const card=textElement('article','','deploy-segment');
    card.append(textElement('h3',name),textElement('p',nodes.join(' · '),'muted'));
    const field=(caption,value,placeholder,save)=>{
      const label=textElement('label',caption),input=document.createElement('input');
      input.value=value;input.placeholder=placeholder;input.setAttribute('aria-label',`${name}: ${caption}`);
      input.oninput=()=>{save(input.value);scheduleDeployment();};label.append(input);card.append(label);
    };
    field('Runtime image',settings.base_image || '',deploymentSettings.base_image,value=>{if(value.trim())settings.base_image=value.trim();else delete settings.base_image;});
    field('Published ports',(settings.ports || []).join(', '),'8082:8082',value=>{settings.ports=value.split(',').map(s=>s.trim()).filter(Boolean);});
    const label=textElement('label','Runtime config (TOML)'),config=document.createElement('textarea');
    config.rows=3;config.placeholder='[frontend]\nlisten = "0.0.0.0:8082"';config.setAttribute('aria-label',`${name}: Runtime config`);
    config.value=deploymentConfigText[name] || '';config.oninput=()=>{deploymentConfigText[name]=config.value;scheduleDeployment();};label.append(config);card.append(label);
    return card;
  });
  $('deploy-segments').replaceChildren(...cards);
}
const deploymentConfigText=Object.create(null);
$('deploy-tab').onclick=async()=>{
  if(!state.validated) return;
  state.mode='segments';
  await validateGraph();
  if(!state.validated) return;
  deployOpen=true;renderDeploy();updateMode();scheduleDeployment();
};
for(const [id,key] of [['deploy-base','base_image'],['deploy-build','build_image'],['deploy-hub','hub_image']]) {
  $(id).oninput=()=>{const value=$(id).value.trim();if(value || key!=='hub_image') deploymentSettings[key]=value;else delete deploymentSettings[key];scheduleDeployment();};
}
let deploymentSnapshot=null, deploymentRevision=0, deploymentTimer=null;
let deploymentFile='compose.yaml';
function scheduleDeployment() {
  deploymentRevision++;
  deploymentSnapshot=null;
  $('download-deployment').disabled=true;
  $('deploy-files').replaceChildren();
  $('deploy-code').textContent='';
  $('deploy-filename').textContent='Generated files';
  $('deploy-status').textContent='Updating preview…';
  clearTimeout(deploymentTimer);
  deploymentTimer=setTimeout(generateDeployment,500);
}
async function generateDeployment() {
  const revision=++deploymentRevision;
  deploymentSnapshot=null;
  $('download-deployment').disabled=true;
  $('deploy-status').textContent='Generating files from Git and registry dependencies…';
  try {
    const payload=draft();
    payload.deployment_config=Object.fromEntries(Object.keys(payload.segments).filter(name=>deploymentConfigText[name]?.trim()).map(name=>[name,deploymentConfigText[name]]));
    const response=await fetch('/api/deploy',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});
    const result=await response.json();
    if(revision!==deploymentRevision) return;
    if(!response.ok) throw new Error((result.problems || [JSON.stringify(result)]).join(' · '));
    deploymentSnapshot={...result,name:payload.name};
    renderDeploymentPreview();
    $('download-deployment').disabled=false;
    $('deploy-status').textContent='Preview ready. Download includes these exact files. Cargo fetches dependencies during the image build.';
  } catch(error) {
    if(revision!==deploymentRevision) return;
    $('deploy-status').textContent=error.message;
  }
}
function renderDeploymentPreview() {
  if(!deploymentSnapshot) return;
  const files=deploymentSnapshot.files;
  if(!(deploymentFile in files)) deploymentFile='compose.yaml';
  const rank=name=>name==='compose.yaml'?0:name.endsWith('/Dockerfile')?1:name.endsWith('/config.toml')?2:3;
  const names=Object.keys(files).sort((a,b)=>rank(a)-rank(b)||a.localeCompare(b));
  $('deploy-files').replaceChildren(...names.map(name=>{
    const button=textElement('button',name);
    button.setAttribute('aria-pressed',String(name===deploymentFile));
    button.onclick=()=>{deploymentFile=name;renderDeploymentPreview();};
    return button;
  }));
  $('deploy-filename').textContent=deploymentFile;
  $('deploy-code').textContent=files[deploymentFile];
}
$('download-deployment').onclick=()=>{
  if(!deploymentSnapshot) return;
  const bytes=Uint8Array.from(atob(deploymentSnapshot.archive),character=>character.charCodeAt(0));
  const link=document.createElement('a');
  link.href=URL.createObjectURL(new Blob([bytes],{type:'application/gzip'}));
  link.download=`${deploymentSnapshot.name}-compose.tar.gz`;
  link.click();setTimeout(()=>URL.revokeObjectURL(link.href),1000);
  $('deploy-status').textContent='Reviewed bundle downloaded. Build and run it on your Docker host.';
};
