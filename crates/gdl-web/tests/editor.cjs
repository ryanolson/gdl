const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function editor() {
  const elements = new Map();
  const context = vm.createContext({
    document: {createElement() {return {append() {}};}, querySelector() { return {dataset:{}}; }, getElementById(id) {
      if (!elements.has(id)) elements.set(id, {value: '', style:{}, addEventListener() {}, classList: {toggle() {}}, setAttribute() {}, replaceChildren() {}});
      return elements.get(id);
    }},
    window: {addEventListener(name, callback) {this[name] = callback;}},
    fetch: () => new Promise(() => {}),
    setTimeout, clearTimeout, console,
  });
  vm.runInContext(fs.readFileSync(require.resolve('../assets/app.js'), 'utf8'), context);
  return (code) => vm.runInContext(code, context);
}

test('source mode maps a message to one request or stream items, with an optional header', () => {
  const run = editor();
  run(`state.entries = [{package:'p', version:'1', source:'local', messages:[{name:'Prompt', path:'p::Prompt'}]}];
    $('source-request').value = 'p@1 p::Prompt';
    $('input-mode').value = 'single';`);
  assert.equal(run('sourcePort().head.path'), 'p::Prompt');
  assert.equal(run('sourcePort().item'), null);
  run(`$('input-mode').value = 'stream'`);
  assert.equal(run('sourcePort().head'), null);
  assert.equal(run('sourcePort().item.path'), 'p::Prompt');
  run(`$('source-header').value = 'p@1 p::Prompt'`);
  assert.equal(run('sourcePort().head.path'), 'p::Prompt');
});

test('a response mismatch or an unfinished nested hop cannot unlock segments', () => {
  const run = editor();
  run(`const msg = path => ({package:'p', version:'1', path});
    const request = {head:msg('Prompt'), item:null};
    const response = msg('Chunk');
    const sink = {node:{role:'sink', ports:[{port:'Req',head:msg('Prompt')},{port:'Item',head:msg('Chunk')}]}};
    const hop = {forward:[], inner:sink, nested:null, backward:[]};`);
  assert.equal(run('flowComplete(hop, request, response)'), true);
  assert.equal(run("flowComplete(hop, request, msg('Other'))"), false);
  assert.equal(run('flowComplete(newHop(), request, response)'), false);
  run(`hop.inner = {node:{role:'operator',ports:[{port:'UpReq',head:msg('Prompt')},{port:'UpItem',head:msg('Chunk')},{port:'DownReq',head:msg('Prompt')},{port:'DownItem',head:msg('Chunk')}]}}; hop.nested = newHop();`);
  assert.equal(run('flowComplete(hop, request, response)'), false);
  run('hop.nested.inner = sink');
  assert.equal(run('flowComplete(hop, request, response)'), true);
});


test('only the latest server validation can unlock segments', async () => {
  const run = editor();
  run(`const replies = [];
    fetch = () => new Promise(resolve => replies.push(resolve));
    draft = () => ({source:{name:'Source'}, nodes:[], segments:{s1:['Source']}});
    show = () => {};
    const first = validateGraph();
    const second = validateGraph();`);
  run(`replies[1]({ok:false, json:async () => ({ok:false, problems:['invalid']})})`);
  await run('second');
  assert.equal(run('state.validated'), false);
  run(`replies[0]({ok:true, json:async () => ({ok:true})})`);
  await run('first');
  assert.equal(run('state.validated'), false);
  assert.equal(run("$('segments-tab').disabled"), true);
  run(`const third = validateGraph(); replies[2]({ok:true,json:async () => ({ok:true})})`);
  await run('third');
  assert.equal(run("$('segments-tab').disabled"), false);
});

test('editing an incomplete flow invalidates a pending successful check', async () => {
  const run = editor();
  run(`let reply;
    fetch = () => new Promise(resolve => { reply = resolve; });
    draft = () => ({source:{name:'Source'}, nodes:[], segments:{s1:['Source']}});
    show = () => {};
    const pending = validateGraph();
    scheduleValidation();
    reply({ok:true,json:async () => ({ok:true})});`);
  await run('pending');
  assert.equal(run('state.validated'), false);
  assert.equal(run("$('export').disabled"), true);
});

test('catalog refresh preserves distinct repeated node instances', () => {
  const run = editor();
  run(`const node = {name:'Repeat',role:'forward',ports:[]};
    state.entries = [{package:'p', version:'1', source:'local', nodes:[node]}];
    const first = {source:'local',node:{...node}};
    const second = {source:'local',node:{...node}};`);
  assert.equal(run('now(first) === first'), true);
  assert.equal(run('now(second) === second'), true);
  assert.equal(run('now(first) === now(second)'), false);
});

test('segment names enter the draft while typing, without a blur or canvas redraw', () => {
  const run = editor();
  run(`document.createElement = () => ({dataset:{}, children:[], append(...children) {this.children.push(...children);}, setAttribute() {}});
    $('hop').append = () => {};
    $('groups').children = [];
    $('groups').append = child => $('groups').children.push(child);
    $('source-name').value = 'Source';
    render(false);
    const field = $('groups').children[0].children[0];
    field.value = 'front';
    field.oninput();`);
  assert.equal(run('draft().segments.front[0]'), 'Source');
  assert.equal(run("$('groups').children[0].children[0] === field"), true);
  assert.equal(run('state.validated'), false);
  run(`$('groups').children = []; field.onblur?.()`);
  assert.equal(run("$('groups').children.length"), 0, 'blur must not replace the next input before it receives focus');
});

test('node drops accept only matching candidates and create separate instances', () => {
  const run = editor();
  run(`document.createElement = () => ({dataset:{}, children:[], append(...children) {this.children.push(...children);}, add() {}, setAttribute() {}, classList:{remove() {}}});
    Option = function() {};
    const candidate = {entry:'p@1',node:{name:'Engine',role:'sink'}};
    const chosen = [];
    render = () => {};
    const target = slot('choose the inner end', [candidate], node => chosen.push(node));
    const drop = data => dropHandlers.get(target)(data);
    drop({kind:'node',entry:'p@1',name:'Other'});`);
  assert.equal(run('chosen.length'), 0);
  run(`drop({kind:'node',entry:'p@1',name:'Engine'}); drop({kind:'node',entry:'p@1',name:'Engine'});`);
  assert.equal(run('chosen.length'), 2);
  assert.equal(run('chosen[0] === chosen[1]'), false);
  run(`state.mode = 'segments'; drop({kind:'node',entry:'p@1',name:'Engine'});`);
  assert.equal(run('chosen.length'), 2);
});

test('pointer dragging inserts a node at the target without toggling its inspector', () => {
  const run = editor();
  run(`const element = {classList:{add() {}},setPointerCapture() {}, addEventListener(name, callback) {this[name] = callback;}};
    const target = {dataset:{}, classList:{add() {}, remove() {}}, closest() {return this;}};
    let accepted = null;
    document.elementFromPoint = () => target;
    document.createElement = () => ({style:{},remove() {}});
    document.body = {append() {}};
    dropTarget(target, data => {accepted = data; return true;});
    makeDraggable(element, {kind:'node',name:'Engine'}, 'Engine');
    element.onpointerdown({button:0,pointerId:1,clientX:0,clientY:0});
    element.onpointermove({clientX:50,clientY:50});
    element.onpointerup({clientX:50,clientY:50});`);
  assert.equal(run('accepted.name'), 'Engine');
  run(`let prevented = false; element.click({preventDefault(){prevented=true;},stopPropagation(){}});`);
  assert.equal(run('prevented'), true);
});

function installDom(run) {
  run(`document.createElement = tag => ({tag, children:[], className:'', dataset:{}, style:{}, isConnected:true,
    append(...children){this.children.push(...children);}, setAttribute(){}, classList:{add() {}}, innerHTML:''});
    $('result').children = [];
    $('result').append = (...children) => $('result').children.push(...children);`);
}

test('server Mermaid output becomes an SVG diagram', async () => {
  const run = editor();
  installDom(run);
  run(`mermaid = {initialize() {}, render:async (id,text) => ({svg:'<svg aria-label="graph">'+text+'</svg>'})};`);
  await run(`show({mermaid:'flowchart LR; A-->B'})`);
  assert.equal(run(`$('result').children.some(el => el.className === 'diagram' && el.innerHTML.includes('<svg'))`), true);
});

for (const role of ['sink', 'operator']) test(`${role} and its nested flow span both lanes`, () => {
  const run = editor();
  installDom(run);
  run(`prune = () => {}; slot = () => document.createElement('div');
    const inner = {node:{name:'End',role:'${role}',ports:[]}};
    const hop = {forward:[],inner,nested:${role === 'operator' ? 'newHop()' : 'null'},backward:[]};
    const root = document.createElement('div');
    hopEditor(hop, {head:null,item:null}, root, 'Source', new Map([[inner,'End']]));`);
  assert.equal(run(`root.children[0].children.some(el => el.className.includes('endpoint'))`), true);
  assert.equal(run(`root.children[0].children[0].children.some(el => el.className.includes('box'))`), false);
  if (role === 'operator') assert.equal(run(`root.children[0].children.some(el => el.className === 'nested')`), true);
});

test('moving a node to a segment cuts both request and response in one action', () => {
  const run = editor();
  run(`$('source-name').value = 'Source';
    const engine = {node:{name:'Engine'}};
    state.hop.inner = engine;
    placeNode('Engine', 'engine', instances());`);
  assert.deepEqual(JSON.parse(run('JSON.stringify(segments(instances()))')), {s1:['Source'], engine:['Engine']});
  run("placeNode('Engine', 's1', instances())");
  assert.equal(run('Object.keys(segments(instances())).length'), 1);
});

test('crates.io search debounces keystrokes and ignores responses after a new query', async () => {
  const run = editor();
  run(`let pending; setTimeout = callback => { pending = callback; return 1; }; clearTimeout = () => { pending = null; };
    const requests = []; fetch = url => new Promise(resolve => requests.push({url,resolve}));
    $('crate-query').value = 'se'; searchCrates();
    $('crate-query').value = 'serde'; searchCrates();`);
  assert.equal(run('requests.length'), 0);
  run('const first = pending()');
  assert.equal(run('requests.length'), 1);
  assert.equal(run('requests[0].url'), '/api/crates?q=serde');
  run(`$('crate-query').value = 'tokio'; searchCrates();
    requests[0].resolve({ok:true,json:async () => ({crates:[]})});`);
  await run('first');
  assert.equal(run("$('crate-results').textContent"), 'Searching…');
  run(`const second = pending(); requests[1].resolve({ok:true,json:async () => ({crates:[]})});`);
  await run('second');
  assert.equal(run("$('crate-results').textContent"), 'No packages found.');
});

test('catalog search matches names and Rust paths without matching serialized metadata', () => {
  const run = editor();
  run(`const catalog = {package:'chat-front-nodes', messages:[{name:'Prompt',path:'protocols::Prompt',public:true,wire:true}], nodes:[
    {name:'Truncate',ty:'front::Truncate',ports:[{stream:true}]},
    {name:'Number',ty:'front::Number',ports:[{stream:true}]}
  ]};`);
  assert.equal(run("catalogResources(catalog,'tr').nodes.map(n => n.name).join(',')"),'Truncate');
  assert.equal(run("catalogResources(catalog,'tr').messages.length"),0);
  assert.equal(run("catalogResources(catalog,'nu').nodes.map(n => n.name).join(',')"),'Number');
  assert.equal(run("catalogResources(catalog,'front::num').nodes[0].name"),'Number');
  assert.equal(run("catalogResources(catalog,'chat-front').nodes.length"),2);
});

test('selecting a frontend sets its contract and includes its source binding in the draft', () => {
  const run = editor();
  run(`const prompt = {package:'http',version:'1',path:'http::Prompt',source:'local'};
    const chunk = {...prompt,path:'http::Chunk'};
    state.entries = [{package:'http',version:'1',source:'local',messages:[{name:'Prompt',path:'http::Prompt'},{name:'Chunk',path:'http::Chunk'}],nodes:[]}];
    $('source-name').value = 'Http'; render = () => {};
    selectFrontend({entry:'http@1',source:'local',node:{name:'HttpFrontend',role:'source',ports:[{port:'Req',head:prompt},{port:'Item',head:chunk}]}});`);
  assert.equal(run("$('source-request').value"),'http@1 http::Prompt');
  assert.equal(run("$('source-response').value"),'http@1 http::Chunk');
  assert.equal(run('draft().nodes[0].name'),'Http');
  assert.equal(run('draft().nodes[0].node'),'HttpFrontend');
});

for (const interruption of ['lostpointercapture', 'blur', 'released']) {
  test(`a drag preview is removed after ${interruption}`, () => {
    const run = editor();
    run(`const element = {classList:{add() {}},setPointerCapture() {}, addEventListener() {}};
      let removed = 0;
      document.elementFromPoint = () => null;
      document.createElement = () => ({style:{},remove() {removed++;}});
      document.body = {append() {}};
      makeDraggable(element, {kind:'node',name:'Echo'}, 'Echo');
      element.onpointerdown({button:0,pointerId:1,clientX:0,clientY:0});
      element.onpointermove({buttons:1,clientX:50,clientY:50});`);
    if (interruption === 'released') run('element.onpointermove({buttons:0,clientX:60,clientY:60})');
    else if (interruption === 'blur') run('window.blur()');
    else run('element.onlostpointercapture?.()');
    assert.equal(run('removed'), 1);
  });
}

test('graph name editing saves on blur or Save and cancels on Escape', () => {
  const run = editor();
  run(`let renders = 0; render = () => renders++;
    $('name').value = 'composed'; $('edit-name').focus = () => {}; $('name').focus = () => {}; $('name').select = () => {};
    $('edit-name').onclick(); $('name').value = 'renamed'; $('name').onblur();`);
  assert.equal(run("$('name-display').textContent"), 'renamed');
  assert.equal(run("$('name').hidden"), true);
  assert.equal(run('renders'), 1);
  run(`$('edit-name').onclick(); $('name').value = 'discard'; $('name').onkeydown({key:'Escape',preventDefault(){}});`);
  assert.equal(run("$('name').value"), 'renamed');
  run(`$('edit-name').onclick(); $('name').value = 'saved'; $('save-name').onclick();`);
  assert.equal(run("$('name-display').textContent"), 'saved');
  run(`$('edit-name').onclick(); $('name').value = '  '; $('name').onblur();`);
  assert.equal(run("$('name').value"), 'saved');
});

test('graph config includes only marked fields and keeps per-instance overrides', () => {
  const run = editor();
  run(`const candidate = {node:{config:[{name:'enabled_chat',default:true}]},config:{enabled_chat:false,listen:'private'}};`);
  assert.equal(run('JSON.stringify(graphConfig(candidate))'), '{"enabled_chat":false}');
  assert.equal(run('JSON.stringify(graphConfig({node:{config:[{name:"enabled_chat",default:true}]}}))'), '{"enabled_chat":true}');
});

test('service flags select required entry pipelines without losing disabled wiring', () => {
  const run = editor();
  run(`state.frontend = {node:{config:[{name:'enabled_chat',default:true},{name:'enabled_responses',default:true}],entries:[{name:'chat',enabled:'enabled_chat'},{name:'responses',enabled:'enabled_responses'}]},config:{enabled_responses:false},arms:{responses:{inner:{node:{name:'ResponsesAdapter'}}}}};`);
  assert.equal(run('enabledEntries().map(e=>e.name).join(",")'),'chat');
  run('state.frontend.config.enabled_responses = true');
  assert.equal(run('enabledEntries().map(e=>e.name).join(",")'),'chat,responses');
  assert.equal(run('state.frontend.arms.responses.inner.node.name'),'ResponsesAdapter');
});

test('service selection does not infer or connect a common internal pipeline', () => {
  const run=editor();
  run(`const origin = path => ({package:'api',version:'1',source:'local',path});
    const p=(port,path)=>({port,head:origin(path)});
    const frontend={entry:'api@1',node:{name:'Api',entries:[{name:'chat',enabled:'enabled_chat',request:p('Req','api::Chat'),response:p('Item','api::ChatReply')}],config:[{name:'enabled_chat',default:true}]}};
    state.entries=[{package:'api',version:'1',source:'local',nodes:[{name:'ChatAdapter',role:'operator',ports:[p('UpReq','api::Chat'),p('UpItem','api::ChatReply'),p('DownReq','api::Prompt'),p('DownItem','api::Chunk')]}]}];
    render=()=>{};
    selectFrontend(frontend);`);
  assert.equal(run('state.pipelines.length'),0);
  assert.equal(run('pipelineRoots().length'),0);
});

test('entry arms join only when both contracts match and expose response repair', () => {
  const run = editor();
  run(`const o = path => ({package:'api',version:'1',path});
    const req = {head:o('Public'),item:null}, shared = {head:o('Internal'),item:null};
    const op = {node:{name:'Adapter',role:'operator',ports:[{port:'UpReq',head:o('Public')},{port:'UpItem',head:o('Wrong')},{port:'DownReq',head:o('Internal')},{port:'DownItem',head:o('Chunk')}]}};
    state.frontend={node:{entries:[{}]}};state.pipelines=[{id:1,name:'processing',request:shared,response:o('Chunk'),hop:newHop()}];
    const arm = newHop(); arm.inner=op; arm.nested={...newHop(),target:1};
    const repair = {node:{role:'backward',ports:[{port:'In',head:o('Wrong')},{port:'Out',head:o('Reply')}]}};`);
  assert.equal(run("armComplete(arm,req,o('Reply'),shared,o('Chunk'))"), false);
  assert.equal(run("armResponse(arm,o('Chunk')).path"), 'Wrong');
  run('arm.backward.push(repair)');
  assert.equal(run("armComplete(arm,req,o('Reply'),shared,o('Chunk'))"), true);
  run("state.pipelines[0].response=o('Other')");
  assert.equal(run("armComplete(arm,req,o('Reply'))"), false);
});

test('endpoint display labels never replace serialized keys', () => {
  const run=editor();
  run(`state.frontend={node:{config:[{name:'enable_chat',label:'Chat Completions',subtext:'Chat API',default:true}]}};`);
  assert.equal(run("entryLabel({name:'chat',enabled:'enable_chat'})"),'Chat Completions');
  assert.equal(run("JSON.stringify(graphConfig(state.frontend))"),'{"enable_chat":true}');
});

test('workspace zoom is bounded and panning ignores node controls', () => {
  const run=editor();
  run(`$('graph-board').style={}; setWorkspaceZoom(5);`);
  assert.equal(run('workspaceZoom'),1.5);
  run('setWorkspaceZoom(.1)');
  assert.equal(run('workspaceZoom'),.35);
  run(`$('graph-viewport').scrollLeft=10; $('graph-viewport').scrollTop=20;
    $('graph-viewport').setPointerCapture=()=>{};
    $('graph-viewport').onpointerdown({button:0,clientX:100,clientY:100,pointerId:1,target:{closest:()=>null}});
    $('graph-viewport').onpointermove({clientX:80,clientY:90});`);
  assert.equal(run("$('graph-viewport').scrollLeft"),30);
  assert.equal(run("$('graph-viewport').scrollTop"),30);
  run(`$('graph-viewport').onpointerup();
    $('graph-viewport').onpointerdown({button:0,target:{closest:()=>({})}});`);
  assert.equal(run('pan'),null);
});

for (const [layout, request, response] of [['horizontal', 'Message →', '← stream Message'], ['vertical', 'Message ↓', '↑ stream Message']]) {
test(`${layout} arms label request and response flow directions`, () => {
  const run=editor(); installDom(run);
  run(`workspaceLayout=${JSON.stringify(layout)}; slot=()=>document.createElement('div');
    const m={package:'p',version:'1',path:'p::Message'};
    sourcePort=()=>({head:m,item:null});
    state.entries=[{package:'p',version:'1',messages:[{name:'Message',path:'p::Message'}]}];
    $('source-response').value='p@1 p::Message';
    const arm=armEditor(newHop(),{head:m,item:null},m);`);
  assert.equal(run('arm.children[0].children[0].textContent'),request);
  assert.equal(run('arm.children[2].children[0].textContent'),response);
});
}

test('Fit contains both axes and can fit a large graph below the manual zoom limit', () => {
  const run=editor();
  run(`$('graph-board').style={}; $('graph-board').querySelector=()=>null;
    $('graph-board').scrollWidth=1000; $('graph-board').scrollHeight=4000;
    $('graph-board').getBoundingClientRect=()=>({width:1000,height:4000});
    $('graph-viewport').clientWidth=800; $('graph-viewport').clientHeight=600;
    $('zoom-fit').onclick();`);
  assert.ok(run('workspaceZoom * 4000 <= 568'));
  assert.ok(run('workspaceZoom * 1000 <= 768'));
});

test('network cuts require codecs on every newly crossing request and response', () => {
  const run=editor();
  run(`$('source-name').value='Source'; $('input-mode').value='stream';
    const m=path=>({package:'p',version:'1',path});
    state.entries=[{package:'p',version:'1',messages:[{path:'Header',wire:true},{path:'Request',wire:true},{path:'Reply',wire:false}]}];
    $('source-request').value='p@1 Request'; $('source-header').value='p@1 Header';
    state.hop.inner={node:{name:'Engine',role:'sink',ports:[{port:'Item',head:m('Reply')}]}};`);
  assert.match(run("boundaryProblem('Engine','remote')"), /Reply/);
  run('state.entries[0].messages[2].wire=true');
  assert.equal(run("boundaryProblem('Engine','remote')"),'');
  run('state.entries[0].messages[0].wire=false');
  assert.match(run("boundaryProblem('Engine','remote')"), /Header/);
  assert.equal(run("boundaryProblem('Engine','s1')"),'');
});

test('undo and redo restore graph config and placement and discard a divergent redo', () => {
  const run=editor();
  run(`render=()=>{}; recordHistory(); state.frontend={entry:'api@1',node:{name:'Api'},config:{enabled_chat:true}}; recordHistory();
    state.placement.Engine='remote'; recordHistory(); undoEdit();`);
  assert.equal(run('state.placement.Engine'),undefined);
  assert.equal(run('state.frontend.config.enabled_chat'),true);
  run('redoEdit()');
  assert.equal(run('state.placement.Engine'),'remote');
  run('undoEdit(); undoEdit()');
  assert.equal(run('state.frontend'),null);
  run("state.placement.Engine='different'; recordHistory(); redoEdit()");
  assert.equal(run('state.placement.Engine'),'different');
});

test('local-only edges explain the missing codec and do not open the cutter', () => {
  const run=editor(); installDom(run);
  run(`state.mode='segments'; $('source-name').value='Source';
    boundaryProblem=()=> 'PrivateMessage has no network codec';
    let opened=false; openBoundary=()=>{opened=true;};
    const edge=arrow('Source','Engine','↓','PrivateMessage'); edge.onclick();`);
  assert.equal(run('opened'),false);
  assert.match(run('edge.title'), /PrivateMessage/);
});

test('segment slots replace add controls with cutters and cannot add nodes', () => {
  const run=editor(); installDom(run);
  run(`state.mode='segments'; let chosen=false, opened=false;
    boundaryProblem=()=>''; openBoundary=()=>{opened=true;};
    const control=slot('Add request node',[],()=>{chosen=true;},['Source','Engine']);
    control.children[0].onclick();`);
  assert.equal(run('opened'),true);
  assert.equal(run('chosen'),false);
  assert.match(run('control.children[0].className'),/cut-node/);
  run(`const local=slot('Add request node',[],()=>{chosen=true;}); local.children[0].onclick?.();`);
  assert.equal(run('local.children[0].disabled'),true);
  assert.match(run('local.children[0].title'),/Complete this typed connection/);
  assert.equal(run('chosen'),false);
});

test('catalog compatibility ignores disabled endpoint arms and restores them when enabled', () => {
  const run=editor();
  run(`const m=path=>({package:'api',version:'1',path});
    state.entries=[{package:'api',version:'1',messages:[{name:'Prompt',path:'Prompt'},{name:'Chunk',path:'Chunk'}]}];
    $('input-mode').value='single'; $('source-request').value='api@1 Prompt'; $('source-response').value='api@1 Chunk';
    state.frontend={node:{config:[{name:'chat',default:true},{name:'anthropic',default:false}],entries:[
      {name:'chat',enabled:'chat',request:{head:m('Chat')},response:{head:m('ChatReply')}},
      {name:'anthropic',enabled:'anthropic',request:{head:m('Anthropic')},response:{head:m('AnthropicReply')}}]},arms:{anthropic:{forward:[],backward:[],inner:{node:{role:'operator',ports:[{port:'DownReq',head:m('Hidden')}] }},nested:newHop()}}};
    const adapter={role:'operator',ports:[{port:'UpReq',head:m('Anthropic')}]};
    const chat={role:'operator',ports:[{port:'UpReq',head:m('Chat')}]};`);
  assert.equal(run('catalogNodeCompatible(adapter)'),false);
  assert.equal(run('catalogNodeCompatible(chat)'),true);
  assert.equal(run("catalogNodeCompatible({role:'forward',ports:[{port:'In',head:m('Hidden')}]})"),false);
  run('state.frontend.config={anthropic:true}');
  assert.equal(run('catalogNodeCompatible(adapter)'),false, 'the enabled endpoint already has an operator');
  assert.equal(run("catalogNodeCompatible({role:'forward',ports:[{port:'In',head:m('Hidden')}]})"),true);
  run('state.frontend.arms.anthropic.inner=null');
  assert.equal(run('catalogNodeCompatible(adapter)'),true);
  assert.equal(run("catalogNodeCompatible({role:'forward',ports:[{port:'In',item:m('Prompt')}]})"),false);
});

test('catalog separates nodes from message types while keeping search scoped', () => {
  const run=editor();
  run(`const entry={package:'api',nodes:[{name:'Adapter'}],messages:[{name:'Request'}]};`);
  assert.equal(run("catalogResources(entry,'','nodes').messages.length"),0);
  assert.equal(run("catalogResources(entry,'','nodes').nodes.length"),1);
  assert.equal(run("catalogResources(entry,'','messages').nodes.length"),0);
  assert.equal(run("catalogResources(entry,'req','messages').messages.length"),1);
  assert.equal(run("catalogResources(entry,'req','nodes').nodes.length"),0);
});

test('catalog offers only node roles and types accepted by open insertion slots', () => {
  const run=editor();
  run(`const m=path=>({package:'p',version:'1',path});
    const source={role:'source',ports:[]};
    const sink={role:'sink',ports:[{port:'Req',head:m('B')},{port:'Item',head:m('C')}]};
    const operator={role:'operator',ports:[{port:'UpReq',head:m('B')}]};
    const forward=path=>({role:'forward',ports:[{port:'In',head:m(path)}]});
    state.entries=[{package:'p',version:'1',messages:['A','C'].map(path=>({path}))}];
    $('input-mode').value='single'; $('source-request').value='p@1 A'; $('source-response').value='p@1 C';
    state.hop.forward=[{node:{role:'forward',ports:[{port:'Out',head:m('B')}]}}];`);
  assert.equal(run('catalogNodeCompatible(source)'),true);
  assert.equal(run('catalogNodeCompatible(sink)'),true);
  assert.equal(run("catalogNodeCompatible(forward('A'))"),false);
  assert.equal(run("catalogNodeCompatible(forward('B'))"),true);
  run('state.frontend={node:{entries:[],config:[]}}; state.hop.inner={node:sink};');
  assert.equal(run('catalogNodeCompatible(source)'),false);
  assert.equal(run('catalogNodeCompatible(sink)'),false);
  assert.equal(run('catalogNodeCompatible(operator)'),false);
  assert.equal(run("catalogNodeCompatible(forward('B'))"),true);
  run("state.mode='segments'");
  assert.equal(run("catalogNodeCompatible(forward('B'))"),false);
});

test('a disclosure click is not captured before a drag begins', () => {
  const run=editor();
  run(`let captured=false;
    const element={classList:{add(){}},setPointerCapture(){captured=true;},addEventListener(){}};
    makeDraggable(element,{kind:'node'},'Node');
    element.onpointerdown({button:0,pointerId:1,clientX:10,clientY:10});
    element.onpointerup({clientX:10,clientY:10});`);
  assert.equal(run('captured'),false);
});

test('request router policies export only on active request boundaries and support undo', () => {
  const run=editor();
  run(`$('source-name').value='Source'; state.hop.inner={node:{name:'Engine',role:'sink',ports:[]}};
    render=()=>{}; placeNode('Engine','remote',instances()); recordHistory();
    state.routing.Engine='random'; recordHistory();`);
  assert.equal(run("activeRouting().Engine"),'random');
  run('undoEdit()');
  assert.equal(run("activeRouting().Engine"),'round_robin');
  run('redoEdit()');
  assert.equal(run("activeRouting().Engine"),'random');
  run("placeNode('Engine','s1',instances())");
  assert.equal(run('Object.keys(activeRouting()).length'),0);
});

test('endpoint arms require an explicit named pipeline even when types match', () => {
  const run=editor();
  run(`const req={package:'p',version:'1',path:'Req'}, reply={package:'p',version:'1',path:'Reply'};
    state.frontend={node:{entries:[{name:'chat',enabled:'chat'}],config:[{name:'chat',default:true}]}};
    state.pipelines=[{id:1,name:'processing-pipeline-1',request:{head:req,item:null},response:reply,hop:newHop()}];
    const arm=newHop();`);
  assert.equal(run('armComplete(arm,{head:req,item:null},reply)'),false);
  run('arm.target=1');
  assert.equal(run('armComplete(arm,{head:req,item:null},reply)'),true);
  run('arm.target=2');
  assert.equal(run('armComplete(arm,{head:req,item:null},reply)'),false);
});

test('two arms select independent pipelines or explicitly share one destination', () => {
  const run=editor();
  run(`$('source-name').value='Source';
    const req={package:'p',version:'1',path:'Req'}, reply={package:'p',version:'1',path:'Reply'};
    state.frontend={node:{entries:[{name:'chat',enabled:'chat'},{name:'responses',enabled:'responses'}],config:[{name:'chat',default:true},{name:'responses',default:true}]},arms:{chat:{...newHop(),target:1},responses:{...newHop(),target:2}}};
    state.pipelines=[1,2].map(id=>({id,name:'processing-pipeline-'+id,request:{head:req,item:null},response:reply,hop:newHop()}));`);
  assert.equal(run('armDraft(state.frontend.arms.chat).target'),'Source');
  assert.equal(run('armDraft(state.frontend.arms.responses).target'),'Pipeline2');
  run('state.frontend.arms.responses.target=1');
  assert.equal(run('armDraft(state.frontend.arms.responses).target'),'Source');
  assert.equal(run('pipelineRoots().length'),2);
});

test('removing an independent pipeline does not rename the remaining node or move its segment', () => {
  const run=editor();
  run(`$('source-name').value='Source';state.frontend={node:{entries:[{}]}};
    const make=id=>({id,name:'p'+id,request:{head:null,item:null},response:{path:'Reply'},hop:{...newHop(),inner:{node:{name:'Echo'}}}});
    state.pipelines=[make(1),make(2)];
    const initial=instances();const second=state.pipelines[1].hop.inner;
    placeNode(initial.get(second),'worker-two',initial);state.pipelines.shift();`);
  assert.equal(run('instances().get(second)'),'Echo2');
  assert.equal(run('segments(instances())["worker-two"].includes("Echo2")'),true);
});

test('Segments catalog presents router policies instead of graph resources', () => {
  const run=editor();installDom(run);
  run(`const cards=[];$('entries').append=(...items)=>cards.push(...items);$('entries').replaceChildren=()=>{};
    $('search').parentElement={};state.mode='segments';renderCatalog();`);
  assert.equal(run("$('catalog-title').textContent"),'Routers');
  assert.equal(run('cards.length'),2);
  assert.equal(run('cards[0].children[0].textContent'),'Round robin');
  assert.equal(run('cards[1].children[0].textContent'),'Random');
});

test('Segments catalog offers the selectors the server lists, and a selector exports as a table', () => {
  const run=editor();installDom(run);
  run(`selectorTypes=[{name:'affinity',summary:'Same key, same replica.'},{name:'least_loaded',summary:'Fewest in flight.'}];
    const cards=[];$('entries').append=(...items)=>cards.push(...items);$('entries').replaceChildren=()=>{};
    $('search').parentElement={};state.mode='segments';renderCatalog();`);
  assert.equal(run('cards.length'),4);
  assert.equal(run('cards[2].children[0].textContent'),'affinity');
  assert.equal(run('cards[3].children[1].textContent'),'Fewest in flight.');
  run(`$('source-name').value='Source'; state.hop.inner={node:{name:'Engine',role:'sink',ports:[]}};
    render=()=>{}; placeNode('Engine','remote',instances());
    state.routing.Engine='least_loaded';`);
  assert.equal(run('JSON.stringify(activeRouting().Engine)'),'{"selector":"least_loaded"}');
  assert.equal(run("routerLabel('least_loaded')"),'least_loaded');
  assert.equal(run("routerLabel('random')"),'Random');
  run(`state.routing.Engine='random';`);
  assert.equal(run('activeRouting().Engine'),'random');
});

test('explicitly connecting an existing sink exports one node and survives undo snapshots', () => {
  const run=editor();
  run(`$('source-name').value='Source';state.frontend={entry:'p@1',node:{name:'Api',entries:[{}]}};
    const sink={entry:'p@1',node:{name:'Echo',role:'sink',ports:[]}};
    state.pipelines=[1,2].map(id=>({id,name:'processing-'+id,request:{head:null,item:null},response:{path:'Reply'},hop:{...newHop(),inner:id===1?sink:null}}));
    instances();
    state.pipelines[1].hop.inner={...sink};
    state.pipelines=JSON.parse(JSON.stringify(state.pipelines));
    state.frontend.node.config=state.pipelines.map(p=>({name:'e'+p.id,default:true}));state.frontend.node.entries=state.pipelines.map(p=>({name:'e'+p.id,enabled:'e'+p.id,request:p.request,response:{head:p.response}}));state.frontend.arms=Object.fromEntries(state.pipelines.map(p=>['e'+p.id,{...newHop(),target:p.id}]));
    const output=draft();`);
  assert.equal(run('output.nodes.filter(n=>n.node==="Echo").length'),1);
  assert.equal(run('output.hop.inner'),run('output.pipelines[0].hop.inner'));
  assert.equal(run('instances().size'),1);
});

test('empty pipeline contracts do not appear as deployable nodes', () => {
  const run=editor();installDom(run);
  run(`$('source-name').value='Source';state.mode='segments';state.frontend={node:{entries:[{}]}};
    state.pipelines=[1,2].map(id=>({id,name:'processing-'+id,request:{head:null,item:null},response:{path:'Reply'},hop:newHop()}));
    hopEditor=()=>{};const container=document.createElement('div');container.replaceChildren=()=>{};
    renderPipelines(container,instances());`);
  assert.equal(run('container.children[0].children[0].hidden'),true);
  assert.equal(run('container.children[1].children[0].hidden'),true);
  assert.equal(run('container.children[1].children[0].dataset.segment'),undefined);
});

test('identity operator and request node deploy independently with one shared sink', () => {
  const run=editor();
  run(`$('source-name').value='Source';state.frontend={node:{entries:[{}]}};
    const req={head:null,item:null}, reply={path:'Reply'};
    const sink={node:{name:'Echo',role:'sink',ports:[{port:'Item',head:reply}]}};
    const operator=identityNode('operator',req,reply);operator.instanceName='ProcessingOne';
    const forward=identityNode('forward',req);forward.instanceName='ProcessingTwo';
    state.pipelines=[{id:1,name:'p1',request:req,response:reply,hop:{...newHop(),inner:operator,nested:{...newHop(),inner:sink}}},
      {id:2,name:'p2',request:req,response:reply,hop:{...newHop(),forward:[forward],inner:sink}}];
    state.frontend.node.config=state.pipelines.map(p=>({name:'e'+p.id,default:true}));state.frontend.node.entries=state.pipelines.map(p=>({name:'e'+p.id,enabled:'e'+p.id,request:p.request,response:{head:p.response}}));state.frontend.arms=Object.fromEntries(state.pipelines.map(p=>['e'+p.id,{...newHop(),target:p.id}]));
    instances();placeNode('ProcessingOne','one',instances());placeNode('ProcessingTwo','two',instances());placeNode('Echo','sink',instances());`);
  assert.equal(run('Object.keys(segments(instances())).length'),4);
  assert.equal(run('draft().identities.ProcessingOne.role'),'operator');
  assert.equal(run('draft().identities.ProcessingTwo.role'),'forward');
  assert.equal(run('draft().nodes.some(n=>n.entry==="built-in")'),false);
  assert.equal(run("networkEdges().some(e=>!e.request && e.from==='Echo' && e.to==='ProcessingOne')"),true);
  assert.equal(run("networkEdges().some(e=>!e.request && e.from==='Echo' && e.to==='Source_entry_e2')"),true);
  assert.equal(run("networkEdges().some(e=>!e.request && e.to==='ProcessingTwo')"),false);
  run("state.placement={};state.segmentNames={};placeDownstream('ProcessingOne','one')");
  assert.equal(run("segments(instances()).one.includes('Echo')"),true);
  run("placeDownstream('ProcessingTwo','two')");
  assert.equal(run("segments(instances()).one.includes('Echo')"),true);
  run("placeDownstream('Echo','sink')");
  assert.equal(run('Object.keys(segments(instances())).length'),4);
});

test('a response identity carries only its response message type', () => {
  const run=editor();
  run(`const identity=identityNode('backward',null,{path:'Reply'});`);
  assert.equal(run('identitySpec(identity).role'),'backward');
  assert.equal(run('identitySpec(identity).response.path'),'Reply');
  assert.equal(run('identitySpec(identity).request'),undefined);
});

test('identity choices survive catalog refresh and pruning', () => {
  const run=editor();run(`const req={head:null,item:null}, reply={path:'Reply'};
    const op=identityNode('operator',req,reply), forward=identityNode('forward',req), backward=identityNode('backward',null,reply);
    const hop={forward:[forward],inner:op,nested:newHop(),backward:[backward]};prune(hop,req);`);
  assert.equal(run('hop.forward.length'),1);
  assert.equal(run('hop.inner.node.identity'),true);
  assert.equal(run('hop.backward.length'),1);
});

test('a cut includes downstream response transforms before the upstream owner', () => {
  const run=editor();run(`$('source-name').value='Source';
    const req={head:null,item:null}, reply={path:'Reply'};
    state.hop={forward:[{...identityNode('forward',req),instanceName:'RequestPass'}],inner:{node:{name:'Echo',role:'sink',ports:[{port:'Item',head:reply}]}},nested:null,
      backward:[{...identityNode('backward',null,reply),instanceName:'ResponsePass'}]};
    placeDownstream('RequestPass','worker');`);
  assert.deepEqual(JSON.parse(run('JSON.stringify(segments(instances()).worker.sort())')),['Echo','RequestPass','ResponsePass']);
});

test('segment view hides pipeline contract headers rather than placing them in the frontend', () => {
  const run=editor();
  run(`$('source-name').value='Source';state.frontend={node:{entries:[{}]}};
    state.pipelines=[{id:1,name:'processing-pipeline-1',request:{head:null,item:null},response:{path:'Reply'},hop:newHop()}];
    textElement=(tag,text,cls)=>({tag,text,className:cls,dataset:{},children:[],append(...nodes){this.children.push(...nodes);},setAttribute(){}});
    hopEditor=()=>{};
    const into={children:[],replaceChildren(){this.children=[];},append(node){this.children.push(node);}};
    state.mode='segments';renderPipelines(into,new Map());`);
  assert.equal(run('into.children[0].children[0].hidden'),true);
  assert.equal(run('into.children[0].children[0].dataset.segment'),undefined);
  run(`state.mode='graph';renderPipelines(into,new Map());`);
  assert.notEqual(run('into.children[0].children[0].hidden'),true);
});

test('clear graph removes composition and placement but undo restores them', () => {
  const run=editor();
  run(`render=()=>recordHistory();$('name').value='Example';$('source-name').value='Source';
    state.entries=[{package:'keep'}];state.frontend={node:{name:'Frontend'}};
    state.hop={...newHop(),inner:{node:{name:'Sink'}}};state.pipelines=[{id:1}];
    state.placement={Sink:'worker'};state.routing={Sink:'random'};state.segmentNames={Sink:'worker'};
    recordHistory();clearGraph();`);
  assert.equal(run('state.hop.inner'),null);
  assert.equal(run('state.frontend'),null);
  assert.equal(run('state.pipelines.length'),0);
  assert.equal(run('Object.keys(state.placement).length'),0);
  assert.equal(run('Object.keys(state.routing).length'),0);
  assert.equal(run('state.entries[0].package'),'keep');
  run('graphComplete=()=>false;undoEdit();');
  assert.equal(run('state.hop.inner.node.name'),'Sink');
  assert.equal(run('state.placement.Sink'),'worker');
});

test('node selector groups identity specializations into one choice', () => {
  const run=editor();
  run(`const req={head:{path:'Prompt'},item:null}, reply={path:'Chunk'};
    const choices=selectorNodes([identityNode('forward',req),identityNode('operator',req,reply),{node:{name:'Echo'}}]);`);
  assert.equal(run('choices.length'),2);
  assert.equal(run('choices[0].node.name'),'Identity');
  assert.equal(run('choices[0].specializations.length'),2);
  assert.equal(run('choices[0].specializations[1].node.role'),'operator');
});

test('an unconnected endpoint offers response nodes by their output contract', () => {
  const run=editor();
  run(`const msg=path=>({path});const reply=msg('ChatReply'), chunk=msg('Chunk');
    const format={node:{name:'FormatChat',role:'backward',ports:[{port:'In',head:chunk},{port:'Out',head:reply}]}};
    const arm=newHop();const contract=responseInsertion(arm,reply);`);
  assert.equal(run("contract.port"),'Out');
  assert.equal(run("same(port(format.node,contract.port).head,contract.message)"),true);
  run(`arm.backward.unshift(format);`);
  assert.equal(run('responseInsertion(arm,reply).message.path'),'Chunk');
  run(`state.frontend={node:{config:[{name:'chat',default:true}],entries:[{name:'chat',enabled:'chat',request:{head:msg('ChatRequest')},response:{head:reply}}]},arms:{chat:arm}};`);
  assert.equal(run("catalogNodeCompatible({role:'backward',ports:[{port:'In',head:msg('Raw')},{port:'Out',head:chunk}]})"),true);
  run(`state.pipelines=[{id:1,request:{head:msg('Prompt')},response:chunk,hop:newHop()}];arm.target=1;`);
  assert.equal(run('responseInsertion(arm,reply).port'),'In');
  assert.equal(run('responseInsertion(arm,reply).message.path'),'ChatReply');
});

test('endpoint connections can create a sink and explicitly reuse its single instance', () => {
  const run=editor();
  run(`const m=path=>({package:'p',version:'1',path});const request={head:m('Prompt'),item:null},response=m('Chunk');
    const sink={entry:'p@1',node:{name:'Echo',role:'sink',ports:[{port:'Req',...request},{port:'Item',head:response}]}};
    state.entries=[{package:'p',version:'1',nodes:[sink.node],messages:[{path:'Prompt'},{path:'Chunk'}]}];
    state.frontend={node:{entries:[]},arms:{}};
    const first=newHop(),second=newHop(),third=newHop();
    state.frontend.node.entries=[{name:'chat'},{name:'responses'}];
    connectSink(first,request,response,sink);
    const shared=[...instances().keys()][0];connectSink(second,request,response,shared);connectSink(third,request,response,shared);`);
  assert.equal(run('state.pipelines.length'),3);
  assert.equal(run('new Set(state.pipelines.map(p=>p.name)).size'),3);
  assert.equal(run('instances().size'),1);
  assert.equal(run('directSink(state.pipelines[0]).node.name'),'Echo');
  assert.equal(run('first.target !== second.target'),true);
  assert.equal(run("sinkChoices(request,response).filter(c=>c.existing).length"),1);
  assert.equal(run("sinkChoices(request,{path:'Other'}).length"),0);
  assert.throws(()=>run("connectSink(newHop(),request,{path:'Other'},shared)"),/must match/);
});

test('endpoint adapters participate in the deployed graph and independent placement', () => {
  const run=editor();
  run(`const m=path=>({package:'p',version:'1',path});const req={head:m('Prompt'),item:null},reply=m('Chunk');
    const extract={entry:'p@1',node:{name:'ExtractChat',role:'forward',ports:[{port:'In',head:m('ChatRequest')},{port:'Out',head:m('Prompt')}]}};
    const format={entry:'p@1',node:{name:'FormatChat',role:'backward',ports:[{port:'In',head:reply},{port:'Out',head:m('ChatReply')}]}};
    const op={entry:'p@1',node:{name:'ResponsesAdapter',role:'operator',ports:[{port:'UpReq',head:m('ResponseRequest')},{port:'UpItem',head:m('ResponseReply')},{port:'DownReq',...req},{port:'DownItem',head:reply}]}};
    const sink={entry:'p@1',node:{name:'Echo',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]}};
    $('source-name').value='Source';state.frontend={entry:'p@1',node:{name:'Api',config:[{name:'chat',default:true},{name:'responses',default:true}],entries:[{name:'chat',enabled:'chat',request:{head:m('ChatRequest')},response:{head:m('ChatReply')}},{name:'responses',enabled:'responses',request:{head:m('ResponseRequest')},response:{head:m('ResponseReply')}}]},arms:{chat:{...newHop(),forward:[extract],backward:[format]},responses:{...newHop(),inner:op,nested:newHop()}}};
    connectSink(state.frontend.arms.chat,req,reply,sink);instances();connectSink(state.frontend.arms.responses.nested,req,reply,[...instances().keys()].find(n=>n.node.role==='sink'));
    state.placement={ExtractChat:'extract',ResponsesAdapter:'responses',Echo:'sink'};const output=draft();`);
  assert.equal(run("output.nodes.some(n=>n.name==='ExtractChat')"),true);
  assert.equal(run("output.nodes.some(n=>n.name==='FormatChat')"),true);
  assert.equal(run('output.source.request.head.path'),'ChatRequest');
  assert.equal(run('output.entries.chat.forward.length'),0);
  assert.equal(run('output.hop.forward[0]'),'ExtractChat');
  assert.equal(run('output.hop.backward[0]'),'FormatChat');
  assert.equal(run('output.pipelines[0].hop.inner'),'ResponsesAdapter');
  assert.equal(run('output.segments.s1.includes("FormatChat")'),true);
  assert.equal(run('networkEdges().some(e=>e.from==="Echo" && e.to==="FormatChat")'),true);
});

test('inserting a type-changing chain preserves its downstream endpoint while incomplete', () => {
  const run=editor();
  run(`const m=path=>({path});const req={head:m('X'),item:null};
    const node=(name,input,output)=>({source:'test',node:{name,role:'forward',ports:[{port:'In',head:m(input)},{port:'Out',head:m(output)}]}});
    const c=node('C','X','Y'),d=node('D','Y','Z'),e=node('E','Z','X');
    const sink={source:'test',node:{name:'B',role:'sink',ports:[{port:'Req',head:m('X')},{port:'Item',head:m('Reply')}]}};
    state.entries=[{package:'p',version:'1',source:'test',nodes:[c.node,d.node,e.node,sink.node]}];
    const hop={...newHop(),forward:[c],inner:sink};prune(hop,req);`);
  assert.equal(run('hop.inner.node.name'),'B');
  assert.equal(run("flowComplete(hop,req,m('Reply'))"),false);
  run('hop.forward.push(d,e);prune(hop,req);');
  assert.equal(run('hop.inner.node.name'),'B');
  assert.equal(run("flowComplete(hop,req,m('Reply'))"),true);
});

test('deployment view groups every node once and retains independently routed request and response edges', () => {
  const run=editor();
  run(`const groups={s1:['Source','FormatChat'],extract:['ExtractChat'],responses:['ResponsesAdapter'],sink:['Echo']};
    const connections=[{from:'Source',to:'ExtractChat',request:true},{from:'ExtractChat',to:'Echo',request:true},{from:'Echo',to:'FormatChat'},{from:'FormatChat',to:'Source'},{from:'Source',to:'ResponsesAdapter',request:true},{from:'ResponsesAdapter',to:'Echo',request:true},{from:'Echo',to:'ResponsesAdapter'},{from:'ResponsesAdapter',to:'Source'}];
    const view=deploymentView(groups,connections);`);
  assert.equal(run("view.filter(s=>s.name==='s1').length"),1);
  assert.equal(run("view.find(s=>s.name==='s1').nodes.map(n=>n.name).join(',')"),'Source,FormatChat');
  assert.equal(run('view.flatMap(s=>s.nodes).length'),5);
  assert.equal(run('view.flatMap(s=>s.nodes.flatMap(n=>n.edges)).length'),8);
  assert.equal(run("view.find(s=>s.name==='sink').nodes[0].edges.find(e=>e.to==='FormatChat').destination"),'s1');
  assert.equal(run("view.find(s=>s.name==='s1').rank"),0);
  assert.equal(run("view.find(s=>s.name==='sink').rank"),2);
});

test('deployment rendering keeps one container per process and exposes boundary actions on the real edges', () => {
  const run=editor();installDom(run);
  run(`const makeElement=document.createElement;document.createElement=tag=>{const el=makeElement(tag);el.style.setProperty=()=>{};el.setAttribute=(key,value)=>{el[key]=value;};return el;};
    const msg={path:'Message'};
    instances=()=>new Map([[{node:{role:'backward'}},'Format'],[{node:{role:'sink'}},'Echo']]);
    deployedRoots=()=>[{root:'Source',name:'Chat'}];
    segments=()=>({s1:['Source','Format'],sink:['Echo']});
    networkEdges=()=>[{from:'Source',to:'Echo',types:[msg,null],request:true},{from:'Echo',to:'Format',types:[msg]},{from:'Format',to:'Source',types:[msg]}];
    boundaryProblem=name=>name==='Format'?'No codec':'';
    settingsButton=()=>document.createElement('button');
    const board=document.createElement('div');renderDeployment(board);
    const flatten=el=>[el,...el.children.flatMap(flatten)];const rendered=flatten(board);
    const cards=rendered.filter(el=>el.className==='deployment-segment');`);
  assert.equal(run('cards.length'),2);
  assert.equal(run("flatten(cards[0]).filter(el=>el.className==='deployment-source').length"),1);
  assert.equal(run("flatten(cards[1]).filter(el=>el.className==='deployment-source').length"),0);
  assert.equal(run("flatten(cards[0]).filter(el=>el.dataset.deploymentMember).map(el=>el.dataset.deploymentMember).join(',')"),'Source,Format');
  assert.equal(run("flatten(cards[1]).filter(el=>el.dataset.deploymentMember).map(el=>el.dataset.deploymentMember).join(',')"),'Echo');
  assert.equal(run("rendered.filter(el=>el.className==='request-router').length"),1);
  assert.equal(run("rendered.find(el=>el['aria-label']==='Echo → Format: edit network boundary').disabled"),true);
  run(`let selected;openBoundary=(from,to)=>{selected=[from,to];};rendered.find(el=>el['aria-label']==='Source → Echo: edit network boundary').onclick();`);
  assert.equal(run("selected.join(',')"),'Source,Echo');
});

test('process ordering terminates when placement creates a request cycle', () => {
  const run=editor();
  assert.equal(run("deploymentView({s1:['A','C'],worker:['B']},[{from:'A',to:'B',request:true},{from:'B',to:'C',request:true}]).length"),2);
});

test('segment node layout preserves endpoint lanes and orders request and return adapters below their source', () => {
  const run=editor();
  run(`const links=[{from:'Chat',to:'Extract',request:true},{from:'Extract',to:'Echo',request:true},{from:'Echo',to:'Format'},{from:'Format',to:'Chat'},{from:'Responses',to:'Adapter',request:true},{from:'Adapter',to:'Echo',request:true},{from:'Echo',to:'Adapter'},{from:'Adapter',to:'Responses'}];
    const layout=deploymentNodeLayout(['Chat','Responses','Format','Adapter'],links,['Chat','Responses']);`);
  assert.equal(run('layout.Chat.column'),0);
  assert.equal(run('layout.Responses.column'),1);
  assert.equal(run('layout.Format.column'),0);
  assert.equal(run('layout.Adapter.column'),1);
  assert.ok(run('layout.Format.row > layout.Chat.row'));
  assert.ok(run('layout.Adapter.row > layout.Responses.row'));
});

test('clicking a node connection disclosure does not start workspace panning', () => {
  const run=editor();
  run(`$('graph-viewport').setPointerCapture=()=>{throw new Error('captured disclosure click');};
    $('graph-viewport').onpointerdown({button:0,clientX:0,clientY:0,pointerId:1,target:{closest:selector=>selector.split(',').includes('summary')?{}:null}});`);
  assert.equal(run('pan'),null);
});

test('wire routing uses the direct gap and detours only around obstructing nodes', () => {
  const run=editor();
  run(`const a={x:100,y:80},b={x:100,y:260};
    const clear=routeDeploymentWire(a,b,[],[]);
    const obstacle={left:70,right:130,top:120,bottom:210};
    const detour=routeDeploymentWire(a,b,[obstacle],[]);`);
  assert.equal(run('clear.every(p=>p.x===100)'),true);
  assert.equal(run('detour.some(p=>p.x<70 || p.x>130)'),true);
  assert.equal(run('wireIntersections(detour,[obstacle])'),0);
  assert.ok(run('Math.max(...detour.map(p=>Math.abs(p.x-100))) < 60'));
});

test('wire routing separates parallel tracks and avoids segment headings', () => {
  const run=editor();
  run(`const heading={left:50,right:200,top:100,bottom:130};
    const route=routeDeploymentWire({x:100,y:50},{x:100,y:180},[heading],[]);
    const parallel=routeDeploymentWire({x:110,y:50},{x:110,y:180},[heading],[route]);`);
  assert.equal(run('wireIntersections(route,[heading])'),0);
  assert.equal(run('wireIntersections(parallel,[heading])'),0);
  assert.notEqual(run('JSON.stringify(route.slice(1,-1))'),run('JSON.stringify(parallel.slice(1,-1))'));
});

test('routing enforces parallel track clearance', () => {
  const run=editor();
  run(`const previous=[{x:50,y:0},{x:50,y:250}];
    const points=routeDeploymentWire({x:60,y:0},{x:60,y:250},[],[previous]);`);
  assert.equal(run('wireClearance(points,[previous],24)'),true);
});

test('arrow routes reserve a straight terminal run at both node ports', () => {
  const run=editor();
  run(`const routed=terminalWire({x:0,y:0},{x:80,y:200},[],[]);`);
  assert.equal(run('routed[1].x'),0);
  assert.ok(run('routed[1].y>=36'));
  assert.equal(run('routed.at(-2).x'),80);
  assert.ok(run('200-routed.at(-2).y>=36'));
});

test('four-segment layout routes all twelve connections with terminal and track clearance', () => {
  const run=editor();
  run(`const geo=${fs.readFileSync(require.resolve('./fixtures/routing.json'),'utf8')};
    const geometryNodes=new Map(geo.nodes), routed=[];
    const links=[['Source','ExtractChat',true],['ExtractChat','Echo',true],['Echo','FormatChat',false],['FormatChat','Source',false],['Source_entry_responses','ResponsesAdapter',true],['ResponsesAdapter','Source_entry_responses',false],['ResponsesAdapter','Echo',true],['Echo','ResponsesAdapter',false],['Source_entry_anthropic','AnthropicAdapter',true],['AnthropicAdapter','Source_entry_anthropic',false],['AnthropicAdapter','Echo',true],['Echo','AnthropicAdapter',false]];
    const socket=(name,edge)=>{
      const rect=geometryNodes.get(name),other=geometryNodes.get(edge[0]===name?edge[1]:edge[0]);
      const peers=links.filter(e=>(e[0]===name||e[1]===name) && (geometryNodes.get(e[0]===name?e[1]:e[0]).y>rect.y)===(other.y>rect.y))
        .sort((a,b)=>geometryNodes.get(a[0]===name?a[1]:a[0]).x-geometryNodes.get(b[0]===name?b[1]:b[0]).x||Number(b[2])-Number(a[2]));
      return {x:rect.x+(peers.indexOf(edge)-(peers.length-1)/2)*wireGap,y:other.y>rect.y?rect.bottom:rect.top};
    };
    const results=links.map(edge=>{
      const start=socket(edge[0],edge),end=socket(edge[1],edge);
      const obstacles=[...geometryNodes.values()].map(r=>({left:r.left-6,right:r.right+6,top:r.top-6,bottom:r.bottom+6}));
      obstacles.push(...geo.headings,...links.filter(e=>e!==edge).flatMap(e=>terminalReservations(socket(e[0],e),socket(e[1],e))));
      const points=terminalWire(start,end,obstacles,routed);if(points) routed.push(points);return points;
    });`);
  assert.equal(run('results.filter(Boolean).length'),12);
  assert.equal(run('routed.every((p,i)=>wireClearance(p,routed.slice(0,i)))'),true);
  assert.equal(run('routed.every(p=>Math.abs(p[1].y-p[0].y)>=40 && Math.abs(p.at(-1).y-p.at(-2).y)>=40)'),true);
});

test('graph and segments retain independent zoom including fitted scales', () => {
  const run = editor();
  run(`$('graph-board').style={}; render=()=>{}; state.validated=true;
    setWorkspaceZoom(.72); $('segments-tab').onclick();`);
  assert.equal(run('workspaceZoom'), 1);
  run(`setWorkspaceZoom(.24,true); $('graph-tab').onclick();`);
  assert.equal(run('workspaceZoom'), .72);
  run(`$('segments-tab').onclick();`);
  assert.equal(run('workspaceZoom'), .24);
});

test('icon tooltips use the accessible action name when no explanation is provided', () => {
  const run = editor();
  assert.equal(run(`actionTooltip({getAttribute:n=>n==='aria-label'?'Source settings':null})`),'Source settings');
  assert.equal(run(`actionTooltip({getAttribute:n=>n==='title'?'Cannot cut: message has no codec':'Cut'})`),'Cannot cut: message has no codec');
});

test('deployment settings include only current segments and retain per-image overrides', () => {
  const run=editor();
  run(`deploymentSettings={base_image:'debian:bookworm-slim',build_image:'rust:1.93-slim-bookworm',hub_image:'ubuntu:24.04',segment:{s1:{base_image:'ubuntu:24.04',ports:['8082:8082']},removed:{base_image:'unused'}}};`);
  const value=JSON.parse(run(`JSON.stringify(deploymentDraft({s1:['Source']}))`));
  assert.deepEqual(Object.keys(value.segment),['s1']);
  assert.equal(value.segment.s1.base_image,'ubuntu:24.04');
  assert.equal(value.hub_image,'ubuntu:24.04');
});

test('deployment preview rejects stale generation and disables download during edits', async () => {
  const run=editor();
  run(`const replies=[]; fetch=()=>new Promise(resolve=>replies.push(resolve));
    draft=()=>({name:'demo',segments:{s1:['Source']}});
    renderDeploymentPreview=()=>{};
    const first=generateDeployment(); const second=generateDeployment();`);
  run(`replies[1]({ok:true,json:async()=>({files:{'compose.yaml':'current'},archive:'current'})});`);
  await run('second');
  run(`replies[0]({ok:true,json:async()=>({files:{'compose.yaml':'stale'},archive:'stale'})});`);
  await run('first');
  assert.equal(run('deploymentSnapshot.archive'),'current');
  run('scheduleDeployment(); clearTimeout(deploymentTimer);');
  assert.equal(run('deploymentSnapshot'),null);
  assert.equal(run("$('download-deployment').disabled"),true);
});

test('load examples imports the advertised Git revision and reports failures', async () => {
  const run=editor();
  run(`exampleSource={source:'git',url:'https://github.com/ryanolson/gdl',revision:'pinned',path:'examples/api'};
    let imported=null; load=async()=>{};
    fetch=async(url,options)=>{imported=JSON.parse(options.body);return {ok:true,json:async()=>({})};};`);
  await run(`$('load-examples').onclick()`);
  assert.equal(run('imported.revision'),'pinned');
  assert.equal(run('imported.path'),'examples/api');
  run(`fetch=async()=>({ok:false,json:async()=>({problems:['Import failed']})});`);
  await run(`$('load-examples').onclick()`);
  assert.equal(run(`$('adding').textContent`),'Import failed');
  assert.equal(run(`$('load-examples').disabled`),false);
});

test('clear catalog requires confirmation and invalidates deployment snapshots', async () => {
  const run=editor();
  run(`let requests=0; window.confirm=()=>false;
    fetch=async()=>{requests++;return {ok:true,json:async()=>({backup:'/tmp/catalog-backup'})};};`);
  await run(`$('clear-catalog').onclick()`);
  assert.equal(run('requests'),0);
  run(`window.confirm=()=>true;clearGraph=()=>{};load=async()=>{};recordHistory=()=>{};
    deploymentSnapshot={archive:'stale'};deployOpen=true;`);
  await run(`$('clear-catalog').onclick()`);
  assert.equal(run('requests'),1);
  assert.equal(run('deploymentSnapshot'),null);
  assert.equal(run('deployOpen'),false);
  assert.equal(run('editHistory.length'),0);
});
