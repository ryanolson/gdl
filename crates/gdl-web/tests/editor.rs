//! Page edits that must preserve unary placement and calls.

use std::path::Path;
use std::process::{Command, Stdio};

fn page(script: &str) {
    let harness = include_str!("editor.cjs")
        .split("\ntest(")
        .next()
        .expect("page harness");
    let output = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"))
        .args(["-e", &format!("{harness}\n{script}")])
        .stdin(Stdio::null())
        .output()
        .expect("node");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn undo_uses_current_catalogue_defaults_for_frontend_and_hosted_sink() {
    page(
        r#"
(async () => {
const run = editor();
run(`document.createElement = tag => ({tag,children:[],dataset:{},style:{setProperty(){}},
  append(...children){this.children.push(...children);},setAttribute(){},
  classList:{add(){},toggle(){}},add(){}});
  Option = function(){};
  setTimeout=()=>0;clearTimeout=()=>{};
  const element=document.getElementById;
  document.getElementById=id=>{const e=element(id);e.append ||=()=>{};e.add ||=()=>{};return e;};
  for(const id of ['hop','groups','service-nodes','sink-nodes','placements']) $(id).append=()=>{};
  for(const id of ['source-request','source-header','source-response','source-frontend']) $(id).add=()=>{};
  renderCatalog=()=>{};
  $('source-name').value='Source';$('name').value='composed';
  const m=path=>({package:'proto',version:'1',path:'proto::'+path});
  const req={head:m('Prompt'),item:null},reply=m('Chunk');
  const catalogue=(version,value)=>[{package:'p',version,source:'local',messages:[],nodes:[
    {name:'Api',role:'source',ports:[],config:[{name:'chat',default:true},{name:'limit',default:value}],
      entries:[{name:'chat',enabled:'chat',request:req,response:{head:reply}}]},
    {name:'Engine',role:'sink',config:[{name:'limit',default:value}],
      ports:[{port:'Req',...req},{port:'Item',head:reply}]},
    {name:'Tok',role:'unary',config:[{name:'limit',default:value}],ports:[{port:'Req',...req},{port:'Resp',head:reply}]}
  ]}];
  state.entries=catalogue('1',2);
  state.frontend={...nodes().find(n=>n.node.role==='source'),arms:{chat:newHop()}};
  connectSink(state.frontend.arms.chat,req,reply,nodes().find(n=>n.node.role==='sink'));
  addService('p@1',nodes().find(n=>n.node.role==='unary').node);
  render();
  fetch=async()=>({ok:true,json:async()=>({entries:catalogue('2',7)})});`);
const limits = () => JSON.parse(run('JSON.stringify(draft().nodes.map(n=>[n.name,n.entry,n.config.limit]))'));
assert.equal(run('graphComplete()'),true);
assert.deepEqual(limits(),[['Source','p@1',2],['Engine','p@1',2],['Tok','p@1',2]]);
await run('load()');
const expected=[['Source','p@2',7],['Engine','p@2',7],['Tok','p@2',7]];
assert.deepEqual(limits(),expected,'the refreshed catalogue supplies every default');
run('undoEdit()');
assert.equal(run('graphComplete()'),true);
assert.deepEqual(limits(),expected,'undo must not pair a current entry with removed catalogue defaults');
})().catch(error=>{console.error(error);process.exitCode=1;});
"#,
    );
}

#[test]
fn renaming_the_source_keeps_entry_roots_distinct_from_unary_nodes() {
    page(
        r#"
const run = editor();
run(`document.createElement=tag=>({tag,children:[],dataset:{},style:{setProperty(){}},
  append(...children){this.children.push(...children);},setAttribute(){},classList:{add(){},toggle(){}},add(){}});
  const element=document.getElementById;
  document.getElementById=id=>{const e=element(id);e.append ||=()=>{};return e;};
  for(const id of ['hop','groups','service-nodes','sink-nodes','placements']) $(id).append=()=>{};
  setTimeout=()=>0;clearTimeout=()=>{};
  $('source-name').value='Source';$('name').value='composed';
  const m=path=>({package:'p',version:'1',path:'p::'+path});
  const req={head:m('Prompt'),item:null},reply=m('Chunk');
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const tok={name:'Next_entry_responses',role:'unary',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  const frontend={name:'Api',role:'source',ports:[],
    config:[{name:'chat',default:true},{name:'responses',default:true}],
    entries:['chat','responses'].map(name=>({name,enabled:name,request:req,response:{head:reply}}))};
  state.entries=[{package:'p',version:'1',nodes:[frontend,engine,tok]}];
  state.frontend={...nodes().find(n=>n.node.role==='source'),arms:{chat:newHop(),responses:newHop()}};
  connectSink(state.frontend.arms.chat,req,reply,nodes().find(n=>n.node.role==='sink'));
  const sink=[...instances().keys()].find(n=>n.node.role==='sink');
  connectSink(state.frontend.arms.responses,req,reply,sink);
  addService('p@1',tok);render();`);
assert.equal(run('graphComplete()'),true);
assert.equal(run('draft().nodes.find(n=>n.node===tok.name).name'),'Next_entry_responses');
run("$('source-name').value='Next';$('source-name').oninput();");
const output=JSON.parse(run('JSON.stringify(draft())'));
assert.equal(run('graphComplete()'),true);
const declarations=[output.source.name,...output.pipelines.map(p=>p.source.name),
  ...output.nodes.filter(n=>n.node!=='Api').map(n=>n.name)];
assert.equal(new Set(declarations).size,declarations.length,
  'renaming a source must not generate an entry root already held by a unary node');
"#,
    );
}

#[test]
fn a_segment_rename_cannot_merge_two_copies_of_a_unary_node() {
    page(
        r#"
const run = editor();
run(`document.createElement = tag => ({tag, children:[], className:'', dataset:{}, style:{setProperty(){}},
  append(...children){this.children.push(...children);}, setAttribute(){}, classList:{add(){},toggle(){}}, add(){}});
  Option = function(label,value){this.label=label;this.value=value;};
  for (const id of ['hop','groups','service-nodes','sink-nodes','placements']) {
    $(id).children=[]; $(id).append=(...children)=>$(id).children.push(...children);
  }
  $('source-name').value='Source';
  const m=path=>({package:'p',version:'1',path});
  const tok={name:'Tok',role:'unary',ports:[{port:'Req',head:m('p::Prompt')},{port:'Resp',head:m('p::Chunk')}]};
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',head:m('p::Prompt')},{port:'Item',head:m('p::Chunk')}]};
  state.entries=[{package:'p',version:'1',nodes:[tok,engine],messages:[{name:'Prompt',path:'p::Prompt'},{name:'Chunk',path:'p::Chunk'}]}];
  $('source-request').value='p@1 p::Prompt';$('source-response').value='p@1 p::Chunk';
  $('input-mode').value='single';state.hop.inner={entry:'p@1',node:engine};
  addService('p@1',tok);
  placeService('Tok','one',true);placeService('Tok','two',true);
  placeService('Tok','s1',false);
  state.mode='segments';
  render(false);`);
const placement = () => JSON.parse(run('JSON.stringify(draft().segments)'));
assert.equal(run('graphComplete()'),true);
const before = {s1:['Source','Engine'],one:['Tok'],two:['Tok']};
assert.deepEqual(placement(),before);
run(`const rename=$('groups').children[1].children[0];
  assertRename=rename;
  rename.value='two';rename.oninput();`);
assert.deepEqual(placement(),before,'an existing segment name must be refused');
assert.equal(run('assertRename.value'),'one');
"#,
    );
}

#[test]
fn a_removed_unary_node_leaves_no_call_in_a_disabled_endpoint() {
    page(
        r#"
const run=editor();
run(`const m=path=>({package:'p',version:'1',path});
  const req={head:m('Prompt'),item:null},reply=m('Chunk');
  const extract={entry:'p@1',node:{name:'ExtractChat',role:'forward',ports:[{port:'In',...req},{port:'Out',...req}]},calls:['Tok']};
  const sink={entry:'p@1',node:{name:'Echo',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]}};
  const tok={name:'Tok',role:'unary',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  $('source-name').value='Source';
  state.frontend={entry:'p@1',node:{name:'Api',config:[{name:'chat',default:true},{name:'responses',default:true}],entries:[
    {name:'chat',enabled:'chat',request:req,response:{head:reply}},
    {name:'responses',enabled:'responses',request:req,response:{head:reply}}
  ]},arms:{chat:{...newHop(),forward:[extract]},responses:newHop()}};
  state.entries=[{package:'p',version:'1',nodes:[tok,extract.node,sink.node,state.frontend.node]}];
  connectSink(state.frontend.arms.chat,req,reply,sink);
  const shared=[...instances().keys()].find(n=>n.node.role==='sink');
  connectSink(state.frontend.arms.responses,req,reply,shared);
  addService('p@1',tok);`);
assert.equal(run('graphComplete()'),true);
assert.equal(run('draft().nodes.find(n=>n.name==="ExtractChat").calls.join()'),'Tok');
run('state.frontend.config={chat:false};');
assert.equal(run('graphComplete()'),true);
assert.equal(run('draft().nodes.some(n=>n.name==="ExtractChat")'),false);
run("removeService('Tok');state.frontend.config.chat=true;");
assert.equal(run('state.services.length'),0);
assert.equal(run('graphComplete()'),true);
assert.deepEqual(JSON.parse(run('JSON.stringify(draft().nodes.find(n=>n.name==="ExtractChat").calls)')),[],
  're-enabling an endpoint must not restore a call to a removed unary node');
"#,
    );
}

#[test]
fn a_catalogue_refresh_preserves_a_source_frontends_calls() {
    page(
        r#"
const run=editor();
run(`Option=function(){};$('source-frontend').add=()=>{};
  $('source-name').value='Source';
  const front={name:'Frontend',role:'source',ports:[]};
  const tok={name:'Tok',role:'unary',ports:[]};
  state.entries=[{package:'p',version:'1',source:'local',nodes:[front,tok]}];
  state.frontend={...nodes().find(n=>n.node.role==='source'),calls:['Tok']};
  addService('p@1',tok);`);
assert.equal(run('draft().nodes.find(n=>n.name==="Source").calls.join()'),'Tok');
run('fillFrontends();');
assert.equal(run('state.frontend.node.name'),'Frontend');
assert.equal(run('state.services[0].instanceName'),'Tok');
assert.deepEqual(JSON.parse(run('JSON.stringify(draft().nodes.find(n=>n.name==="Source").calls)')),['Tok'],
  'adding or refreshing a catalogue crate must preserve the source calls');
"#,
    );
}

#[test]
fn an_identity_cannot_take_a_unary_nodes_name() {
    page(
        r#"
const run = editor();
run(`document.createElement = tag => ({tag, children:[], dataset:{},
  append(...children){this.children.push(...children);}, setAttribute(){}});
  Option = function(label,value){this.label=label;this.value=value;};
  for (const id of ['identity-request','identity-header','identity-response']) $(id).add=()=>{};
  $('identity-role').append=()=>{};
  $('identity-form').querySelector=()=>({});
  $('identity-dialog').showModal=()=>{};$('identity-dialog').close=()=>{};
  let validity='';
  $('identity-name').setCustomValidity=message=>{validity=message;};
  $('identity-name').reportValidity=()=>{};
  render=()=>{};
  $('source-name').value='Source';
  const m=path=>({package:'p',version:'1',source:'local',path});
  const req={head:m('p::Prompt'),item:null};
  const reply=m('p::Chunk');
  const tok={name:'Tok',role:'unary',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  state.entries=[{package:'p',version:'1',source:'local',nodes:[tok,engine],messages:[
    {name:'Prompt',path:'p::Prompt',public:true},{name:'Chunk',path:'p::Chunk',public:true}]}];
  $('source-request').value=key(req.head);$('source-response').value=key(reply);
  $('input-mode').value='single';state.hop.inner={entry:'p@1',node:engine};
  addService('p@1',tok);
  const addIdentity=name=>{
    openIdentity(identityNode('forward',req),instance=>state.hop.forward.push(instance));
    $('identity-name').value=name;$('identity-form').onsubmit({preventDefault(){}});
  };
  addIdentity('Pass');`);
assert.equal(run('graphComplete()'),true);
assert.equal(run('state.hop.forward.length'),1,'a distinct identity name is accepted');
run("addIdentity('Tok');");
assert.equal(run('state.hop.forward.length'),1,'a unary node reserves its name before an identity is added');
assert.equal(run('validity'),'Choose a unique node name.');
"#,
    );
}

#[test]
fn a_local_catalogue_refresh_updates_unary_instances() {
    page(
        r#"
(async () => {
const run = editor();
run(`document.createElement = tag => ({tag, children:[], dataset:{}, style:{setProperty(){}},
  append(...children){this.children.push(...children);}, setAttribute(){}, classList:{add(){},toggle(){}}, add(){}});
  Option = function(label,value){this.label=label;this.value=value;};
  for (const id of ['hop','groups','service-nodes','sink-nodes']) $(id).append=()=>{};
  for (const id of ['source-request','source-header','source-response','source-frontend']) $(id).add=()=>{};
  renderCatalog=()=>{};
  $('name').value='composed';$('source-name').value='Source';
  const m=path=>({package:'proto',version:'1',source:'registry',path});
  const req={head:m('proto::Prompt'),item:null},reply=m('proto::Chunk');
  const tok={name:'Tok',role:'unary',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const source='path+file:///nodes';
  const catalogue=version=>[
    {package:'proto',version:'1',source:'registry',nodes:[],messages:[
      {name:'Prompt',path:'proto::Prompt',public:true},{name:'Chunk',path:'proto::Chunk',public:true}]},
    {package:'p',version,source,nodes:[engine,tok],messages:[]}
  ];
  state.entries=catalogue('1');
  $('source-request').value=key(req.head);$('source-response').value=key(reply);
  $('input-mode').value='single';
  state.hop.inner={...nodes().find(n=>n.node.role==='sink'),calls:['Tok']};
  addService('p@1',tok);placeService('Tok','worker',true);
  render(false);clearTimeout(state.timer);
  fetch=async()=>({ok:true,json:async()=>({entries:catalogue('2')})});`);
assert.equal(run('graphComplete()'),true);
const before=JSON.parse(run('JSON.stringify(draft())'));
assert.equal(before.nodes.find(n=>n.name==='Tok').entry,'p@1');
await run('load();');
run('clearTimeout(state.timer);');
const after=JSON.parse(run('JSON.stringify(draft())'));
assert.equal(after.nodes.find(n=>n.name==='Engine').entry,'p@2','pipeline nodes follow the local package replacement');
assert.deepEqual(after.nodes.find(n=>n.name==='Engine').calls,['Tok']);
assert.deepEqual(after.segments,before.segments,'refresh keeps every unary placement');
assert.equal(after.nodes.find(n=>n.name==='Tok').entry,'p@2','unary nodes must follow the same catalogue replacement');
})().catch(error=>{console.error(error);process.exitCode=1;});
"#,
    );
}

#[test]
fn undo_after_a_catalogue_refresh_keeps_unary_instances_current() {
    page(
        r#"
(async () => {
const run = editor();
run(`document.createElement = tag => ({tag, children:[], dataset:{}, style:{setProperty(){}},
  append(...children){this.children.push(...children);}, setAttribute(){}, classList:{add(){},toggle(){}}, add(){}});
  Option = function(label,value){this.label=label;this.value=value;};
  for (const id of ['hop','groups','service-nodes','sink-nodes']) $(id).append=()=>{};
  for (const id of ['source-request','source-header','source-response','source-frontend']) $(id).add=()=>{};
  renderCatalog=()=>{};
  $('name').value='composed';$('source-name').value='Source';
  const m=path=>({package:'proto',version:'1',source:'registry',path});
  const req={head:m('proto::Prompt'),item:null},reply=m('proto::Chunk');
  const tok={name:'Tok',role:'unary',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const source='path+file:///nodes';
  const catalogue=version=>[
    {package:'proto',version:'1',source:'registry',nodes:[],messages:[
      {name:'Prompt',path:'proto::Prompt',public:true},{name:'Chunk',path:'proto::Chunk',public:true}]},
    {package:'p',version,source,nodes:[engine,tok],messages:[]}
  ];
  state.entries=catalogue('1');
  $('source-request').value=key(req.head);$('source-response').value=key(reply);
  $('input-mode').value='single';
  state.hop.inner={...nodes().find(n=>n.node.role==='sink'),calls:['Tok']};
  addService('p@1',tok);placeService('Tok','worker',true);
  render();clearTimeout(state.timer);
  fetch=async()=>({ok:true,json:async()=>({entries:catalogue('2')})});`);
const draft = () => JSON.parse(run('JSON.stringify(draft())'));
const before = draft();
// One edit before the reload, so that undo lands on a snapshot taken
// while p@1 was current. A reload changes nothing a snapshot holds, so it
// records none of its own.
run("placeService('Tok','extra',true);render();clearTimeout(state.timer);");
assert.equal(run('historyIndex'),1);
await run('load();');
run('clearTimeout(state.timer);');
assert.equal(draft().nodes.find(n=>n.name==='Tok').entry,'p@2','refresh updates the unary instance');
run('undoEdit();clearTimeout(state.timer);');
const after = draft();
assert.equal(after.nodes.find(n=>n.name==='Tok').entry,'p@2','undo must not restore a removed catalogue version');
assert.equal(run('graphComplete()'),true);
assert.deepEqual(after.segments,before.segments,'undo retains both unary placements');
assert.deepEqual(after.nodes.find(n=>n.name==='Engine').calls,['Tok']);
assert.equal(after.nodes.find(n=>n.name==='Engine').entry,'p@2','hop nodes still use the current catalogue');
})().catch(error=>{console.error(error);process.exitCode=1;});
"#,
    );
}

#[test]
fn a_unary_node_cannot_take_a_hosted_entry_roots_name() {
    page(
        r#"
const run = editor();
run(`$('source-name').value='Source';
  const m=path=>({package:'p',version:'1',path});
  const req={head:m('Prompt'),item:null},reply=m('Chunk');
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const unary={name:'Source_entry_responses',role:'unary',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  const frontend={name:'Api',role:'source',ports:[],
    config:[{name:'chat',default:true},{name:'responses',default:true}],
    entries:['chat','responses'].map(name=>({name,enabled:name,request:req,response:{head:reply}}))};
  state.entries=[{package:'p',version:'1',nodes:[frontend,engine,unary]}];
  state.frontend={...nodes().find(n=>n.node.role==='source'),arms:{chat:newHop(),responses:newHop()}};
  connectSink(state.frontend.arms.chat,req,reply,nodes().find(n=>n.node.role==='sink'));
  const sink=[...instances().keys()].find(n=>n.node.role==='sink');
  connectSink(state.frontend.arms.responses,req,reply,sink);`);
assert.equal(run('graphComplete()'),true);
assert.equal(run('deployedRoots()[1].root'),'Source_entry_responses');
run("addService('p@1',unary);");
const output=JSON.parse(run('JSON.stringify(draft())'));
assert.equal(output.pipelines[0].source.name,'Source_entry_responses');
assert.equal(output.nodes.filter(n=>n.node==='Source_entry_responses').length,1);
assert.notEqual(output.nodes.find(n=>n.node==='Source_entry_responses').name,
  output.pipelines[0].source.name,'a unary node must not redeclare a hosted entry root');
"#,
    );
}

#[test]
fn a_catalogue_reload_keeps_the_selected_unary_package() {
    page(
        r#"
(async () => {
const run = editor();
run(`document.createElement = tag => ({tag, children:[], dataset:{}, style:{setProperty(){}},
  append(...children){this.children.push(...children);}, setAttribute(){}, classList:{add(){},toggle(){}}, add(){}});
  Option = function(label,value){this.label=label;this.value=value;};
  for (const id of ['hop','groups','service-nodes','sink-nodes']) $(id).append=()=>{};
  for (const id of ['source-request','source-header','source-response','source-frontend']) $(id).add=()=>{};
  renderCatalog=()=>{};
  $('name').value='composed';$('source-name').value='Source';
  const source='registry+https://github.com/rust-lang/crates.io-index';
  const m=path=>({package:'proto',version:'1',source,path});
  const req={head:m('proto::Prompt'),item:null},reply=m('proto::Chunk');
  const tok={name:'Tok',role:'unary',type:'primary::Tok',ports:[{port:'Req',...req},{port:'Resp',head:reply}]};
  const vendor={...tok,type:'vendor::Tok'};
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const catalogue=[
    {package:'primary',version:'1',source,nodes:[engine,tok],messages:[]},
    {package:'proto',version:'1',source,nodes:[],messages:[
      {name:'Prompt',path:'proto::Prompt',public:true},{name:'Chunk',path:'proto::Chunk',public:true}]},
    {package:'vendor',version:'1',source,nodes:[vendor],messages:[]}
  ];
  state.entries=catalogue;
  $('source-request').value=key(req.head);$('source-response').value=key(reply);
  $('input-mode').value='single';
  state.hop.inner={...nodes().find(n=>n.node.role==='sink'),calls:['Tok']};
  addService('vendor@1',vendor);placeService('Tok','worker',true);
  render(false);clearTimeout(state.timer);
  fetch=async()=>({ok:true,json:async()=>({entries:catalogue})});`);
const draft = () => JSON.parse(run('JSON.stringify(draft())'));
const before=draft();
assert.equal(run('graphComplete()'),true);
assert.equal(before.nodes.find(n=>n.name==='Tok').entry,'vendor@1');
await run('load();');
run('clearTimeout(state.timer);');
const after=draft();
assert.deepEqual(after.segments,before.segments);
assert.deepEqual(after.nodes.find(n=>n.name==='Engine').calls,['Tok']);
assert.equal(after.nodes.find(n=>n.name==='Engine').entry,'primary@1');
assert.equal(after.nodes.find(n=>n.name==='Tok').entry,'vendor@1',
  'an unchanged catalogue must not replace a unary node with a different package');
})().catch(error=>{console.error(error);process.exitCode=1;});
"#,
    );
}

#[test]
fn undo_while_versions_disagree_on_role_keeps_the_nested_hop() {
    page(
        r#"
(async () => {
const run = editor();
run(`document.createElement = tag => ({tag, children:[], dataset:{}, style:{setProperty(){}},
  append(...children){this.children.push(...children);}, setAttribute(){},
  setCustomValidity(){}, reportValidity(){}, classList:{add(){},toggle(){}}, add(){}, querySelector(){return {};}});
  Option = function(){}; setTimeout=()=>0; clearTimeout=()=>{};
  const element=document.getElementById;
  document.getElementById=id=>{const e=element(id);e.append ||=()=>{};e.add ||=()=>{};return e;};
  renderCatalog=()=>{};
  const source='registry+https://github.com/rust-lang/crates.io-index';
  const m=path=>({package:'proto',version:'1.0.0',source,path});
  const req={head:m('proto::Prompt'),item:null}, reply=m('proto::Chunk');
  const operator={name:'Op',role:'operator',ports:[
    {port:'UpReq',...req},{port:'UpItem',head:reply},{port:'DownReq',...req},{port:'DownItem',head:reply}]};
  const sink={name:'Op',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const engine={name:'Engine',role:'sink',ports:[{port:'Req',...req},{port:'Item',head:reply}]};
  const proto={package:'proto',version:'1.0.0',source,nodes:[],messages:[
    {name:'Prompt',path:'proto::Prompt',public:true},{name:'Chunk',path:'proto::Chunk',public:true}]};
  const p=(version,node)=>({package:'p',version,source,nodes:[node,engine],messages:[]});
  let catalogue=[p('2.0.0',operator),proto];
  fetch=async()=>({ok:true,json:async()=>({entries:catalogue})});
  state.entries=catalogue;
  $('source-name').value='Source'; $('name').value='composed';
  $('source-request').value=key(req.head); $('source-response').value=key(reply); $('input-mode').value='single';
  state.hop.inner=copyNode(nodes().find(n=>n.node.name==='Op'));
  state.hop.nested={...newHop(),inner:copyNode(nodes().find(n=>n.node.name==='Engine'))};
  render();
  addVersion=()=>{catalogue=[p('1.0.0',sink),p('2.0.0',operator),proto];};
  removeVersion=()=>{catalogue=[p('2.0.0',operator),proto];};`);
assert.equal(run('graphComplete()'),true);
run("$('name').value='renamed';render();addVersion();");
await run('load()');
assert.equal(run('state.hop.inner.node.role'),'operator','the live node keeps its role');
assert.equal(run('state.hop.nested?.inner?.instanceName'),'Engine');
run('undoEdit();');
assert.ok(run('draftProblems().length')>0,'the ambiguous reference is reported');
run('removeVersion();');
await run('load()');
assert.equal(run('state.hop.inner.node.role'),'operator');
assert.equal(run('JSON.stringify(draftProblems())'),'[]');
assert.equal(run('state.hop.nested?.inner?.instanceName'),'Engine',
  'undo while roles differ must not delete the nested engine');
assert.equal(run('graphComplete()'),true);
})().catch(error=>{console.error(error);process.exitCode=1;});
"#,
    );
}
