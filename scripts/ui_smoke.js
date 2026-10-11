/* In-process UI/API smoke test. Run: node scripts/ui_smoke.js
 * Uses the actual app.js and FastAPI API with a minimal DOM implementation.
 * This tests state and request logic; it does NOT verify browser rendering,
 * CSS layout, native keyboard behavior, or browser accessibility.
 * Requires Python dependencies from requirements-dev.txt; no Node dependencies.
 */
const fs = require('fs');
const vm = require('vm');
const {spawn} = require('child_process');
const {createInterface} = require('readline');
const assert = require('assert').strict;
const path=require('path');
const repository=path.resolve(__dirname,'..');
const root=path.join(repository,'fleet_relay','static');
class E {
 constructor(tag='div',id='') {this.tagName=tag;this.id=id;this.children=[];this.listeners={};this.attributes={};this.hidden=false;this.disabled=false;this.value='';this.checked=false;this._text='';this.className='';this.classList={add:(s)=>this.className+=' '+s,toggle:(s,on)=>{const set=new Set(this.className.split(' '));if(on)set.add(s);else set.delete(s);this.className=[...set].join(' ');}};}
 get textContent(){return this._text+this.children.map(c=>c.textContent||'').join('');}
 set textContent(v){this._text=String(v);this.children=[];}
 append(...c){this.children.push(...c)}
 replaceChildren(...c){this._text='';this.children=c}
 setAttribute(k,v){this.attributes[k]=String(v)}
 getAttribute(k){return this.attributes[k]||null}
 removeAttribute(k){delete this.attributes[k]}
 addEventListener(k,fn){(this.listeners[k]??=[]).push(fn)}
 querySelector(q){return this.children.find(x=>x.tagName===q)||null}
 async fire(type,event={}){for(const fn of this.listeners[type]||[])await fn({target:this,preventDefault(){},...event})}
 reportValidity(){const n=Number(elements['sample-interval'].value);return Number.isInteger(n)&&n>=5&&n<=3600}
 showModal(){this.open=true}
 close(){this.open=false}
 getBoundingClientRect(){return {left:0,top:0,right:100,bottom:100}}
}
const elements={};
const html=fs.readFileSync(root+'/index.html','utf8');
for(const match of html.matchAll(/<([a-z][a-z0-9]*)\b[^>]*\bid="([^"]+)"[^>]*>/gi))elements[match[2]]=new E(match[1],match[2]);
for(const id of ['refresh-button','ingest-button','dispatch-button','save-config'])elements[id].append(new E('span'));
const links=['overview','events','configuration'].map(x=>{const e=new E('a');e.setAttribute('href','#'+x);return e});
const document={getElementById:(id)=>elements[id]||null,createElement:t=>new E(t),createElementNS:(_,t)=>new E(t),createTextNode:text=>{const e=new E('#text');e.textContent=text;return e},querySelectorAll:()=>links};
const child=spawn(process.env.PYTHON || 'python',[path.join(__dirname,'ui_api_bridge.py')]);
let seq=0;const requests=new Map();const errors=[];const checks=[];
child.stderr.on('data',b=>{const s=b.toString();if(!s.includes('StarletteDeprecationWarning')&&!s.includes('from starlette.testclient'))errors.push(s)});
createInterface({input:child.stdout}).on('line',line=>{const r=JSON.parse(line);const task=requests.get(r.id);requests.delete(r.id);if(r.error)task.reject(new Error(r.error));else task.resolve({ok:r.status>=200&&r.status<300,status:r.status,json:async()=>r.body,headers:{get:k=>r.headers[k.toLowerCase()]||null}})});
let failReads=false, holdReads=false;const held=[];
function realFetch(path,opt={}){return new Promise((resolve,reject)=>{const id=++seq;requests.set(id,{resolve,reject});child.stdin.write(JSON.stringify({id,path,method:opt.method,headers:opt.headers,body:opt.body})+'\n')})}
async function fetch(path,opt={}) {if(failReads&&(!opt.method||opt.method==='GET'))throw new TypeError('Network disconnected');if(holdReads&&(!opt.method||opt.method==='GET'))await new Promise(r=>held.push(r));return realFetch(path,opt)}
const context={document,window:{addEventListener(){}},location:{hash:''},fetch,AbortController,setTimeout,clearTimeout,Intl,Date,Number,String,Map,Set,Error,TypeError,JSON,console,crypto:require('crypto').webcrypto};context.globalThis=context;
async function idle(){for(let i=0;i<250;i++){if(!elements['refresh-button'].disabled)return;await new Promise(r=>setTimeout(r,20));}throw Error('UI did not become idle: '+elements['connection-detail'].textContent);}
async function click(id){await elements[id].fire('click');await idle()}
async function check(name,fn){await fn();checks.push(name);console.log('PASS '+name)}
const text=id=>elements[id].textContent;
vm.runInNewContext(fs.readFileSync(root+'/app.js','utf8'),context,{filename:'app.js'});
(async()=>{
 await idle();
 await check('Empty initial tenant uses actual API zeros',async()=>{assert.equal(text('metric-accepted'),'0');assert.equal(text('metric-receipts'),'0');assert.equal(elements['events-state'].hidden,false);assert.equal(elements['config-version'].textContent,'v1');assert.equal(elements['config-fields'].disabled,false)});
 await check('Ingest accepts 3 events and enables replay',async()=>{await click('ingest-button');assert.equal(text('metric-accepted'),'3');assert.equal(text('metric-awaiting'),'3');assert.equal(elements['events-body'].children.length,3);assert.equal(elements['replay-button'].disabled,false)});
 await check('Replay uses same keys and preserves 3 event count',async()=>{await click('replay-button');assert.equal(text('metric-accepted'),'3');assert.match(text('toast-region'),/3 existing events reused/)});
 await check('First dispatch delivers normal and schedules both failures',async()=>{await click('dispatch-button');assert.equal(text('metric-receipts'),'1');assert.equal(text('metric-dead'),'0');assert.equal(text('metric-awaiting'),'2');assert.match(text('dispatch-result'),/3 processed.*1 delivered.*2 retried.*0 dead/)});
 await new Promise(r=>setTimeout(r,2100));
 await check('Retry recovers and durable receipts increment once',async()=>{await click('dispatch-button');assert.equal(text('metric-receipts'),'2');assert.equal(text('metric-awaiting'),'1');await click('dispatch-button');assert.equal(text('metric-receipts'),'2');assert.match(text('toast-region'),/No deliveries are ready/)});
 await new Promise(r=>setTimeout(r,4100));
 await check('Poison event becomes dead after three failed attempts',async()=>{await click('dispatch-button');assert.equal(text('metric-receipts'),'2');assert.equal(text('metric-dead'),'1');assert.equal(text('metric-awaiting'),'0')});
 await check('Event dialog opens with payload and closes',async()=>{const inspect=elements['events-body'].children[0].children[5].children[0];await inspect.fire('click');assert.equal(elements['event-dialog'].open,true);assert.match(text('event-detail'),/synthetic/);await elements['close-dialog'].fire('click');assert.equal(elements['event-dialog'].open,false)});
 await check('Configuration writes If-Match and increments version',async()=>{elements['sample-interval'].value='45';await elements['config-form'].fire('input');await elements['config-form'].fire('submit');await idle();assert.equal(text('config-version'),'v2');assert.equal(Number(elements['sample-interval'].value),45)});
 await check('Stale ETag conflict retains draft and blocks overwrite',async()=>{const r=await realFetch('/v1/config',{method:'PUT',headers:{Authorization:'Bearer fleet-demo-north','Content-Type':'application/json','If-Match':'"2"'},body:JSON.stringify({sample_interval_seconds:60,enabled:true,labels:{source:'qa'}})});assert.equal(r.status,200);elements['sample-interval'].value='50';await elements['config-form'].fire('input');await elements['config-form'].fire('submit');assert.equal(elements['config-conflict'].hidden,false);assert.equal(elements['save-config'].disabled,true);assert.equal(Number(elements['sample-interval'].value),50);await click('refresh-button');assert.equal(Number(elements['sample-interval'].value),50)});
 await check('Reload recovers stale configuration and preserves labels',async()=>{await click('reload-config');assert.equal(text('config-version'),'v3');assert.equal(Number(elements['sample-interval'].value),60);assert.equal(elements['config-conflict'].hidden,true);elements['sample-interval'].value='75';await elements['config-form'].fire('input');await elements['config-form'].fire('submit');await idle();const r=await realFetch('/v1/config',{headers:{Authorization:'Bearer fleet-demo-north'}});assert.equal((await r.json()).config.labels.source,'qa')});
 await check('Tenant switch clears all prior tenant data and replay state',async()=>{elements['tenant-select'].value='south';await elements['tenant-select'].fire('change');await idle();assert.equal(text('metric-accepted'),'0');assert.equal(text('metric-receipts'),'0');assert.equal(text('config-version'),'v1');assert.equal(elements['replay-button'].disabled,true);assert.match(text('events-state'),/Solstice Lab/)});
 await check('Tenant data remains isolated after south ingest',async()=>{await click('ingest-button');assert.equal(text('metric-accepted'),'3');elements['tenant-select'].value='north';await elements['tenant-select'].fire('change');await idle();assert.equal(text('metric-accepted'),'3');assert.equal(text('metric-receipts'),'2');assert.equal(text('config-version'),'v4')});
 await check('Cursor pagination and newest reset use real pages',async()=>{for(let n=0;n<3;n++)await click('ingest-button');assert.equal(text('metric-accepted'),'12');assert.equal(elements['events-body'].children.length,10);assert.equal(elements['next-page'].disabled,false);await click('next-page');assert.equal(elements['events-body'].children.length,2);assert.equal(elements['next-page'].disabled,true);assert.match(text('page-description'),/Page 2/);await click('reset-page');assert.equal(elements['events-body'].children.length,10);assert.match(text('page-description'),/Page 1/)});
 await check('Read error shows explicit state and refresh recovers',async()=>{failReads=true;await click('refresh-button');assert.equal(elements['connection-alert'].hidden,false);assert.match(text('events-state'),/unavailable/);assert.equal(text('metric-accepted'),'12');failReads=false;await click('retry-connection');assert.equal(elements['connection-alert'].hidden,true);assert.equal(elements['events-body'].children.length,10)});
 await check('Repeated clicks during loading do not submit extra batches',async()=>{holdReads=true;const p=elements['refresh-button'].fire('click');await new Promise(r=>setTimeout(r,5));await elements['ingest-button'].fire('click');holdReads=false;held.splice(0).forEach(r=>r());await p;await idle();assert.equal(text('metric-accepted'),'12')});
 assert.equal(errors.length,0,errors.join(''));
 fs.writeFileSync(path.join(repository,'artifacts','ui-qa-results.json'),JSON.stringify({passed:checks.length,checks,browser_verification:'Blocked: Chromium socket permission error and browser localhost connection refused. DOM harness uses real FastAPI TestClient, not a renderer.'},null,2));
 console.log('ALL '+checks.length+' IN-PROCESS UI/API CHECKS PASSED');
 child.stdin.end();await new Promise(resolve=>child.on('exit',resolve));process.exit(0);
})().catch(e=>{console.error(e);console.error(errors.join(''));child.kill();process.exit(1)});
