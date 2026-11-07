/* Explicit real-device regression test. Opens an existing news item, presses Home,
   relaunches and force-stops/restarts this app only. Does not collect or reset data.
   Normal cutoff-based automatic cleanup still applies on app startup/resume.
   ANDROID_SERIAL=<serial> node tests/android-resume.cjs (Node 22+ and adb). */
'use strict';
const {execFileSync}=require('node:child_process');
const assert=require('node:assert/strict');
const serial=process.env.ANDROID_SERIAL;
if(!serial) throw Error('Specify ANDROID_SERIAL to select the test device.');
const packageName='kr.dailyreport.app';
const port=Number(process.env.CDP_PORT || 9224);
function adb(...args){return execFileSync('adb',['-s',serial,...args],{encoding:'utf8',timeout:30000}).trim();}
function evaluate(tab,expression){
  return new Promise((resolve,reject)=>{
    const ws=new WebSocket(tab.webSocketDebuggerUrl);
    const timer=setTimeout(()=>{ws.close();reject(Error('App WebView evaluation timed out'));},15000);
    ws.onopen=()=>ws.send(JSON.stringify({id:1,method:'Runtime.evaluate',params:{expression,returnByValue:true,awaitPromise:true}}));
    ws.onerror=()=>{clearTimeout(timer);ws.close();reject(Error('App WebView connection failed'));};
    ws.onmessage=event=>{
      const data=JSON.parse(event.data); if(data.id!==1)return;
      clearTimeout(timer);ws.close();
      if(data.result?.exceptionDetails)reject(Error(data.result.exceptionDetails.text));
      else resolve(data.result?.result?.value);
    };
  });
}
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function connect(){
  const pid=adb('shell','pidof',packageName).split(/\s+/)[0];assert.ok(pid,'App process is not running');
  adb('forward',`tcp:${port}`,`localabstract:webview_devtools_remote_${pid}`);
  const deadline=Date.now()+20000;
  while(Date.now()<deadline){
    try{
      const tabs=await(await fetch(`http://127.0.0.1:${port}/json/list`,{signal:AbortSignal.timeout(2000)})).json();
      const appTabs=tabs.filter(tab=>tab.url==='https://appassets.androidplatform.net/assets/mobile/index.html');
      const active=appTabs.find(tab=>JSON.parse(tab.description || '{}').visible);
      if(active)return {tab:active,count:appTabs.length};
    }catch(_){}
    await delay(200);
  }
  throw Error('No visible app WebView');
}
async function waitFor(tab,expression){
  const deadline=Date.now()+25000;
  while(Date.now()<deadline){if(await evaluate(tab,expression))return;await delay(100);}
  throw Error('App condition timed out: '+expression);
}
function launch(){adb('shell','am','start','-W','-a','android.intent.action.MAIN','-c','android.intent.category.LAUNCHER','-n',packageName+'/.MainActivity');}
(async()=>{
  try{
    launch();let {tab,count}=await connect();assert.equal(count,1,'Launcher must reuse one app screen');
    await waitFor(tab,"document.getElementById('loading').hidden && !!window.MobileApp");
    await evaluate(tab,`(()=>{
      if(!document.getElementById('reader-screen').hidden)window.MobileApp.back();
      document.querySelector('[data-tab="news"]').click();
      document.querySelector('[data-read="all"]').click();
      const search=document.getElementById('search');search.value='';search.dispatchEvent(new Event('input'));
      document.getElementById('range-filter').value='all';document.getElementById('range-filter').dispatchEvent(new Event('change'));
      const card=document.querySelector('.item-card.is-read') || document.querySelector('.item-card');
      if(!card)throw Error('Collect news before running this test.');
      card.querySelector('.card-title').click();
    })()`);
    await waitFor(tab,"!document.getElementById('reader-screen').hidden && !document.getElementById('reader-status').textContent.includes('불러오는 중')");
    await waitFor(tab,"[...document.querySelectorAll('#reader-body img')].every(image=>image.complete)");
    const expected=await evaluate(tab,`(()=>{
      window.scrollTo(0,Math.min(480,Math.max(0,document.documentElement.scrollHeight-innerHeight)));
      window.MobileApp.suspend();
      const ui=JSON.parse(localStorage.getItem('dailyreport.ui'));
      return {id:ui.selectedId,title:document.getElementById('reader-title').textContent,scroll:ui.readerScroll,images:(ui.readerImages || []).length};
    })()`);
    assert.ok(expected.id,'Selected news must be persisted');
    adb('shell','input','keyevent','KEYCODE_HOME');launch();
    ({tab,count}=await connect());assert.equal(count,1);
    await waitFor(tab,"!document.getElementById('reader-screen').hidden");
    assert.equal(await evaluate(tab,"document.getElementById('reader-title').textContent"),expected.title);
    assert.ok(Math.abs(await evaluate(tab,'window.scrollY')-expected.scroll)<3,'Scroll must survive Home and launcher');
    console.log('PASS: Home → launcher preserves news and scroll; one Activity/WebView.');
    adb('shell','input','keyevent','KEYCODE_HOME');
    adb('shell','am','force-stop',packageName);launch();
    ({tab,count}=await connect());assert.equal(count,1);
    await waitFor(tab,"!document.getElementById('reader-screen').hidden && !document.getElementById('reader-status').textContent.includes('불러오는 중')");
    assert.equal(await evaluate(tab,"document.getElementById('reader-title').textContent"),expected.title);
    await waitFor(tab,"[...document.querySelectorAll('#reader-body img')].every(image=>image.complete)");
    await waitFor(tab,`Math.abs(window.scrollY-${expected.scroll})<3`);
    console.log('PASS: process restart restores news and scroll ('+expected.scroll+'px, '+expected.images+' image layouts); saved database retained.');
  }finally{try{adb('forward','--remove',`tcp:${port}`);}catch(_){} }
})().catch(error=>{console.error(error);process.exitCode=1;});
