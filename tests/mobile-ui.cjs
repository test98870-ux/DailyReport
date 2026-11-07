/* Run with PLAYWRIGHT_MODULE=/path/to/playwright node tests/mobile-ui.cjs. Uses mocked bridge data, never live publishers. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '../android/app/src/main/assets/mobile');
const sources = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../sources.json')));
const output = path.resolve(__dirname, '../artifacts/mobile-preview');
fs.mkdirSync(output, {recursive:true});
const server = http.createServer((req, res) => {
  const filename = req.url === '/' ? 'index.html' : req.url.slice(1);
  if (!['index.html','app.js','style.css'].includes(filename)) { res.writeHead(404); res.end(); return; }
  res.setHeader('Content-Type', filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html');
  res.end(fs.readFileSync(path.join(root,filename)));
});
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = await chromium.launch({executablePath:process.env.CHROME_PATH || '/opt/google/chrome/chrome', headless:true, args:['--no-sandbox']});
  try {
    const page = await browser.newPage({viewport:{width:412,height:915},deviceScaleFactor:1,isMobile:true,hasTouch:true});
    let imageDelay = 100;
    await page.route('https://images.example.test/fixture.svg', async route => {
      await new Promise(resolve => setTimeout(resolve, imageDelay));
      await route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="600"><rect width="1000" height="600" fill="#eaf0f8"/></svg>'});
    });
    const errors = []; page.on('pageerror', error => errors.push(error.message));
    await page.addInitScript(({sources}) => {
      const labels = ['삼성전자, 반도체 업황 개선과 실적 회복에 주목','이번 주 시장을 움직인 주요 경제 소식','기업 공시, 오늘 확인할 주요 내용','새로운 기술과 산업의 변화, 알아둘 핵심 동향'];
      const kinds = ['report','news','disclosure'];
      const items = Array.from({length:125}, (_,i) => ({id:i+1,title:labels[i%labels.length] + (i > 3 ? ' ' + (i+1) : ''),url:'https://example.com/'+i,
        source:['네이버 금융 종목분석','다음 뉴스 경제','OpenDART 오늘 공시'][i%3],published:new Date(Date.now()-i*60000).toISOString(),kind:kinds[i%3],
        category:['기업','경제','DART'][i%3],read:i%5===0,starred:i%9===0}));
      items[4].title = '<img src=x onerror="window.__xss=1"> 안전하게 표시할 제목';
      const cleanup={cutoff:'2026-11-07T16:00:00+09:00',blockedCount:12,lastRun:{removed:12,finishedAt:new Date().toISOString()}};
      const snapshot = {items,lastCollection:new Date().toISOString(),lastResult:null,days:1,cleanup};
      const config = {days:1,dartConfigured:false,cleanup,sources:sources.map(source=>({name:source.name,type:source.source_type,enabled:true}))};
      window.__commands = [];
      window.__empty = () => { snapshot.items = []; };
      window.DailyReport = {postMessage(raw) {
        const request=JSON.parse(raw); window.__commands.push(request.command);
        let data={};
        if(request.command==='load') data=snapshot;
        if(request.command==='settings') data=config;
        if(request.command==='saveSettings') {config.days=request.args.days; data=config;}
        if(request.command==='article') data={id:request.args.itemId,blocks:[...(request.args.itemId===2?[{type:'image',url:'https://images.example.test/fixture.svg',alt:'복원 검증용 사진'}]:[]),{type:'text',text:'기사 본문을 읽기 좋은 크기와 줄 간격으로 표시합니다. 휴대폰에 저장한 본문은 인터넷 없이도 다시 읽을 수 있습니다.'},{type:'text',text:request.args.itemId===5 ? '<script>window.__xss=1</script> 이 문장은 실행되지 않고 글자로 표시됩니다.' : '관심 있는 소식을 보관하고 나중에 다시 읽어보세요. 원문과 리포트 PDF는 아래 원문 보기 버튼으로 확인할 수 있습니다.'},...Array.from({length:20},(_,index)=>({type:'text',text:'읽던 위치 복원 검증을 위한 본문 문단 '+index+'. 휴대폰 홈으로 나갔다 돌아와도 같은 기사와 위치를 이어서 읽습니다.'}))],cached:true};
        if(request.command==='mark') {const item=items.find(item=>item.id===request.args.itemId);item[request.args.field]=request.args.value;data={};}
        if(request.command==='collect') {snapshot.lastResult={cancelled:false,stored:125,fetched:125,finishedAt:new Date().toISOString(),sources:[{name:'다음 뉴스 경제',status:'ok',fetched:125}]};data=snapshot;}
        setTimeout(()=>window.MobileApp.receive({type:'result',id:request.id,data}),10);
      }};
    },{sources});
    await page.goto('http://127.0.0.1:'+server.address().port);
    await page.waitForSelector('.item-card');
    assert.equal(await page.locator('.item-card').count(),40);
    assert.deepEqual(await page.evaluate(()=>window.__commands),['load']);
    await page.screenshot({path:path.join(output,'01-home-412.png')});
    for(const size of [{width:320,height:740},{width:360,height:800},{width:412,height:915},{width:600,height:960},{width:896,height:412}]) {
      await page.setViewportSize(size);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth <= window.innerWidth),true,`Horizontal overflow at ${size.width}`);
      const nav=await page.locator('#bottom-nav').boundingBox(); assert.ok(nav.y+nav.height<=size.height+1);
    }
    await page.setViewportSize({width:412,height:915});
    await page.locator('#load-more').click(); assert.equal(await page.locator('.item-card').count(),80);
    await page.locator('[data-tab="news"]').click();
    assert.equal(await page.locator('#page-title').innerText(),'뉴스');
    assert.equal(await page.locator('.item-card .kind-label').first().innerText(),'뉴스 · 경제');
    await page.locator('#search').fill('안전하게 표시');
    assert.equal(await page.locator('.item-card').count(),1);
    await page.locator('.card-title').click();
    await page.waitForFunction(()=>document.getElementById('reader-status').textContent.includes('저장된 본문'));
    assert.equal(await page.locator('#reader-title img').count(),0);
    assert.equal(await page.evaluate(()=>window.__xss),undefined);
    assert.ok((await page.locator('#reader-body').innerText()).includes('<script>'));
    await page.locator('#reader-back').click();
    await page.locator('#search').fill('');
    await page.locator('.card-title').first().click();
    await page.waitForFunction(()=>document.getElementById('reader-status').textContent.includes('저장된 본문'));
    await page.locator('#font-up').click();
    assert.equal(await page.locator('#reader-body').evaluate(node=>getComputedStyle(node).fontSize),'20px');
    await page.screenshot({path:path.join(output,'02-reader-412.png')});
    await page.waitForFunction(()=>[...document.querySelectorAll('#reader-body img')].every(image=>image.complete && image.naturalWidth>0));
    const readingTitle = await page.locator('#reader-title').innerText();
    await page.evaluate(()=>{window.scrollTo(0,600); window.MobileApp.suspend();});
    await page.waitForFunction(()=>JSON.parse(localStorage.getItem('dailyreport.ui')).readerScroll===600);
    imageDelay = 800;
    await page.reload();
    await page.waitForSelector('#reader-screen:not([hidden])');
    await page.waitForFunction(()=>Math.abs(window.scrollY-600)<2);
    assert.equal(await page.locator('#reader-title').innerText(),readingTitle);
    await page.waitForFunction(()=>[...document.querySelectorAll('#reader-body img')].every(image=>image.complete && image.naturalWidth>0));
    assert.ok(Math.abs(await page.evaluate(()=>window.scrollY)-600)<2,'Delayed image must not move restored reading position');
    assert.equal(await page.locator('#reader-body').evaluate(node=>getComputedStyle(node).fontSize),'20px');
    const loadCount = await page.evaluate(()=>window.__commands.filter(command=>command==='load').length);
    await page.evaluate(()=>window.MobileApp.receive({type:'resume',data:{}}));
    await page.waitForFunction(count=>window.__commands.filter(command=>command==='load').length>count,loadCount);
    assert.equal(await page.locator('#reader-screen').isVisible(),true);
    assert.ok(Math.abs(await page.evaluate(()=>window.scrollY)-600)<2);
    await page.locator('#reader-star').click();
    await page.waitForFunction(()=>document.getElementById('reader-star').getAttribute('aria-pressed')==='true');
    await page.locator('#reader-back').click();
    await page.locator('[data-tab="saved"]').click();
    assert.equal(await page.locator('#page-title').innerText(),'보관함');
    assert.ok(await page.locator('.item-card').count()>0);
    await page.locator('#settings-open').click();
    await page.waitForSelector('#settings-screen:not([hidden])');
    assert.equal(await page.locator('#source-options input').count(),sources.length);
    assert.ok((await page.locator('#cleanup-cutoff').innerText()).includes('16:00'));
    assert.ok((await page.locator('#cleanup-history').innerText()).includes('12건'));
    assert.ok((await page.locator('#settings-screen').innerText()).includes('이미 정리한 자료는 돌아오지 않습니다'));
    await page.evaluate(()=>{window.__beforeSettingsLoad=window.__commands.filter(c=>c==='load').length;});
    await page.screenshot({path:path.join(output,'03-settings-412.png')});
    await page.locator('#sources-toggle').click();
    await page.locator('#settings-save').click();
    assert.ok((await page.locator('#toast').innerText()).includes('하나 이상'));
    await page.locator('#sources-toggle').click();
    await page.locator('#collection-days').selectOption('3');
    await page.locator('#settings-save').click();
    await page.waitForSelector('#home-screen:not([hidden])');
    assert.ok(await page.evaluate(()=>window.__commands.filter(c=>c==='load').length>window.__beforeSettingsLoad));
    await page.locator('#collect').click();
    await page.waitForSelector('#result-open:not([hidden])');
    assert.equal(await page.evaluate(()=>window.__commands.filter(c=>c==='collect').length),1);
    await page.locator('#result-open').click();
    assert.ok((await page.locator('#source-results').innerText()).includes('125건'));
    await page.locator('#results-back').click();
    await page.locator('[data-tab="all"]').click();
    await page.evaluate(()=>{window.__empty(); window.MobileApp.receive({type:'resume',data:{}});});
    await page.waitForSelector('#empty:not([hidden])');
    await page.screenshot({path:path.join(output,'04-empty-412.png')});
    await page.evaluate(()=>{const ui=JSON.parse(localStorage.getItem('dailyreport.ui'));ui.screen='reader';ui.selectedId=999999;localStorage.setItem('dailyreport.ui',JSON.stringify(ui));});
    await page.reload();
    await page.waitForSelector('.item-card');
    assert.equal(await page.locator('#home-screen').isVisible(),true);
    assert.equal(await page.evaluate(()=>window.__commands.includes('collect')),false);
    assert.deepEqual(errors,[]);
    console.log('Mobile UI checks passed: 5 viewport sizes, pagination, search, safe rendering, reader, font size, bookmark, settings, manual collection, results, empty state, reader restoration after reload/resume, missing-item recovery.');
  } finally { await browser.close(); server.close(); }
})().catch(error=>{console.error(error);process.exitCode=1;server.close();});
