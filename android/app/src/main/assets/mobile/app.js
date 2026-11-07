'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const pending = new Map();
  let requestId = 0, toastTimer, scrollSaveTimer;
  const titles = {all: '모아보기', news: '뉴스', report: '리포트', disclosure: '공시', saved: '보관함'};
  const kinds = {news: '뉴스', report: '리포트', disclosure: '공시'};
  let savedUi = {};
  try { savedUi = JSON.parse(window.DailyReport?.restoreUiState?.() || localStorage.getItem('dailyreport.ui') || '{}'); } catch (_) {}
  if (!savedUi || typeof savedUi !== 'object' || Array.isArray(savedUi)) savedUi = {};
  const scrollValue = value => Number.isFinite(value) ? Math.max(0, Math.min(10000000, value)) : 0;
  let listScroll = scrollValue(savedUi.listScroll), readerScroll = scrollValue(savedUi.readerScroll);
  let readerImages = Array.isArray(savedUi.readerImages) ? savedUi.readerImages.filter(image => image
    && Number.isSafeInteger(image.index) && image.index >= 0 && Number.isFinite(image.width) && image.width > 0
    && Number.isFinite(image.height) && image.height > 0).slice(0, 64) : [];
  let restoringSession = true, pendingReaderScroll = null;
  const restoreReaderId = savedUi.screen === 'reader' && Number.isSafeInteger(savedUi.selectedId) ? savedUi.selectedId : null;
  const state = {
    items: [], tab: Object.hasOwn(titles, savedUi.tab) ? savedUi.tab : 'all',
    read: ['all', 'read', 'unread'].includes(savedUi.read) ? savedUi.read : 'all',
    range: ['all', '1', '3', '7'].includes(savedUi.range) ? savedUi.range : 'all',
    category: typeof savedUi.category === 'string' ? savedUi.category : '전체',
    query: typeof savedUi.query === 'string' ? savedUi.query : '',
    limit: Number.isSafeInteger(savedUi.limit) ? Math.max(40, Math.min(10000, savedUi.limit)) : 40,
    screen: 'home', selectedId: null,
    font: [16, 18, 20, 22, 24].includes(savedUi.font) ? savedUi.font : 18,
    collecting: false, cancelling: false, loaded: false, settings: null, lastResult: null
  };
  function persistUi() {
    if (!state.loaded || restoringSession) return;
    const session = JSON.stringify({tab: state.tab, read: state.read, range: state.range,
      font: state.font, category: state.category, query: state.query, limit: state.limit,
      screen: state.screen === 'reader' ? 'reader' : 'home', selectedId: state.selectedId,
      listScroll, readerScroll, readerImages});
    try { window.DailyReport?.saveUiState?.(session); } catch (_) {}
    try { localStorage.setItem('dailyreport.ui', session); } catch (_) {}
  }
  function capturePosition() {
    if (restoringSession) return;
    if (state.screen === 'home') listScroll = scrollValue(window.scrollY);
    if (state.screen === 'reader' && pendingReaderScroll === null) {
      readerScroll = scrollValue(window.scrollY);
      readerImages = Array.from($('reader-body').querySelectorAll('img')).map(image => ({
        index: Number(image.dataset.imageIndex), width: image.naturalWidth || image.width,
        height: image.naturalHeight || image.height
      })).filter(image => image.width > 0 && image.height > 0).slice(0, 64);
    }
  }
  function suspend() {
    capturePosition(); persistUi();
  }
  function icon(name) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('class', 'icon'); svg.setAttribute('aria-hidden', 'true');
    const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
    use.setAttribute('href', '#i-' + name); svg.append(use); return svg;
  }
  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function toast(text) {
    $('toast').textContent = text; $('toast').hidden = false;
    clearTimeout(toastTimer); toastTimer = setTimeout(() => { $('toast').hidden = true; }, 4200);
  }
  function request(command, args = {}) {
    return new Promise((resolve, reject) => {
      if (!window.DailyReport?.postMessage) { reject(new Error('Android 앱에서 실행해 주세요.')); return; }
      const id = ++requestId;
      const timeout = command === 'collect' ? null : setTimeout(() => {
        pending.delete(id); reject(new Error('응답이 지연되고 있습니다. 잠시 후 다시 시도해 주세요.'));
      }, 60000);
      pending.set(id, {resolve, reject, timeout});
      window.DailyReport.postMessage(JSON.stringify({id, command, args}));
    });
  }
  function dateText(value, full = false) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return new Intl.DateTimeFormat('ko-KR', {timeZone: 'Asia/Seoul', month: 'short', day: 'numeric',
      ...(full ? {hour: '2-digit', minute: '2-digit', hour12: false} : {})}).format(date);
  }
  function displayScreen(screen) {
    capturePosition();
    state.screen = screen;
    for (const name of ['home', 'reader', 'settings', 'results']) $(name + '-screen').hidden = name !== screen;
    window.scrollTo(0, screen === 'home' ? listScroll : 0);
    persistUi();
  }
  function applySnapshot(data) {
    state.items = data.items || [];
    state.lastResult = data.lastResult;
    state.loaded = true;
    $('loading').hidden = true;
    $('collection-time').textContent = data.lastCollection ? '마지막 수집 · ' + dateText(data.lastCollection, true)
      : '직접 모으고, 차분히 읽는 하루';
    $('stat-total').textContent = state.items.length.toLocaleString('ko-KR');
    $('stat-unread').textContent = state.items.filter(item => !item.read).length.toLocaleString('ko-KR');
    $('stat-starred').textContent = state.items.filter(item => item.starred).length.toLocaleString('ko-KR');
    showResultSummary(); render();
    updateCleanupInfo(data.cleanup);
    if (restoringSession) {
      if (restoreReaderId !== null && state.items.some(item => item.id === restoreReaderId)) {
        openReader(restoreReaderId, readerScroll, readerImages);
        restoringSession = false;
        persistUi();
      } else {
        restoringSession = false;
        if (restoreReaderId !== null) toast('읽던 자료가 정리되었거나 없어 목록을 표시합니다.');
        requestAnimationFrame(() => { window.scrollTo(0, listScroll); capturePosition(); persistUi(); });
      }
    } else if (state.screen === 'reader') {
      if (selected()) syncReaderActions();
      else {
        state.selectedId = null; $('reader-body').replaceChildren(); displayScreen('home');
        toast('읽던 자료가 정리되었거나 없어 목록을 표시합니다.');
      }
    }
  }
  async function load() {
    try { applySnapshot(await request('load')); }
    catch (error) {
      $('loading').hidden = true; $('empty').hidden = false;
      $('empty-title').textContent = '자료를 불러오지 못했습니다';
      $('empty-message').textContent = error.message;
      $('empty-action').textContent = '다시 시도'; $('empty-action').dataset.retry = 'true';
      $('list-count').textContent = '';
    }
  }
  function baseItems() {
    const cutoff = state.range === 'all' ? 0 : Date.now() - Number(state.range) * 86400000;
    const query = state.query.trim().toLocaleLowerCase('ko-KR');
    return state.items.filter(item => (state.tab === 'all' || (state.tab === 'saved' ? item.starred : item.kind === state.tab))
      && (state.read === 'all' || (state.read === 'read' ? item.read : !item.read))
      && (!cutoff || Date.parse(item.published) >= cutoff)
      && (!query || (item.title + ' ' + item.source).toLocaleLowerCase('ko-KR').includes(query)));
  }
  function render() {
    $('page-title').textContent = titles[state.tab];
    document.querySelectorAll('[data-tab]').forEach(button => {
      if (button.dataset.tab === state.tab) button.setAttribute('aria-current', 'page');
      else button.removeAttribute('aria-current');
    });
    document.querySelectorAll('[data-read]').forEach(button => button.setAttribute('aria-pressed', button.dataset.read === state.read));
    $('range-filter').value = state.range;
    const base = baseItems();
    const categories = [...new Set(base.map(item => item.category))];
    if (state.loaded && !categories.includes(state.category)) state.category = '전체';
    const categoryScroll = $('categories').scrollLeft;
    $('categories').replaceChildren();
    for (const category of ['전체', ...categories]) {
      const button = element('button', 'category-chip', category);
      button.setAttribute('aria-pressed', category === state.category);
      button.onclick = () => { state.category = category; state.limit = 40; render(); };
      $('categories').append(button);
    }
    $('categories').scrollLeft = categoryScroll;
    $('categories').hidden = categories.length < 2;
    const filtered = base.filter(item => state.category === '전체' || item.category === state.category);
    $('list-heading').textContent = state.tab === 'saved' ? '내가 보관한 자료' : state.query ? '검색 결과' : '최신 자료';
    $('list-count').textContent = filtered.length.toLocaleString('ko-KR') + '건';
    const cards = document.createDocumentFragment();
    for (const item of filtered.slice(0, state.limit)) cards.append(renderCard(item));
    $('item-list').replaceChildren(cards);
    $('load-more').hidden = filtered.length <= state.limit;
    $('load-more').textContent = '더 보기 · ' + Math.max(0, filtered.length - state.limit) + '건 남음';
    $('empty').hidden = filtered.length > 0 || !state.loaded;
    if (!filtered.length && state.loaded) {
      const initial = !state.items.length;
      $('empty-title').textContent = initial ? '첫 자료를 모아보세요' : state.tab === 'saved' && !state.query ? '다시 읽을 자료를 담아두세요' : '조건에 맞는 자료가 없습니다';
      $('empty-message').textContent = initial ? '뉴스, 리포트, 공시를 내 휴대폰에.\n지금 수집을 누르면 시작합니다.'
        : state.tab === 'saved' && !state.query ? '목록의 보관 버튼을 누르면\n이곳에서 모아볼 수 있습니다.' : '검색어 또는 필터를 바꿔보세요.';
      $('empty-action').hidden = !initial;
      $('empty-action').textContent = '첫 수집 시작'; delete $('empty-action').dataset.retry;
    }
    $('collect').disabled = state.collecting;
    $('empty-action').disabled = state.collecting;
    persistUi();
  }
  function renderCard(item) {
    const card = element('article', 'item-card' + (item.read ? ' is-read' : ''));
    card.dataset.itemId = item.id;
    const top = element('div', 'card-top');
    const badge = element('span', 'kind-label', kinds[item.kind] + ' · ' + item.category);
    badge.dataset.kind = item.kind; top.append(badge);
    if (!item.read) top.append(element('span', 'unread-dot'));
    const title = element('button', 'card-title', item.title); title.onclick = () => openReader(item.id);
    const meta = element('p', 'card-meta'); meta.append(element('span', '', item.source), element('time', '', dateText(item.published, true)));
    const actions = element('div', 'card-actions');
    const read = element('button', 'read'); read.setAttribute('aria-pressed', item.read);
    read.setAttribute('aria-label', (item.read ? '안 읽음으로 변경: ' : '읽음으로 표시: ') + item.title);
    read.append(icon('check'), element('span', '', item.read ? '읽음' : '읽음 표시'));
    read.onclick = () => changeMark(item, 'read', !item.read);
    const original = element('button', 'original'); original.append(element('span', '', '원문'), icon('external'));
    original.setAttribute('aria-label', '원문 열기: ' + item.title); original.onclick = () => openOriginal(item);
    const star = element('button', 'star'); star.setAttribute('aria-pressed', item.starred);
    star.setAttribute('aria-label', (item.starred ? '보관 해제: ' : '보관: ') + item.title);
    star.append(icon('bookmark')); star.onclick = () => changeMark(item, 'starred', !item.starred);
    actions.append(read, element('span', 'spacer'), original, star);
    card.append(top, title, meta, actions); return card;
  }
  async function changeMark(item, field, value) {
    try {
      await request('mark', {itemId: item.id, field, value});
      const current = state.items.find(row => row.id === item.id);
      if (current) current[field] = value;
      $('stat-unread').textContent = state.items.filter(row => !row.read).length.toLocaleString('ko-KR');
      $('stat-starred').textContent = state.items.filter(row => row.starred).length.toLocaleString('ko-KR');
      render(); syncReaderActions();
    } catch (error) { toast(error.message); }
  }
  function openOriginal(item) {
    if (!item?.url || !/^https?:\/\//i.test(item.url)) { toast('원문 주소가 없습니다.'); return; }
    request('external', {url: item.url}).catch(error => toast(error.message));
  }
  function selected() { return state.items.find(item => item.id === state.selectedId); }
  function syncReaderActions() {
    const item = selected(); if (!item) return;
    $('reader-star').setAttribute('aria-pressed', item.starred);
    $('reader-star').querySelector('span').textContent = item.starred ? '보관됨' : '보관';
    $('reader-read').setAttribute('aria-pressed', item.read);
    $('reader-read').querySelector('span').textContent = item.read ? '읽음' : '안 읽음';
  }
  function openReader(id, scroll = 0, images = []) {
    const item = state.items.find(row => row.id === id); if (!item) return;
    state.selectedId = id;
    readerScroll = scroll;
    readerImages = images;
    pendingReaderScroll = scroll;
    $('reader-title').textContent = item.title;
    $('reader-category').textContent = kinds[item.kind] + ' · ' + item.category;
    $('reader-category').dataset.kind = item.kind;
    $('reader-meta').textContent = item.source + ' · ' + dateText(item.published, true);
    $('reader-body').replaceChildren(); $('reader-retry').hidden = true;
    displayScreen('reader'); syncReaderActions(); setFont(state.font);
    if (!item.read) changeMark(item, 'read', true);
    loadArticle(id);
  }
  function restoreReaderPosition(id) {
    if (state.selectedId !== id || state.screen !== 'reader' || pendingReaderScroll === null) return;
    requestAnimationFrame(() => {
      if (state.selectedId !== id || state.screen !== 'reader' || pendingReaderScroll === null) return;
      window.scrollTo(0, pendingReaderScroll);
      pendingReaderScroll = null;
      capturePosition(); persistUi();
    });
  }
  async function loadArticle(id) {
    $('reader-status').textContent = '본문을 불러오는 중입니다…'; $('reader-retry').hidden = true;
    try {
      const data = await request('article', {itemId: id});
      if (state.selectedId !== id) return;
      $('reader-body').replaceChildren();
      let imageIndex = 0;
      for (const block of data.blocks || []) {
        if (block.type === 'image') {
          const index = imageIndex++;
          if (!/^https?:\/\//i.test(block.url || '')) continue;
          const image = document.createElement('img');
          image.dataset.imageIndex = String(index);
          const dimensions = readerImages.find(saved => saved.index === index);
          if (dimensions) {
            image.width = dimensions.width; image.height = dimensions.height;
            // width:auto otherwise ignores the HTML width hint until the image finishes loading.
            image.style.width = dimensions.width + 'px';
            image.style.aspectRatio = dimensions.width + ' / ' + dimensions.height;
          }
          image.src = block.url; image.alt = block.alt || '기사 사진';
          image.loading = 'lazy'; image.referrerPolicy = 'no-referrer';
          image.onerror = () => image.replaceWith(element('p', 'help', '사진을 불러오지 못했습니다.'));
          $('reader-body').append(image);
        } else if (block.type === 'text') $('reader-body').append(element('p', '', block.text));
      }
      $('reader-status').textContent = data.message || (data.cached ? '휴대폰에 저장된 본문입니다. 사진은 인터넷 연결이 필요할 수 있습니다.' : '본문이 휴대폰에 저장되었습니다.');
      restoreReaderPosition(id);
    } catch (error) {
      if (state.selectedId !== id) return;
      $('reader-status').textContent = error.message; $('reader-retry').hidden = false;
      restoreReaderPosition(id);
    }
  }
  function setFont(value) {
    state.font = Math.min(24, Math.max(16, value));
    document.documentElement.style.setProperty('--reader-size', state.font + 'px');
    $('font-down').disabled = state.font <= 16; $('font-up').disabled = state.font >= 24; persistUi();
  }
  async function collect() {
    if (state.collecting) return;
    state.collecting = true; state.cancelling = false;
    $('collection-progress').hidden = false; $('progress-title').textContent = '수집 준비 중';
    $('progress-detail').textContent = '선택한 소스에서 자료를 가져옵니다.';
    $('progress-bar').value = 0; $('cancel-collection').disabled = false;
    $('result-open').hidden = true; render();
    try {
      const data = await request('collect'); applySnapshot(data);
      const result = data.lastResult;
      toast(result?.cancelled ? '수집을 중지했습니다. 저장한 자료는 유지됩니다.' : '수집을 마쳤습니다. 소스별 결과를 확인해 보세요.');
    } catch (error) { toast(error.message); }
    finally {
      state.collecting = false; state.cancelling = false; $('collection-progress').hidden = true; render(); showResultSummary();
    }
  }
  function updateProgress(data) {
    $('progress-title').textContent = state.cancelling ? '수집을 중지하는 중' : '자료를 모으고 있어요';
    $('progress-detail').textContent = (data.source || '준비 중') + ' · ' + data.index + '/' + data.total + '개 소스 · ' + data.stored + '건 처리';
    $('progress-bar').value = data.total ? data.index / data.total * 100 : 0;
  }
  function showResultSummary() {
    const result = state.lastResult; $('result-open').hidden = !result || state.collecting;
    if (!result) return;
    const failures = (result.sources || []).filter(row => row.status === 'error').length;
    $('result-label').textContent = (result.cancelled ? '중지됨' : '수집 완료') + ' · ' + result.stored + '건 처리'
      + (failures ? ' · 실패 ' + failures + '개 소스' : '') + ' · 결과 보기';
  }
  function showResults() {
    const result = state.lastResult; if (!result) return;
    $('results-description').textContent = dateText(result.finishedAt, true) + ' · ' + (result.cancelled ? '중지된 수집' : '마지막 수집')
      + '\n' + result.fetched + '건 수집 · ' + result.stored + '건 저장 처리 (기존 자료 갱신 포함)';
    const fragment = document.createDocumentFragment();
    for (const row of result.sources || []) {
      const section = element('section', 'source-result'); const head = element('div');
      const badge = element('span', 'result-badge', row.status === 'ok' ? row.fetched + '건' : {empty: '항목 없음', skipped: '건너뜀', error: '실패'}[row.status] || row.status);
      badge.dataset.status = row.status; head.append(element('strong', '', row.name), badge); section.append(head);
      if (row.message) section.append(element('p', '', row.message)); fragment.append(section);
    }
    if (!(result.sources || []).length) fragment.append(element('p', 'help', '완료한 소스가 없습니다. 다음 수집에서 다시 시도하세요.'));
    $('source-results').replaceChildren(fragment); displayScreen('results');
  }
  async function openSettings() {
    if (state.collecting) { toast('수집을 마치거나 중지한 뒤 설정을 열어 주세요.'); return; }
    $('settings-open').disabled = true;
    try {
      const settings = await request('settings'); state.settings = settings;
      $('collection-days').value = String(settings.days); $('dart-key').value = ''; $('dart-clear').checked = false;
      updateCleanupInfo(settings.cleanup);
      $('dart-status').textContent = settings.dartConfigured ? '인증키가 저장되어 있습니다. 비워두면 기존 키를 유지합니다.' : '인증키 없이도 다른 소스는 수집할 수 있습니다.';
      const fragment = document.createDocumentFragment(); let previous = '';
      for (const source of settings.sources) {
        if (source.type !== previous) { fragment.append(element('p', 'source-group-label', source.type)); previous = source.type; }
        const label = element('label', 'check-row'); const check = document.createElement('input');
        check.type = 'checkbox'; check.checked = source.enabled; check.value = source.name;
        label.append(check, element('span', '', source.name)); fragment.append(label);
      }
      $('source-options').replaceChildren(fragment); updateSourceToggle(); displayScreen('settings');
    } catch (error) { toast(error.message); }
    finally { $('settings-open').disabled = false; }
  }
  function updateSourceToggle() {
    const checked = Array.from($('source-options').querySelectorAll('input')).every(input => input.checked);
    $('sources-toggle').textContent = checked ? '전체 해제' : '전체 선택';
  }
  function updateCleanupInfo(info) {
    $('cleanup-cutoff').textContent = info?.cutoff ? '현재 기준 · ' + dateText(info.cutoff, true) + ' 이전 발행 자료 정리' : '선택한 수집 시작 시각에 맞춰 정리합니다.';
    $('cleanup-history').textContent = '재등록 제외 · ' + Number(info?.blockedCount || 0).toLocaleString('ko-KR') + '건'
      + (info?.lastRun ? ' · 최근 ' + dateText(info.lastRun.finishedAt, true) + '에 ' + info.lastRun.removed + '건 정리' : '');
  }
  async function saveSettings() {
    const enabled = Array.from($('source-options').querySelectorAll('input:checked')).map(input => input.value);
    if (!enabled.length) { toast('수집할 소스를 하나 이상 선택해 주세요.'); return; }
    const key = $('dart-key').value.trim();
    if (key && !/^[a-fA-F0-9]{40}$/.test(key)) { toast('OpenDART 인증키는 40자리 영문·숫자(16진수)입니다.'); return; }
    $('settings-save').disabled = true;
    try {
      await request('saveSettings', {days: Number($('collection-days').value), enabled, dartKey: key, clearDartKey: $('dart-clear').checked});
      $('dart-key').value = ''; await load(); displayScreen('home'); toast('설정을 저장하고 정리 기준을 적용했습니다.');
    } catch (error) { toast(error.message); }
    finally { $('settings-save').disabled = false; }
  }
  window.MobileApp = {
    suspend,
    receive(event) {
      if (event.type === 'progress') { updateProgress(event.data); return; }
      if (event.type === 'resume') { if (state.loaded && !state.collecting) load(); return; }
      const handler = pending.get(event.id); if (!handler) return;
      pending.delete(event.id); if (handler.timeout) clearTimeout(handler.timeout);
      if (event.type === 'error') handler.reject(new Error(event.data.message)); else handler.resolve(event.data);
    },
    back() {
      if (state.screen !== 'home') { $('dart-key').value = ''; displayScreen('home'); return true; }
      if (state.query) { state.query = ''; $('search').value = ''; state.limit = 40; render(); return true; }
      return false;
    }
  };
  $('today').textContent = new Intl.DateTimeFormat('ko-KR', {timeZone: 'Asia/Seoul', month: 'long', day: 'numeric', weekday: 'long'}).format(new Date());
  $('collect').onclick = collect;
  $('empty-action').onclick = () => $('empty-action').dataset.retry ? load() : collect();
  $('cancel-collection').onclick = () => {
    state.cancelling = true; $('progress-title').textContent = '수집을 중지하는 중';
    $('cancel-collection').disabled = true; request('cancel').catch(error => toast(error.message));
  };
  $('search').oninput = event => { state.query = event.target.value; state.limit = 40; render(); };
  $('range-filter').onchange = event => { state.range = event.target.value; state.limit = 40; render(); };
  $('read-filters').onclick = event => { const button = event.target.closest('[data-read]'); if (!button) return; state.read = button.dataset.read; state.limit = 40; render(); };
  $('bottom-nav').onclick = event => {
    const button = event.target.closest('[data-tab]'); if (!button) return;
    state.tab = button.dataset.tab; state.category = '전체'; state.limit = 40; render(); window.scrollTo(0, 0);
  };
  $('load-more').onclick = () => { state.limit += 40; render(); };
  $('reader-back').onclick = $('settings-back').onclick = $('results-back').onclick = () => window.MobileApp.back();
  $('reader-star').onclick = () => { const item = selected(); if (item) changeMark(item, 'starred', !item.starred); };
  $('reader-read').onclick = () => { const item = selected(); if (item) changeMark(item, 'read', !item.read); };
  $('reader-original').onclick = () => openOriginal(selected());
  $('reader-retry').onclick = () => loadArticle(state.selectedId);
  $('font-down').onclick = () => setFont(state.font - 2); $('font-up').onclick = () => setFont(state.font + 2);
  $('settings-open').onclick = openSettings; $('settings-save').onclick = saveSettings;
  $('sources-toggle').onclick = () => {
    const inputs = Array.from($('source-options').querySelectorAll('input')); const all = inputs.every(input => input.checked);
    inputs.forEach(input => { input.checked = !all; }); updateSourceToggle();
  };
  $('source-options').onchange = updateSourceToggle;
  $('result-open').onclick = showResults;
  window.addEventListener('scroll', () => {
    capturePosition();
    clearTimeout(scrollSaveTimer); scrollSaveTimer = setTimeout(persistUi, 100);
  }, {passive: true});
  window.addEventListener('pagehide', suspend);
  document.addEventListener('visibilitychange', () => { if (document.hidden) suspend(); });
  for (const event of ['touchstart', 'wheel']) window.addEventListener(event, () => {
    if (pendingReaderScroll !== null) { pendingReaderScroll = null; capturePosition(); persistUi(); }
  }, {passive: true});
  $('search').value = state.query;
  render(); load();
})();
