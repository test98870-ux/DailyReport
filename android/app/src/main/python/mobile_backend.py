"""On-device clipping. No HTTP server, scheduler, AI, or automatic collection."""
from __future__ import annotations

import json
import hashlib
import os
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import article_reader
import config
import db
import sources
from pipeline import dedupe_items

_root: Path | None = None
_defaults: list[dict] = []
_collection_lock = threading.Lock()


class CollectionCancelled(BaseException):
    """Bypass source adapters' broad Exception handlers to stop pagination."""


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def _atomic_json(path: Path, value) -> None:
    temporary = path.with_suffix('.tmp')
    temporary.write_text(_json(value), encoding='utf-8')
    temporary.replace(path)


def initialize(directory: str) -> str:
    global _root, _defaults
    _root = Path(directory)
    _root.mkdir(parents=True, exist_ok=True)
    _defaults = json.loads((Path(sources.__file__).parent / 'sources.json').read_text(encoding='utf-8'))
    config.ENV_FILE = _root / '.env'  # Never load or package a developer's PC secrets.
    db.DB_PATH = _root / 'daily_report.db'
    sources.DEFAULT_CONFIG_FILE = _root / 'sources.json'
    db.init_db()
    with db.get_connection() as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS mobile_marks (
                item_key TEXT PRIMARY KEY, is_read INTEGER NOT NULL DEFAULT 0,
                starred INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS mobile_articles (item_key TEXT PRIMARY KEY, blocks TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS mobile_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS mobile_removed (
                identity TEXT PRIMARY KEY, removed_at TEXT NOT NULL
            );
        ''')
    _apply_settings(_settings())
    return snapshot(cleanup=False)


def _settings() -> dict:
    path = _root / 'settings.json'
    if path.exists():
        return json.loads(path.read_text(encoding='utf-8'))
    return {'days': 1, 'dart_key': '', 'enabled': [row['name'] for row in _defaults]}


def _apply_settings(settings: dict) -> None:
    os.environ['DAILY_REPORT_COLLECTION_DAYS'] = str(settings['days'])
    os.environ['OPENDART_API_KEY'] = settings.get('dart_key', '')
    rows = [dict(row, enabled=row['name'] in settings['enabled']) for row in _defaults]
    _atomic_json(sources.DEFAULT_CONFIG_FILE, rows)


def settings() -> str:
    value = _settings()
    return _json({'days': value['days'], 'dartConfigured': bool(value.get('dart_key')),
                  'cleanup': _cleanup_info(),
                  'sources': [{'name': row['name'], 'type': row['source_type'],
                               'enabled': row['name'] in value['enabled']} for row in _defaults]})


def save_settings(raw: str) -> str:
    value = json.loads(raw)
    days = value.get('days')
    if type(days) is not int or days not in (1, 3, 7):
        raise ValueError('수집 기간은 1일, 3일, 7일 중 선택하세요.')
    names = value.get('enabled')
    available = {row['name'] for row in _defaults}
    if not isinstance(names, list) or not all(isinstance(name, str) and name in available for name in names):
        raise ValueError('수집 소스 설정이 올바르지 않습니다.')
    previous = _settings()
    key = value.get('dartKey', '').strip()
    if key and not re.fullmatch(r'[a-fA-F0-9]{40}', key):
        raise ValueError('OpenDART 인증키는 40자리 영문·숫자(16진수)입니다.')
    next_settings = {'days': days, 'enabled': list(dict.fromkeys(names)),
                     'dart_key': '' if value.get('clearDartKey') else key or previous.get('dart_key', '')}
    _atomic_json(_root / 'settings.json', next_settings)
    _apply_settings(next_settings)
    _run_cleanup()
    return settings()


def _key(row) -> str:
    return row['source_type'] + '|' + (sources.normalize_news_url(row['url'] or '') or str(row['id']))


def _cleanup_identity(row) -> str:
    """A durable identity, independent of title edits, source aliases and database row IDs."""
    url = row['url'] or ''
    normalized = sources.normalize_news_url(url)
    if normalized:
        parsed = urlparse(url)
        if parsed.hostname == 'stock.naver.com':
            match = re.fullmatch(r'/research/(company|daily|industry|invest|economy)/(\d+)/?', parsed.path)
            if match:
                normalized = 'naver-research:' + match[1] + ':' + match[2]
        elif parsed.hostname == 'finance.naver.com':
            match = re.fullmatch(r'/research/(company|market_info|industry|invest|economy)_read.naver', parsed.path)
            nid = parse_qs(parsed.query).get('nid', [''])[0]
            if match and nid.isdigit():
                category = 'daily' if match[1] == 'market_info' else match[1]
                normalized = 'naver-research:' + category + ':' + nid
        identity = 'url:' + normalized
    else:
        try:
            published = sources.parse_published_datetime(row['published_at']).astimezone(sources.KST).isoformat()
        except (ValueError, TypeError):
            published = row['published_at']
        identity = _json(['no-url', row['source_type'], row['source_name'],
                          sources.normalize_whitespace(row['title']), published])
    # Keep only an irreversible fingerprint, not deleted titles, URLs or article bodies.
    return hashlib.sha256(identity.encode('utf-8')).hexdigest()


def _cleanup_info() -> dict:
    with db.get_connection() as conn:
        blocked = conn.execute('SELECT COUNT(*) FROM mobile_removed').fetchone()[0]
        row = conn.execute("SELECT value FROM mobile_meta WHERE key = 'last_cleanup'").fetchone()
    return {'cutoff': sources.news_collection_window_start().isoformat(), 'blockedCount': blocked,
            'lastRun': json.loads(row['value']) if row else None}


def _run_cleanup() -> dict:
    """Local foreground maintenance using the same publication cutoff as news collection."""
    cutoff = sources.news_collection_window_start()
    result = {'removed': 0, 'removedRows': 0, 'cutoff': cutoff.isoformat(),
              'finishedAt': datetime.now(sources.KST).isoformat()}
    with db.get_connection() as conn:
        # Tombstone creation and row/cache deletion commit together, including concurrent collection.
        conn.execute('BEGIN IMMEDIATE')
        marks = {row['item_key']: dict(row) for row in conn.execute('SELECT * FROM mobile_marks')}
        groups = {}
        for row in conn.execute('SELECT * FROM items'):
            groups.setdefault(_cleanup_identity(row), []).append(row)
        for identity, rows in groups.items():
            if any(marks.get(_key(row), {}).get('starred') for row in rows):
                continue
            try:
                expired = all(sources.parse_published_datetime(row['published_at']) < cutoff for row in rows)
            except (ValueError, TypeError):
                continue
            if not expired:
                continue
            conn.execute('INSERT OR IGNORE INTO mobile_removed VALUES (?, ?)', (identity, result['finishedAt']))
            for row in rows:
                key = _key(row)
                conn.execute('DELETE FROM mobile_articles WHERE item_key = ?', (key,))
                conn.execute('DELETE FROM mobile_marks WHERE item_key = ?', (key,))
                conn.execute('DELETE FROM items WHERE id = ?', (row['id'],))
            result['removed'] += 1
            result['removedRows'] += len(rows)
        if result['removed']:
            conn.execute('INSERT OR REPLACE INTO mobile_meta VALUES (?, ?)', ('last_cleanup', _json(result)))
    if result['removed']:
        clear = getattr(article_reader.load_article, 'cache_clear', None)
        if callable(clear):
            clear()
    return result


def _store_collected_item(item) -> bool:
    identity = _cleanup_identity(vars(item))
    with db.get_connection() as conn:
        # One SQL statement avoids a check/insert race with automatic cleanup.
        cursor = conn.execute('''
            INSERT INTO items (source_type, source_name, title, url, published_at, content, summary, tags)
            SELECT ?, ?, ?, ?, ?, ?, '', ?
            WHERE NOT EXISTS (SELECT 1 FROM mobile_removed WHERE identity = ?)
            ON CONFLICT(source_name, title, published_at, url) DO UPDATE SET
                content = excluded.content, summary = excluded.summary, tags = excluded.tags
        ''', (item.source_type, item.source_name, item.title, item.url, item.published_at,
              item.content, ', '.join(item.tags), identity))
        return cursor.rowcount > 0


def _category(row) -> str:
    name = row['source_name']
    if row['source_type'] == '뉴스':
        return name.removeprefix('다음 뉴스 ').removeprefix('네이버 뉴스 ') if '뉴스 ' in name else '커뮤니티'
    if 'EDGAR' in name or row['source_type'] == '미국 공시':
        return '미국 공시'
    if '공시' in row['source_type']:
        return 'DART' if 'DART' in name else 'KIND' if 'KIND' in name else '공시'
    for keyword, category in [('종목', '기업'), ('산업', '산업'), ('시황', '시황'),
                              ('전략', '전략'), ('경제', '경제'), ('컨센서스', '컨센서스')]:
        if keyword in name:
            return category
    return '리포트'


def _meta(key: str, value) -> None:
    with db.get_connection() as conn:
        conn.execute('INSERT OR REPLACE INTO mobile_meta VALUES (?, ?)', (key, _json(value)))


def snapshot(cleanup: bool = True) -> str:
    if cleanup:
        _run_cleanup()
    rows = db.fetch_recent_items(limit=None, within_hours=None)
    with db.get_connection() as conn:
        marks = {r['item_key']: dict(r) for r in conn.execute('SELECT * FROM mobile_marks')}
        meta = {r['key']: json.loads(r['value']) for r in conn.execute('SELECT * FROM mobile_meta')}
    items = []
    for row in rows:
        key = _key(row)
        mark = marks.get(key, {})
        kind = 'news' if row['source_type'] == '뉴스' else 'disclosure' if '공시' in row['source_type'] else 'report'
        items.append({'id': row['id'], 'title': row['title'], 'url': row['url'], 'source': row['source_name'],
                      'published': row['published_at'], 'kind': kind, 'category': _category(row),
                      'read': bool(mark.get('is_read')), 'starred': bool(mark.get('starred'))})
    return _json({'items': items, 'lastCollection': meta.get('last_collection'),
                  'lastResult': meta.get('last_result'), 'days': _settings()['days'], 'cleanup': _cleanup_info()})


def mark(item_id: int, field: str, enabled: bool) -> str:
    if field not in ('read', 'starred'):
        raise ValueError('지원하지 않는 표시입니다.')
    with db.get_connection() as conn:
        row = conn.execute('SELECT * FROM items WHERE id = ?', (item_id,)).fetchone()
        if row is None:
            raise ValueError('저장된 항목을 찾을 수 없습니다.')
        key = _key(row)
        conn.execute('INSERT OR IGNORE INTO mobile_marks (item_key) VALUES (?)', (key,))
        column = 'is_read' if field == 'read' else 'starred'
        conn.execute(f'UPDATE mobile_marks SET {column} = ? WHERE item_key = ?', (int(enabled), key))
    return _json({'id': item_id, 'field': field, 'value': bool(enabled)})


def article(item_id: int, callback=None) -> str:
    with db.get_connection() as conn:
        row = conn.execute('SELECT * FROM items WHERE id = ?', (item_id,)).fetchone()
        if row is None:
            raise ValueError('저장된 항목을 찾을 수 없습니다.')
        key = _key(row)
        cached = conn.execute('SELECT blocks FROM mobile_articles WHERE item_key = ?', (key,)).fetchone()
    if cached:
        return _json({'id': item_id, 'blocks': json.loads(cached['blocks']), 'cached': True})
    if row['source_type'] != '뉴스':
        if row['url'] and row['url'].startswith('https://stock.naver.com/research/'):
            blocks = [{'type': 'text', 'text': paragraph.strip()} for paragraph in row['content'].split('\n') if paragraph.strip()]
            return _json({'id': item_id, 'blocks': blocks, 'cached': True,
                          'message': '리포트 소개 본문입니다. 전체 보고서와 PDF는 원문에서 확인하세요.'})
        return _json({'id': item_id, 'blocks': [], 'message': '리포트와 공시는 원문에서 확인하세요.'})
    if callback is not None and callback.isCancelled():
        raise ValueError('앱 화면에서 다시 시도해 주세요.')
    blocks = article_reader.load_article(row['url'])
    with db.get_connection() as conn:
        cursor = conn.execute('''INSERT INTO mobile_articles (item_key, blocks)
            SELECT ?, ? WHERE EXISTS (SELECT 1 FROM items WHERE id = ?)
                AND NOT EXISTS (SELECT 1 FROM mobile_removed WHERE identity = ?)
            ON CONFLICT(item_key) DO UPDATE SET blocks = excluded.blocks''',
            (key, _json(blocks), item_id, _cleanup_identity(row)))
        if not cursor.rowcount:
            raise ValueError('정리된 자료입니다. 목록에서 다른 자료를 선택해 주세요.')
    return _json({'id': item_id, 'blocks': blocks, 'cached': False})


def collect(callback) -> str:
    # An old Activity may still be awaiting an in-flight HTTP timeout after rotation/destruction.
    with _collection_lock:
        return _collect(callback)


def _collect(callback) -> str:
    """Only called by the explicit native collection command while visible."""
    results = []
    fetched = stored = 0
    cancelled = False
    original_fetch = sources.fetch_text

    def check():
        if callback.isCancelled():
            raise CollectionCancelled()

    def guarded_fetch(*args, **kwargs):
        check()
        result = original_fetch(*args, **kwargs)
        check()
        return result

    sources.fetch_text = guarded_fetch
    try:
        check()
        _run_cleanup()
        adapters = list(sources.build_sources())
        total = len(adapters)
        if not total:
            raise ValueError('설정에서 수집할 소스를 하나 이상 선택하세요.')
        for index, source in enumerate(adapters):
            check()
            if isinstance(source, sources.NaverResearchSource):
                # Keep the latest reports available on weekends; retain source count limits.
                source.within_hours = None
            name = getattr(source, 'name', getattr(source, 'source_name', source.__class__.__name__))
            callback.onProgress(_json({'index': index, 'total': total, 'source': name,
                                       'fetched': fetched, 'stored': stored}))
            try:
                items = dedupe_items(source.fetch())
                check()
                fetched += len(items)
                for item in items:
                    check()
                    stored += int(_store_collected_item(item))
                skipped = getattr(source, 'skip_reason', '')
                results.append({'name': name, 'status': 'skipped' if skipped else 'ok' if items else 'empty',
                                'fetched': len(items), 'message': skipped})
            except Exception as error:
                # Never persist exception text, which may contain API credentials or request URLs.
                results.append({'name': name, 'status': 'error', 'fetched': 0,
                                'message': '연결 또는 사이트 응답을 확인해 주세요. (' + type(error).__name__ + ')'})
            callback.onProgress(_json({'index': index + 1, 'total': total, 'source': name,
                                       'fetched': fetched, 'stored': stored}))
    except CollectionCancelled:
        cancelled = True
    finally:
        sources.fetch_text = original_fetch
    result = {'cancelled': cancelled, 'fetched': fetched, 'stored': stored, 'sources': results,
              'finishedAt': datetime.now(sources.KST).isoformat()}
    _meta('last_result', result)
    if not cancelled:
        _meta('last_collection', result['finishedAt'])
    return snapshot()
