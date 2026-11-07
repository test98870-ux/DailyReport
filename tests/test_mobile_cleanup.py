import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'android/app/src/main/python'))
import mobile_backend as mobile
import article_reader
import config
import db
import sources
from test_mobile_backend import Callback, FakeSource


class MobileCleanupTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.directory = directory.name
        self.addCleanup(directory.cleanup)
        for target, field in [(db, 'DB_PATH'), (config, 'ENV_FILE'), (sources, 'DEFAULT_CONFIG_FILE')]:
            guard = patch.object(target, field, getattr(target, field))
            guard.start()
            self.addCleanup(guard.stop)
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.cutoff = datetime.now(sources.KST).replace(hour=16, minute=0, second=0, microsecond=0) - timedelta(days=1)
        window = patch.object(sources, 'news_collection_window_start', return_value=self.cutoff)
        self.window = window.start()
        self.addCleanup(window.stop)
        mobile.initialize(self.directory)

    def add(self, slug, when=None, kind='뉴스', url=None):
        item = sources.SourceItem(kind, '테스트 소스', '제목 ' + slug,
                                  url if url is not None else 'https://example.com/' + slug,
                                  (when or self.cutoff + timedelta(hours=1)).isoformat(), '본문', [])
        self.assertTrue(mobile._store_collected_item(item))
        with db.get_connection() as conn:
            row = conn.execute('SELECT * FROM items ORDER BY id DESC LIMIT 1').fetchone()
        return item, row['id']

    def counts(self):
        with db.get_connection() as conn:
            return {table: conn.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                    for table in ['items', 'mobile_marks', 'mobile_articles', 'mobile_removed']}

    def test_exact_kst_cutoff_keeps_boundary_and_newer_items(self):
        self.add('before', self.cutoff - timedelta(microseconds=1))
        self.add('exactly', self.cutoff)
        self.add('after', self.cutoff + timedelta(microseconds=1))
        result = json.loads(mobile.snapshot())
        self.assertEqual({item['title'] for item in result['items']}, {'제목 exactly', '제목 after'})
        self.assertEqual(result['cleanup']['blockedCount'], 1)
        self.assertEqual(mobile._run_cleanup()['removed'], 0)
        self.assertEqual(self.counts()['mobile_removed'], 1)

    def test_read_and_unread_expire_but_bookmarks_and_their_bodies_survive(self):
        _, read_id = self.add('read', self.cutoff - timedelta(hours=1))
        self.add('unread', self.cutoff - timedelta(hours=2), kind='공시')
        _, saved_id = self.add('saved', self.cutoff - timedelta(days=8), kind='증권사 리포트')
        mobile.mark(read_id, 'read', True)
        mobile.mark(saved_id, 'starred', True)
        with db.get_connection() as conn:
            for item_id in (read_id, saved_id):
                row = conn.execute('SELECT * FROM items WHERE id=?', (item_id,)).fetchone()
                conn.execute('INSERT INTO mobile_articles VALUES (?, ?)', (mobile._key(row), '[]'))
        result = mobile._run_cleanup()
        self.assertEqual(result['removed'], 2)
        self.assertEqual(self.counts(), {'items': 1, 'mobile_marks': 1, 'mobile_articles': 1, 'mobile_removed': 2})
        self.assertEqual(json.loads(mobile.snapshot())['items'][0]['id'], saved_id)

    def test_tombstone_survives_restart_period_expansion_and_title_source_date_changes(self):
        original, _ = self.add('original', self.cutoff - timedelta(hours=1), url='https://v.daum.net/v/20261107090000123?utm_source=a')
        mobile._run_cleanup()
        mobile.initialize(self.directory)
        self.window.return_value = self.cutoff - timedelta(days=6)
        settings = json.loads(mobile.settings())
        mobile.save_settings(json.dumps({'days': 7, 'enabled': [settings['sources'][0]['name']]}))
        renamed = sources.SourceItem('공시', '다른 소스', '제목이 달라진 같은 항목',
                                     'https://v.daum.net/v/20261107090000123?tracking=new',
                                     (self.cutoff + timedelta(hours=1)).isoformat(), '', [])
        self.assertFalse(mobile._store_collected_item(original))
        self.assertFalse(mobile._store_collected_item(renamed))
        self.assertEqual(self.counts()['items'], 0)
        self.assertEqual(self.counts()['mobile_removed'], 1)
        different = sources.SourceItem('뉴스', '다른 소스', original.title, 'https://v.daum.net/v/20261107090000999',
                                       original.published_at, '', [])
        self.assertTrue(mobile._store_collected_item(different))

    def test_cleanup_failure_rolls_back_tombstone_marks_and_article_deletion(self):
        _, item_id = self.add('rollback', self.cutoff - timedelta(hours=1))
        mobile.mark(item_id, 'read', True)
        with db.get_connection() as conn:
            row = conn.execute('SELECT * FROM items WHERE id=?', (item_id,)).fetchone()
            conn.execute('INSERT INTO mobile_articles VALUES (?, ?)', (mobile._key(row), '[]'))
            conn.execute("CREATE TRIGGER fail_cleanup BEFORE DELETE ON items BEGIN SELECT RAISE(ABORT, 'test rollback'); END")
        before = self.counts()
        with self.assertRaises(sqlite3.IntegrityError):
            mobile._run_cleanup()
        self.assertEqual(self.counts(), before)

    def test_duplicate_rows_are_removed_together_and_any_bookmark_protects_group(self):
        item, item_id = self.add('one', self.cutoff - timedelta(hours=1), url='https://example.com/same')
        _, other_id = self.add('two', self.cutoff - timedelta(hours=2), kind='공시', url=item.url)
        mobile.mark(other_id, 'starred', True)
        self.assertEqual(mobile._run_cleanup()['removed'], 0)
        mobile.mark(other_id, 'starred', False)
        result = mobile._run_cleanup()
        self.assertEqual((result['removed'], result['removedRows']), (1, 2))
        self.assertEqual(self.counts()['mobile_removed'], 1)

    def test_aliases_and_url_less_items_have_stable_fingerprints(self):
        first, _ = self.add('legacy', self.cutoff - timedelta(hours=1), kind='증권사 리포트',
                            url='https://finance.naver.com/research/market_info_read.naver?nid=12&page=1')
        second = sources.SourceItem(first.source_type, first.source_name, '새 제목',
                                    'https://stock.naver.com/research/daily/12', first.published_at, '', [])
        self.assertEqual(mobile._cleanup_identity(vars(first)), mobile._cleanup_identity(vars(second)))
        mobile._run_cleanup()
        self.assertFalse(mobile._store_collected_item(second))
        no_url, _ = self.add('no-url', self.cutoff - timedelta(hours=1), url='')
        mobile._run_cleanup()
        self.assertFalse(mobile._store_collected_item(no_url))
        equivalent = dict(vars(no_url), published_at=sources.parse_published_datetime(no_url.published_at).astimezone(sources.timezone.utc).isoformat())
        self.assertEqual(mobile._cleanup_identity(vars(no_url)), mobile._cleanup_identity(equivalent))

    def test_inflight_article_cannot_recreate_removed_body_cache(self):
        _, item_id = self.add('inflight', self.cutoff - timedelta(hours=1))
        def response(url):
            mobile._run_cleanup()
            return [{'type': 'text', 'text': '이미 정리된 본문'}]
        with patch.object(article_reader, 'load_article', side_effect=response):
            with self.assertRaises(ValueError):
                mobile.article(item_id)
        self.assertEqual(self.counts()['mobile_articles'], 0)
        self.assertEqual(self.counts()['mobile_removed'], 1)

    def test_new_old_source_entries_are_cleaned_once_then_skipped_before_storage(self):
        item = sources.SourceItem('뉴스', '소스', '오래된 항목', 'https://example.com/old',
                                  (self.cutoff - timedelta(days=1)).isoformat(), '', [])
        with patch.object(sources, 'build_sources', return_value=[FakeSource([item])]):
            first = json.loads(mobile.collect(Callback()))
        self.assertEqual(first['items'], [])
        self.assertEqual(first['cleanup']['blockedCount'], 1)
        with patch.object(sources, 'build_sources', return_value=[FakeSource([item])]):
            again = json.loads(mobile.collect(Callback()))
        self.assertEqual(again['lastResult']['stored'], 0)

    def test_settings_save_applies_new_cutoff_and_date_only_reports_use_midnight(self):
        self.add('dated-report', self.cutoff.replace(hour=0), kind='증권사 리포트')
        self.add('recent')
        settings = json.loads(mobile.settings())
        result = json.loads(mobile.save_settings(json.dumps({'days': 1, 'enabled': [settings['sources'][0]['name']]})))
        self.assertEqual(result['cleanup']['blockedCount'], 1)
        self.assertEqual(len(json.loads(mobile.snapshot())['items']), 1)
