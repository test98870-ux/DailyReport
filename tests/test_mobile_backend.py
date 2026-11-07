import json
import os
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


class Callback:
    def __init__(self):
        self.cancelled = False
        self.events = []

    def isCancelled(self):
        return self.cancelled

    def onProgress(self, raw):
        self.events.append(json.loads(raw))


class FakeSource:
    name = '다음 뉴스 경제'

    def __init__(self, items=None, failure=False):
        self.items = items or []
        self.failure = failure

    def fetch(self):
        if self.failure:
            raise RuntimeError('secret-key-do-not-persist')
        return self.items


class MobileBackendTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        for target, field in [(db, 'DB_PATH'), (config, 'ENV_FILE'), (sources, 'DEFAULT_CONFIG_FILE')]:
            context = patch.object(target, field, getattr(target, field))
            context.start()
            self.addCleanup(context.stop)
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        mobile.initialize(self.directory.name)

    def item(self, *, title='테스트 뉴스', url='https://v.daum.net/v/20261107120000001', days=0):
        return sources.SourceItem('뉴스', '다음 뉴스 경제', title, url,
                                  (datetime.now(sources.KST) - timedelta(days=days)).isoformat(), '', [])

    def collect(self, *adapters, callback=None):
        with patch.object(sources, 'build_sources', return_value=adapters):
            return json.loads(mobile.collect(callback or Callback()))

    def test_initialization_and_settings_never_fetch_network(self):
        with patch.object(sources, 'fetch_text', side_effect=AssertionError('automatic network request')):
            self.assertEqual(json.loads(mobile.initialize(self.directory.name))['items'], [])
            settings = json.loads(mobile.settings())
            self.assertGreater(len(settings['sources']), 10)
            saved = json.loads(mobile.save_settings(json.dumps({'days': 3, 'enabled': [settings['sources'][0]['name']]})))
            self.assertEqual(saved['days'], 3)

    def test_manual_collection_deduplicates_and_persists_read_and_star_marks(self):
        item = self.item()
        first = self.collect(FakeSource([item, item]))
        self.assertEqual(len(first['items']), 1)
        item_id = first['items'][0]['id']
        mobile.mark(item_id, 'read', True)
        mobile.mark(item_id, 'starred', True)
        second = self.collect(FakeSource([self.item(title='제목이 바뀐 뉴스')]))
        self.assertEqual(len(second['items']), 1)
        self.assertTrue(second['items'][0]['read'])
        self.assertTrue(second['items'][0]['starred'])
        self.assertTrue(json.loads(mobile.initialize(self.directory.name))['items'][0]['starred'])

    def test_partial_source_failure_keeps_saved_items_and_redacts_error(self):
        self.collect(FakeSource([self.item()]))
        result = self.collect(FakeSource(failure=True), FakeSource([]))
        self.assertEqual(len(result['items']), 1)
        self.assertEqual([s['status'] for s in result['lastResult']['sources']], ['error', 'empty'])
        self.assertNotIn('secret-key', json.dumps(result))

    def test_cancellation_stops_pagination_even_when_source_catches_exceptions(self):
        callback = Callback()
        calls = []

        class PaginatingSource(FakeSource):
            def fetch(self):
                for page in range(3):
                    try:
                        sources.fetch_text('https://example.com/' + str(page))
                    except Exception:
                        continue
                return []

        def fetch(url):
            calls.append(url)
            callback.cancelled = True
            return 'page'

        with patch.object(sources, 'fetch_text', side_effect=fetch):
            result = self.collect(PaginatingSource(), callback=callback)
        self.assertEqual(len(calls), 1)
        self.assertTrue(result['lastResult']['cancelled'])
        self.assertIsNone(result['lastCollection'])

    def test_pre_cancelled_job_never_builds_or_fetches_sources(self):
        callback = Callback()
        callback.cancelled = True
        with patch.object(sources, 'build_sources') as adapters:
            result = json.loads(mobile.collect(callback))
        adapters.assert_not_called()
        self.assertTrue(result['lastResult']['cancelled'])

    def test_expired_items_are_cleaned_and_not_registered_again(self):
        result = self.collect(FakeSource([self.item(days=40)]))
        self.assertEqual(len(result['items']), 0)
        self.assertEqual(result['cleanup']['blockedCount'], 1)
        again = self.collect(FakeSource([self.item(days=40)]))
        self.assertEqual(again['lastResult']['stored'], 0)

    def test_article_is_cached_locally_and_available_without_network(self):
        item_id = self.collect(FakeSource([self.item()]))['items'][0]['id']
        blocks = [{'type': 'text', 'text': '<script>not executable</script>'}]
        with patch.object(article_reader, 'load_article', return_value=blocks) as fetch:
            first = json.loads(mobile.article(item_id))
            second = json.loads(mobile.article(item_id))
        self.assertFalse(first['cached'])
        self.assertTrue(second['cached'])
        self.assertEqual(second['blocks'], blocks)
        fetch.assert_called_once()

    def test_dart_key_is_private_and_can_be_kept_or_deleted(self):
        key = 'a' * 40
        name = json.loads(mobile.settings())['sources'][0]['name']
        saved = mobile.save_settings(json.dumps({'days': 1, 'enabled': [name], 'dartKey': key}))
        self.assertNotIn(key, saved)
        self.assertTrue(json.loads(saved)['dartConfigured'])
        mobile.save_settings(json.dumps({'days': 7, 'enabled': [name], 'dartKey': ''}))
        self.assertEqual(os.environ['OPENDART_API_KEY'], key)
        mobile.save_settings(json.dumps({'days': 1, 'enabled': [name], 'clearDartKey': True}))
        self.assertEqual(os.environ['OPENDART_API_KEY'], '')
        for invalid in [{'days': 0, 'enabled': [name]}, {'days': True, 'enabled': [name]},
                        {'days': 1, 'enabled': ['unknown']}, {'days': 1, 'enabled': [name], 'dartKey': 'invalid'}]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                mobile.save_settings(json.dumps(invalid))

    def test_report_intro_is_stored_text_and_does_not_fetch_article_pages(self):
        report = self.item()
        report.source_type = '증권사 리포트'
        report.url = 'https://stock.naver.com/research/company/123'
        report.content = '리포트 소개 본문\n두 번째 문단'
        item_id = self.collect(FakeSource([report]))['items'][0]['id']
        with patch.object(article_reader, 'load_article', side_effect=AssertionError('unexpected network')):
            result = json.loads(mobile.article(item_id))
        self.assertEqual([block['text'] for block in result['blocks']], ['리포트 소개 본문', '두 번째 문단'])
        self.assertIn('PDF', result['message'])
