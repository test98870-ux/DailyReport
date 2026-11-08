import json
import unittest
from datetime import datetime
from unittest.mock import patch

import sources

NOW = datetime(2026, 11, 6, 9, 0, tzinfo=sources.KST)


class NaverResearchTests(unittest.TestCase):
    def source(self, path='company_list.naver', limit=1):
        return sources.NaverResearchSource(name='네이버 리서치', list_path=path,
                                           source_type='증권사 리포트', limit=limit, include_item_name=True)

    def test_public_api_keeps_korean_title_and_original_link(self):
        payload = {'items': [{'nid': '123', 'title': '실적 &amp; 전망', 'writeDate': '2026-11-06',
                              'brokerName': '증권사', 'itemName': '삼성전자', 'content': '<p>리포트 본문</p>'},
                             {'nid': '124', 'title': '두 번째', 'writeDate': '2026-11-06'}]}
        with patch.object(sources, 'fetch_text', return_value=json.dumps(payload)) as fetch, \
                patch.object(sources, 'report_window_start_date', return_value=NOW.date()):
            items = self.source().fetch()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].title, '삼성전자: 실적 & 전망')
        self.assertEqual(items[0].url, 'https://stock.naver.com/research/company/123')
        self.assertIn('리포트 본문', items[0].summary)
        self.assertEqual(fetch.call_args.kwargs['encoding'], 'utf-8')
        self.assertEqual(fetch.call_args.kwargs['headers']['User-Agent'], 'Mozilla/5.0')

    def test_market_data_maps_to_daily_detail_page(self):
        payload = {'items': [{'nid': '12', 'title': '시황', 'writeDate': '2026-11-06'}]}
        with patch.object(sources, 'fetch_text', return_value=json.dumps(payload)) as fetch, \
                patch.object(sources, 'report_window_start_date', return_value=NOW.date()):
            items = self.source('market_info_list.naver').fetch()
        self.assertIn('/researches/v2/market?', fetch.call_args.args[0])
        self.assertEqual(items[0].url, 'https://stock.naver.com/research/daily/12')

    def test_server_window_drops_older_reports_and_rejects_invalid_response(self):
        payload = {'items': [None, {'nid': 'bad', 'title': '잘못된 ID', 'writeDate': '2026-11-06'},
                             {'nid': '123', 'title': '오래된 항목', 'writeDate': '2026-11-05'}], 'hasNext': True}
        with patch.object(sources, 'fetch_text', return_value=json.dumps(payload)) as fetch, \
                patch.object(sources, 'report_window_start_date', return_value=NOW.date()):
            self.assertEqual(self.source(limit=0).fetch(), [])
        fetch.assert_called_once()
        with patch.object(sources, 'fetch_text', return_value='{"error": "unavailable"}'):
            with self.assertRaises(RuntimeError):
                self.source().fetch()

    def test_mobile_disables_window_and_reads_only_latest_page(self):
        source = self.source(limit=0)
        source.within_hours = None
        payload = {'items': [{'nid': '1', 'title': '지난주 리포트', 'writeDate': '2026-10-30'}], 'hasNext': True}
        with patch.object(sources, 'fetch_text', return_value=json.dumps(payload)) as fetch:
            items = source.fetch()
        self.assertEqual([item.title for item in items], ['지난주 리포트'])
        fetch.assert_called_once()
