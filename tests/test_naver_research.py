import json
import unittest
from unittest.mock import patch

import sources


class NaverResearchTests(unittest.TestCase):
    def source(self, path='company_list.naver'):
        return sources.NaverResearchSource(name='네이버 리서치', list_path=path,
                                           source_type='증권사 리포트', limit=1, include_item_name=True)

    def test_current_public_api_keeps_korean_title_and_original_link(self):
        source = self.source()
        source.within_hours = None
        payload = {'items': [{'nid': '123', 'title': '실적 &amp; 전망', 'writeDate': '2026-11-06',
                              'brokerName': '증권사', 'itemName': '삼성전자', 'content': '<p>리포트 본문</p>'},
                             {'nid': '124', 'title': '두 번째', 'writeDate': '2026-11-06'}]}
        with patch.object(sources, 'fetch_text', side_effect=['/_next/static/chunks/app.js', json.dumps(payload)]) as fetch:
            items = source.fetch()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].title, '삼성전자: 실적 & 전망')
        self.assertEqual(items[0].url, 'https://stock.naver.com/research/company/123')
        self.assertEqual(items[0].content, '리포트 본문')
        self.assertEqual(fetch.call_args.kwargs['encoding'], 'utf-8')
        self.assertEqual(fetch.call_args.kwargs['headers']['User-Agent'], 'Mozilla/5.0')

    def test_market_data_maps_to_daily_detail_page(self):
        source = self.source('market_info_list.naver')
        source.within_hours = None
        payload = {'items': [{'nid': '12', 'title': '시황', 'writeDate': '2026-11-06'}]}
        with patch.object(sources, 'fetch_text', side_effect=['/_next/static/app.js', json.dumps(payload)]) as fetch:
            items = source.fetch()
        self.assertIn('/researches/v2/market?', fetch.call_args.args[0])
        self.assertEqual(items[0].url, 'https://stock.naver.com/research/daily/12')

    def test_server_default_date_filter_and_invalid_api_response(self):
        payload = {'items': [None, {'nid': 'bad', 'title': '잘못된 ID', 'writeDate': '2026-11-06'},
                             {'nid': '123', 'title': '오래된 항목', 'writeDate': '2026-11-06'}]}
        with patch.object(sources, 'fetch_text', side_effect=['/_next/static/app.js', json.dumps(payload)]), \
                patch.object(sources, 'within_last_hours', return_value=False):
            self.assertEqual(self.source().fetch(), [])
        with patch.object(sources, 'fetch_text', side_effect=['/_next/static/app.js', '{"error": "unavailable"}']):
            with self.assertRaises(ValueError):
                self.source().fetch()

    def test_legacy_html_keeps_existing_pdf_link_and_does_not_call_api(self):
        body = '''<tr><td><a class="stock_item">삼성전자</a></td>
            <td><a href="company_read.naver?nid=42">기존 리포트</a></td><td>증권사</td>
            <td class="date">26.11.06</td><td><a href="https://stock.pstatic.net/report.pdf">PDF</a></td></tr>'''
        with patch.object(sources, 'fetch_text', return_value=body) as fetch, \
                patch.object(sources, 'within_last_hours', return_value=True):
            items = self.source().fetch()
        self.assertEqual(items[0].url, 'https://stock.pstatic.net/report.pdf')
        self.assertEqual(items[0].title, '삼성전자: 기존 리포트')
        fetch.assert_called_once()
