import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import db
import sources


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 10, 2, 7, tzinfo=sources.KST)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


def naver_row(nid, write_date, title="제목", item_name="삼성전자"):
    return {
        "nid": str(nid),
        "title": title,
        "content": "<p><strong>요점</strong></p><p>본문 1<br>본문 2</p>",
        "brokerName": "테스트증권",
        "writeDate": write_date,
        "itemName": item_name,
        "goalPrice": "120000",
        "opinionText": "Buy",
    }


def hankyung_row(report_idx, day, title):
    return f"""<tr>
      <td class="first txt_number">{day}</td><td>기업</td>
      <td class="text_l"><a href="/analysis/downpdf?report_idx={report_idx}" target="_blank">{title} </a>
        <div class="layerPop"><div class="pop01 disNone"><strong>{title}</strong><ul><li>핵심 요약</li></ul></div></div>
      </td><td>홍길동</td><td>테스트증권</td></tr>"""


class ReportSourceTests(unittest.TestCase):
    def setUp(self):
        clock = patch("sources.datetime", FixedDateTime)
        clock.start()
        self.addCleanup(clock.stop)
        env = patch.dict(os.environ, {"DAILY_REPORT_COLLECTION_DAYS": "1"})
        env.start()
        self.addCleanup(env.stop)

    def test_naver_research_reads_api_pages_until_window_start(self):
        pages = [
            {"hasNext": True, "items": [naver_row(3, "2026-10-02"), naver_row(2, "2026-10-01")]},
            {"hasNext": True, "items": [naver_row(1, "2026-10-01"), naver_row(0, "2026-09-30")]},
        ]
        source = sources.NaverResearchSource(name="네이버 금융 종목분석", category="company", source_type="증권사 리포트", include_item_name=True)
        with patch("sources.fetch_text", side_effect=[json.dumps(page) for page in pages]) as fetch:
            items = source.fetch()
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual([item.url for item in items], [f"https://stock.naver.com/research/company/{nid}" for nid in (3, 2, 1)])
        self.assertEqual(items[1].published_at, "2026-10-01T00:00:00+09:00")
        self.assertEqual(items[0].title, "삼성전자: 제목")
        self.assertEqual(items[0].summary.splitlines(), ["테스트증권 · 삼성전자 · Buy · 목표가 120,000원", "요점", "본문 1", "본문 2"])

    def test_naver_research_maps_legacy_list_path_and_rejects_changed_payload(self):
        source = sources.NaverResearchSource(name="n", list_path="market_info_list.naver", source_type="증권사 리포트")
        self.assertEqual(source.category, "market")
        with patch("sources.fetch_text", return_value=json.dumps({"result": []})):
            with self.assertRaises(RuntimeError):
                source.fetch()

    def test_hankyung_uses_link_text_as_title_and_keeps_previous_day(self):
        page = "<table>" + hankyung_row(11, "2026-10-01", "삼성전자(005930) 좋은 실적") + hankyung_row(10, "2026-09-30", "오래된 리포트") + "</table>"
        source = sources.HankyungConsensusSource(name="한경 컨센서스", source_type="증권사 리포트")
        with patch("sources.fetch_text", return_value=page) as fetch:
            items = source.fetch()
        self.assertIn("sdate=2026-10-01", fetch.call_args.args[0])
        self.assertEqual([item.title for item in items], ["삼성전자(005930) 좋은 실적"])
        self.assertEqual(items[0].summary.splitlines(), ["테스트증권 · 홍길동 · 기업", "핵심 요약"])

    def test_hankyung_without_table_is_an_error(self):
        source = sources.HankyungConsensusSource(name="한경 컨센서스", source_type="증권사 리포트")
        with patch("sources.fetch_text", return_value="<html>점검 중</html>"):
            with self.assertRaises(RuntimeError):
                source.fetch()


class DamoangBoardTests(unittest.TestCase):
    def setUp(self):
        clock = patch("sources.datetime", FixedDateTime)
        clock.start()
        self.addCleanup(clock.stop)

    def test_reads_rss_items_inside_news_window(self):
        feed = """<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>주식한당</title>
          <item><title>새 글 &amp; 질문</title><link>https://damoang.net/stock/2</link><author>앙</author>
            <pubDate>Thu, 01 Oct 2026 22:00:00 GMT</pubDate></item>
          <item><title>오래된 글</title><link>https://damoang.net/stock/1</link><author>앙</author>
            <pubDate>Thu, 01 Oct 2026 06:00:00 GMT</pubDate></item>
        </channel></rss>"""
        source = sources.DamoangBoardSource(name="다모앙 주식한당", board="stock")
        with patch("sources.fetch_text", return_value=feed) as fetch:
            items = source.fetch()
        self.assertEqual(fetch.call_args.args[0], "https://damoang.net/rss/stock")
        self.assertEqual([(item.title, item.published_at) for item in items], [("새 글 & 질문", "2026-10-02T07:00:00+09:00")])

    def test_feed_without_channel_is_an_error(self):
        source = sources.DamoangBoardSource(name="다모앙 주식한당", board="stock")
        with patch("sources.fetch_text", return_value="<html/>"):
            with self.assertRaises(RuntimeError):
                source.fetch()


class DateOnlyDisplayTests(unittest.TestCase):
    def test_date_only_reports_from_cutoff_date_are_listed_but_news_is_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("db.DB_PATH", Path(tmp) / "test.db"):
                db.init_db()
                rows = [
                    ("증권사 리포트", "네이버 금융 종목분석", "어제 리포트", "https://r/1", "2026-10-01T00:00:00+09:00"),
                    ("증권사 리포트", "네이버 금융 종목분석", "그제 리포트", "https://r/2", "2026-09-30T00:00:00+09:00"),
                    ("뉴스", "다음 뉴스 홈", "자정 뉴스", "https://n/1", "2026-10-01T00:00:00+09:00"),
                    ("뉴스", "다음 뉴스 홈", "저녁 뉴스", "https://n/2", "2026-10-01T17:00:00+09:00"),
                ]
                for source_type, source_name, title, url, published_at in rows:
                    db.upsert_item(source_type=source_type, source_name=source_name, title=title, url=url,
                                   published_at=published_at, content="", summary="", tags="")
                since = datetime(2026, 10, 1, 16, tzinfo=sources.KST)
                titles = {row["title"] for row in db.fetch_recent_items(limit=None, since=since)}
        self.assertEqual(titles, {"어제 리포트", "저녁 뉴스"})


if __name__ == "__main__":
    unittest.main()
