import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import config
import sources


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 10, 2, 8, tzinfo=sources.KST)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


def clien_row(article_id, title, timestamp, nickname=""):
    return f'''<div class="list_item symph_row" data-role="list-row">
      <a class="list_subject" href="/service/board/news/{article_id}?po=0">
        <span class="subject_fixed" title="{title}">{title}</span>
      </a><span class="nickname ingname">{nickname}</span>
      <span class="timestamp">{timestamp}</span></div>'''


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.clock = patch("sources.datetime", FixedDateTime)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        env = patch.dict(os.environ, {"DAILY_REPORT_COLLECTION_DAYS": "1"})
        env.start()
        self.addCleanup(env.stop)

    def test_news_window_boundaries_and_naive_kst(self):
        self.assertTrue(sources.within_news_collection_window("2026-10-01T16:00:00+09:00"))
        self.assertTrue(sources.within_news_collection_window("2026-10-01T16:00:00"))
        self.assertFalse(sources.within_news_collection_window("2026-10-01T15:59:59+09:00"))
        self.assertFalse(sources.within_news_collection_window("2026-10-02T08:00:01+09:00"))
        self.assertFalse(sources.within_news_collection_window("invalid"))

    def test_invalid_daum_date_does_not_abort_the_source(self):
        self.assertEqual(sources.parse_daum_article_datetime("https://v.daum.net/v/20261302010000123"), "")

    def test_daum_embedded_fields_can_reorder_or_omit_optional_values(self):
        body = r'''<script>var data={title:"시사 매거진",contents:[
          {id:"hamny-20261002070000123",pcUrl:"https://v.daum.net/v/20261002070000123?utm_source=test",title:"숨겨진 \"카드\"",createdAt:"2026-10-02T07:00:00+09:00",cpName:"언론사"},
          {"id":"hamny-20261002060000123","title":"추가 {카드}","pcUrl":"https:\/\/v.daum.net\/v\/20261002060000123"}
        ]};</script>'''
        source = sources.DaumNewsSectionSource(name="다음 뉴스 홈", section_name="홈")
        with patch("sources.fetch_text", return_value=body):
            items = source.fetch()
        self.assertEqual([i.title for i in items], ['숨겨진 "카드"', "추가 {카드}"])
        self.assertTrue(all(i.source_name == "다음 뉴스 매거진" for i in items))
        self.assertEqual(items[0].url, "https://v.daum.net/v/20261002070000123")

    def test_daum_page_with_only_escaped_embedded_urls(self):
        body = r'''<script>var data={contents:[{id:"hamny-20261002070000123",title:"기사",pcUrl:"https:\/\/v.daum.net\/v\/20261002070000123"}]};</script>'''
        source = sources.DaumNewsSectionSource(name="다음 뉴스 홈", section_name="홈")
        with patch("sources.fetch_text", return_value=body):
            self.assertEqual(len(source.fetch()), 1)

    def test_daum_anchor_queries_duplicates_old_cards_and_newest_limit(self):
        body = '''<a href="https://v.daum.net/v/20260901000000123"><strong class="tit_txt">오래된 기사</strong></a>
          <a href='https://v.daum.net/v/20261002060000123?tracking=1' data-title='첫%20기사'></a>
          <a href="https://v.daum.net/v/20261002060000123"><strong class="tit_txt">중복</strong></a>
          <a href="https://v.daum.net/v/20261302010000123"><strong class="tit_txt">잘못된 날짜</strong></a>
          <a href="https://v.daum.net/v/20261002070000123"><strong class="tit_txt">최신 기사</strong></a>'''
        source = sources.DaumNewsSectionSource(name="다음 뉴스 사회", section_name="사회")
        with patch("sources.fetch_text", return_value=body):
            self.assertEqual([i.title for i in source.fetch()], ["최신 기사", "첫 기사"])
            source.limit = 1
            self.assertEqual([i.title for i in source.fetch()], ["최신 기사"])

    def test_daum_missing_optional_fields_do_not_steal_next_articles_metadata(self):
        body = '''<script>var data={contents:[
          {id:"hamny-20261002070000123",title:"첫 기사",pcUrl:"https://v.daum.net/v/20261002070000123"},
          {id:"hamny-20261002060000123",title:"다음 기사",pcUrl:"https://v.daum.net/v/20261002060000123",cpName:"둘째 언론사",createdAt:"2026-09-01T06:00:00+09:00"}
        ]};</script>'''
        source = sources.DaumNewsSectionSource(name="다음 뉴스 홈", section_name="홈")
        with patch("sources.fetch_text", return_value=body):
            items = source.fetch()
        self.assertEqual([i.title for i in items], ["첫 기사"])
        self.assertNotIn("둘째 언론사", items[0].tags)

    def test_clien_image_nickname_keeps_its_own_timestamp(self):
        body = clien_row(1, "최신 글", "2026-10-02 01:40:27", '<img alt="작성자">')
        body += clien_row(2, "이전 글", "2026-10-01 14:29:47", '<span title="다른 작성자">다른 작성자</span>')
        source = sources.ClienNewsSource(name="클리앙 새로운소식")
        with patch("sources.fetch_text", side_effect=[body, '<div class="list_content"></div>']):
            items = source.fetch()
        self.assertEqual([i.title for i in items], ["최신 글"])
        self.assertEqual(items[0].published_at, "2026-10-02T01:40:27+09:00")
        self.assertIn("작성자", items[0].tags)

    def test_clien_paginates_and_does_not_stop_on_pinned_old_post(self):
        first = clien_row(9, "오래된 고정 글", "2026-09-01 10:00:00")
        first += clien_row(1, "첫 페이지", "2026-10-02 07:00:00")
        second = clien_row(2, "두 번째 페이지", "2026-10-01 18:00:00")
        old = clien_row(3, "수집 기간 밖", "2026-10-01 15:00:00")
        source = sources.ClienNewsSource(name="클리앙 새로운소식")
        with patch("sources.fetch_text", side_effect=[first, second, old]) as fetch:
            items = source.fetch()
        self.assertEqual([i.title for i in items], ["첫 페이지", "두 번째 페이지"])
        self.assertIn("po=1", fetch.call_args_list[1].args[0])
        self.assertEqual(fetch.call_count, 3)

    def test_clien_duplicate_page_and_page_cap(self):
        body = clien_row(1, "한 글", "2026-10-02 07:00:00")
        with patch("sources.fetch_text", return_value=body) as fetch:
            self.assertEqual(len(sources.ClienNewsSource(name="클리앙").fetch()), 1)
            self.assertEqual(fetch.call_count, 2)
        with patch("sources.fetch_text", return_value=body) as fetch:
            self.assertEqual(len(sources.ClienNewsSource(name="클리앙", max_pages=1).fetch()), 1)
            self.assertEqual(fetch.call_count, 1)

    def test_naver_old_featured_article_does_not_hide_newer_article(self):
        source = sources.NaverNewsSectionSource(name="네이버 뉴스 경제", section_id="101", section_name="경제")
        listing = ''.join(f'<a href="https://n.news.naver.com/mnews/article/001/{i}" class="sa_text_title">제목</a>' for i in (1, 2))
        items = [sources.SourceItem("뉴스", source.name, title, str(i), date, title, []) for i, title, date in (
            (1, "이전 기사", "2026-10-01T15:00:00+09:00"), (2, "최신 기사", "2026-10-02T07:00:00+09:00"))]
        with patch("sources.fetch_text", return_value=listing), patch.object(source, "_fetch_article", side_effect=items):
            self.assertEqual([i.title for i in source.fetch()], ["최신 기사"])

    def test_rss_headline_without_description_and_gmt_date(self):
        response = Mock()
        response.read.return_value = b'<rss><channel><item><title>Headline</title><link>https://example.com/news</link><pubDate>Fri, 02 Oct 2026 00:00:00 GMT</pubDate></item></channel></rss>'
        context = Mock()
        context.__enter__ = Mock(return_value=response)
        context.__exit__ = Mock(return_value=False)
        with patch("sources.urlopen", return_value=context):
            items = sources.RssSource("RSS", "https://example.com/feed", "뉴스").fetch()
        self.assertEqual(items[0].content, "Headline")
        self.assertEqual(items[0].published_at, "2026-10-02T00:00:00+00:00")

    def test_dart_empty_response_is_not_a_collection_error(self):
        source = sources.OpenDartSource(name="공시", api_key="test")
        with patch("sources.fetch_text", return_value='{"status":"013","message":"조회된 데이타가 없습니다."}'):
            self.assertEqual(source.fetch(), [])
        with patch("sources.fetch_text", return_value='{"status":"010","message":"인증키 오류"}'):
            with self.assertRaisesRegex(RuntimeError, "인증키 오류"):
                source.fetch()

    def test_env_file_is_loaded_for_sources_and_shell_takes_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env").write_text("OPENDART_API_KEY=from-file\nDAILY_REPORT_COLLECTION_DAYS=3\n")
            (root / "sources.json").write_text(json.dumps([{"type": "opendart", "name": "공시"}]))
            with patch.dict(os.environ, {}, clear=True), patch.object(config, "ENV_FILE", root / ".env"), patch.object(sources, "DEFAULT_CONFIG_FILE", root / "sources.json"):
                self.assertEqual(list(sources.build_sources())[0].api_key, "from-file")
                self.assertEqual(sources.collection_window_days(), 3)
                os.environ["OPENDART_API_KEY"] = "from-shell"
                self.assertEqual(list(sources.build_sources())[0].api_key, "from-shell")

    def test_missing_key_is_visible_and_disabled_sources_are_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sources.json"
            path.write_text(json.dumps([
                {"type": "opendart", "name": "공시"},
                {"type": "clien_news", "name": "사용 안 함", "enabled": False},
            ]))
            with patch.dict(os.environ, {}, clear=True), patch("sources.load_env_file"), patch.object(sources, "DEFAULT_CONFIG_FILE", path):
                result = list(sources.build_sources())
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].skip_reason, "OPENDART_API_KEY 미설정")


if __name__ == "__main__":
    unittest.main()
