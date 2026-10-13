import json
import os
import tempfile
import threading
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
from unittest.mock import Mock, patch

import db
import main
import pipeline
from sources import KST, SkippedSource, SourceItem


def news_item(title="기사", url="https://v.daum.net/v/20261002070000123", published_at=None, name="다음 뉴스 경제"):
    return SourceItem("뉴스", name, title, url, published_at or datetime.now(timezone.utc).isoformat(), title, [])


class PipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        database = patch.object(db, "DB_PATH", Path(temporary.name) / "test.db")
        database.start()
        self.addCleanup(database.stop)
        db.init_db()

    def store(self, item):
        values = asdict(item)
        values.update(summary="", tags=", ".join(item.tags))
        db.upsert_item(**values)

    def test_failed_source_keeps_existing_data_and_continues_other_sources(self):
        existing = news_item("기존 기사")
        self.store(existing)
        original_id = db.fetch_recent_items()[0]["id"]
        failed = Mock(name="failed-source")
        failed.name = "다음 뉴스 경제"
        failed.fetch.side_effect = RuntimeError("네트워크 오류")
        healthy = Mock(name="healthy-source")
        healthy.name = "클리앙 새로운소식"
        healthy.skip_reason = ""
        healthy.fetch.return_value = [news_item("새 기사", "https://www.clien.net/service/board/news/2", name=healthy.name)]
        progress = []
        with patch("pipeline.build_sources", return_value=[failed, healthy]):
            result = pipeline.run_pipeline(progress.append)
        rows = db.fetch_recent_items()
        self.assertEqual({row["title"] for row in rows}, {"기존 기사", "새 기사"})
        self.assertEqual(next(row["id"] for row in rows if row["title"] == "기존 기사"), original_id)
        self.assertEqual(result.stored, 1)
        self.assertEqual(result.errors, ["다음 뉴스 경제: 네트워크 오류"])
        self.assertEqual([row["status"] for row in result.source_results], ["error", "ok"])
        self.assertTrue(any(row.get("current_source") == healthy.name for row in progress))

    def test_empty_or_skipped_collection_keeps_data(self):
        self.store(news_item())
        with patch("pipeline.build_sources", return_value=[SkippedSource("OpenDART", "API 키 미설정")]):
            result = pipeline.run_pipeline()
        self.assertEqual(len(db.fetch_recent_items()), 1)
        self.assertEqual(result.errors, [])
        self.assertEqual(result.source_results[0]["status"], "skipped")

    def test_repeated_collection_and_title_updates_display_one_article(self):
        item = news_item()
        source = Mock()
        source.name = item.source_name
        source.skip_reason = ""
        source.fetch.return_value = [item]
        with patch("pipeline.build_sources", side_effect=lambda: [source]):
            pipeline.run_pipeline()
            original_id = db.fetch_recent_items()[0]["id"]
            pipeline.run_pipeline()
            self.assertEqual(db.fetch_recent_items()[0]["id"], original_id)
            item.title = "수정된 제목"
            pipeline.run_pipeline()
        rows = db.fetch_recent_items()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "수정된 제목")

    def test_dedup_keeps_distinct_links_with_identical_headlines(self):
        items = [news_item(url="https://example.com/1"), news_item(url="https://example.com/2")]
        self.assertEqual(len(pipeline.dedupe_items(items)), 2)
        self.assertEqual(len(pipeline.dedupe_items(items + [news_item(url="https://example.com/1?utm_source=test")])), 2)

    def test_storage_keeps_same_title_and_date_with_different_urls(self):
        published = datetime.now(timezone.utc).isoformat()
        for url in ("https://example.com/1", "https://example.com/2"):
            self.store(news_item(url=url, published_at=published))
        self.assertEqual(len(db.fetch_recent_items()), 2)

    def test_legacy_schema_migration_preserves_rows_and_ids(self):
        with db.get_connection() as connection:
            connection.execute("DROP TABLE items")
            connection.execute('''CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT, source_type TEXT NOT NULL,
                source_name TEXT NOT NULL, title TEXT NOT NULL, url TEXT,
                published_at TEXT NOT NULL, content TEXT NOT NULL, summary TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_name, title, published_at))''')
            connection.execute('''INSERT INTO items
                (id, source_type, source_name, title, url, published_at, content, summary)
                VALUES (42, '뉴스', '다음 뉴스 경제', '기사', 'https://example.com/1', ?, '본문', '요약')''',
                (datetime.now(timezone.utc).isoformat(),))
            before = dict(connection.execute("SELECT * FROM items").fetchone())
        db.init_db()
        db.init_db()  # Migration is safe to run on every startup.
        with db.get_connection() as connection:
            self.assertEqual(dict(connection.execute("SELECT * FROM items").fetchone()), before)
        self.store(news_item(url="https://example.com/2", published_at=before["published_at"]))
        rows = db.fetch_recent_items()
        self.assertEqual(len(rows), 2)
        self.assertGreater(max(row["id"] for row in rows), 42)

    def test_dedup_normalizes_portal_links_and_scopes_linkless_titles(self):
        items = [news_item(), news_item(url="https://v.daum.net/v/20261002070000123?tracking=1")]
        self.assertEqual(len(pipeline.dedupe_items(items)), 1)
        self.assertEqual(pipeline.canonical_url_key("https://n.news.naver.com/mnews/article/001/123?sid=101"), pipeline.canonical_url_key("https://news.naver.com/main/read.naver?oid=001&aid=123"))
        day1 = news_item(url="", published_at="2026-10-01T07:00:00+09:00")
        day2 = news_item(url="", published_at="2026-10-02T07:00:00+09:00")
        self.assertEqual(len(pipeline.dedupe_items([day1, day2])), 2)

    def test_read_markers_do_not_hide_other_urls_with_same_title(self):
        first = {**asdict(news_item(url="https://example.com/1")), "tags": ""}
        second = {**asdict(news_item(url="https://example.com/2")), "tags": ""}
        self.assertFalse(set(main.item_read_keys(first)) & set(main.item_read_keys(second)))

    def test_database_sorts_offsets_before_limit_and_filters_future_dates(self):
        now = datetime.now(timezone.utc)
        older = news_item("이전", "https://example.com/old", (now - timedelta(hours=2)).astimezone(KST).isoformat())
        newest = news_item("최신", "https://example.com/new", (now - timedelta(hours=1)).isoformat())
        future = news_item("미래", "https://example.com/future", (now + timedelta(days=1)).isoformat())
        for item in [older, newest, future]:
            self.store(item)
        self.assertEqual(db.fetch_recent_items(limit=1)[0]["title"], "최신")
        self.assertEqual(len(db.fetch_recent_items(limit=None)), 2)
        self.assertEqual(db.fetch_recent_items(limit=0), [])

    def test_more_than_300_items_reach_dashboard_and_api(self):
        published = datetime.now(timezone.utc).isoformat()
        with db.get_connection() as connection:
            connection.executemany(
                "INSERT INTO items(source_type, source_name, title, url, published_at, content, summary) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [("뉴스", "다음 뉴스 경제", f"검증 기사 {index}", f"https://example.com/{index}", published, "", "") for index in range(325)],
            )
        server = main.ReusableThreadingHTTPServer(("127.0.0.1", 0), main.DashboardHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            with patch.dict(os.environ, {"DAILY_REPORT_COLLECTION_DAYS": "1"}):
                with urlopen(f"{base}/api/items") as response:
                    payload = json.load(response)
                with urlopen(base) as response:
                    page = response.read().decode()
            self.assertEqual(len(payload["items"]), 325)
            self.assertIn("검증 기사 0</button>", page)
            self.assertIn("검증 기사 324</button>", page)
            self.assertIn('id="source-results"', page)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
