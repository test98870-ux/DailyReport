from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from sources import normalize_news_url, parse_published_datetime


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "daily_report.db"


SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    published_at TEXT NOT NULL,
    content TEXT NOT NULL,
    summary TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_name, title, published_at, url)
);
"""


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(SCHEMA)
        old_unique_key = False
        for index in conn.execute("PRAGMA index_list(items)").fetchall():
            if not index["unique"]:
                continue
            columns = [row["name"] for row in conn.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno", (index["name"],)
            )]
            if columns == ["source_name", "title", "published_at"]:
                old_unique_key = True
                break
        if old_unique_key:
            # SQLite cannot change a UNIQUE constraint in place. Keep all rows and IDs.
            conn.execute("ALTER TABLE items RENAME TO items_legacy")
            conn.execute(SCHEMA)
            conn.execute("INSERT INTO items SELECT * FROM items_legacy")
            conn.execute("DROP TABLE items_legacy")


def clear_items() -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM items")
        conn.execute("DELETE FROM sqlite_sequence WHERE name = 'items'")


def upsert_item(
    *,
    source_type: str,
    source_name: str,
    title: str,
    url: str,
    published_at: str,
    content: str,
    summary: str,
    tags: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO items (
                source_type, source_name, title, url, published_at, content, summary, tags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_name, title, published_at, url) DO UPDATE SET
                url = excluded.url,
                content = excluded.content,
                summary = excluded.summary,
                tags = excluded.tags
            """,
            (source_type, source_name, title, url, published_at, content, summary, tags),
        )


def fetch_recent_items(
    limit: int | None = 300,
    within_hours: int | None = 24,
    since: datetime | None = None,
) -> list[sqlite3.Row]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM items
            ORDER BY published_at DESC, id DESC
            """,
        ).fetchall()
    if limit is not None and limit <= 0:
        return []
    now = datetime.now(timezone.utc)
    cutoff = since.astimezone(timezone.utc) if since is not None else (
        now - timedelta(hours=within_hours) if within_hours is not None else None
    )
    dated_rows = []
    for row in rows:
        try:
            published_at = parse_published_datetime(row["published_at"]).astimezone(timezone.utc)
        except ValueError:
            continue
        if published_at <= now and (cutoff is None or published_at >= cutoff):
            dated_rows.append((published_at, row))
    dated_rows.sort(key=lambda entry: (entry[0], entry[1]["id"]), reverse=True)
    filtered: list[sqlite3.Row] = []
    seen_urls: set[tuple[str, str]] = set()
    for _, row in dated_rows:
        url_key = (row["source_type"], normalize_news_url(row["url"] or ""))
        if url_key[1] and url_key in seen_urls:
            continue
        if url_key[1]:
            seen_urls.add(url_key)
        filtered.append(row)
        if limit is not None and len(filtered) >= limit:
            break
    return filtered
