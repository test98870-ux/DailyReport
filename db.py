from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator


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
    UNIQUE(source_name, title, published_at)
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
        conn.executescript(SCHEMA)


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
            ON CONFLICT(source_name, title, published_at) DO UPDATE SET
                url = excluded.url,
                content = excluded.content,
                summary = excluded.summary,
                tags = excluded.tags
            """,
            (source_type, source_name, title, url, published_at, content, summary, tags),
        )


def fetch_recent_items(limit: int = 300, within_hours: int | None = 24) -> list[sqlite3.Row]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM items
            ORDER BY published_at DESC, id DESC
            """,
        ).fetchall()
    if within_hours is None:
        return rows[:limit]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=within_hours)
    filtered: list[sqlite3.Row] = []
    for row in rows:
        try:
            published_at = datetime.fromisoformat(row["published_at"])
        except ValueError:
            continue
        if published_at.astimezone(timezone.utc) >= cutoff:
            filtered.append(row)
        if len(filtered) >= limit:
            break
    return filtered
