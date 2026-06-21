from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable
import re
from urllib.parse import parse_qs, urlparse

from db import clear_items, init_db, upsert_item
from sources import SourceItem, build_sources


@dataclass
class PipelineResult:
    fetched: int
    stored: int
    mode: str
    errors: list[str]


ProgressCallback = Callable[[dict[str, object]], None]


def run_pipeline(progress_callback: ProgressCallback | None = None) -> PipelineResult:
    init_db()
    clear_items()
    errors: list[str] = []
    stored = 0
    items: list[SourceItem] = []
    mode = "title-only"
    source_count = 0

    emit_progress(
        progress_callback,
        {
            "phase": "starting",
            "message": "수집 작업을 준비 중입니다.",
            "fetched": 0,
            "stored": 0,
            "total_items": 0,
            "errors": 0,
            "mode": mode,
        },
    )

    for source in build_sources():
        source_count += 1
        emit_progress(
            progress_callback,
            {
                "phase": "fetching",
                "message": f"{source.__class__.__name__} 수집 중",
                "current_source": source.__class__.__name__,
                "fetched": len(items),
                "stored": stored,
                "total_items": len(items),
                "errors": len(errors),
                "mode": mode,
            },
        )
        try:
            fetched_items = source.fetch()
            items.extend(fetched_items)
            emit_progress(
                progress_callback,
                {
                    "phase": "fetching",
                    "message": f"{source.__class__.__name__}에서 {len(fetched_items)}건 수집",
                    "current_source": source.__class__.__name__,
                    "fetched": len(items),
                    "stored": stored,
                    "total_items": len(items),
                    "errors": len(errors),
                    "mode": mode,
                },
            )
        except Exception as exc:  # pragma: no cover - broad to keep the batch alive
            errors.append(f"{source.__class__.__name__}: {exc}")
            emit_progress(
                progress_callback,
                {
                    "phase": "fetching",
                    "message": f"{source.__class__.__name__} 수집 실패",
                    "current_source": source.__class__.__name__,
                    "fetched": len(items),
                    "stored": stored,
                    "total_items": len(items),
                    "errors": len(errors),
                    "mode": mode,
                },
            )

    emit_progress(
        progress_callback,
        {
            "phase": "storing",
            "message": f"총 {len(items)}건 수집, 중복 제거를 준비합니다.",
            "fetched": len(items),
            "stored": stored,
            "total_items": len(items),
            "errors": len(errors),
            "mode": mode,
            "source_count": source_count,
        },
    )
    items = dedupe_items(items)
    emit_progress(
        progress_callback,
        {
            "phase": "storing",
            "message": f"중복 제거 후 {len(items)}건 저장을 시작합니다.",
            "fetched": len(items),
            "stored": stored,
            "total_items": len(items),
            "errors": len(errors),
            "mode": mode,
            "source_count": source_count,
        },
    )

    for index, item in enumerate(items, start=1):
        emit_progress(
            progress_callback,
            {
                "phase": "storing",
                "message": f"{index}/{len(items)} 저장 중",
                "current_title": item.title,
                "fetched": len(items),
                "stored": stored,
                "current_index": index,
                "total_items": len(items),
                "errors": len(errors),
                "mode": mode,
            },
        )
        try:
            upsert_item(
                source_type=item.source_type,
                source_name=item.source_name,
                title=item.title,
                url=item.url,
                published_at=item.published_at,
                content=item.content,
                summary="",
                tags=", ".join(item.tags),
            )
            stored += 1
            emit_progress(
                progress_callback,
                {
                    "phase": "storing",
                    "message": f"{index}/{len(items)} 저장 완료",
                    "current_title": item.title,
                    "fetched": len(items),
                    "stored": stored,
                    "current_index": index,
                    "total_items": len(items),
                    "errors": len(errors),
                    "mode": mode,
                },
            )
        except Exception as exc:  # pragma: no cover - broad to keep the batch alive
            errors.append(f"{item.title}: {exc}")
            emit_progress(
                progress_callback,
                {
                    "phase": "storing",
                    "message": f"{index}/{len(items)} 처리 중 오류",
                    "current_title": item.title,
                    "fetched": len(items),
                    "stored": stored,
                    "current_index": index,
                    "total_items": len(items),
                    "errors": len(errors),
                    "mode": mode,
                },
            )

    result = PipelineResult(
        fetched=len(items),
        stored=stored,
        mode=mode,
        errors=errors,
    )
    emit_progress(
        progress_callback,
        {
            "phase": "completed",
            "message": f"완료: {stored}건 저장",
            "fetched": len(items),
            "stored": stored,
            "total_items": len(items),
            "errors": len(errors),
            "mode": mode,
        },
    )
    return result


def emit_progress(progress_callback: ProgressCallback | None, payload: dict[str, object]) -> None:
    if progress_callback is not None:
        progress_callback(payload)


def dedupe_items(items: list[SourceItem]) -> list[SourceItem]:
    deduped: list[SourceItem] = []
    seen_keys: set[str] = set()
    for item in items:
        url_key = canonical_url_key(item.url)
        title_key = canonical_title_key(item.title)
        keys = [key for key in [url_key, title_key] if key]
        if any(key in seen_keys for key in keys):
            continue
        seen_keys.update(keys)
        deduped.append(item)
    return deduped


def canonical_url_key(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    host = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.rstrip("/")
    if "n.news.naver.com" in host:
        match = re.search(r"/article/(\d+)/(\d+)", path)
        if match:
            return f"naver:{match.group(1)}:{match.group(2)}"
    if "news.naver.com" in host:
        query = parse_qs(parsed.query)
        oid = query.get("oid", [""])[0]
        aid = query.get("aid", [""])[0]
        if oid and aid:
            return f"naver:{oid}:{aid}"
    if "v.daum.net" in host:
        match = re.search(r"/v/(\d+)", path)
        if match:
            return f"daum:{match.group(1)}"
    query = "&".join(
        f"{key}={value}"
        for key, values in sorted(parse_qs(parsed.query).items())
        if not key.lower().startswith("utm_")
        for value in values
    )
    return f"{host}{path}?{query}" if query else f"{host}{path}"


def canonical_title_key(title: str) -> str:
    normalized = re.sub(r"[^\w가-힣]+", " ", title.lower())
    return re.sub(r"\s+", " ", normalized).strip()


def pipeline_result_to_dict(result: PipelineResult) -> dict[str, object]:
    payload = asdict(result)
    payload["ran_at"] = datetime.now().isoformat(timespec="seconds")
    return payload
