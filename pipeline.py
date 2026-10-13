from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Callable
import re

from db import init_db, upsert_item
from sources import SourceItem, build_sources, normalize_news_url


@dataclass
class PipelineResult:
    fetched: int
    stored: int
    mode: str
    errors: list[str]
    source_results: list[dict[str, object]] = field(default_factory=list)


ProgressCallback = Callable[[dict[str, object]], None]


def run_pipeline(progress_callback: ProgressCallback | None = None) -> PipelineResult:
    init_db()
    errors: list[str] = []
    source_results: list[dict[str, object]] = []
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
        source_name = getattr(source, "name", getattr(source, "source_name", source.__class__.__name__))
        emit_progress(
            progress_callback,
            {
                "phase": "fetching",
                "message": f"{source_name} 수집 중",
                "current_source": source_name,
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
            source_result: dict[str, object] = {"name": source_name, "status": "ok" if fetched_items else "empty", "fetched": len(fetched_items)}
            if getattr(source, "skip_reason", ""):
                source_result.update(status="skipped", message=source.skip_reason)
            source_results.append(source_result)
            emit_progress(
                progress_callback,
                {
                    "phase": "fetching",
                    "message": f"{source_name}에서 {len(fetched_items)}건 수집",
                    "current_source": source_name,
                    "source_results": list(source_results),
                    "fetched": len(items),
                    "stored": stored,
                    "total_items": len(items),
                    "errors": len(errors),
                    "mode": mode,
                },
            )
        except Exception as exc:  # pragma: no cover - broad to keep the batch alive
            errors.append(f"{source_name}: {exc}")
            source_results.append({"name": source_name, "status": "error", "fetched": 0, "message": str(exc)})
            emit_progress(
                progress_callback,
                {
                    "phase": "fetching",
                    "message": f"{source_name} 수집 실패",
                    "current_source": source_name,
                    "source_results": list(source_results),
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
        source_results=source_results,
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
        # Distinct reports/filings can legitimately have the same headline.
        key = f"{item.source_type}|url|{url_key}" if url_key else (
            f"{item.source_type}|{item.source_name}|{canonical_title_key(item.title)}|{item.published_at}"
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(item)
    return deduped


def canonical_url_key(url: str) -> str:
    return normalize_news_url(url)


def canonical_title_key(title: str) -> str:
    normalized = re.sub(r"[^\w가-힣]+", " ", title.lower())
    return re.sub(r"\s+", " ", normalized).strip()


def pipeline_result_to_dict(result: PipelineResult) -> dict[str, object]:
    payload = asdict(result)
    payload["ran_at"] = datetime.now().isoformat(timespec="seconds")
    return payload
