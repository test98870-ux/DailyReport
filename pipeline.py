from __future__ import annotations

import shutil
import subprocess
import tempfile
from io import BytesIO
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
import re
from urllib.request import Request, urlopen
from urllib.parse import parse_qs, urlparse

from pypdf import PdfReader

from db import init_db, upsert_item
from sources import SourceItem, build_sources


BASE_DIR = Path(__file__).resolve().parent


@dataclass
class PipelineResult:
    fetched: int
    stored: int
    mode: str
    errors: list[str]


ProgressCallback = Callable[[dict[str, object]], None]


def run_pipeline(progress_callback: ProgressCallback | None = None) -> PipelineResult:
    init_db()
    errors: list[str] = []
    stored = 0
    items: list[SourceItem] = []
    mode = "codex" if codex_available() else "fallback"
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
            "phase": "summarizing",
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
            "phase": "summarizing",
            "message": f"중복 제거 후 {len(items)}건 요약을 시작합니다.",
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
                "phase": "summarizing",
                "message": f"{index}/{len(items)} 요약 중",
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
            summary = summarize_item(item, mode=mode)
            upsert_item(
                source_type=item.source_type,
                source_name=item.source_name,
                title=item.title,
                url=item.url,
                published_at=item.published_at,
                content=item.content,
                summary=summary,
                tags=", ".join(item.tags),
            )
            stored += 1
            emit_progress(
                progress_callback,
                {
                    "phase": "summarizing",
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
                    "phase": "summarizing",
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


def codex_available() -> bool:
    return shutil.which("codex") is not None


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


def summarize_item(item: SourceItem, *, mode: str) -> str:
    prepared_item = enrich_item_for_summary(item)
    if mode == "codex":
        try:
            return summarize_with_codex(prepared_item)
        except Exception as exc:  # pragma: no cover - keep the batch alive
            return build_digest(prepared_item, error_hint=str(exc))
    return build_digest(prepared_item)


def enrich_item_for_summary(item: SourceItem) -> SourceItem:
    if not looks_like_pdf(item.url):
        return item
    try:
        pdf_text = extract_pdf_text(item.url)
    except Exception:
        return item
    if not pdf_text:
        return item
    enriched = SourceItem(
        source_type=item.source_type,
        source_name=item.source_name,
        title=item.title,
        url=item.url,
        published_at=item.published_at,
        content=f"{item.content}\n\nPDF 원문 발췌:\n{pdf_text}",
        tags=item.tags,
    )
    return enriched


def looks_like_pdf(url: str) -> bool:
    lowered = url.lower()
    return ".pdf" in lowered or "downpdf" in lowered or "stock-research" in lowered


def extract_pdf_text(url: str, *, max_pages: int = 8, max_chars: int = 12000) -> str:
    request = Request(url, headers={"User-Agent": "DailyReportBot/1.0"})
    with urlopen(request, timeout=30) as response:
        body = response.read()
    if not body.startswith(b"%PDF"):
        return ""
    reader = PdfReader(BytesIO(body))
    texts: list[str] = []
    total = 0
    for page in reader.pages[:max_pages]:
        text = (page.extract_text() or "").strip()
        if not text:
            continue
        remaining = max_chars - total
        if remaining <= 0:
            break
        chunk = text[:remaining]
        texts.append(chunk)
        total += len(chunk)
    cleaned = "\n".join(texts).strip()
    return cleaned


def summarize_with_codex(item: SourceItem) -> str:
    prompt = f"""
너는 한국 주식시장 아침 브리핑 에디터다.
아래 자료를 4줄 이내의 간결한 한국어 요약으로 정리해라.
반드시 아래 형식만 출력해라.

핵심:
영향:
체크포인트:

추가 설명, 머리말, 코드블록, 마크다운 제목은 금지한다.
쉘 명령 실행이나 파일 탐색은 하지 말고, 주어진 텍스트만 사용해라.

제목: {item.title}
출처: {item.source_name}
유형: {item.source_type}
게시시각: {item.published_at}
본문:
{item.content}
""".strip()

    with tempfile.NamedTemporaryFile("r+", encoding="utf-8") as temp_file:
        result = subprocess.run(
            [
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "--color",
                "never",
                "-C",
                str(BASE_DIR),
                "-o",
                temp_file.name,
                prompt,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        temp_file.seek(0)
        summary = temp_file.read().strip()

    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip() or "codex exec failed"
        raise RuntimeError(details[:200])
    if not summary:
        raise RuntimeError("codex exec returned empty summary")
    return summary


def build_digest(item: SourceItem, error_hint: str = "") -> str:
    compact = " ".join(item.content.split())
    compact = compact[:280]
    lines = [
        f"핵심: {item.title}",
        f"영향: {compact}",
        "체크포인트: Codex CLI 호출 실패 시 이 대체 요약이 저장됩니다.",
    ]
    if error_hint:
        lines.append(f"비고: {error_hint[:120]}")
    return "\n".join(lines)


def pipeline_result_to_dict(result: PipelineResult) -> dict[str, object]:
    payload = asdict(result)
    payload["ran_at"] = datetime.now().isoformat(timespec="seconds")
    return payload
