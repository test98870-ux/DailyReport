from __future__ import annotations

import html
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from typing import Iterable
from urllib.request import Request, urlopen
from xml.etree import ElementTree


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SAMPLE_FILE = BASE_DIR / "data" / "sample_items.json"
DEFAULT_CONFIG_FILE = BASE_DIR / "sources.json"
KST = timezone(timedelta(hours=9))
NAVER_FINANCE_BASE = "https://finance.naver.com/research/"
NAVER_ROOT = "https://finance.naver.com/"
DART_LIST_API = "https://opendart.fss.or.kr/api/list.json"
DART_VIEWER_BASE = "https://dart.fss.or.kr/dsaf001/main.do"
HANKYUNG_BASE = "https://consensus.hankyung.com"
WISEREPORT_BASE = "https://www.wisereport.co.kr"
SEC_SUBMISSIONS_BASE = "https://data.sec.gov/submissions/"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data/"
KIND_RSS_URL = "http://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute"
NAVER_NEWS_BASE = "https://news.naver.com"
CLIEN_NEWS_URL = "https://www.clien.net/service/board/news"
DAMOANG_NEWS_URL = "https://damoang.net/new"


@dataclass
class SourceItem:
    source_type: str
    source_name: str
    title: str
    url: str
    published_at: str
    content: str
    tags: list[str]


class Source:
    def fetch(self) -> list[SourceItem]:
        raise NotImplementedError


def fetch_text(url: str, encoding: str | None = None, headers: dict[str, str] | None = None) -> str:
    request_headers = {"User-Agent": "DailyReportBot/1.0"}
    if headers:
        request_headers.update(headers)
    request = Request(url, headers=request_headers)
    with urlopen(request, timeout=20) as response:
        body = response.read()
        detected = encoding or response.headers.get_content_charset() or "utf-8"
    try:
        return body.decode(detected, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def strip_tags(fragment: str) -> str:
    normalized = fragment.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    normalized = normalized.replace("</p>", "\n").replace("</div>", "\n")
    text = re.sub(r"<[^>]+>", " ", normalized)
    text = html.unescape(text)
    text = re.sub(r"\r", "", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def parse_naver_date(raw: str) -> str:
    raw = raw.strip()
    year, month, day = raw.split(".")
    return datetime(int(f"20{year}"), int(month), int(day), tzinfo=KST).isoformat()


def parse_slash_date(raw: str) -> str:
    raw = raw.strip()
    year, month, day = raw.split("/")
    return datetime(int(f"20{year}"), int(month), int(day), tzinfo=KST).isoformat()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_news_url(url: str) -> str:
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
    filtered_query = "&".join(
        f"{key}={value}"
        for key, values in sorted(parse_qs(parsed.query).items())
        if not key.lower().startswith("utm_")
        for value in values
    )
    normalized = f"{host}{path}"
    if filtered_query:
        normalized = f"{normalized}?{filtered_query}"
    return normalized


def within_last_hours(published_at: str, hours: int) -> bool:
    try:
        published = datetime.fromisoformat(published_at)
    except ValueError:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return published.astimezone(timezone.utc) >= cutoff


def first_external_link(html_fragment: str, blocked_hosts: set[str]) -> str:
    for match in re.finditer(r"""(?:href|ori-url)=['\"]([^'\"]+)['\"]""", html_fragment):
        url = html.unescape(match.group(1)).strip()
        if not url.startswith("http"):
            continue
        host = urlparse(url).netloc.lower().replace("www.", "")
        if host in blocked_hosts:
            continue
        return url
    return ""


class SampleFileSource(Source):
    def __init__(self, path: Path, source_name: str = "sample-seed") -> None:
        self.path = path
        self.source_name = source_name

    def fetch(self) -> list[SourceItem]:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        items = []
        for row in data:
            items.append(
                SourceItem(
                    source_type=row["source_type"],
                    source_name=row.get("source_name", self.source_name),
                    title=row["title"],
                    url=row.get("url", ""),
                    published_at=row["published_at"],
                    content=row["content"],
                    tags=row.get("tags", []),
                )
            )
        return items


class RssSource(Source):
    def __init__(self, name: str, feed_url: str, source_type: str) -> None:
        self.name = name
        self.feed_url = feed_url
        self.source_type = source_type

    def fetch(self) -> list[SourceItem]:
        request = Request(
            self.feed_url,
            headers={"User-Agent": "DailyReportBot/1.0"},
        )
        with urlopen(request, timeout=15) as response:
            xml_body = response.read()

        root = ElementTree.fromstring(xml_body)
        items: list[SourceItem] = []
        for node in root.findall(".//item"):
            title = (node.findtext("title") or "").strip()
            link = (node.findtext("link") or "").strip()
            description = (node.findtext("description") or "").strip()
            published = self._normalize_date(
                node.findtext("pubDate") or datetime.now(timezone.utc).isoformat()
            )
            if not title or not description:
                continue
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=title,
                    url=link,
                    published_at=published,
                    content=description,
                    tags=[],
                )
            )
        return items

    @staticmethod
    def _normalize_date(raw: str) -> str:
        try:
            return datetime.strptime(raw, "%a, %d %b %Y %H:%M:%S %z").isoformat()
        except ValueError:
            return raw


class KindRssSource(Source):
    def __init__(self, *, name: str, source_type: str = "공시", market_type: int = 0, limit: int = 15) -> None:
        self.name = name
        self.source_type = source_type
        self.market_type = market_type
        self.limit = limit

    def fetch(self) -> list[SourceItem]:
        feed_url = (
            f"{KIND_RSS_URL}&repIsuSrtCd=&mktTpCd={self.market_type}&searchCorpName=&currentPageSize={self.limit}"
        )
        xml_body = fetch_text(feed_url, encoding="utf-8")
        root = ElementTree.fromstring(xml_body)
        items: list[SourceItem] = []
        for node in root.findall(".//item"):
            title = (node.findtext("title") or "").strip()
            link = normalize_kind_url((node.findtext("link") or "").strip())
            author = (node.findtext("author") or "").strip()
            category = (node.findtext("category") or "").strip()
            published = RssSource._normalize_date(
                node.findtext("pubDate") or datetime.now(timezone.utc).isoformat()
            )
            if not title:
                continue
            parts = [
                f"회사: {author or '미기재'}",
                f"분류: {category or '미기재'}",
                f"제목: {title}",
            ]
            detail_text = fetch_kind_detail_text(link)
            if detail_text:
                parts.append(detail_text)
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=title,
                    url=link,
                    published_at=published,
                    content="\n".join(parts),
                    tags=[value for value in [author, category] if value],
                )
            )
        return items


def normalize_kind_url(url: str) -> str:
    return (
        url.strip()
        .replace("http://kind.krx.co.kr:80/", "https://kind.krx.co.kr/")
        .replace("http://kind.krx.co.kr/", "https://kind.krx.co.kr/")
        .replace("https://kind.krx.co.kr:80/", "https://kind.krx.co.kr/")
    )


def fetch_kind_detail_text(viewer_url: str) -> str:
    if not viewer_url:
        return ""
    viewer_html = fetch_text(viewer_url, encoding="utf-8")
    match = re.search(r"parent\.setPath\('([^']*)','([^']*)','([^']*)'", viewer_html)
    if not match:
        doc_match = re.search(r"<option value='(\d+)\|Y'selected=\"selected\">", viewer_html)
        if doc_match:
            contents_html = fetch_text(
                f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&docNo={doc_match.group(1)}",
                encoding="utf-8",
            )
            match = re.search(r"parent\.setPath\('([^']*)','([^']*)','([^']*)'", contents_html)
    if not match:
        return ""
    doc_url = normalize_kind_url(match.group(2))
    detail_html = fetch_text(doc_url, encoding="utf-8")
    body_match = re.search(r"<body[^>]*>(.*)</body>", detail_html, flags=re.S | re.I)
    body_html = body_match.group(1) if body_match else detail_html
    text = strip_tags(body_html)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text[:6000]


class OpenDartSource(Source):
    def __init__(
        self,
        *,
        name: str,
        api_key: str,
        source_type: str = "공시",
        limit: int = 10,
        corp_cls: str | None = None,
        days: int = 1,
    ) -> None:
        self.name = name
        self.api_key = api_key
        self.source_type = source_type
        self.limit = limit
        self.corp_cls = corp_cls
        self.days = days

    def fetch(self) -> list[SourceItem]:
        today = datetime.now(KST).date()
        start = today - timedelta(days=max(self.days - 1, 0))
        params = {
            "crtfc_key": self.api_key,
            "bgn_de": start.strftime("%Y%m%d"),
            "end_de": today.strftime("%Y%m%d"),
            "last_reprt_at": "Y",
            "sort": "date",
            "sort_mth": "desc",
            "page_count": str(self.limit),
        }
        if self.corp_cls:
            params["corp_cls"] = self.corp_cls

        body = fetch_text(f"{DART_LIST_API}?{urlencode(params)}", encoding="utf-8")
        payload = json.loads(body)
        status = payload.get("status")
        if status != "000":
            raise RuntimeError(payload.get("message", f"OpenDART error: {status}"))

        items: list[SourceItem] = []
        for row in payload.get("list", []):
            rcept_no = row["rcept_no"]
            viewer_url = f"{DART_VIEWER_BASE}?rcpNo={rcept_no}"
            pieces = [
                f"법인명: {row.get('corp_name', '')}",
                f"보고서명: {row.get('report_nm', '')}",
                f"제출인: {row.get('flr_nm', '') or '미기재'}",
                f"종목코드: {row.get('stock_code', '') or '비상장/미기재'}",
                f"법인구분: {row.get('corp_cls', '') or '미기재'}",
            ]
            if row.get("rm"):
                pieces.append(f"비고: {row['rm']}")
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=f"{row.get('corp_name', '기업')} - {row.get('report_nm', '공시')}",
                    url=viewer_url,
                    published_at=datetime.strptime(row["rcept_dt"], "%Y%m%d").replace(tzinfo=KST).isoformat(),
                    content="\n".join(pieces),
                    tags=[value for value in [row.get("stock_code"), row.get("corp_cls")] if value],
                )
            )
        return items


class NaverResearchSource(Source):
    def __init__(
        self,
        *,
        name: str,
        list_path: str,
        source_type: str,
        limit: int = 5,
        include_item_name: bool = False,
    ) -> None:
        self.name = name
        self.list_path = list_path
        self.source_type = source_type
        self.limit = limit
        self.include_item_name = include_item_name

    def fetch(self) -> list[SourceItem]:
        listing_html = fetch_text(urljoin(NAVER_FINANCE_BASE, self.list_path), encoding="euc-kr")
        rows = re.findall(r"<tr>(.*?)</tr>", listing_html, flags=re.S)
        items: list[SourceItem] = []
        for row_html in rows:
            detail_path = self._extract_detail_path(row_html)
            if not detail_path:
                continue
            title = self._extract_title(row_html, detail_path)
            broker = self._extract_broker(row_html)
            published_at_raw = self._extract_published_date(row_html)
            if not title or not broker or not published_at_raw:
                continue
            item_name = self._extract_item_name(row_html)
            detail_url = urljoin(NAVER_FINANCE_BASE, detail_path)
            pdf_url = self._extract_pdf_url(row_html)
            content = self._fetch_detail_content(detail_url)
            if not content:
                parts = [title, broker]
                if item_name:
                    parts.insert(0, item_name)
                if pdf_url:
                    parts.append(f"PDF: {pdf_url}")
                content = " | ".join(parts)
            display_title = f"{item_name}: {title}" if item_name and self.include_item_name else title
            tags = [broker]
            if item_name:
                tags.append(item_name)
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=display_title,
                    url=pdf_url or detail_url,
                    published_at=parse_naver_date(published_at_raw),
                    content=content,
                    tags=tags,
                )
            )
            if len(items) >= self.limit:
                break
        return items

    def _extract_detail_path(self, row_html: str) -> str | None:
        match = re.search(r'href="((?:company|market_info|industry|economy|invest)_read\.naver\?[^"]+)"', row_html)
        return html.unescape(match.group(1)) if match else None

    def _extract_title(self, row_html: str, detail_path: str) -> str:
        pattern = rf'href="{re.escape(detail_path)}">(.+?)</a>'
        match = re.search(pattern, row_html, flags=re.S)
        return strip_tags(match.group(1)) if match else ""

    def _extract_broker(self, row_html: str) -> str:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.S)
        cleaned = [strip_tags(cell) for cell in cells if strip_tags(cell)]
        if self.include_item_name and len(cleaned) >= 3:
            return cleaned[2]
        if not self.include_item_name and len(cleaned) >= 2:
            return cleaned[1]
        return ""

    def _extract_item_name(self, row_html: str) -> str:
        match = re.search(r'class="stock_item">(.+?)</a>', row_html, flags=re.S)
        return strip_tags(match.group(1)) if match else ""

    def _extract_pdf_url(self, row_html: str) -> str:
        match = re.search(r'href="(https://stock\.pstatic\.net/[^"]+\.pdf)"', row_html)
        return html.unescape(match.group(1)) if match else ""

    def _extract_published_date(self, row_html: str) -> str:
        match = re.search(r'<td class="date"[^>]*>(\d{2}\.\d{2}\.\d{2})</td>', row_html)
        return match.group(1) if match else ""

    def _fetch_detail_content(self, detail_url: str) -> str:
        detail_html = fetch_text(detail_url, encoding="euc-kr")
        match = re.search(
            r'<td colspan="2" class="view_cnt">\s*<div[^>]*>(.*?)</div>',
            detail_html,
            flags=re.S,
        )
        return strip_tags(match.group(1)) if match else ""


class HankyungConsensusSource(Source):
    def __init__(
        self,
        *,
        name: str,
        source_type: str,
        report_type: str | None = None,
        limit: int = 5,
        sdate: str | None = None,
        edate: str | None = None,
    ) -> None:
        self.name = name
        self.source_type = source_type
        self.report_type = report_type
        self.limit = limit
        today = datetime.now(KST).date().isoformat()
        self.sdate = sdate or today
        self.edate = edate or today

    def fetch(self) -> list[SourceItem]:
        params = {
            "sdate": self.sdate,
            "edate": self.edate,
        }
        if self.report_type:
            params["report_type"] = self.report_type
        html_text = fetch_text(f"{HANKYUNG_BASE}/analysis/list?{urlencode(params)}", encoding="utf-8")
        row_matches = re.findall(r"<tr(?: class=\"[^\"]+\")?>(.*?)</tr>", html_text, flags=re.S)
        items: list[SourceItem] = []
        for row_html in row_matches:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.S)
            if len(cells) < 5:
                continue
            date_text = strip_tags(cells[0])
            category = strip_tags(cells[1])
            link_match = re.search(r'href="(/analysis/downpdf\?report_idx=\d+)"', cells[2])
            title = strip_tags(cells[2])
            analyst = strip_tags(cells[3])
            broker = strip_tags(cells[4])
            if not date_text or not link_match or not title:
                continue
            summary_match = re.search(r'<div id="content_\d+" class="pop01 disNone">(.*?)</div>', cells[2], flags=re.S)
            summary = strip_tags(summary_match.group(1)) if summary_match else ""
            content_parts = [
                f"분류: {category}",
                f"증권사: {broker}",
            ]
            if analyst:
                content_parts.append(f"애널리스트: {analyst}")
            if summary:
                content_parts.append(summary)
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=title,
                    url=urljoin(HANKYUNG_BASE, html.unescape(link_match.group(1))),
                    published_at=datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=KST).isoformat(),
                    content="\n".join(content_parts),
                    tags=[value for value in [category, broker, analyst] if value],
                )
            )
            if len(items) >= self.limit:
                break
        return items


class WiseReportSource(Source):
    def __init__(self, *, name: str, source_type: str, limit: int = 8) -> None:
        self.name = name
        self.source_type = source_type
        self.limit = limit

    def fetch(self) -> list[SourceItem]:
        html_text = fetch_text(f"{WISEREPORT_BASE}/", encoding="utf-8")
        table_match = re.search(r'<tbody id="MainContent_display">(.*?)</tbody>', html_text, flags=re.S)
        if not table_match:
            return []
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), flags=re.S)
        items: list[SourceItem] = []
        for row_html in rows:
            report_match = re.search(
                r"na_Report_Login\('report','(\d+)','(\d+)','([^']+\.pdf)',''\)",
                row_html,
            )
            date_match = re.search(r"<td[^>]*align=\"center\"[^>]*>(\d{2}/\d{2}/\d{2})</td>", row_html)
            if not report_match or not date_match:
                continue
            category_match = re.search(r'<span class="r_pre">\[([^\]]+)\]</span>', row_html)
            category = category_match.group(1).strip() if category_match else ""
            anchor_match = re.search(r"<a[^>]*>(.*?)</a>", row_html, flags=re.S)
            anchor_html = anchor_match.group(1) if anchor_match else row_html
            title = strip_tags(re.sub(r'<span class="r_pre">\[[^\]]+\]</span>', "", anchor_html)).strip()
            if not title:
                continue
            rpt_id, broker_code, pdf_name = report_match.groups()
            url = (
                f"{WISEREPORT_BASE}/comm/LoadReport.aspx?"
                f"rpt_id={rpt_id}&brk_cd={broker_code}&fpath={pdf_name}&view_lang=K"
            )
            content_parts = [
                "출처: WiseReport / FnGuide",
                f"분류: {category or '미분류'}",
                f"리포트ID: {rpt_id}",
                f"브로커코드: {broker_code}",
                f"원문파일: {pdf_name}",
            ]
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=title,
                    url=url,
                    published_at=parse_slash_date(date_match.group(1)),
                    content="\n".join(content_parts),
                    tags=[value for value in [category, "WiseReport", "FnGuide"] if value],
                )
            )
            if len(items) >= self.limit:
                break
        return items


class EdgarCompanySource(Source):
    def __init__(
        self,
        *,
        name: str,
        cik: str,
        ticker: str = "",
        source_type: str = "미국 공시",
        forms: list[str] | None = None,
        limit: int = 5,
    ) -> None:
        self.name = name
        self.cik = cik.zfill(10)
        self.ticker = ticker
        self.source_type = source_type
        self.forms = forms or ["10-K", "10-Q", "8-K"]
        self.limit = limit

    def fetch(self) -> list[SourceItem]:
        headers = {"User-Agent": "DailyReportBot/1.0 dailyreport@example.com"}
        request = Request(f"{SEC_SUBMISSIONS_BASE}CIK{self.cik}.json", headers=headers)
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))

        recent = payload.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accession_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_documents = recent.get("primaryDocument", [])
        primary_descriptions = recent.get("primaryDocDescription", [])

        items: list[SourceItem] = []
        cik_numeric = str(int(self.cik))
        company_name = payload.get("name", self.name)
        ticker = self.ticker or (payload.get("tickers", [""]) or [""])[0]
        for index, form in enumerate(forms):
            if form not in self.forms:
                continue
            accession = accession_numbers[index]
            accession_slug = accession.replace("-", "")
            filing_date = filing_dates[index]
            primary_document = primary_documents[index]
            primary_description = primary_descriptions[index]
            detail_url = f"{SEC_ARCHIVES_BASE}{cik_numeric}/{accession_slug}/{accession}-index.htm"
            description_bits = [
                f"회사명: {company_name}",
                f"티커: {ticker or '미기재'}",
                f"폼: {form}",
                f"문서: {primary_document}",
            ]
            if primary_description:
                description_bits.append(f"설명: {primary_description}")
            items.append(
                SourceItem(
                    source_type=self.source_type,
                    source_name=self.name,
                    title=f"{company_name} {form}",
                    url=detail_url,
                    published_at=datetime.strptime(filing_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat(),
                    content="\n".join(description_bits),
                    tags=[value for value in [ticker, form] if value],
                )
            )
            if len(items) >= self.limit:
                break
        return items


class NaverNewsSectionSource(Source):
    def __init__(
        self,
        *,
        name: str,
        section_id: str,
        section_name: str,
        source_type: str = "뉴스",
        limit: int = 0,
        hours: int = 24,
    ) -> None:
        self.name = name
        self.section_id = section_id
        self.section_name = section_name
        self.source_type = source_type
        self.limit = limit
        self.hours = hours

    def fetch(self) -> list[SourceItem]:
        html_text = fetch_text(
            f"{NAVER_NEWS_BASE}/section/{self.section_id}",
            encoding="utf-8",
            headers={"Referer": NAVER_NEWS_BASE},
        )
        matches = re.findall(
            r'<a href="(https://n\.news\.naver\.com/mnews/article/[^"]+)" class="sa_text_title[^"]*".*?>(.*?)</a>',
            html_text,
            flags=re.S,
        )
        items: list[SourceItem] = []
        seen_urls: set[str] = set()
        for link, title_html in matches:
            normalized_url = normalize_news_url(link)
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)
            title = strip_tags(title_html)
            if not title:
                continue
            item = self._fetch_article(link, title)
            if item is None:
                continue
            if not within_last_hours(item.published_at, self.hours):
                break
            items.append(item)
            if self.limit > 0 and len(items) >= self.limit:
                break
        return items

    def _fetch_article(self, article_url: str, fallback_title: str) -> SourceItem | None:
        html_text = fetch_text(article_url, encoding="utf-8", headers={"Referer": NAVER_NEWS_BASE})
        title_match = re.search(r'<meta property="og:title" content="([^"]+)"', html_text)
        title = html.unescape(title_match.group(1)).strip() if title_match else fallback_title
        timestamp_match = re.search(r'data-date-time="(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"', html_text)
        if not timestamp_match:
            return None
        published_at = datetime.strptime(timestamp_match.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).isoformat()
        press_match = re.search(
            r'<a href="[^"]+" class="media_end_head_top_logo">.*?<img[^>]+alt="([^"]+)"',
            html_text,
            flags=re.S,
        )
        press_name = html.unescape(press_match.group(1)).strip() if press_match else self.name
        body_match = re.search(r'<article id="dic_area"[^>]*>(.*?)</article>', html_text, flags=re.S)
        article_text = strip_tags(body_match.group(1)) if body_match else ""
        if not article_text:
            description_match = re.search(r'<meta name="description" content="([^"]+)"', html_text)
            article_text = html.unescape(description_match.group(1)).strip() if description_match else ""
        article_text = article_text[:6000]
        content_parts = [
            f"섹션: {self.section_name}",
            f"언론사: {press_name}",
            article_text,
        ]
        return SourceItem(
            source_type=self.source_type,
            source_name=self.name,
            title=title,
            url=article_url,
            published_at=published_at,
            content="\n".join(part for part in content_parts if part),
            tags=[value for value in [self.section_name, press_name] if value],
        )


class ClienNewsSource(Source):
    def __init__(self, *, name: str, source_type: str = "뉴스", limit: int = 0, hours: int = 24) -> None:
        self.name = name
        self.source_type = source_type
        self.limit = limit
        self.hours = hours

    def fetch(self) -> list[SourceItem]:
        html_text = fetch_text(CLIEN_NEWS_URL, encoding="utf-8", headers={"Referer": "https://www.clien.net/service/"})
        matches = re.findall(
            r'<div class="list_item[^"]*?symph_row.*?<a class="list_subject" href="(?P<href>/service/board/news/\d+[^"]*)".*?'
            r'<span class="subject_fixed"[^>]*title="(?P<title>[^"]+)".*?'
            r'<span class="nickname">\s*<span title="(?P<author>[^"]+)">.*?'
            r'<span class="timestamp">(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})</span>',
            html_text,
            flags=re.S,
        )
        items: list[SourceItem] = []
        for href, title, author, timestamp in matches:
            published_at = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).isoformat()
            if not within_last_hours(published_at, self.hours):
                break
            detail_url = urljoin("https://www.clien.net", href.split("?", 1)[0])
            items.append(self._fetch_detail(detail_url, title, author, published_at))
            if self.limit > 0 and len(items) >= self.limit:
                break
        return items

    def _fetch_detail(self, detail_url: str, title: str, author: str, published_at: str) -> SourceItem:
        html_text = fetch_text(detail_url, encoding="utf-8", headers={"Referer": CLIEN_NEWS_URL})
        body_match = re.search(r'<div class="post_article"\s*>(.*?)</article>', html_text, flags=re.S)
        body_html = body_match.group(1) if body_match else ""
        body_text = strip_tags(body_html)[:6000]
        original_link = ""
        source_link_match = re.search(r"<strong>출처 : </strong></span><a href='([^']+)'", html_text)
        if source_link_match:
            original_link = html.unescape(source_link_match.group(1)).strip()
        if not original_link:
            original_link = first_external_link(body_html, {"clien.net"})
        content_parts = [
            "커뮤니티: 클리앙 새로운소식",
            f"작성자: {author}",
            f"게시글: {detail_url}",
        ]
        if original_link:
            content_parts.append(f"원문: {original_link}")
        if body_text:
            content_parts.append(body_text)
        return SourceItem(
            source_type=self.source_type,
            source_name=self.name,
            title=html.unescape(title).strip(),
            url=original_link or detail_url,
            published_at=published_at,
            content="\n".join(content_parts),
            tags=[value for value in ["클리앙", author] if value],
        )


class DamoangNewsSource(Source):
    def __init__(self, *, name: str, source_type: str = "뉴스", limit: int = 0, hours: int = 24) -> None:
        self.name = name
        self.source_type = source_type
        self.limit = limit
        self.hours = hours

    def fetch(self) -> list[SourceItem]:
        try:
            html_text = fetch_text(DAMOANG_NEWS_URL, encoding="utf-8", headers={"Referer": "https://damoang.net/"})
        except Exception:
            return []
        if "Attention Required! | Cloudflare" in html_text or "you have been blocked" in html_text.lower():
            return []
        return []


def build_sources() -> Iterable[Source]:
    if DEFAULT_CONFIG_FILE.exists():
        config = json.loads(DEFAULT_CONFIG_FILE.read_text(encoding="utf-8"))
        for row in config:
            if row["type"] == "rss":
                yield RssSource(
                    name=row["name"],
                    feed_url=row["feed_url"],
                    source_type=row["source_type"],
                )
            if row["type"] == "sample_file":
                yield SampleFileSource(Path(row.get("path", DEFAULT_SAMPLE_FILE)), row.get("name", "sample-seed"))
            if row["type"] == "naver_research":
                yield NaverResearchSource(
                    name=row["name"],
                    list_path=row["list_path"],
                    source_type=row["source_type"],
                    limit=int(row.get("limit", 5)),
                    include_item_name=bool(row.get("include_item_name", False)),
                )
            if row["type"] == "opendart":
                api_key = row.get("api_key") or os.environ.get(row.get("api_key_env", "OPENDART_API_KEY"), "")
                if api_key:
                    yield OpenDartSource(
                        name=row["name"],
                        api_key=api_key,
                        source_type=row.get("source_type", "공시"),
                        limit=int(row.get("limit", 10)),
                        corp_cls=row.get("corp_cls"),
                        days=int(row.get("days", 1)),
                    )
            if row["type"] == "hankyung_consensus":
                yield HankyungConsensusSource(
                    name=row["name"],
                    source_type=row.get("source_type", "증권사 리포트"),
                    report_type=row.get("report_type"),
                    limit=int(row.get("limit", 5)),
                    sdate=row.get("sdate"),
                    edate=row.get("edate"),
                )
            if row["type"] == "wisereport":
                yield WiseReportSource(
                    name=row["name"],
                    source_type=row.get("source_type", "증권사 리포트"),
                    limit=int(row.get("limit", 8)),
                )
            if row["type"] == "edgar_company":
                yield EdgarCompanySource(
                    name=row["name"],
                    cik=row["cik"],
                    ticker=row.get("ticker", ""),
                    source_type=row.get("source_type", "미국 공시"),
                    forms=row.get("forms"),
                    limit=int(row.get("limit", 5)),
                )
            if row["type"] == "naver_news_section":
                yield NaverNewsSectionSource(
                    name=row["name"],
                    section_id=str(row["section_id"]),
                    section_name=row["section_name"],
                    source_type=row.get("source_type", "뉴스"),
                    limit=int(row.get("limit", 10)),
                    hours=int(row.get("hours", 24)),
                )
            if row["type"] == "clien_news":
                yield ClienNewsSource(
                    name=row["name"],
                    source_type=row.get("source_type", "뉴스"),
                    limit=int(row.get("limit", 12)),
                    hours=int(row.get("hours", 24)),
                )
            if row["type"] == "damoang_news":
                yield DamoangNewsSource(
                    name=row["name"],
                    source_type=row.get("source_type", "뉴스"),
                    limit=int(row.get("limit", 12)),
                    hours=int(row.get("hours", 24)),
                )
            if row["type"] == "kind_rss":
                yield KindRssSource(
                    name=row["name"],
                    source_type=row.get("source_type", "공시"),
                    market_type=int(row.get("market_type", 0)),
                    limit=int(row.get("limit", 15)),
                )
        return

    yield NaverResearchSource(
        name="네이버 금융 종목분석",
        list_path="company_list.naver",
        source_type="증권사 리포트",
        limit=4,
        include_item_name=True,
    )
    yield NaverResearchSource(
        name="네이버 금융 시황정보",
        list_path="market_info_list.naver",
        source_type="증권사 리포트",
        limit=4,
    )
    yield SampleFileSource(DEFAULT_SAMPLE_FILE)
