"""Extract article text and photographs without embedding publisher pages."""
from __future__ import annotations

import re
from functools import lru_cache
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

ALLOWED_HOSTS = {'v.daum.net', 'news.naver.com', 'n.news.naver.com', 'www.clien.net', 'clien.net', 'damoang.net', 'www.damoang.net'}
BODY_IDS = {'harmonyContainer', 'dic_area', 'newsct_article', 'articleBodyContents', 'article-view-content-div'}
BODY_CLASSES = {'article_view', 'news_view', 'post_article', 'view-content', 'article-body', 'article_body', 'newsct_article'}
VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}
SKIP_TAGS = {'script', 'style', 'iframe', 'object', 'embed', 'form', 'button', 'nav', 'aside', 'footer', 'noscript', 'svg'}
NOISE = re.compile(r'(?:^|[-_\s])(ad|ads|advertisement|advert|banner|related|recommend|share|social|copyright|promotion)(?:$|[-_\s])', re.I)


def allowed_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {'http', 'https'} and parsed.hostname in ALLOWED_HOSTS and parsed.port in {None, 80, 443} and not parsed.username


class ArticleRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not allowed_url(newurl):
            raise ValueError('지원하지 않는 기사 이동 주소입니다.')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Node:
    def __init__(self, tag='', attrs=None):
        self.tag = tag
        self.attrs = dict(attrs or [])
        self.children = []


class ArticleParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node()
        self.stack = [self.root]
        self.candidates = []

    def handle_starttag(self, tag, attrs):
        node = Node(tag, attrs)
        self.stack[-1].children.append(node)
        classes = set(node.attrs.get('class', '').split())
        if node.attrs.get('id') in BODY_IDS or classes & BODY_CLASSES:
            self.candidates.append((2, node))
        elif tag == 'article':
            self.candidates.append((1, node))
        if tag not in VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        self.stack[-1].children.append(data)


def extract_article(markup: str, url: str) -> list[dict[str, str]]:
    parser = ArticleParser()
    parser.feed(markup)
    if not parser.candidates:
        return []
    # Prefer publisher body containers to broad article wrappers.
    priority = max(rank for rank, _ in parser.candidates)
    candidates = [node for rank, node in parser.candidates if rank == priority]

    def extract(root):
        blocks = []
        text = []
        images = set()

        def flush():
            value = re.sub(r'\s+', ' ', ''.join(text)).strip()
            text.clear()
            if value and not re.match(r'^(?:Copyright\b|ⓒ|©|저작권자\s*[©ⓒ]|무단\s*전재)', value, re.I):
                blocks.append({'type': 'text', 'text': value})

        def visit(node):
            if isinstance(node, str):
                text.append(node)
                return
            marker = node.attrs.get('id', '') + ' ' + node.attrs.get('class', '')
            if node.tag in SKIP_TAGS or NOISE.search(marker) or 'hidden' in node.attrs or node.attrs.get('aria-hidden') == 'true':
                return
            if node.tag == 'img':
                src = node.attrs.get('data-original') or node.attrs.get('data-src') or node.attrs.get('src', '')
                src = urljoin(url, src)
                if urlparse(src).scheme not in {'http', 'https'} or src in images:
                    return
                if any(node.attrs.get(key, '').isdigit() and int(node.attrs[key]) <= 3 for key in ('width', 'height')):
                    return
                flush()
                images.add(src)
                blocks.append({'type': 'image', 'url': src, 'alt': node.attrs.get('alt', '')})
                return
            boundary = node.tag in {'p', 'div', 'section', 'br', 'figure', 'figcaption', 'h2', 'h3', 'li', 'blockquote'}
            if boundary:
                flush()
            for child in node.children:
                visit(child)
            if boundary:
                flush()
        visit(root)
        flush()
        return blocks

    results = [extract(node) for node in candidates]
    return max(results, key=lambda blocks: sum(len(b.get('text', '')) for b in blocks), default=[])


@lru_cache(maxsize=128)
def load_article(url: str) -> list[dict[str, str]]:
    if not allowed_url(url):
        raise ValueError('이 사이트의 본문 읽기는 아직 지원하지 않습니다.')
    request = Request(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; DailyReport/1.0)'})
    with build_opener(ArticleRedirectHandler()).open(request, timeout=15) as response:
        charset = response.headers.get_content_charset() or 'utf-8'
        body = response.read(5 * 1024 * 1024 + 1)
        if len(body) > 5 * 1024 * 1024:
            raise ValueError('기사 페이지가 너무 큽니다.')
        blocks = extract_article(body.decode(charset, errors='replace'), response.url)
    if not blocks:
        raise ValueError('본문을 가져올 수 없습니다. 원문 링크에서 확인해 주세요.')
    return blocks
