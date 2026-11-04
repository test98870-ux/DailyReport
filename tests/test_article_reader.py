import unittest
from article_reader import extract_article, allowed_url


class ArticleReaderTests(unittest.TestCase):
    def test_extracts_body_in_order_without_ads_or_scripts(self):
        markup = '''<header>메뉴</header><article><h1>제목</h1>
        <div class="article_view"><p>기사 <b>첫 문단</b></p>
        <aside>광고</aside><div class="ad-banner"><img src="ad.jpg">광고</div>
        <script>alert(1)</script><figure><img data-src="/photo.jpg" alt="현장"><figcaption>사진 설명</figcaption></figure>
        <p>두 번째 문단</p><div class="related">추천 기사</div></div></article><footer>메뉴</footer>'''
        blocks = extract_article(markup, 'https://v.daum.net/v/123')
        self.assertEqual(blocks, [
            {'type': 'text', 'text': '기사 첫 문단'},
            {'type': 'image', 'url': 'https://v.daum.net/photo.jpg', 'alt': '현장'},
            {'type': 'text', 'text': '사진 설명'},
            {'type': 'text', 'text': '두 번째 문단'},
        ])

    def test_missing_body_does_not_show_whole_page(self):
        self.assertEqual(extract_article('<body><nav>메뉴</nav><p>광고</p></body>', 'https://v.daum.net'), [])

    def test_skips_unsafe_and_tracking_images(self):
        blocks = extract_article('<div id="dic_area"><p>본문</p><img src="javascript:alert(1)"><img src="/pixel" width="1"></div>', 'https://n.news.naver.com/x')
        self.assertEqual(blocks, [{'type': 'text', 'text': '본문'}])

    def test_restricts_fetch_to_news_sources(self):
        self.assertTrue(allowed_url('https://v.daum.net/v/123'))
        for url in ['file:///etc/passwd', 'http://127.0.0.1', 'https://v.daum.net.evil.test', 'https://v.daum.net:8080/', 'https://user@v.daum.net/']:
            self.assertFalse(allowed_url(url))
