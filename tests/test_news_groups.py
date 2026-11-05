import unittest
from datetime import datetime, timedelta, timezone

import news_groups

BASE = datetime(2026, 11, 5, 9, tzinfo=timezone.utc)


def item(title, minutes=0, kind="news"):
    return {"title": title, "at": BASE - timedelta(minutes=minutes), "kind": kind}


def group(items):
    return news_groups.group_similar(items, title=lambda i: i["title"], published=lambda i: i["at"], groupable=lambda i: i["kind"] == "news")


class NewsGroupTests(unittest.TestCase):
    def test_near_identical_titles_group_under_first_item(self):
        groups = group([
            item('트럼프 "이란전 탓 패배 대체로 맞지만 헤그세스 안바꿔"'),
            item("다른 기사 제목입니다 전혀 관련 없음", 1),
            item('[美중간선거] 트럼프 "이란전 탓 패배 대체로 맞지만 헤그세스 안 바꿔"', 2),
            item('北 "신형 대공미사일 시험발사…장거리순항미사일 훈련도"', 3),
            item('북 "신형 대공미사일 시험발사...장거리순항미사일 훈련도"', 4),
        ])
        self.assertEqual([len(g) for g in groups], [2, 1, 2])
        self.assertTrue(groups[0][0]["title"].startswith("트럼프"))

    def test_same_title_far_apart_or_non_news_stays_separate(self):
        groups = group([item("코스피 2% 급등 마감 외국인 순매수"), item("코스피 2% 급등 마감 외국인 순매수", 7 * 60)])
        self.assertEqual(len(groups), 2)
        groups = group([item("삼성전자 실적 리뷰 메모리 회복", kind="report"), item("삼성전자 실적 리뷰 메모리 회복", 1, kind="report")])
        self.assertEqual(len(groups), 2)

    def test_series_headers_differing_only_in_bracket_stay_separate(self):
        groups = group([item("【오늘의 국회일정】 2026년 11월 5일"), item("【오늘의 법조】 2026년 11월 5일", 1)])
        self.assertEqual(len(groups), 2)

    def test_loosely_related_headlines_stay_separate(self):
        groups = group([item("SKT, 부산불꽃축제에 AI 통신 품질 관리"), item('SKT, 부산불꽃축제 통신망 점검…"AI로 체감품질 관리"', 1)])
        self.assertEqual(len(groups), 2)


if __name__ == "__main__":
    unittest.main()
