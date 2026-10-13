# DailyReport

증권사 리포트, 공시, 뉴스를 여러 사이트에서 모아두고, 제목과 원문 링크 중심으로 보여주는 대시보드입니다.

## 포함된 기능

- SQLite 기반 저장소
- Python 표준 라이브러리만 사용하는 웹 서버
- 네이버 금융 리서치 다중 카테고리 수집
- 한경 컨센서스 수집
- OpenDART 공시 수집 확장
- KIND 오늘의 공시 수집
- SEC EDGAR 회사별 공시 수집
- 다음 뉴스 홈/섹션 수집
- 클리앙 새로운소식 수집
- `sources.json` 기반 소스 관리

## 빠른 시작

```bash
python3 main.py
```

브라우저에서 `http://127.0.0.1:8000` 으로 접속하세요. 서버 시작 시 수집하고, 이후 3시간마다 갱신합니다. `지금 수집 실행` 버튼으로 수동 갱신할 수도 있습니다.

`소스별 수집 결과`에서 수집 건수, 수집 기간 내 항목 없음, API 키 미설정, 실패 사유를 확인할 수 있습니다. 수집에 실패해도 기존 저장 자료는 유지됩니다.

## 수집 범위와 중복 처리

- 다음 뉴스는 홈·섹션 HTML과 페이지에 포함된 카드 데이터를 함께 읽습니다. 매거진과 첫 화면에 숨겨진 카드도 포함하지만, 다음의 모든 과거 기사를 조회하는 방식은 아닙니다.
- 클리앙은 이미지 닉네임을 포함해 게시글별 제목·날짜를 읽고, 다음 페이지도 확인합니다. 기본 상한은 10페이지이며 `max_pages`로 변경할 수 있습니다.
- 뉴스 수집과 화면 표시의 기본 시작은 **한국 시간 어제 16시**입니다. `.env`의 `DAILY_REPORT_COLLECTION_DAYS=3`이면 3일 전 16시부터 표시합니다. 뉴스 소스의 기존 `hours` 필드 대신 이 공통 기준을 사용합니다.
- 화면과 `/api/items`는 기간 내 저장 항목 전체를 보여줍니다. 같은 URL은 한 번 표시하며, 제목이 같아도 URL이 다른 자료는 유지합니다.
- 기존 저장 자료는 갱신 시 삭제하지 않습니다. 기간이 지난 항목은 화면에서 제외됩니다.
- 기존 데이터베이스의 중복 제약은 시작 시 자동으로 갱신하며, 저장된 행과 ID는 보존합니다.
- 각 소스에 `"enabled": false`를 지정하면 수집 대상에서 제외됩니다. 증권사 리포트·공시의 기존 `limit`은 수집 건수 상한으로 유지됩니다.

## 소스 연결 방식

기본값은 루트의 `sources.json` 을 읽습니다.

현재 예시 설정에는 아래 소스가 포함되어 있습니다.

- 네이버 금융 종목분석
- 네이버 금융 시황정보
- 네이버 금융 산업분석
- 네이버 금융 투자전략
- 네이버 금융 경제분석
- OpenDART 오늘 공시
- KIND 오늘의 공시
- 한경 컨센서스
- SEC EDGAR 예시 회사
- 다음 뉴스 홈
- 다음 뉴스 주요 섹션
- 클리앙 새로운소식

설정 형식 예시는 아래와 같습니다.

```json
[
  {
    "type": "naver_research",
    "name": "네이버 금융 종목분석",
    "list_path": "company_list.naver",
    "source_type": "증권사 리포트",
    "include_item_name": true,
    "limit": 4
  },
  {
    "type": "opendart",
    "name": "OpenDART 오늘 공시",
    "source_type": "공시",
    "api_key_env": "OPENDART_API_KEY",
    "corp_cls": "Y",
    "days": 1,
    "limit": 8
  },
  {
    "type": "kind_rss",
    "name": "KIND 오늘의 공시",
    "source_type": "공시",
    "market_type": 0,
    "limit": 10
  },
  {
    "type": "hankyung_consensus",
    "name": "한경 컨센서스",
    "source_type": "증권사 리포트",
    "limit": 6
  },
  {
    "type": "edgar_company",
    "name": "SEC EDGAR AAPL",
    "source_type": "미국 공시",
    "cik": "0000320193",
    "ticker": "AAPL",
    "forms": ["10-K", "10-Q", "8-K"],
    "limit": 3
  }
]
```

## OpenDART 사용

공시를 함께 수집하려면 `.env.example`을 참고해 루트의 `.env`에 `OPENDART_API_KEY`를 설정하거나 환경변수를 넣어 실행하세요. `.env`는 자동으로 읽으며, 이미 설정된 환경변수가 우선합니다. 제목·링크 수집에는 OpenAI API 키가 필요하지 않습니다.

```bash
export OPENDART_API_KEY=your_opendart_key
python3 main.py
```

키가 없으면 OpenDART 소스만 자동으로 건너뛰고, 나머지 설정된 소스는 계속 수집됩니다.

## EDGAR 사용

미국 상장사는 `edgar_company` 타입으로 추가할 수 있습니다.

```json
{
  "type": "edgar_company",
  "name": "SEC EDGAR MSFT",
  "source_type": "미국 공시",
  "cik": "0000789019",
  "ticker": "MSFT",
  "forms": ["10-K", "10-Q", "8-K"],
  "limit": 3
}
```

## 참고

- `DART`, `OpenDART`, `SEC EDGAR`는 공식 공시 축입니다.
- `KIND`는 거래소 공시와 투자주의/불성실공시 문맥 확인에 좋은 보조 공식 축입니다.
- `네이버 금융`, `한경 컨센서스`는 증권사 리포트 축입니다.

## 아침 자동 실행 예시

매일 오전 7시에 수집을 돌리고 싶다면 cron에서 아래처럼 호출할 수 있습니다.

```bash
0 7 * * 1-5 cd /home/deck/work/DailyReport && python3 -c "from pipeline import run_pipeline; print(run_pipeline())"
```

## 다음 확장 추천

- 증권사 HTML/PDF 리포트 파서 추가
- 종목코드 기준 필터링
- 종목 중요도 점수와 섹터별 묶음 보기

## 테스트

```bash
python3 -m unittest discover -s tests -v
```

테스트는 네트워크 응답을 대체하고 임시 SQLite 파일을 사용하므로 실제 저장 자료에 영향을 주지 않습니다.
