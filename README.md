# DailyReport

증권사 리포트와 공시를 여러 사이트에서 모아두고, 로컬의 Codex CLI로 자동 요약해 보여주는 대시보드 MVP입니다.

## 포함된 기능

- SQLite 기반 저장소
- Python 표준 라이브러리만 사용하는 웹 서버
- Codex CLI 기반 자동 요약
- 네이버 금융 리서치 다중 카테고리 수집
- 한경 컨센서스 수집
- OpenDART 공시 수집 확장
- KIND 오늘의 공시 수집
- SEC EDGAR 회사별 공시 수집
- `sources.json` 기반 소스 관리

## 빠른 시작

```bash
python3 main.py
```

브라우저에서 `http://127.0.0.1:8000` 으로 접속한 뒤 `지금 수집 실행` 버튼을 누르면 샘플 데이터가 저장됩니다.
수집이 끝나면 Codex CLI가 각 항목을 자동으로 요약해 저장합니다.

## Codex CLI 사용 조건

- 로컬에 `codex` CLI가 설치되어 있어야 합니다.
- `codex login` 으로 ChatGPT 구독 계정 로그인 상태가 유지되어야 합니다.
- 이 버전은 OpenAI API 키가 필요 없습니다.

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

공시를 함께 수집하려면 `OPENDART_API_KEY` 환경변수를 넣어 실행하세요.

```bash
export OPENDART_API_KEY=your_opendart_key
python3 main.py
```

키가 없으면 네이버 금융 리서치만 수집되고, OpenDART 소스는 자동으로 건너뜁니다.

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

매일 오전 7시에 수집과 요약을 돌리고 싶다면 cron에서 아래처럼 호출할 수 있습니다.

```bash
0 7 * * 1-5 cd /home/deck/work/DailyReport && python3 -c "from pipeline import run_pipeline; print(run_pipeline())"
```

## 다음 확장 추천

- OpenDART API 전용 어댑터 추가
- 증권사 HTML/PDF 리포트 파서 추가
- 종목코드 기준 필터링
- 중요도 점수와 섹터별 묶음 요약
