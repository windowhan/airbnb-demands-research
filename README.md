# Airbnb Seoul Demand Research Crawler

서울 지하철역 주변 Airbnb 숙소 데이터를 자동으로 수집하고, 예약률·수익률을 분석하는 시스템입니다.

## 기능

- 서울 지하철 1~9호선 주요 역(약 300개) 주변 Airbnb 숙소 자동 수집
- 시간별 검색 스냅샷 (숙소 수, 평균가격, 가용성)
- 일별 캘린더 크롤링 (향후 90일 예약 가용성/가격)
- 예약률 및 수익률 추정 분석
- Streamlit 대시보드 + Jupyter Notebook 분석 템플릿

## 요구사항

- Python 3.11+
- macOS / Linux

## 설치

### 1. 가상 환경 생성 및 의존성 설치

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Playwright 브라우저 설치

```bash
playwright install chromium
```

## 실행

### DB 초기화 (최초 1회)

```bash
source .venv/bin/activate
python main.py --init
```

역 데이터를 DB에 로드하고 테이블을 생성합니다.

### API 키 자동 추출

```bash
python main.py --extract-key
```

Airbnb 웹사이트에서 내부 API 키와 GraphQL 해시를 자동으로 추출하여 `data/.api_credentials.json`에 저장합니다.
크롤링 전 반드시 실행해야 합니다. (이후 캐시 유효 기간 내 자동 재사용)

### 크롤링 실행

```bash
# 검색 크롤링 1회 (역별 숙소 목록 수집)
python main.py --once search

# 캘린더 크롤링 1회 (숙소별 예약 가용성 수집)
python main.py --once calendar

# 전체 크롤링 1회 (검색 + 캘린더)
python main.py --once all
```

### 스케줄러 모드 (지속 운영)

```bash
python main.py
```

| 작업 | 주기 |
|------|------|
| 검색 스냅샷 | 매 시간 |
| 캘린더 크롤링 | 매일 새벽 |
| 숙소 상세 갱신 | 매주 |

### 상태 확인

```bash
python main.py --status
```

### 대시보드 실행

```bash
streamlit run dashboard/app.py
```

브라우저에서 `http://localhost:8501` 접속

### Jupyter Notebook

```bash
jupyter lab
```

`notebooks/` 디렉토리에서 분석 템플릿을 확인하세요.

## 프로젝트 구조

```
airbnb-demands-research/
├── config/
│   ├── settings.py          # 전역 설정 (검색 반경, 요청 주기 등)
│   └── stations.json        # 서울 지하철역 좌표 데이터
├── crawler/
│   ├── airbnb_client.py     # Airbnb 내부 API 클라이언트 (curl_cffi TLS 위장)
│   ├── api_key_extractor.py # API 키 자동 추출
│   ├── search_crawler.py    # 역별 숙소 목록 크롤러
│   ├── calendar_crawler.py  # 숙소 캘린더/가용성 크롤러
│   ├── listing_crawler.py   # 숙소 상세 정보 크롤러
│   ├── rate_limiter.py      # Rate limit 및 차단 감지
│   └── proxy_manager.py     # 프록시 관리
├── models/
│   ├── database.py          # DB 연결, 세션 관리
│   └── schema.py            # SQLAlchemy 모델 정의
├── scheduler/
│   └── jobs.py              # APScheduler 작업 정의
├── analysis/
│   ├── booking_rate.py      # 예약률 계산
│   ├── revenue.py           # 수익률 추정
│   └── aggregator.py        # 지역별/숙소별 집계
├── dashboard/
│   ├── app.py               # Streamlit 메인 앱
│   ├── pages/               # 대시보드 페이지 (현황/역별/유형별/수익 지도)
│   └── components/          # 재사용 차트 컴포넌트
├── notebooks/               # Jupyter 분석 템플릿 (4종)
├── tests/                   # pytest 테스트 (612개)
├── data/                    # SQLite DB, API 자격증명 (gitignore)
├── logs/                    # 크롤러 로그
├── main.py                  # CLI 진입점
└── requirements.txt
```

## 테스트

```bash
# 전체 테스트 실행
pytest

# 커버리지 포함
pytest --cov=. --cov-report=term-missing

# 특정 모듈만 테스트
pytest tests/test_search_crawler.py -v
```

## 주의사항

- `data/.api_credentials.json` — 민감 정보 포함, **절대 커밋하지 않습니다**
- Rate limit 준수: 요청 간 2~5초 딜레이 (기본값)
- 개인 연구 목적으로 사용하며, 과도한 요청은 자제하세요
- Airbnb가 API 구조를 변경하면 `crawler/search_crawler.py`의 `_extract_listings()` 메서드를 업데이트해야 합니다
