# CLAUDE.md - Airbnb Seoul Demand Research Crawler

## Project Overview

서울 지하철역 주변 Airbnb 숙소 데이터를 크롤링하여 예약률/수익률을 분석하는 시스템.

## Tech Stack

- **Language**: Python 3.11+
- **Async HTTP**: httpx, curl_cffi (TLS fingerprint)
- **Browser Automation**: Playwright (API key extraction)
- **ORM/DB**: SQLAlchemy + SQLite
- **Scheduler**: APScheduler
- **Analysis**: pandas, plotly, folium, geopy
- **Dashboard**: Streamlit
- **Testing**: pytest, pytest-asyncio

## Project Structure

```
airbnb-demands-research/
├── config/          # 설정 (settings.py, stations.json)
├── crawler/         # 크롤러 모듈 (API client, search/calendar/listing crawler)
├── models/          # SQLAlchemy 모델 (database.py, schema.py)
├── scheduler/       # APScheduler 작업 정의
├── analysis/        # 분석 로직 (booking_rate, revenue, aggregator)
├── dashboard/       # Streamlit 대시보드
├── notebooks/       # Jupyter Notebook 분석 템플릿
├── tests/           # 테스트 (pytest)
├── scripts/         # 유틸리티 스크립트
├── data/            # SQLite DB, API credentials
├── logs/            # 크롤러 로그
└── main.py          # CLI 진입점
```

## Commands

### Run

```bash
python main.py --init            # DB 초기화 + 역 데이터 로드
python main.py --extract-key     # API 키 자동 추출
python main.py --once search     # 검색 크롤링 1회 실행
python main.py --once calendar   # 캘린더 크롤링 1회 실행
python main.py --once all        # 전체 크롤링 1회 실행
python main.py --status          # 상태 조회
python main.py                   # 스케줄러 모드 (기본)
```

### Dashboard

```bash
streamlit run dashboard/app.py
```

### Test

```bash
# 전체 테스트 실행
pytest

# 특정 모듈 테스트
pytest tests/test_airbnb_client.py
pytest tests/test_search_crawler.py

# 커버리지 포함 실행
pytest --cov=. --cov-report=term-missing

# 특정 테스트 함수만 실행
pytest tests/test_models.py::TestStation::test_create_station -v
```

## Testing Requirements

### 100% Test Coverage 필수

- **모든 코드 변경 시 테스트 커버리지 100%를 달성해야 합니다.**
- 새로운 기능 추가 시 반드시 해당 기능에 대한 테스트를 함께 작성합니다.
- 기존 코드 수정 시 관련 테스트가 모두 통과하는지 확인합니다.
- 커버리지 확인 명령어: `pytest --cov=. --cov-report=term-missing`
- 커버리지가 100% 미만인 경우, 누락된 라인에 대한 테스트를 추가해야 합니다.

### Testing Conventions

- 테스트 파일: `tests/test_<module_name>.py`
- 테스트 클래스: `Test<ClassName>`
- 테스트 함수: `test_<description>`
- 공통 fixture: `tests/conftest.py`
- 비동기 테스트: `pytest-asyncio` 사용 (`asyncio_mode = auto`)
- 외부 API 호출은 반드시 mock 처리

### Test Coverage Targets

| 모듈 | 커버리지 목표 |
|------|-------------|
| `crawler/` | 100% |
| `models/` | 100% |
| `analysis/` | 100% |
| `scheduler/` | 100% |
| `dashboard/` | 100% |
| `config/` | 100% |
| `main.py` | 100% |

## Code Style

- Python 타입 힌트 사용
- 비동기 코드는 `async/await` 패턴
- 로깅은 `logging` 모듈 사용 (`print` 대신)
- 문자열은 f-string 선호
- import 순서: stdlib → third-party → local

## Git Workflow

- 브랜치: `claude/airbnb-demand-crawler-ZV8vN`
- 커밋 메시지: 한글 또는 영문, 변경 내용을 명확히 기술
- Phase 단위로 커밋 관리

## Important Notes

- `data/.api_credentials.json`은 민감 정보 포함 — 커밋하지 않음
- Rate limit 준수: 요청 간 2~5초 딜레이
- 크롤링 대상: 서울 지하철 1~9호선 + 주요 광역노선 (약 300개 역)
- 검색 반경: 역 중심 반경 1km
