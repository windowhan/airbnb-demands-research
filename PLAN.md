# Airbnb 서울 지역별 수요 조사 크롤러 - 프로젝트 계획

## 1. 프로젝트 개요

서울 지하철역 주변의 Airbnb 숙소 데이터를 실시간 크롤링하여
시간별/일자별 예약률, 수익률을 분석하는 시스템.

---

## 2. 기술 스택

| 구분 | 기술 |
|------|------|
| 언어 | Python 3.11+ |
| 크롤링 | httpx (비동기 HTTP) + playwright (필요시 브라우저 렌더링) |
| 스케줄러 | APScheduler |
| DB | SQLite (+ SQLAlchemy ORM) |
| 대시보드 | Streamlit |
| 분석 | Jupyter Notebook + pandas + matplotlib/plotly |
| 좌표/지도 | folium (지도 시각화), geopy (거리 계산) |

---

## 3. 프로젝트 구조

```
airbnb-demands-research/
├── config/
│   ├── settings.py          # 전역 설정 (검색 반경, 주기 등)
│   └── stations.json        # 서울 지하철역 좌표 데이터
├── crawler/
│   ├── __init__.py
│   ├── airbnb_client.py     # Airbnb 내부 API 클라이언트
│   ├── search_crawler.py    # 검색 결과 크롤러 (숙소 목록)
│   ├── calendar_crawler.py  # 캘린더/가용성 크롤러
│   ├── listing_crawler.py   # 개별 숙소 상세 정보 크롤러
│   └── proxy_manager.py     # 프록시/Rate Limit 관리
├── models/
│   ├── __init__.py
│   ├── database.py          # DB 연결, 세션 관리
│   └── schema.py            # SQLAlchemy 모델 정의
├── scheduler/
│   ├── __init__.py
│   └── jobs.py              # 스케줄러 작업 정의
├── analysis/
│   ├── __init__.py
│   ├── booking_rate.py      # 예약률 계산 로직
│   ├── revenue.py           # 수익률 추정 로직
│   └── aggregator.py        # 지역별/숙소별 집계
├── dashboard/
│   ├── app.py               # Streamlit 메인 앱
│   ├── pages/
│   │   ├── overview.py      # 전체 현황 대시보드
│   │   ├── station_detail.py # 역별 상세 분석
│   │   ├── listing_type.py  # 숙소 유형별 분석
│   │   └── revenue_map.py   # 수익률 지도
│   └── components/
│       └── charts.py        # 재사용 차트 컴포넌트
├── notebooks/
│   ├── 01_data_exploration.ipynb     # 데이터 탐색
│   ├── 02_booking_rate_analysis.ipynb # 예약률 심층 분석
│   ├── 03_revenue_analysis.ipynb     # 수익률 심층 분석
│   └── 04_regional_comparison.ipynb  # 지역 비교 분석
├── main.py                  # 크롤러 실행 진입점
├── requirements.txt
└── PLAN.md
```

---

## 4. 데이터 모델 (SQLite)

### 4.1 stations (지하철역)
```
id            INTEGER PRIMARY KEY
name          TEXT          -- 역 이름 (예: "강남")
line          TEXT          -- 호선 (예: "2호선")
district      TEXT          -- 구 (예: "강남구")
latitude      REAL
longitude     REAL
```

### 4.2 listings (숙소)
```
id            INTEGER PRIMARY KEY
airbnb_id     TEXT UNIQUE   -- Airbnb 숙소 ID
name          TEXT
host_id       TEXT
room_type     TEXT          -- entire_home / private_room / shared_room
latitude      REAL
longitude     REAL
nearest_station_id  INTEGER FK
bedrooms      INTEGER
bathrooms     REAL
max_guests    INTEGER
base_price    REAL          -- 기본 1박 가격 (KRW)
first_seen    DATETIME
last_seen     DATETIME
```

### 4.3 search_snapshots (검색 스냅샷 - 시간별)
```
id            INTEGER PRIMARY KEY
station_id    INTEGER FK
crawled_at    DATETIME      -- 크롤링 시각
total_listings    INTEGER   -- 검색된 총 숙소 수
avg_price         REAL      -- 평균 1박 가격
min_price         REAL
max_price         REAL
available_count   INTEGER   -- 예약 가능 숙소 수
unavailable_count INTEGER   -- 예약 불가 숙소 수
checkin_date      DATE      -- 검색 기준 체크인 날짜
checkout_date     DATE      -- 검색 기준 체크아웃 날짜
```

### 4.4 calendar_snapshots (캘린더 스냅샷 - 일별)
```
id            INTEGER PRIMARY KEY
listing_id    INTEGER FK
crawled_at    DATETIME
date          DATE          -- 해당 날짜
available     BOOLEAN       -- 예약 가능 여부
price         REAL          -- 해당 날짜 가격
min_nights    INTEGER       -- 최소 숙박일
```

### 4.5 daily_stats (일별 집계)
```
id            INTEGER PRIMARY KEY
station_id    INTEGER FK
date          DATE
room_type     TEXT
total_listings    INTEGER
booked_count      INTEGER   -- 예약된 숙소 수 (추정)
booking_rate      REAL      -- 예약률 (0~1)
avg_daily_price   REAL      -- 평균 일일 가격
estimated_revenue REAL      -- 추정 일일 총 수익
```

---

## 5. 핵심 로직

### 5.1 Airbnb 내부 API 역공학

Airbnb 웹사이트는 내부적으로 다음 API를 호출합니다:

1. **검색 API**: `GET /api/v3/StaysSearch`
   - 파라미터: 위도/경도, 체크인/체크아웃, 게스트 수, 필터
   - 응답: 숙소 목록 + 가격 + 평점 + 위치

2. **캘린더 API**: `GET /api/v3/PdpAvailabilityCalendar`
   - 파라미터: listing_id, 월
   - 응답: 날짜별 가용성 + 가격

3. **숙소 상세 API**: `GET /api/v3/StayListing`
   - 파라미터: listing_id
   - 응답: 상세 정보 (방 유형, 편의시설, 호스트 정보 등)

### 5.2 예약률 계산

```
예약률 = 예약불가 날짜 수 / 전체 날짜 수

주의: "예약불가"가 반드시 "예약됨"을 의미하지는 않음.
호스트가 직접 차단한 날짜도 포함될 수 있음.
→ 시간 경과에 따른 변화를 추적하여 보정
  (이전에 "가능"이었다가 "불가"로 바뀐 날짜 = 실제 예약)
```

### 5.3 수익률 추정

```
일일 추정 수익 = 예약된 날짜의 가격 합계
월간 추정 수익 = 해당 월의 일일 추정 수익 합계
연간 수익률 = (연간 추정 수익 / 숙소 추정 가치) × 100
```

---

## 6. 스케줄링 전략

| 작업 | 주기 | 설명 |
|------|------|------|
| 검색 스냅샷 | 매 시간 | 각 역 주변 검색 결과 수, 평균가격, 가용성 |
| 캘린더 크롤링 | 매일 1회 (새벽) | 전체 숙소의 향후 90일 캘린더 |
| 숙소 상세 갱신 | 매주 1회 | 신규 숙소 발견, 기존 숙소 정보 업데이트 |
| 일별 통계 집계 | 매일 1회 | daily_stats 테이블 계산 및 저장 |

### Rate Limit 대응
- 요청 간 랜덤 딜레이 (2~5초)
- User-Agent 로테이션
- 필요시 프록시 로테이션
- 실패 시 지수 백오프 재시도

---

## 7. 서울 지하철역 데이터

- **대상**: 서울 지하철 1~9호선 + 주요 광역노선 (총 약 300개 역)
- **검색 반경**: 각 역 중심 반경 1km
- **우선순위**: 주요 관광/비즈니스 지역 역부터 시작
  - 1순위: 강남, 홍대입구, 명동, 이태원, 잠실, 동대문, 종로 등 (약 30개)
  - 2순위: 기타 주요 역 (약 70개)
  - 3순위: 나머지 전체 역

---

## 8. 대시보드 (Streamlit)

### 페이지 구성

1. **전체 현황**
   - 서울 지도 위에 역별 예약률 히트맵
   - 실시간 크롤링 상태 모니터
   - 오늘의 주요 지표 (총 숙소 수, 평균 예약률, 평균 가격)

2. **역별 상세**
   - 특정 역 선택 → 시간별 예약률 추이 그래프
   - 주변 숙소 목록 + 지도
   - 가격대 분포

3. **숙소 유형별 분석**
   - 전체집 / 개인실 / 호텔 등 유형별 비교
   - 유형별 예약률, 평균가격, 수익률

4. **수익률 지도**
   - 지역별 추정 월 수익 히트맵
   - 상위 수익 숙소/지역 랭킹

---

## 9. Jupyter Notebook 분석

1. **데이터 탐색**: 수집된 데이터 기본 통계, 분포, 이상치 확인
2. **예약률 심층 분석**: 요일별/시간대별/시즌별 예약 패턴
3. **수익률 심층 분석**: 가격 전략, 수익 최적화 포인트
4. **지역 비교 분석**: 구별/역별 경쟁도, 포화도, 수익 잠재력

---

## 10. 구현 순서 (Phase)

### Phase 1: 기반 구축
- [ ] 프로젝트 구조 생성
- [ ] 서울 지하철역 좌표 데이터 수집/정리
- [ ] SQLite DB 스키마 구축
- [ ] Airbnb API 클라이언트 기본 구현

### Phase 2: 핵심 크롤러
- [ ] 검색 크롤러 구현 (역 주변 숙소 목록)
- [ ] 캘린더 크롤러 구현 (가용성 데이터)
- [ ] 숙소 상세 크롤러 구현
- [ ] Rate limit / 에러 핸들링

### Phase 3: 스케줄러 + 데이터 파이프라인
- [ ] APScheduler 설정 (시간별/일별 작업)
- [ ] 예약률 계산 로직
- [ ] 수익률 추정 로직
- [ ] 일별 집계 파이프라인

### Phase 4: 시각화
- [ ] Streamlit 대시보드 기본 레이아웃
- [ ] 지도 시각화 (folium)
- [ ] 시계열 차트 (plotly)
- [ ] Jupyter Notebook 분석 템플릿

### Phase 5: 고도화
- [ ] 프록시 로테이션
- [ ] 데이터 품질 모니터링
- [ ] 알림 시스템 (예약률 급변 등)
- [ ] 데이터 내보내기 (CSV/Excel)

---

## 11. 주의사항 및 리스크

| 리스크 | 대응 |
|--------|------|
| Airbnb가 API 구조를 변경할 수 있음 | API 응답 파싱을 모듈화하여 빠른 대응 가능하게 설계 |
| IP 차단 가능성 | Rate limit 준수, 프록시 사용, 요청 간격 조절 |
| "예약불가 = 호스트 차단" 오탐 | 시계열 추적으로 실제 예약 vs 호스트 차단 구분 |
| 법적 리스크 (웹 스크래핑) | 개인 연구 목적 사용, robots.txt 존중, 과도한 요청 자제 |
| 300개 역 × 매시간 = 대량 요청 | 우선순위 기반 단계적 확장 |
