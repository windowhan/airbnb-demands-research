#!/usr/bin/env python3
"""
Airbnb API 엔드투엔드 테스트 스크립트

1단계: API 키 자동 추출 (또는 캐시 로드)
2단계: 강남역 주변 숙소 검색 테스트
3단계: 결과 출력

사용법:
    python scripts/test_api.py               # 기본 (캐시 사용)
    python scripts/test_api.py --extract     # API 키 새로 추출
    python scripts/test_api.py --visible     # 브라우저 보이게 추출
"""

import asyncio
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.api_key_extractor import extract_api_credentials, get_cached_credentials
from crawler.airbnb_client import AirbnbClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def test_search(client: AirbnbClient):
    """강남역 주변 검색 테스트."""
    print("\n[TEST] 강남역 주변 숙소 검색...")
    checkin = date.today() + timedelta(days=7)
    checkout = checkin + timedelta(days=1)

    result = await client.search_stays(
        lat=37.4981,
        lng=127.0276,
        checkin=checkin,
        checkout=checkout,
        guests=2,
    )

    if result is None:
        print("  FAIL: 응답 없음 (차단되었거나 API 키가 유효하지 않음)")
        return False

    # 응답 구조 확인
    print(f"  응답 키: {list(result.keys())}")

    # 에러 체크
    if "errors" in result:
        print(f"  FAIL: API 에러: {json.dumps(result['errors'], indent=2, ensure_ascii=False)[:500]}")
        return False

    # 데이터 추출 시도
    try:
        search_results = (
            result.get("data", {})
            .get("presentation", {})
            .get("staysSearch", {})
            .get("results", {})
            .get("searchResults", [])
        )
        print(f"  검색된 숙소 수: {len(search_results)}")

        if search_results:
            first = search_results[0]
            listing = first.get("listing", {})
            pricing = first.get("pricingQuote", {})
            print(f"  첫 번째 숙소:")
            print(f"    이름: {listing.get('name', 'N/A')}")
            print(f"    ID: {listing.get('id', 'N/A')}")
            print(f"    유형: {listing.get('roomTypeCategory', 'N/A')}")
            print(f"    좌표: {listing.get('coordinate', {})}")
            print(f"    평점: {listing.get('avgRating', 'N/A')}")
            print(f"    리뷰: {listing.get('reviewsCount', 'N/A')}개")
            print(f"    가격 데이터: {json.dumps(pricing, ensure_ascii=False)[:200]}")
            return True
        else:
            # 대체 구조 탐색
            print("  표준 경로에서 결과 없음. 응답 구조 탐색 중...")
            print(f"  응답 미리보기: {json.dumps(result, ensure_ascii=False)[:500]}")
            return False

    except Exception as e:
        print(f"  ERROR: 파싱 실패: {e}")
        print(f"  응답 미리보기: {json.dumps(result, ensure_ascii=False)[:500]}")
        return False


async def test_calendar(client: AirbnbClient, listing_id: str):
    """캘린더 조회 테스트."""
    print(f"\n[TEST] 숙소 {listing_id} 캘린더 조회...")
    today = date.today()

    result = await client.get_calendar(
        listing_id=listing_id,
        month=today.month,
        year=today.year,
        count=1,
    )

    if result is None:
        print("  FAIL: 응답 없음")
        return False

    if "errors" in result:
        print(f"  FAIL: API 에러: {json.dumps(result['errors'], ensure_ascii=False)[:300]}")
        return False

    print(f"  응답 키: {list(result.keys())}")
    try:
        calendar_data = (
            result.get("data", {})
            .get("merlin", {})
            .get("pdpAvailabilityCalendar", {})
            .get("calendarMonths", [])
        )
        if calendar_data:
            month = calendar_data[0]
            days = month.get("days", [])
            available = sum(1 for d in days if d.get("available"))
            print(f"  {month.get('month', '?')}월: {len(days)}일 중 {available}일 예약 가능")
            if days:
                sample = days[0]
                print(f"  첫 날: available={sample.get('available')}, "
                      f"price={sample.get('price', {})}")
            return True
        else:
            print(f"  응답 미리보기: {json.dumps(result, ensure_ascii=False)[:500]}")
            return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


async def main():
    args = sys.argv[1:]
    do_extract = "--extract" in args
    visible = "--visible" in args

    print("=" * 60)
    print("  Airbnb API End-to-End Test")
    print("=" * 60)

    # Step 1: API 키 확보
    print("\n[STEP 1] API 키 확보...")
    creds = get_cached_credentials()

    if do_extract or not creds or not creds.get("api_key"):
        print("  캐시 없음 → Playwright로 자동 추출 시작...")
        creds = await extract_api_credentials(headless=not visible)
    else:
        print(f"  캐시에서 로드: {creds['api_key'][:8]}...{creds['api_key'][-4:]}")

    if not creds.get("api_key"):
        print("\n  FATAL: API 키를 추출할 수 없습니다.")
        print("  '--visible' 옵션으로 재시도하거나 수동으로 AIRBNB_API_KEY 환경변수를 설정하세요.")
        sys.exit(1)

    print(f"  API Key: {creds['api_key'][:8]}...{creds['api_key'][-4:]}")
    print(f"  Operation Hashes: {list(creds.get('hashes', {}).keys())}")

    # Step 2: 검색 테스트
    print("\n[STEP 2] API 클라이언트 초기화...")
    client = AirbnbClient(api_key=creds["api_key"])

    search_ok = await test_search(client)

    # Step 3: 캘린더 테스트 (검색 성공 시)
    if search_ok:
        # 검색 결과에서 listing_id 추출
        result = await client.search_stays(
            lat=37.4981, lng=127.0276,
            checkin=date.today() + timedelta(days=7),
            checkout=date.today() + timedelta(days=8),
        )
        if result:
            try:
                listings = (
                    result.get("data", {})
                    .get("presentation", {})
                    .get("staysSearch", {})
                    .get("results", {})
                    .get("searchResults", [])
                )
                if listings:
                    lid = listings[0].get("listing", {}).get("id")
                    if lid:
                        await test_calendar(client, str(lid))
            except Exception:
                pass

    await client.close()

    # 결과 요약
    print("\n" + "=" * 60)
    print("  테스트 완료!")
    if search_ok:
        print("  검색 API: OK")
    else:
        print("  검색 API: FAIL")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
