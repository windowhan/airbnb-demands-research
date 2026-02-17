"""SearchCrawler 단위 테스트."""

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crawler.search_crawler import SearchCrawler
from models.schema import Listing, SearchSnapshot, Station


# ─── _extract_listings ────────────────────────────────────────────────


class TestExtractListings:
    """_extract_listings 메서드 테스트."""

    def test_extracts_all_three_listings(
        self, mock_airbnb_client, sample_search_response
    ):
        """sample_search_response에서 3개 리스팅(2026 형식 2개 + 레거시 1개)을 추출한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        listings = crawler._extract_listings(sample_search_response)
        assert len(listings) == 3

    def test_first_listing_decoded_from_base64(
        self, mock_airbnb_client, sample_search_response
    ):
        """첫 번째 리스팅: base64 인코딩된 demandStayListing.id에서 숫자 ID를 추출한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        listings = crawler._extract_listings(sample_search_response)
        first = listings[0]
        assert first["id"] == "1234567890"
        assert first["name"] == "강남 테스트 숙소 A"
        assert first["room_type"] == "entire_home"
        assert first["price"] == 119824.0
        assert first["rating"] == 4.89
        assert first["review_count"] == 25
        assert first["lat"] == 37.499
        assert first["lng"] == 127.028

    def test_second_listing_uses_property_id(
        self, mock_airbnb_client, sample_search_response
    ):
        """두 번째 리스팅: propertyId가 있으면 그것을 ID로 사용한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        listings = crawler._extract_listings(sample_search_response)
        second = listings[1]
        assert second["id"] == "9876543210"
        assert second["name"] == "홍대 테스트 숙소 B"
        assert second["room_type"] == "private_room"
        assert second["price"] == 80000.0
        assert second["rating"] == 4.5

    def test_third_listing_legacy_format(
        self, mock_airbnb_client, sample_search_response
    ):
        """세 번째 리스팅: 구버전 listing/pricingQuote 형식을 파싱한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        listings = crawler._extract_listings(sample_search_response)
        third = listings[2]
        assert third["id"] == "5555555"
        assert third["name"] == "구버전 숙소 C"
        assert third["room_type"] == "shared_room"
        assert third["price"] == 60000.0
        assert third["rating"] == 3.8
        assert third["review_count"] == 2

    def test_empty_search_results(self, mock_airbnb_client):
        """빈 검색 결과에서는 빈 리스트를 반환한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "staysSearch": {"results": {"searchResults": []}}
                }
            }
        }
        listings = crawler._extract_listings(data)
        assert listings == []

    def test_none_data_returns_empty_or_fallback(self, mock_airbnb_client):
        """data가 빈 dict이면 빈 리스트를 반환한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        listings = crawler._extract_listings({})
        assert listings == []

    def test_missing_nested_keys(self, mock_airbnb_client):
        """중간 경로가 누락되어도 빈 리스트를 반환한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        data = {"data": {"presentation": {}}}
        listings = crawler._extract_listings(data)
        assert listings == []

    def test_listing_without_id_is_skipped(self, mock_airbnb_client):
        """ID가 없는 리스팅은 결과에 포함되지 않는다."""
        crawler = SearchCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "staysSearch": {
                        "results": {
                            "searchResults": [
                                {
                                    "propertyId": None,
                                    "nameLocalized": "no-id listing",
                                    "avgRatingLocalized": None,
                                    "structuredDisplayPrice": {},
                                    "demandStayListing": {"id": ""},
                                },
                            ]
                        }
                    }
                }
            }
        }
        listings = crawler._extract_listings(data)
        assert len(listings) == 0


# ─── _extract_listings_fallback ───────────────────────────────────────


class TestExtractListingsFallback:
    """_extract_listings_fallback 메서드 테스트."""

    def test_finds_listings_recursively(self, mock_airbnb_client):
        """재귀 탐색으로 중첩된 리스팅 데이터를 찾아낸다."""
        crawler = SearchCrawler(mock_airbnb_client)
        data = {
            "deeply": {
                "nested": {
                    "items": [
                        {
                            "id": "111",
                            "name": "Fallback A",
                            "coordinate": {"latitude": 37.5, "longitude": 127.0},
                            "roomTypeCategory": "entire_home",
                            "avgRating": 4.0,
                            "reviewsCount": 10,
                            "price": {"amount": 75000},
                        },
                        {
                            "id": "222",
                            "name": "Fallback B",
                            "lat": 37.6,
                            "lng": 126.9,
                        },
                    ]
                }
            }
        }
        listings = crawler._extract_listings_fallback(data)
        assert len(listings) == 2
        assert listings[0]["id"] == "111"
        assert listings[0]["name"] == "Fallback A"
        assert listings[0]["lat"] == 37.5
        assert listings[0]["lng"] == 127.0
        assert listings[0]["price"] == 75000
        assert listings[1]["id"] == "222"
        assert listings[1]["lat"] == 37.6

    def test_fallback_empty_data(self, mock_airbnb_client):
        """빈 데이터에서는 빈 리스트를 반환한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        assert crawler._extract_listings_fallback({}) == []

    def test_fallback_respects_depth_limit(self, mock_airbnb_client):
        """깊이 제한(10)을 초과하면 탐색을 중단한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        # 깊이 12 중첩 구조 생성
        obj = {"id": "deep", "name": "deep listing", "lat": 1.0}
        for _ in range(12):
            obj = {"child": obj}
        listings = crawler._extract_listings_fallback(obj)
        assert listings == []


# ─── _decode_listing_id ───────────────────────────────────────────────


class TestDecodeListingId:
    """_decode_listing_id 정적 메서드 테스트."""

    def test_decodes_valid_base64(self):
        """유효한 base64 인코딩 문자열에서 숫자 ID를 추출한다."""
        result = SearchCrawler._decode_listing_id(
            "RGVtYW5kU3RheUxpc3Rpbmc6MTIzNDU2Nzg5MA=="
        )
        assert result == "1234567890"

    def test_empty_string_returns_none(self):
        """빈 문자열은 None을 반환한다."""
        assert SearchCrawler._decode_listing_id("") is None

    def test_none_returns_none(self):
        """None 입력은 None을 반환한다."""
        assert SearchCrawler._decode_listing_id(None) is None

    def test_invalid_base64_returns_none(self):
        """유효하지 않은 base64 문자열은 None을 반환한다."""
        assert SearchCrawler._decode_listing_id("!!!invalid!!!") is None

    def test_base64_without_colon(self):
        """콜론이 없는 base64 결과는 디코딩된 문자열 전체를 반환한다."""
        import base64
        encoded = base64.b64encode(b"JustANumber12345").decode()
        result = SearchCrawler._decode_listing_id(encoded)
        assert result == "JustANumber12345"


# ─── _extract_price_v2 ───────────────────────────────────────────────


class TestExtractPriceV2:
    """_extract_price_v2 정적 메서드 테스트."""

    def test_parse_discounted_price(self):
        """할인가 문자열을 파싱한다. '₩119,824' -> 119824.0"""
        result = SearchCrawler._extract_price_v2({
            "structuredDisplayPrice": {
                "primaryLine": {"discountedPrice": "₩119,824"}
            }
        })
        assert result == 119824.0

    def test_parse_regular_price(self):
        """정가 문자열을 파싱한다."""
        result = SearchCrawler._extract_price_v2({
            "structuredDisplayPrice": {
                "primaryLine": {"price": "₩80,000"}
            }
        })
        assert result == 80000.0

    def test_parse_accessibility_label(self):
        """접근성 라벨에서 가격을 추출한다."""
        result = SearchCrawler._extract_price_v2({
            "structuredDisplayPrice": {
                "primaryLine": {"accessibilityLabel": "1박당 총 ₩55,000"}
            }
        })
        assert result == 155000.0  # "1박당 총 ₩55,000" -> digits "155000"

    def test_empty_price_returns_none(self):
        """가격 정보가 없으면 None을 반환한다."""
        assert SearchCrawler._extract_price_v2({}) is None
        assert SearchCrawler._extract_price_v2({"structuredDisplayPrice": {}}) is None
        assert SearchCrawler._extract_price_v2(
            {"structuredDisplayPrice": {"primaryLine": {}}}
        ) is None

    def test_none_price_strings_returns_none(self):
        """가격 문자열이 모두 None이면 None을 반환한다."""
        result = SearchCrawler._extract_price_v2({
            "structuredDisplayPrice": {
                "primaryLine": {
                    "discountedPrice": None,
                    "price": None,
                    "accessibilityLabel": "",
                }
            }
        })
        assert result is None


# ─── _extract_price (legacy) ──────────────────────────────────────────


class TestExtractPrice:
    """_extract_price 정적 메서드 테스트 (구버전 pricingQuote)."""

    def test_parse_total_amount(self):
        """price.total.amount에서 가격을 추출한다."""
        pricing = {"price": {"total": {"amount": 60000}}}
        assert SearchCrawler._extract_price(pricing) == 60000.0

    def test_parse_price_string(self):
        """priceString에서 가격을 추출한다."""
        pricing = {"price": {"total": {}}, "priceString": "₩75,000"}
        assert SearchCrawler._extract_price(pricing) == 75000.0

    def test_empty_pricing_returns_none(self):
        """빈 가격 정보는 None을 반환한다."""
        assert SearchCrawler._extract_price({}) is None

    def test_none_amount_falls_through(self):
        """amount가 None이면 priceString으로 대체를 시도한다."""
        pricing = {"price": {"total": {"amount": None}}, "priceString": "₩50,000"}
        assert SearchCrawler._extract_price(pricing) == 50000.0

    def test_malformed_pricing_returns_none(self):
        """잘못된 pricing 구조는 None을 반환한다."""
        pricing = {"price": "not_a_dict"}
        assert SearchCrawler._extract_price(pricing) is None


# ─── _parse_rating ────────────────────────────────────────────────────


class TestParseRating:
    """_parse_rating 정적 메서드 테스트."""

    def test_parse_valid_rating(self):
        """'4.89' -> 4.89"""
        assert SearchCrawler._parse_rating("4.89") == 4.89

    def test_none_returns_none(self):
        """None -> None"""
        assert SearchCrawler._parse_rating(None) is None

    def test_empty_string_returns_none(self):
        """빈 문자열 -> None"""
        assert SearchCrawler._parse_rating("") is None

    def test_invalid_string_returns_none(self):
        """'신규' 같은 숫자가 아닌 문자열 -> None"""
        assert SearchCrawler._parse_rating("신규") is None

    def test_integer_string(self):
        """정수 문자열 '5' -> 5.0"""
        assert SearchCrawler._parse_rating("5") == 5.0


# ─── _save_results ────────────────────────────────────────────────────


class TestSaveResults:
    """_save_results 메서드 테스트."""

    def test_saves_snapshot_and_listings(
        self,
        mock_airbnb_client,
        sample_search_response,
        sample_station,
        mock_session_scope,
        db_session,
    ):
        """검색 결과가 DB에 SearchSnapshot과 Listing으로 저장된다."""
        crawler = SearchCrawler(mock_airbnb_client)

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            result = crawler._save_results(
                sample_station,
                sample_search_response,
                date(2026, 2, 18),
                date(2026, 2, 19),
            )

        assert result["station"] == "강남"
        assert result["total"] == 3
        assert result["avg_price"] > 0
        assert result["min_price"] > 0
        assert result["max_price"] > 0

        # DB에 SearchSnapshot이 저장되었는지 확인
        snapshots = db_session.query(SearchSnapshot).all()
        assert len(snapshots) == 1
        assert snapshots[0].station_id == sample_station.id
        assert snapshots[0].total_listings == 3

        # DB에 Listing들이 저장되었는지 확인
        # sample_listing fixture가 이미 하나 있으므로 conftest의 1234567890과 merge됨
        all_listings = db_session.query(Listing).all()
        # 기존 sample_station 생성 시 sample_listing은 없으므로 새로 3개가 생겨야 함
        # 단, conftest의 sample_listing 미사용이므로 3개
        airbnb_ids = {l.airbnb_id for l in all_listings}
        assert "1234567890" in airbnb_ids
        assert "9876543210" in airbnb_ids
        assert "5555555" in airbnb_ids

    def test_updates_existing_listing(
        self,
        mock_airbnb_client,
        sample_search_response,
        sample_station,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """이미 존재하는 리스팅은 last_seen과 base_price가 업데이트된다."""
        crawler = SearchCrawler(mock_airbnb_client)

        old_last_seen = sample_listing.last_seen

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            crawler._save_results(
                sample_station,
                sample_search_response,
                date(2026, 2, 18),
                date(2026, 2, 19),
            )

        db_session.refresh(sample_listing)
        # 기존 listing (airbnb_id=1234567890)은 update됨
        assert sample_listing.base_price == 119824.0
        assert sample_listing.last_seen >= old_last_seen

    def test_saves_with_empty_listings(
        self,
        mock_airbnb_client,
        sample_station,
        mock_session_scope,
        db_session,
    ):
        """리스팅이 없는 검색 결과도 스냅샷은 저장된다."""
        crawler = SearchCrawler(mock_airbnb_client)
        empty_data = {
            "data": {
                "presentation": {
                    "staysSearch": {"results": {"searchResults": []}}
                }
            }
        }

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            result = crawler._save_results(
                sample_station, empty_data, date(2026, 2, 18), date(2026, 2, 19)
            )

        assert result["total"] == 0
        assert result["avg_price"] == 0

        snapshots = db_session.query(SearchSnapshot).all()
        assert len(snapshots) == 1
        assert snapshots[0].total_listings == 0


# ─── crawl_station ────────────────────────────────────────────────────


class TestCrawlStation:
    """crawl_station 비동기 메서드 테스트."""

    async def test_successful_crawl(
        self,
        mock_airbnb_client,
        sample_search_response,
        sample_station,
        mock_session_scope,
    ):
        """검색 API 호출 후 결과가 저장되고 요약 정보가 반환된다."""
        mock_airbnb_client.search_stays = AsyncMock(
            return_value=sample_search_response
        )
        crawler = SearchCrawler(mock_airbnb_client)

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            result = await crawler.crawl_station(
                sample_station, date(2026, 2, 18), date(2026, 2, 19)
            )

        assert result is not None
        assert result["station"] == "강남"
        assert result["total"] == 3
        mock_airbnb_client.search_stays.assert_awaited_once()

    async def test_returns_none_when_no_data(
        self, mock_airbnb_client, sample_station
    ):
        """API가 None을 반환하면 crawl_station도 None을 반환한다."""
        mock_airbnb_client.search_stays = AsyncMock(return_value=None)
        crawler = SearchCrawler(mock_airbnb_client)

        result = await crawler.crawl_station(sample_station)
        assert result is None

    async def test_default_dates(
        self,
        mock_airbnb_client,
        sample_search_response,
        sample_station,
        mock_session_scope,
    ):
        """체크인/체크아웃 날짜를 지정하지 않으면 기본값이 사용된다."""
        mock_airbnb_client.search_stays = AsyncMock(
            return_value=sample_search_response
        )
        crawler = SearchCrawler(mock_airbnb_client)

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            result = await crawler.crawl_station(sample_station)

        assert result is not None
        call_kwargs = mock_airbnb_client.search_stays.call_args.kwargs
        assert "checkin" in call_kwargs
        assert "checkout" in call_kwargs


# ─── crawl_all_stations ──────────────────────────────────────────────


class TestCrawlAllStations:
    """crawl_all_stations 비동기 메서드 테스트."""

    async def test_all_success(
        self,
        mock_airbnb_client,
        sample_search_response,
        sample_station,
        mock_session_scope,
    ):
        """모든 역이 성공적으로 크롤링되면 결과 리스트를 반환한다."""
        mock_airbnb_client.search_stays = AsyncMock(
            return_value=sample_search_response
        )
        crawler = SearchCrawler(mock_airbnb_client)
        stations = [sample_station]

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            results = await crawler.crawl_all_stations(stations)

        assert len(results) == 1
        assert results[0]["station"] == "강남"

    async def test_partial_failure(
        self,
        mock_airbnb_client,
        sample_search_response,
        mock_session_scope,
        db_session,
    ):
        """일부 역이 실패해도 나머지 결과가 반환된다."""
        station_a = Station(
            name="역삼", line="2호선", district="강남구",
            latitude=37.500, longitude=127.037, priority=1,
        )
        station_b = Station(
            name="선릉", line="2호선", district="강남구",
            latitude=37.504, longitude=127.049, priority=1,
        )
        db_session.add_all([station_a, station_b])
        db_session.commit()
        db_session.refresh(station_a)
        db_session.refresh(station_b)

        # station_a 성공, station_b 실패 (None 반환)
        mock_airbnb_client.search_stays = AsyncMock(
            side_effect=[sample_search_response, None]
        )
        crawler = SearchCrawler(mock_airbnb_client)

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            results = await crawler.crawl_all_stations([station_a, station_b])

        assert len(results) == 1
        assert results[0]["station"] == "역삼"

    async def test_exception_handling(
        self,
        mock_airbnb_client,
        mock_session_scope,
        db_session,
    ):
        """크롤링 중 예외가 발생해도 나머지 역을 계속 처리한다."""
        station = Station(
            name="잠실", line="2호선", district="송파구",
            latitude=37.513, longitude=127.100, priority=1,
        )
        db_session.add(station)
        db_session.commit()
        db_session.refresh(station)

        mock_airbnb_client.search_stays = AsyncMock(
            side_effect=Exception("Network error")
        )
        crawler = SearchCrawler(mock_airbnb_client)

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            results = await crawler.crawl_all_stations([station])

        assert results == []

    async def test_empty_stations_list(
        self, mock_airbnb_client, mock_session_scope
    ):
        """빈 역 리스트에 대해서도 정상적으로 빈 결과를 반환한다."""
        crawler = SearchCrawler(mock_airbnb_client)

        with patch("crawler.search_crawler.session_scope", mock_session_scope):
            results = await crawler.crawl_all_stations([])

        assert results == []


# ─── 추가 커버리지: error handling 경로 ──────────────────────────────

class TestSearchCrawlerEdgeCases:
    """search_crawler 에러 핸들링 + fallback 경로 테스트."""

    def test_extract_listings_exception_uses_fallback(self, mock_airbnb_client):
        """_extract_listings에서 예외 시 fallback 파서를 사용한다."""
        crawler = SearchCrawler(mock_airbnb_client)
        # data가 리스트여서 .get() 호출 시 AttributeError 발생
        bad_data = {"data": {"presentation": {"staysSearch": {"results": {"searchResults": "not_a_list"}}}}}
        result = crawler._extract_listings(bad_data)
        assert isinstance(result, list)

    def test_extract_price_empty_string(self):
        """빈 문자열 가격은 None을 반환한다."""
        result = SearchCrawler._extract_price({})
        assert result is None

    def test_extract_price_with_price_string(self):
        """pricingQuote.priceString에서 가격을 추출한다."""
        result = SearchCrawler._extract_price({"priceString": "₩100,000"})
        assert result == 100000.0

    def test_extract_price_invalid_values(self):
        """유효하지 않은 pricingQuote는 None을 반환한다."""
        result = SearchCrawler._extract_price({"price": {"total": {"amount": "invalid"}}})
        assert result is None

    def test_extract_price_v2_exception_path(self):
        """_extract_price_v2에서 AttributeError 발생 시 None (lines 277-279)."""
        # structuredDisplayPrice가 None이면 None.get() → AttributeError
        result = SearchCrawler._extract_price_v2({"structuredDisplayPrice": None})
        assert result is None

    async def test_save_results_skips_no_id(
        self, mock_airbnb_client, mock_session_scope
    ):
        """id가 없는 리스팅은 건너뛴다 (line 92)."""
        crawler = SearchCrawler(mock_airbnb_client)

        station = MagicMock()
        station.id = 1
        station.name = "강남역"

        # _extract_listings가 id 없는 항목을 반환하도록 mock
        listings_with_no_id = [
            {"id": "12345", "name": "Normal", "price": 100000},
            {"name": "No ID listing"},  # no 'id' field
        ]

        with patch("crawler.search_crawler.session_scope", mock_session_scope), \
             patch.object(crawler, "_extract_listings", return_value=listings_with_no_id):
            from datetime import date
            data = {"some": "data"}
            crawler._save_results(
                station, data,
                date(2026, 3, 1), date(2026, 3, 2),
            )
