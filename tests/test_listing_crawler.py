"""ListingCrawler 단위 테스트."""

import base64
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crawler.listing_crawler import ListingCrawler
from models.schema import CrawlLog, Listing, Station


# ─── _extract_detail ──────────────────────────────────────────────────


class TestExtractDetail:
    """_extract_detail 메서드 테스트."""

    def test_extracts_full_detail(
        self, mock_airbnb_client, sample_pdp_sections_response
    ):
        """sample_pdp_sections_response에서 모든 상세 정보를 추출한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        detail = crawler._extract_detail(sample_pdp_sections_response)

        assert detail is not None
        assert detail["max_guests"] == 4
        assert detail["room_type"] == "entire_home"
        assert detail["bedrooms"] == 2
        assert detail["bathrooms"] == 1
        assert detail["rating"] == 4.9
        assert detail["review_count"] == 150
        assert detail["host_id"] == "123456"

    def test_book_it_sidebar_max_guests(
        self, mock_airbnb_client, sample_pdp_sections_response
    ):
        """BOOK_IT_SIDEBAR 섹션에서 maxGuestCapacity를 추출한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        detail = crawler._extract_detail(sample_pdp_sections_response)
        assert detail["max_guests"] == 4

    def test_availability_calendar_description_items(
        self, mock_airbnb_client, sample_pdp_sections_response
    ):
        """AVAILABILITY_CALENDAR_DEFAULT 섹션의 descriptionItems에서 방 정보를 추출한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        detail = crawler._extract_detail(sample_pdp_sections_response)
        assert detail["room_type"] == "entire_home"
        assert detail["bedrooms"] == 2
        assert detail["bathrooms"] == 1

    def test_meet_your_host_section(
        self, mock_airbnb_client, sample_pdp_sections_response
    ):
        """MEET_YOUR_HOST 섹션에서 호스트 정보와 리뷰 수를 추출한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        detail = crawler._extract_detail(sample_pdp_sections_response)
        assert detail["host_id"] == "123456"
        assert detail["rating"] == 4.9
        assert detail["review_count"] == 150

    def test_policies_max_guests_fallback(self, mock_airbnb_client):
        """POLICIES_DEFAULT에서 '게스트 정원 N명'을 추출한다 (max_guests가 없을 때)."""
        crawler = ListingCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {
                            "sections": [
                                {
                                    "sectionComponentType": "POLICIES_DEFAULT",
                                    "section": {
                                        "houseRules": [
                                            {"title": "게스트 정원 6명"},
                                        ],
                                    },
                                },
                            ]
                        }
                    }
                }
            }
        }
        detail = crawler._extract_detail(data)
        assert detail is not None
        assert detail["max_guests"] == 6

    def test_overview_section_legacy(self, mock_airbnb_client):
        """OVERVIEW 섹션(구버전)에서 방 정보를 추출한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {
                            "sections": [
                                {
                                    "sectionComponentType": "OVERVIEW_DEFAULT",
                                    "section": {
                                        "roomTypeCategory": "private_room",
                                        "bedrooms": 1,
                                        "bathrooms": 1.5,
                                        "personCapacity": 3,
                                    },
                                },
                            ]
                        }
                    }
                }
            }
        }
        detail = crawler._extract_detail(data)
        assert detail is not None
        assert detail["room_type"] == "private_room"
        assert detail["bedrooms"] == 1
        assert detail["bathrooms"] == 1.5
        assert detail["max_guests"] == 3

    def test_host_profile_legacy(self, mock_airbnb_client):
        """HOST_PROFILE 섹션(구버전)에서 호스트 ID를 추출한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {
                            "sections": [
                                {
                                    "sectionComponentType": "HOST_PROFILE_DEFAULT",
                                    "section": {
                                        "hostAvatar": {"userId": "12345"},
                                    },
                                },
                            ]
                        }
                    }
                }
            }
        }
        detail = crawler._extract_detail(data)
        assert detail is not None
        assert detail["host_id"] == "12345"

    def test_empty_sections_returns_none(self, mock_airbnb_client):
        """섹션이 빈 리스트이면 None을 반환한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {"sections": []}
                    }
                }
            }
        }
        detail = crawler._extract_detail(data)
        assert detail is None

    def test_missing_data_returns_none(self, mock_airbnb_client):
        """data 키가 없으면 None을 반환한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        assert crawler._extract_detail({}) is None

    def test_none_section_value_handled(self, mock_airbnb_client):
        """section이 None인 항목이 있어도 에러 없이 처리한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {
                            "sections": [
                                {
                                    "sectionComponentType": "BOOK_IT_SIDEBAR",
                                    "section": None,
                                },
                                {
                                    "sectionComponentType": "MEET_YOUR_HOST",
                                    "section": {"cardData": {"ratingAverage": 4.5}},
                                },
                            ]
                        }
                    }
                }
            }
        }
        detail = crawler._extract_detail(data)
        assert detail is not None
        assert detail["rating"] == 4.5


# ─── _parse_description_items ─────────────────────────────────────────


class TestParseDescriptionItems:
    """_parse_description_items 정적 메서드 테스트."""

    def test_entire_home(self):
        """'공동 주택 전체' -> entire_home"""
        detail = {}
        section = {"descriptionItems": [{"title": "공동 주택 전체"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "entire_home"

    def test_private_room(self):
        """'개인실' -> private_room"""
        detail = {}
        section = {"descriptionItems": [{"title": "개인실"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "private_room"

    def test_shared_room(self):
        """'다인실' -> shared_room"""
        detail = {}
        section = {"descriptionItems": [{"title": "다인실"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "shared_room"

    def test_shared_room_via_공유(self):
        """'공유 공간' -> shared_room"""
        detail = {}
        section = {"descriptionItems": [{"title": "공유 공간"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "shared_room"

    def test_hotel(self):
        """'호텔 객실' -> hotel"""
        detail = {}
        section = {"descriptionItems": [{"title": "호텔 객실"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "hotel"

    def test_bedrooms_count(self):
        """'침실 2개'에서 침실 수를 추출한다."""
        detail = {}
        section = {"descriptionItems": [{"title": "침실 2개"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["bedrooms"] == 2

    def test_bed_count_as_bedroom_fallback(self):
        """침실 수가 없으면 '침대 1개'에서 대체한다."""
        detail = {}
        section = {"descriptionItems": [{"title": "침대 1개"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["bedrooms"] == 1

    def test_bathroom_count(self):
        """'욕실 1개'에서 욕실 수를 추출한다."""
        detail = {}
        section = {"descriptionItems": [{"title": "욕실 1개"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["bathrooms"] == 1

    def test_multiple_items_combined(self):
        """여러 descriptionItems에서 모든 정보를 한꺼번에 추출한다."""
        detail = {}
        section = {
            "descriptionItems": [
                {"title": "공동 주택 전체"},
                {"title": "침실 3개"},
                {"title": "욕실 2개"},
            ]
        }
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "entire_home"
        assert detail["bedrooms"] == 3
        assert detail["bathrooms"] == 2

    def test_none_description_items(self):
        """descriptionItems가 None이면 에러 없이 처리한다."""
        detail = {}
        section = {"descriptionItems": None}
        ListingCrawler._parse_description_items(section, detail)
        assert detail == {}

    def test_missing_description_items_key(self):
        """descriptionItems 키가 없어도 에러 없이 처리한다."""
        detail = {}
        section = {}
        ListingCrawler._parse_description_items(section, detail)
        assert detail == {}

    def test_does_not_overwrite_existing_room_type(self):
        """이미 room_type이 설정되어 있으면 덮어쓰지 않는다."""
        detail = {"room_type": "private_room"}
        section = {"descriptionItems": [{"title": "공동 주택 전체"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert detail["room_type"] == "private_room"

    def test_unrecognized_title_ignored(self):
        """인식할 수 없는 title은 무시한다."""
        detail = {}
        section = {"descriptionItems": [{"title": "무료 주차장"}]}
        ListingCrawler._parse_description_items(section, detail)
        assert "room_type" not in detail


# ─── _decode_user_id ──────────────────────────────────────────────────


class TestDecodeUserId:
    """_decode_user_id 정적 메서드 테스트."""

    def test_decodes_valid_base64(self):
        """유효한 base64 인코딩 문자열에서 숫자 ID를 추출한다."""
        result = ListingCrawler._decode_user_id("RGVtYW5kVXNlcjoxMjM0NTY=")
        assert result == "123456"

    def test_returns_original_on_invalid_base64(self):
        """유효하지 않은 base64 문자열은 원본 문자열을 반환한다."""
        result = ListingCrawler._decode_user_id("!!!invalid!!!")
        assert result == "!!!invalid!!!"

    def test_plain_numeric_id(self):
        """숫자만 있는 base64 인코딩 (콜론 없음)은 디코딩된 문자열을 반환한다."""
        encoded = base64.b64encode(b"987654321").decode()
        result = ListingCrawler._decode_user_id(encoded)
        assert result == "987654321"

    def test_empty_string_returns_empty(self):
        """빈 문자열은 빈 문자열을 반환한다 (base64('')는 b'' -> '')."""
        # base64.b64decode("") -> b"" -> ""로 디코딩됨
        result = ListingCrawler._decode_user_id("")
        assert result == ""


# ─── _update_listing ──────────────────────────────────────────────────


class TestUpdateListing:
    """_update_listing 메서드 테스트."""

    def test_updates_all_fields(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """모든 상세 필드가 업데이트된다."""
        crawler = ListingCrawler(mock_airbnb_client)
        detail = {
            "room_type": "private_room",
            "bedrooms": 2,
            "bathrooms": 1.5,
            "max_guests": 4,
            "host_id": "999888",
            "rating": 4.95,
            "review_count": 200,
        }

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            crawler._update_listing(sample_listing, detail)

        db_session.refresh(sample_listing)
        assert sample_listing.room_type == "private_room"
        assert sample_listing.bedrooms == 2
        assert sample_listing.bathrooms == 1.5
        assert sample_listing.max_guests == 4
        assert sample_listing.host_id == "999888"
        assert sample_listing.rating == 4.95
        assert sample_listing.review_count == 200

    def test_partial_update(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """일부 필드만 있는 detail은 해당 필드만 업데이트한다."""
        crawler = ListingCrawler(mock_airbnb_client)
        original_room_type = sample_listing.room_type
        detail = {"rating": 4.8, "review_count": 50}

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            crawler._update_listing(sample_listing, detail)

        db_session.refresh(sample_listing)
        assert sample_listing.rating == 4.8
        assert sample_listing.review_count == 50
        assert sample_listing.room_type == original_room_type

    def test_nonexistent_listing_ignored(
        self,
        mock_airbnb_client,
        mock_session_scope,
        db_session,
    ):
        """DB에 존재하지 않는 listing은 에러 없이 무시된다."""
        crawler = ListingCrawler(mock_airbnb_client)
        fake_listing = MagicMock()
        fake_listing.id = 99999  # 존재하지 않는 ID

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            # 예외 없이 실행되어야 한다
            crawler._update_listing(fake_listing, {"rating": 5.0})

    def test_last_seen_updated(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """_update_listing 호출 시 last_seen이 업데이트된다."""
        crawler = ListingCrawler(mock_airbnb_client)
        old_last_seen = sample_listing.last_seen

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            crawler._update_listing(sample_listing, {"rating": 4.0})

        db_session.refresh(sample_listing)
        assert sample_listing.last_seen >= old_last_seen


# ─── crawl_listing_detail ────────────────────────────────────────────


class TestCrawlListingDetail:
    """crawl_listing_detail 비동기 메서드 테스트."""

    async def test_successful_crawl(
        self,
        mock_airbnb_client,
        sample_pdp_sections_response,
        sample_listing,
        mock_session_scope,
    ):
        """상세 API 호출 후 결과가 DB에 저장되고 True를 반환한다."""
        mock_airbnb_client.get_listing_detail = AsyncMock(
            return_value=sample_pdp_sections_response
        )
        crawler = ListingCrawler(mock_airbnb_client)

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            result = await crawler.crawl_listing_detail(sample_listing)

        assert result is True
        mock_airbnb_client.get_listing_detail.assert_awaited_once_with(
            sample_listing.airbnb_id
        )

    async def test_returns_false_when_no_data(
        self, mock_airbnb_client, sample_listing
    ):
        """API가 None을 반환하면 False를 반환한다."""
        mock_airbnb_client.get_listing_detail = AsyncMock(return_value=None)
        crawler = ListingCrawler(mock_airbnb_client)

        result = await crawler.crawl_listing_detail(sample_listing)
        assert result is False

    async def test_returns_false_when_no_detail(
        self, mock_airbnb_client, sample_listing
    ):
        """API 응답에서 detail을 추출할 수 없으면 False를 반환한다."""
        mock_airbnb_client.get_listing_detail = AsyncMock(
            return_value={"data": {"presentation": {"stayProductDetailPage": {"sections": {"sections": []}}}}}
        )
        crawler = ListingCrawler(mock_airbnb_client)

        result = await crawler.crawl_listing_detail(sample_listing)
        assert result is False


# ─── crawl_all_listings ──────────────────────────────────────────────


class TestCrawlAllListings:
    """crawl_all_listings 비동기 메서드 테스트."""

    async def test_all_success(
        self,
        mock_airbnb_client,
        sample_pdp_sections_response,
        sample_listing,
        mock_session_scope,
    ):
        """모든 리스팅이 성공적으로 크롤링되면 success 요약을 반환한다."""
        mock_airbnb_client.get_listing_detail = AsyncMock(
            return_value=sample_pdp_sections_response
        )
        crawler = ListingCrawler(mock_airbnb_client)

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings([sample_listing])

        assert summary["total"] == 1
        assert summary["success"] == 1
        assert summary["failed"] == 0

    async def test_partial_failure(
        self,
        mock_airbnb_client,
        sample_pdp_sections_response,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """일부 리스팅이 실패하면 partial 상태로 기록된다."""
        listing_b = Listing(
            airbnb_id="fail_listing",
            name="실패 숙소",
            room_type="entire_home",
            latitude=37.5,
            longitude=127.0,
            nearest_station_id=sample_listing.nearest_station_id,
            first_seen=datetime.utcnow(),
            last_seen=datetime.utcnow(),
        )
        db_session.add(listing_b)
        db_session.commit()
        db_session.refresh(listing_b)

        # 첫 번째 성공, 두 번째 None 반환(실패)
        mock_airbnb_client.get_listing_detail = AsyncMock(
            side_effect=[sample_pdp_sections_response, None]
        )
        crawler = ListingCrawler(mock_airbnb_client)

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings(
                [sample_listing, listing_b]
            )

        assert summary["total"] == 2
        assert summary["success"] == 1
        assert summary["failed"] == 1

    async def test_exception_handling(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
    ):
        """크롤링 중 예외가 발생해도 나머지를 계속 처리한다."""
        mock_airbnb_client.get_listing_detail = AsyncMock(
            side_effect=Exception("Connection refused")
        )
        crawler = ListingCrawler(mock_airbnb_client)

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings([sample_listing])

        assert summary["total"] == 1
        assert summary["success"] == 0
        assert summary["failed"] == 1

    async def test_empty_listings_list(
        self, mock_airbnb_client, mock_session_scope
    ):
        """빈 리스팅 리스트에 대해서도 정상적으로 반환한다."""
        crawler = ListingCrawler(mock_airbnb_client)

        with patch("crawler.listing_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings([])

        assert summary["total"] == 0
        assert summary["success"] == 0
        assert summary["failed"] == 0


# ─── 추가 커버리지: 리스팅 파싱 에러 경로 ─────────────────────────────

class TestListingCrawlerEdgeCases:
    """listing_crawler 에러 핸들링 + fallback 경로 테스트."""

    def test_review_count_value_error(self, mock_airbnb_client):
        """review_count가 숫자가 아니면 ValueError를 잡는다 (lines 106-107)."""
        crawler = ListingCrawler(mock_airbnb_client)
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {
                            "sections": [
                                {
                                    "sectionComponentType": "MEET_YOUR_HOST",
                                    "section": {
                                        "cardData": {
                                            "userId": "12345",
                                            "ratingAverage": 4.5,
                                            "stats": [
                                                {"type": "REVIEW_COUNT", "value": "not_a_number"},
                                            ],
                                        },
                                    },
                                },
                            ]
                        }
                    }
                }
            }
        }
        detail = crawler._extract_detail(data)
        assert detail is not None
        assert detail["host_id"] == "12345"
        assert detail["rating"] == 4.5
        assert "review_count" not in detail

    def test_extract_detail_type_error(self, mock_airbnb_client):
        """파싱 중 TypeError 발생 시 None을 반환한다 (lines 137-139)."""
        crawler = ListingCrawler(mock_airbnb_client)
        # section이 dict가 아닌 문자열이면 .get() 호출 시 AttributeError 발생
        data = {
            "data": {
                "presentation": {
                    "stayProductDetailPage": {
                        "sections": {
                            "sections": ["not_a_dict"]
                        }
                    }
                }
            }
        }
        result = crawler._extract_detail(data)
        assert result is None
