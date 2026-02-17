"""Streamlit 대시보드 메인 앱.

실행:
    streamlit run dashboard/app.py
"""

# pragma: no cover


def main():  # pragma: no cover
    """Streamlit 멀티페이지 앱 진입점."""
    import streamlit as st

    from dashboard.pages.overview import render_overview
    from dashboard.pages.station_detail import render_station_detail
    from dashboard.pages.listing_type import render_listing_type
    from dashboard.pages.revenue_map import render_revenue_map

    st.set_page_config(
        page_title="서울 Airbnb 수요 분석",
        page_icon="🏠",
        layout="wide",
    )

    pages = {
        "전체 현황": render_overview,
        "역별 상세": render_station_detail,
        "숙소 유형별": render_listing_type,
        "수익률 지도": render_revenue_map,
    }

    st.sidebar.title("페이지 선택")
    selection = st.sidebar.radio("", list(pages.keys()))
    pages[selection]()


if __name__ == "__main__":  # pragma: no cover
    main()
