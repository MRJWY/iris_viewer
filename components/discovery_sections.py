from components.dashboard import render_dashboard_search, render_section, render_summary_cards


def render_lists(st, latest, recommended, closing, *, all_notices, today, api):
    st.markdown(
        '<section class="discovery-heading">'
        f'<div class="discovery-eyebrow">{today:%Y.%m.%d} · 한국 시간 기준</div>'
        '<h1>오늘의 공고</h1>'
        '<p>카드를 눌러 공고를 확인하거나, 원하는 공고를 검색하세요.</p>'
        '</section>', unsafe_allow_html=True,
    )
    selected = st.query_params.get("dashboard_section", "")
    render_summary_cards(st, latest, recommended, closing, all_notices, selected=selected, api=api)
    render_dashboard_search(st, api=api)
    if selected == "latest":
        render_section(st, "신규 공고", "등록일 기준 최신 5건", latest, key="latest", api=api,
                       empty_message="등록일이 확인된 공고가 없습니다.")
    elif selected == "recommended":
        render_section(st, "추천 RFP 목록", "접수 중인 키워드 후보 과제 · 최신 등록순", recommended,
                       key="recommended_rfp", api=api, rfp=True, empty_message="현재 접수 중인 추천 RFP가 없습니다.")
    elif selected == "closing":
        render_section(st, "마감 예정 공고 (7일 내)", "오늘 마감 포함 · 마감이 빠른 순", closing,
                       key="closing", api=api, empty_message="7일 이내 마감 예정인 공고가 없습니다.")
