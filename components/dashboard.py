"""Daily overview using the award_list summary cards and project board."""
from html import escape
from streamlit import fragment
from components.notice_discovery import discovery_href

from components.notice_board import render_notice_board
from components.pagination import clamp_page, render_pagination


def render_summary_cards(st, latest, recommended, closing, all_notices, *, selected="", api=None):
    cards = []
    items = [
        ("latest", "신규 공고", len(latest), "등록일 기준 최신 5건", ""),
        ("recommended", "추천 RFP 목록", len(recommended), "접수 중인 키워드 후보 과제", ""),
        ("closing", "마감 예정 공고", len(closing), "오늘부터 7일 이내 · 오늘 마감 포함", ""),
        ("all_notices", "전체 공고", len(all_notices), "수집된 모든 공고 · 마감 공고 포함", ""),
    ]
    for key, title, count, description, tone in items:
        active = selected == key
        params = {"source": "dashboard", "page": "dashboard", "view": "table"}
        if key == "all_notices":
            params.update(source="notices", page="notice_queue")
        elif not active:
            params["dashboard_section"] = key
        href = escape(discovery_href(params, api=api), quote=True)
        state = " is-selected" if active else ""
        action = "검색하기 →" if key == "all_notices" else "목록 접기 ↑" if active else "목록 보기 →"
        expanded = '' if key == "all_notices" else f' aria-expanded="{str(active).lower()}"'
        cards.append(
            f'<a class="discovery-summary-card {tone}{state}" href="{href}" target="_self" '
            f'aria-label="{escape(title)} {count:,}건 {action}"{expanded}>'
            f'<div class="discovery-card-title" role="heading" aria-level="2">{escape(title)}</div>'
            f'<div class="discovery-summary-value">{count:,}<small>건</small></div>'
            f'<p>{escape(description)}</p><span class="discovery-card-action">{action}</span></a>'
        )
    st.markdown('<div class="discovery-summary-grid">' + "".join(cards) + '</div>', unsafe_allow_html=True)


def render_dashboard_search(st, *, api=None):
    with st.form("dashboard_search_form", border=False):
        search_col, button_col = st.columns([8, 1], gap="small")
        with search_col:
            query = st.text_input("공고 검색", placeholder="공고명, 기관명, 과제명 또는 키워드로 검색", label_visibility="collapsed")
        with button_col:
            submitted = st.form_submit_button("검색", type="primary", use_container_width=True)
    if submitted:
        # Start a fresh full search, including when the user previously set filters.
        for name, value in (("q", query.strip()), ("status", "전체"), ("provider", "전체"), ("page_no", 1)):
            st.session_state[f"all_notices_{name}"] = value
        attach_auth = getattr(api, "with_auth_params", None)
        auth_params = attach_auth({}) if callable(attach_auth) else {}
        st.query_params.clear()
        st.query_params.update(source="notices", page="notice_queue", view="table", discovery_q=query.strip())
        st.query_params.update(auth_params)
        st.rerun()


@fragment
def render_section(st, title, subtitle, rows, *, key, api, rfp=False, empty_message):
    st.markdown(
        f'<div class="discovery-section-heading"><h2>{escape(title)} <span>{len(rows):,}</span></h2>'
        f'<p>{escape(subtitle)} · 행을 누르면 상세정보가 펼쳐집니다.</p></div>',
        unsafe_allow_html=True,
    )
    if rows.empty:
        st.info(empty_message)
        return
    page_size = 5
    total_pages = (len(rows) + page_size - 1) // page_size
    # Use the existing return-route field so detail -> list restores this page.
    page_param = "discovery_page_no"
    page = clamp_page(st.query_params.get(page_param, 1), total_pages)
    render_notice_board(st, rows.iloc[(page - 1) * page_size:page * page_size], api=api, rfp=rfp)
    render_pagination(st, current=page, total=total_pages, page_param=page_param, key=key)
