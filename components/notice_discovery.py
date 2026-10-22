"""Shared, read-only UI for recommendation and notice search pages."""
from __future__ import annotations

import math
from pathlib import Path
from urllib.parse import urlencode

from jobs_common.notice_discovery import filter_notices


def discovery_href(params, *, api=None):
    # The standalone viewer keeps its existing signed-session URL mechanism.
    attach_auth = getattr(api, "with_auth_params", None)
    return "?" + urlencode(attach_auth(params) if callable(attach_auth) else params)


def inject_styles(st) -> None:
    css = (Path(__file__).resolve().parents[1] / "assets" / "discovery.css").read_text(encoding="utf-8")
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def _reset(st, prefix: str) -> None:
    for name, default in (("q", ""), ("status", "전체"), ("provider", "전체"), ("page_no", 1)):
        st.session_state[f"{prefix}_{name}"] = default
        st.query_params.pop(f"discovery_{name}", None)


def _set_page(st, prefix: str, page: int) -> None:
    st.session_state[f"{prefix}_page_no"] = page
    st.query_params["discovery_page_no"] = str(page)


def render_search(st, rows, *, prefix: str):
    source_map = dict(zip(rows.get("source_key", []), rows.get("Source", [])))
    sources = ["전체"] + sorted(key for key in source_map if key)
    statuses = ["전체", "접수중", "예정", "마감"]
    for name, default in (("q", ""), ("status", "전체"), ("provider", "전체"), ("page_no", 1)):
        key = f"{prefix}_{name}"
        if key not in st.session_state:
            value = st.query_params.get(f"discovery_{name}", default)
            if name == "page_no":
                try:
                    value = max(1, int(value))
                except (TypeError, ValueError):
                    value = 1
            st.session_state[key] = value
    for name, options in (("status", statuses), ("provider", sources)):
        if st.session_state[f"{prefix}_{name}"] not in options:
            st.session_state[f"{prefix}_{name}"] = "전체"

    with st.form(f"{prefix}_search_form", border=False):
        search_col, button_col = st.columns([8, 1], gap="small")
        with search_col:
            query = st.text_input("공고 검색", key=f"{prefix}_q", placeholder="공고명, 기관명, 과제명 또는 키워드로 검색", label_visibility="collapsed")
        with button_col:
            submitted = st.form_submit_button("검색", type="primary", use_container_width=True)
        with st.expander("상세 조건", expanded=False):
            left, right = st.columns(2)
            with left:
                status = st.selectbox("접수 상태", statuses, key=f"{prefix}_status")
            with right:
                source = st.selectbox("공고 출처", sources, key=f"{prefix}_provider", format_func=lambda key: source_map.get(key, key))
    if submitted:
        _set_page(st, prefix, 1)
        for name, value in (("q", query), ("status", status), ("provider", source)):
            if value and value != "전체":
                st.query_params[f"discovery_{name}"] = value
            else:
                st.query_params.pop(f"discovery_{name}", None)
    filtered = filter_notices(rows, query, status=status, source=source)
    if query or status != "전체" or source != "전체":
        label = " · ".join(value for value in (query, status if status != "전체" else "", source_map.get(source, "") if source != "전체" else "") if value)
        st.caption(f"적용된 검색 조건: {label}")
        st.button("검색 초기화", key=f"{prefix}_reset", on_click=_reset, args=(st, prefix))
    return filtered


def notice_href(row, *, api) -> str:
    notice_id = api.clean(row.get("Notice ID"))
    source = api.clean(row.get("source_key"))
    if not notice_id or not source:
        return ""
    params = api.apply_return_route({"source": "notices", "page": "notice_queue", "view": "detail", "id": notice_id, "notice_source": source})
    return discovery_href(params, api=api)


def render_notice_list(st, rows, *, api, recommended: bool = False) -> None:
    if rows.empty:
        st.info("조건에 맞는 추천 공고가 없습니다. 전체 공고 검색에서 더 많은 공고를 확인해 보세요." if recommended else "검색 결과가 없습니다. 검색어를 줄이거나 상세 조건을 초기화해 보세요.")
        return
    from components.notice_board import render_notice_board
    render_notice_board(st, rows, api=api)


def render_results(st, rows, *, prefix: str, api, recommended: bool = False) -> None:
    page_size = 5 if recommended else 20
    total_pages = max(1, math.ceil(len(rows) / page_size))
    page = max(1, min(int(st.session_state.get(f"{prefix}_page_no", 1)), total_pages))
    st.session_state[f"{prefix}_page_no"] = page
    start = (page - 1) * page_size
    st.caption(f"{'추천 공고' if recommended else '검색 결과'} {len(rows):,}건 · 최신 등록순")
    render_notice_list(st, rows.iloc[start:start + page_size], api=api, recommended=recommended)
    if total_pages > 1:
        left, middle, right = st.columns([1, 3, 1])
        with left:
            st.button("← 이전", key=f"{prefix}_prev", disabled=page <= 1, on_click=_set_page, args=(st, prefix, page - 1), use_container_width=True)
        with middle:
            st.markdown(f'<div class="discovery-page-count">{page} / {total_pages}</div>', unsafe_allow_html=True)
        with right:
            st.button("다음 →", key=f"{prefix}_next", disabled=page >= total_pages, on_click=_set_page, args=(st, prefix, page + 1), use_container_width=True)
