from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

import shared_app as core


def render_public_opportunity_detail_from_row(row: dict) -> None:
    if not row:
        st.info("표시할 Opportunity가 없습니다.")
        return

    source_key = core.resolve_notice_source_key(row)
    detail_link = core.resolve_external_detail_link(row, source_key=source_key)
    download_path = core.resolve_local_file_path(row)
    ctx = core._queue_row_context(row)
    score_value = core._score_value(core.first_non_empty(row, "llm_fit_score", "rfp_score"))
    period = core.first_non_empty(row, "notice_period", "period", "접수기간", "신청기간") or "-"
    period_end = core.extract_period_end(period)
    deadline_label = period_end.strftime("%Y-%m-%d") if pd.notna(period_end) else "-"
    story = core.build_analysis_story_bundle(row, period_text=period)
    summary_text = core.clean(story["summary_text"]) or ctx["reason"] or "-"
    detail_text = core.clean(story["detail_text"]) or "-"
    objective_text = core.clean(story["objective_text"]) or "-"
    eligibility_text = core.clean(story["eligibility_text"]) or "-"
    support_type = core.first_non_empty(row, "support_type", "사업유형", "business_type", "document_type") or "-"
    keyword_text = core.first_non_empty(row, "llm_keywords", "keywords")
    target_market_text = core.first_non_empty(row, "target_market")
    overview_steps = story["overview_steps"]

    core.render_page_header("RFP Analysis", "", eyebrow="Analysis")
    badges = "".join(
        [
            core._pill_html(ctx["recommendation"], base_class="detail-badge"),
            core._pill_html(ctx["score"], kind="score", base_class="detail-badge"),
            core._pill_html(ctx["deadline"], kind="deadline", base_class="detail-badge"),
        ]
    )
    st.markdown(
        (
            '<div class="analysis-hero">'
            f'<div class="detail-badge-row">{badges}</div>'
            f'<div class="analysis-title">{escape(ctx["project"])}</div>'
            f'<div class="analysis-subtitle">{escape(ctx["notice"])}</div>'
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    info_col, summary_col = st.columns([1.65, 0.95], gap="large")
    with info_col:
        _, favorite_col = st.columns([4.2, 1.1], gap="small")
        with favorite_col:
            core.render_favorite_scrap_button(
                notice_id=core.clean(row.get("notice_id")),
                current_value=core.clean(row.get("review_status")),
                source_key=source_key,
                notice_title=core.first_non_empty(row, "notice_title", "공고명"),
                button_key=f"favorite_opportunity_main_{core.clean(row.get('_row_id') or row.get('notice_id'))}",
            )
        core.render_notice_detail_rows_panel(
            "주요 정보",
            [
                {"label": "지원유형", "value": support_type},
                {"label": "핵심 키워드", "value": keyword_text, "kind": "chips"},
                {"label": "관심영역", "value": target_market_text, "kind": "chips"},
                {"label": "지원금", "value": ctx["budget"], "kind": "accent"},
                {"label": "지원 가능 기관", "value": eligibility_text, "kind": "multiline"},
                {"label": "공고 등록일", "value": ctx["registered_at"]},
                {"label": "공고 마감일", "value": deadline_label},
                {"label": "신청 기간", "value": period, "kind": "deadline"},
            ],
            tone="blue",
        )
    with summary_col:
        core.render_notice_detail_rows_panel(
            "빠른 요약",
            [
                {"label": "주관 부처", "value": ctx["ministry"]},
                {"label": "전문 기관", "value": ctx["agency"]},
                {"label": "추천 상태", "value": ctx["recommendation"], "kind": "success"},
                {"label": "적합 점수", "value": str(score_value if score_value else "-"), "kind": "accent"},
                {"label": "공고 상태", "value": ctx["status"]},
                {"label": "문서 단서", "value": ctx["file_name"], "kind": "multiline"},
            ],
            tone="green",
        )

    action_cols = st.columns([1, 1, 1.2])
    with action_cols[0]:
        if detail_link:
            st.link_button("원문 보기", detail_link, use_container_width=True)
    with action_cols[1]:
        if download_path:
            with open(download_path, "rb") as file_handle:
                st.download_button(
                    "RFP 다운로드",
                    data=file_handle.read(),
                    file_name=download_path.name,
                    mime="application/octet-stream",
                    use_container_width=True,
                )
    with action_cols[2]:
        if st.button(
            "관련 공고 보기",
            key=f"oppty_notice_detail_{core.clean(row.get('_row_id'))}",
            use_container_width=True,
        ):
            core.navigate_to_notice_detail(source_key, core.clean(row.get("notice_id")))

    core.render_notice_detail_text_panel("과제 요약", summary_text, tone="blue")
    core.render_notice_detail_rows_panel(
        "지원 요건",
        [
            {"label": "지원 가능 기관", "value": eligibility_text, "kind": "multiline"},
            {"label": "지원 유형", "value": support_type},
            {"label": "핵심 키워드", "value": keyword_text, "kind": "chips"},
            {"label": "관심영역", "value": target_market_text, "kind": "chips"},
            {
                "label": "지원 내용 및 혜택",
                "value": core.clean(story["support_plan_text"]) or core.clean(story["support_need_text"]),
                "kind": "multiline",
            },
        ],
        tone="amber",
    )
    core.render_notice_detail_steps_panel("과제 개요", overview_steps, tone="blue")
    core.render_notice_detail_rows_panel(
        "과제 세부 내용",
        [
            {"label": "공고명", "value": core.first_non_empty(row, "notice_title", "공고명"), "kind": "multiline"},
            {"label": "RFP 제목", "value": core.first_non_empty(row, "llm_rfp_title", "rfp_title"), "kind": "multiline"},
            {"label": "활용 분야", "value": objective_text, "kind": "multiline"},
            {"label": "상세 내용", "value": detail_text, "kind": "multiline"},
        ],
        tone="blue",
    )


def render_public_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    del df

    working = core.ensure_opportunity_row_ids(core.filter_current_opportunity_rows(opportunity_df.copy()))
    if working.empty:
        st.info("표시할 분석 대상이 없습니다.")
        return

    selected_row_id = core.clean(core.get_query_param("id"))
    if not selected_row_id or selected_row_id not in working["_row_id"].fillna("").astype(str).tolist():
        working = working.sort_values(by=["rfp_score", "project_name"], ascending=[False, True], na_position="last")
        selected_row_id = core.clean(working.iloc[0].get("_row_id"))

    selected_row = core.get_row_by_column_value(working, "_row_id", selected_row_id)
    render_public_opportunity_detail_from_row(selected_row)


def render_public_notice_queue_page(datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None) -> None:
    core.consume_favorite_toggle_query_action()
    source_df = core.build_crawled_notice_collection(datasets, source_datasets)

    current_view, selected_id = core.get_route_state("notice")
    if current_view == "detail":
        selected_row = core.get_row_by_column_value(source_df, "_collection_id", selected_id)
        back_col, info_col = st.columns([1, 5])
        with back_col:
            if st.button("목록으로", key="notice_back_to_table", use_container_width=True):
                core.switch_to_table("notice")
        with info_col:
            st.markdown('<div class="page-note">브라우저 뒤로가기로도 목록 화면으로 돌아갈 수 있습니다.</div>', unsafe_allow_html=True)
        core.render_notice_detail_from_row(selected_row, datasets["opportunity_all"])
        return

    core.render_page_header(
        "Notice Queue",
        "IRIS, MSS, NIPA에서 수집한 공고를 한 곳에서 확인합니다.",
        eyebrow="Notices",
    )
    core.render_notice_queue_ui_styles()
    if source_df.empty:
        st.info("표시할 공고가 아직 없습니다.")
        return

    iris_rows = source_df[source_df["source_key"].eq("iris") & source_df["_notice_scope"].isin(["current", "scheduled"])].copy()
    mss_rows = source_df[source_df["source_key"].eq("tipa") & source_df["_notice_scope"].eq("current")].copy()
    nipa_rows = source_df[source_df["source_key"].eq("nipa") & source_df["_notice_scope"].eq("current")].copy()
    archive_rows = source_df[source_df["_notice_scope"].eq("archive")].copy()
    favorite_rows = source_df[source_df["검토여부"].fillna("").astype(str).str.strip().eq(core.FAVORITE_REVIEW_STATUS)].copy()

    core.render_metrics(
        [
            ("전체 공고", str(len(source_df))),
            ("IRIS", str(len(iris_rows))),
            ("MSS", str(len(mss_rows))),
            ("NIPA", str(len(nipa_rows))),
            ("마감/보관", str(len(archive_rows))),
        ]
    )

    search_col, reset_col = st.columns([6, 1])
    with search_col:
        search_text = st.text_input(
            "공고명",
            key="public_notice_queue_search_text",
            placeholder="공고명을 입력하세요",
        )
    with reset_col:
        st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
        st.button(
            "초기화",
            key="public_notice_queue_search_reset",
            use_container_width=True,
            on_click=core.clear_widget_value,
            args=("public_notice_queue_search_text",),
        )

    filtered_source_df = core.filter_notice_queue_rows(source_df, search_text=search_text)
    if core.clean(search_text):
        st.caption(f"검색 결과 {len(filtered_source_df)}건")
    else:
        st.caption(f"전체 {len(source_df)}건")

    iris_rows = filtered_source_df[filtered_source_df["source_key"].eq("iris") & filtered_source_df["_notice_scope"].isin(["current", "scheduled"])].copy()
    mss_rows = filtered_source_df[filtered_source_df["source_key"].eq("tipa") & filtered_source_df["_notice_scope"].eq("current")].copy()
    nipa_rows = filtered_source_df[filtered_source_df["source_key"].eq("nipa") & filtered_source_df["_notice_scope"].eq("current")].copy()
    archive_rows = filtered_source_df[filtered_source_df["_notice_scope"].eq("archive")].copy()
    favorite_rows = filtered_source_df[filtered_source_df["검토여부"].fillna("").astype(str).str.strip().eq(core.FAVORITE_REVIEW_STATUS)].copy()

    tab_iris, tab_mss, tab_nipa, tab_archive, tab_favorites = st.tabs(["IRIS", "MSS", "NIPA", "Archive", "Favorites"])
    with tab_iris:
        core.render_crawled_notice_rows(iris_rows, key_prefix="notice_iris")
    with tab_mss:
        core.render_crawled_notice_rows(mss_rows, key_prefix="notice_mss")
    with tab_nipa:
        core.render_crawled_notice_rows(nipa_rows, key_prefix="notice_nipa")
    with tab_archive:
        core.render_crawled_notice_rows(archive_rows, key_prefix="notice_archive")
    with tab_favorites:
        core.render_crawled_notice_rows(favorite_rows, key_prefix="notice_favorites")


def render_public_opportunity_page(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    page_key = page_key or ("opportunity_archive" if archive else "opportunity")
    title = title or ("Opportunity Archive" if archive else "RFP Queue")
    source_df = core.ensure_opportunity_row_ids(df)
    filtered = core.filter_archived_opportunity_rows(source_df) if archive else core.filter_current_opportunity_rows(source_df)

    current_view, selected_document_id = core.get_route_state(page_key)
    if current_view == "detail":
        selected_row = core.get_row_by_column_value(source_df, "_row_id", selected_document_id)
        if selected_row is None and all_df is not None and not all_df.empty:
            selected_row = core.get_row_by_column_value(
                core.ensure_opportunity_row_ids(all_df),
                "_row_id",
                selected_document_id,
            )
        back_col, info_col = st.columns([1, 4])
        with back_col:
            if st.button("목록으로", key=f"{page_key}_back_to_table_ui", use_container_width=True):
                core.switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">브라우저 뒤로가기로도 리스트 화면으로 돌아갈 수 있습니다.</div>', unsafe_allow_html=True)
        render_public_opportunity_detail_from_row(selected_row)
        return

    core.render_page_header(
        title,
        "사업공고 내 지원 가능한 RFP를 추천합니다." if not archive else "보관된 Opportunity 항목을 가볍게 탐색할 수 있습니다.",
        eyebrow="Opportunity",
    )
    st.markdown(
        '<div class="queue-shell-note">추천 상태와 공고 상태만 빠르게 좁히고, 결과 행 전체를 눌러 상세 공고와 RFP 내용을 바로 확인할 수 있게 구성했습니다.</div>',
        unsafe_allow_html=True,
    )

    working = core._build_queue_filter_frame(filtered)
    if working.empty:
        st.info("표시할 RFP가 없습니다.")
        return
    recommendation_options = sorted(
        [
            value
            for value in working["_queue_recommendation"].dropna().astype(str).unique().tolist()
            if core.clean(value) and value != "-"
        ]
    )
    status_options = sorted(
        [
            value
            for value in working["_queue_status"].dropna().astype(str).unique().tolist()
            if core.clean(value) and value != "-"
        ]
    )
    archive_reason_options = sorted(
        [
            value
            for value in working["_queue_archive_reason"].dropna().astype(str).unique().tolist()
            if core.clean(value) and value != "-"
        ]
    )
    st.markdown('<div class="queue-filter-label">요건 / 필터</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="queue-filter-help">추천 상태와 공고 상태만 빠르게 좁히고, 지금 볼 공고를 추려볼 수 있습니다.</div>',
        unsafe_allow_html=True,
    )
    filter_cols = st.columns(3 if archive else 2)
    with filter_cols[0]:
        selected_recommendation = st.multiselect(
            "추천 상태",
            options=recommendation_options,
            default=[],
            key=f"{page_key}_filter_recommendation",
            placeholder="전체",
        )
    with filter_cols[1]:
        selected_status = st.multiselect(
            "공고 상태",
            options=status_options,
            default=[],
            key=f"{page_key}_filter_status",
            placeholder="전체",
        )
    selected_archive_reason: list[str] = []
    if archive:
        with filter_cols[2]:
            selected_archive_reason = st.multiselect(
                "보관 사유",
                options=archive_reason_options,
                default=[],
                key=f"{page_key}_filter_archive_reason",
                placeholder="전체",
            )
    st.caption("추천 결과의 행 아무 곳이나 누르면 상세 공고와 RFP 내용으로 이동합니다.")

    filtered = working.copy()
    if selected_recommendation:
        filtered = filtered[filtered["_queue_recommendation"].isin(selected_recommendation)]
    if selected_status:
        filtered = filtered[filtered["_queue_status"].isin(selected_status)]
    if selected_archive_reason:
        filtered = filtered[filtered["_queue_archive_reason"].isin(selected_archive_reason)]

    if filtered.empty:
        st.info("검색 조건에 맞는 RFP가 없습니다.")
        return

    filtered = filtered.sort_values(
        by=["rfp_score", "_queue_deadline_sort", "project_name"],
        ascending=[False, True, True],
        na_position="last",
    )

    st.markdown('<div class="queue-results-label">추천 결과</div>', unsafe_allow_html=True)
    core._render_rfp_queue_list(filtered.head(30), page_key=page_key)
