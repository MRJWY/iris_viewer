from __future__ import annotations

from html import escape
import re

import pandas as pd
import streamlit as st


FAVORITE_REVIEW_STATUS = "관심공고"
UNFAVORITE_REVIEW_STATUS = "검토전"

STATUS_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("전체", "전체"),
    ("진행중", "진행중"),
    ("예정", "예정"),
    ("마감", "마감"),
]

RECOMMENDATION_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("전체", "전체"),
    ("추천", "추천"),
    ("검토권장", "검토권장"),
    ("보통", "보통"),
]

TOP_TAB_OPTIONS: list[tuple[str, str]] = [
    ("전체", "all"),
    ("IRIS", "iris"),
    ("MSS", "tipa"),
    ("NIPA", "nipa"),
    ("관심공고", "favorite"),
    ("보관/마감", "archive"),
]

RECOMMENDATION_RANK = {
    "추천": 3,
    "검토권장": 2,
    "보통": 1,
    "비추천": 0,
    "": -1,
}


def apply_notice_browser_overrides(ns: dict, *, detail_page_key: str) -> None:
    clean = ns["clean"]
    first_non_empty = ns["first_non_empty"]
    normalize_notice_status_label = ns["normalize_notice_status_label"]
    resolve_route_source_key_for_row = ns["resolve_route_source_key_for_row"]
    build_route_href = ns["build_route_href"]
    build_favorite_toggle_href = ns["build_favorite_toggle_href"]
    render_notice_detail_from_row = ns["render_notice_detail_from_row"]
    build_crawled_notice_collection = ns["build_crawled_notice_collection"]
    get_row_by_column_value = ns["get_row_by_column_value"]
    get_route_state = ns["get_route_state"]
    switch_to_table = ns["switch_to_table"]
    render_page_header = ns["render_page_header"]
    render_notice_queue_ui_styles = ns["render_notice_queue_ui_styles"]
    get_query_param = ns["get_query_param"]
    get_query_params_dict = ns["get_query_params_dict"]
    series_from_candidates = ns["series_from_candidates"]
    resolve_external_detail_link = ns.get("resolve_external_detail_link")
    replace_query_params = ns.get("replace_query_params")
    with_auth_params = ns.get("with_auth_params")
    update_notice_review_status = ns.get("update_notice_review_status")
    update_mss_review_status = ns.get("update_mss_review_status")
    update_nipa_review_status = ns.get("update_nipa_review_status")
    save_review_status = ns.get("save_review_status")
    is_user_scoped_operations_enabled = ns.get("is_user_scoped_operations_enabled")
    upsert_user_review_status = ns.get("upsert_user_review_status")
    get_current_operation_scope_key = ns.get("get_current_operation_scope_key")

    def _replace_params(params: dict[str, str]) -> None:
        if callable(replace_query_params):
            replace_query_params(params)
            return
        st.query_params.clear()
        if params:
            st.query_params.update(params)

    def _auth_params(params: dict[str, str]) -> dict[str, str]:
        if callable(with_auth_params):
            return with_auth_params(params)
        return params

    def _clear_notice_caches() -> None:
        for name in (
            "load_sheet_as_dataframe",
            "load_optional_sheet_as_dataframe",
            "load_app_datasets",
            "build_source_datasets",
            "load_user_review_statuses",
            "clear_public_viewer_caches",
        ):
            fn = ns.get(name)
            clear_fn = getattr(fn, "clear", None)
            if callable(clear_fn):
                clear_fn()
            elif callable(fn) and name == "clear_public_viewer_caches":
                fn()

    def _safe_series(rows: pd.DataFrame, columns: list[str]) -> pd.Series:
        if rows is None or rows.empty:
            return pd.Series(dtype="object")
        return series_from_candidates(rows, columns).fillna("").astype(str).str.strip()

    def _review_value(row: dict | pd.Series | None) -> str:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
        return clean(first_non_empty(row_dict, "review_status", "검토 여부", "검토여부"))

    def _review_series(rows: pd.DataFrame) -> pd.Series:
        return _safe_series(rows, ["review_status", "검토 여부", "검토여부"])

    def _is_favorite(row_or_value: dict | pd.Series | str | None) -> bool:
        value = _review_value(row_or_value) if isinstance(row_or_value, (dict, pd.Series)) else clean(row_or_value)
        return value == FAVORITE_REVIEW_STATUS

    def _favorite_button_label(current_value: str) -> tuple[bool, str]:
        is_favorite = _is_favorite(current_value)
        return is_favorite, "★ 관심공고 저장됨" if is_favorite else "☆ 관심공고 저장"

    def _favorite_badge_html() -> str:
        return '<span class="notice-chip notice-chip-favorite">관심</span>'

    def _sync_user_scoped_review(*, notice_id: str, source_key: str, notice_title: str, review_status: str) -> None:
        if not callable(is_user_scoped_operations_enabled) or not callable(upsert_user_review_status):
            return
        if not is_user_scoped_operations_enabled():
            return
        user_scope_key = get_current_operation_scope_key() if callable(get_current_operation_scope_key) else ""
        if not clean(user_scope_key):
            return
        upsert_user_review_status(
            user_id=user_scope_key,
            source_key=source_key,
            notice_id=notice_id,
            notice_title=notice_title,
            review_status=review_status,
        )

    def _persist_review_status(*, notice_id: str, source_key: str, review_status: str, notice_title: str = "") -> None:
        normalized_source = clean(source_key) or "iris"
        if normalized_source == "tipa":
            if callable(update_mss_review_status):
                update_mss_review_status(notice_id, review_status)
            elif callable(save_review_status):
                save_review_status(notice_id=notice_id, review_status=review_status, source_key=normalized_source)
        elif normalized_source == "nipa":
            if callable(update_nipa_review_status):
                update_nipa_review_status(notice_id, review_status)
            elif callable(save_review_status):
                save_review_status(notice_id=notice_id, review_status=review_status, source_key=normalized_source)
        else:
            if callable(update_notice_review_status):
                update_notice_review_status(notice_id, review_status)
            elif callable(save_review_status):
                save_review_status(notice_id=notice_id, review_status=review_status, source_key=normalized_source)
        try:
            _sync_user_scoped_review(
                notice_id=notice_id,
                source_key=normalized_source,
                notice_title=notice_title,
                review_status=review_status,
            )
        finally:
            _clear_notice_caches()

    def consume_favorite_toggle_query_action() -> None:
        return

    def render_favorite_scrap_button(
        *,
        notice_id: str,
        current_value: str,
        source_key: str = "iris",
        notice_title: str = "",
        button_key: str,
        compact: bool = False,
        use_container_width: bool | None = None,
    ) -> None:
        if not clean(notice_id):
            return
        is_favorite, button_label, _ = favorite_button_props(current_value)
        next_value = UNFAVORITE_REVIEW_STATUS if is_favorite else FAVORITE_REVIEW_STATUS
        safe_key = _css_safe_key(button_key)
        if compact:
            st.markdown(
                f"""
                <style>
                .st-key-{safe_key} {{
                  display: flex;
                  justify-content: flex-end;
                }}
                .st-key-{safe_key} button {{
                  min-height: 36px !important;
                  padding: 0.15rem 0.8rem !important;
                  border-radius: 999px !important;
                  font-size: 0.88rem !important;
                  font-weight: 800 !important;
                  white-space: nowrap !important;
                }}
                </style>
                """,
                unsafe_allow_html=True,
            )
        if use_container_width is None:
            use_container_width = not compact
        if st.button(button_label, key=button_key, use_container_width=use_container_width, type="secondary"):
            _persist_review_status(
                notice_id=notice_id,
                source_key=clean(source_key) or "iris",
                review_status=next_value,
                notice_title=clean(notice_title),
            )
            st.rerun()

    def favorite_button_props(current_value: str) -> tuple[bool, str, str]:
        is_favorite, label = _favorite_button_label(current_value)
        return is_favorite, label, "primary" if is_favorite else "secondary"

    def _normalize_status_filter(value: str) -> str:
        normalized = clean(value).lower()
        alias_map = {
            "all": "전체",
            "전체": "전체",
            "current": "진행중",
            "진행중": "진행중",
            "scheduled": "예정",
            "예정": "예정",
            "archive": "마감",
            "closed": "마감",
            "마감": "마감",
        }
        return alias_map.get(normalized, "전체")

    def _normalize_recommendation_value(value: object) -> str:
        text = clean(value)
        lowered = text.lower()
        if not text:
            return ""
        if any(marker in lowered for marker in ("비추천", "미추천", "not recommend", "reject")):
            return "비추천"
        if "검토권장" in text:
            return "검토권장"
        if "보통" in text:
            return "보통"
        if "추천" in text or "recommend" in lowered:
            return "추천"
        if "검토" in text or "보류" in text or "hold" in lowered:
            return "검토권장"
        return text

    def _normalize_recommendation_filter(value: str) -> str:
        normalized = _normalize_recommendation_value(value)
        if normalized in {option for option, _ in RECOMMENDATION_FILTER_OPTIONS if option != "전체"}:
            return normalized
        lowered = clean(value).lower()
        if lowered in {"all", "전체"}:
            return "전체"
        return "전체"

    def _status_filter_state_key() -> str:
        return f"{detail_page_key}_selected_status_filter"

    def _recommendation_filter_state_key() -> str:
        return f"{detail_page_key}_selected_recommendation_filter"

    def _search_state_key() -> str:
        return f"{detail_page_key}_search_text"

    def _selected_notice_state_key() -> str:
        return f"{detail_page_key}_selected_notice_id"

    def _resolve_notice_id(row: dict | pd.Series | None) -> str:
        if row is None:
            return ""
        return clean(first_non_empty(row, "怨듦퀬ID", "notice_id"))

    def _get_notice_row_by_id(rows: pd.DataFrame, notice_id: str) -> dict | pd.Series | None:
        selected_notice_id = clean(notice_id)
        if rows is None or rows.empty or not selected_notice_id:
            return None
        selected_row = get_row_by_column_value(rows, "怨듦퀬ID", selected_notice_id)
        if selected_row:
            return selected_row
        return get_row_by_column_value(rows, "notice_id", selected_notice_id)

    def _consume_notice_filter_query_actions() -> None:
        st.session_state.setdefault(_status_filter_state_key(), "all")
        st.session_state.setdefault(_recommendation_filter_state_key(), "all")
        status_param = get_query_param("notice_status_filter_select")
        recommendation_param = get_query_param("notice_recommendation_filter_select")
        if not clean(status_param) and not clean(recommendation_param):
            return
        params = get_query_params_dict()
        params["page"] = detail_page_key
        params["view"] = "table"
        params.pop("notice_source_filter_select", None)
        params.pop("notice_status_filter_select", None)
        params.pop("notice_recommendation_filter_select", None)
        _replace_params(_auth_params(params))
        st.rerun()

    def _build_notice_analysis_summary(opportunity_df: pd.DataFrame) -> pd.DataFrame:
        if opportunity_df is None or opportunity_df.empty or "notice_id" not in opportunity_df.columns:
            return pd.DataFrame(
                columns=[
                    "notice_id",
                    "_queue_recommendation",
                    "_queue_project_name",
                    "_queue_budget",
                    "_queue_reason",
                    "_queue_keywords",
                ]
            )

        working = opportunity_df.copy()
        working["notice_id"] = working["notice_id"].fillna("").astype(str).str.strip()
        working = working[working["notice_id"].ne("")].copy()
        if working.empty:
            return pd.DataFrame(
                columns=[
                    "notice_id",
                    "_queue_recommendation",
                    "_queue_project_name",
                    "_queue_budget",
                    "_queue_reason",
                    "_queue_keywords",
                ]
            )

        working["_queue_recommendation"] = _safe_series(
            working,
            ["llm_recommendation", "recommendation", "추천여부", "Recommendation"],
        ).apply(_normalize_recommendation_value)
        working["_queue_project_name"] = _safe_series(
            working,
            ["llm_project_name", "project_name", "rfp_title", "Project"],
        )
        working["_queue_budget"] = _safe_series(
            working,
            [
                "llm_total_budget_text",
                "total_budget_text",
                "llm_per_project_budget_text",
                "per_project_budget_text",
                "budget",
                "Budget",
            ],
        )
        working["_queue_reason"] = _safe_series(working, ["llm_reason", "reason", "Reason"])
        working["_queue_keywords"] = _safe_series(working, ["llm_keywords", "keywords", "Keywords"])
        working["_queue_score"] = pd.to_numeric(
            series_from_candidates(working, ["llm_fit_score", "rfp_score", "Score"]),
            errors="coerce",
        ).fillna(0)
        working["_queue_recommendation_rank"] = (
            working["_queue_recommendation"].map(RECOMMENDATION_RANK).fillna(-1)
        )

        working = working.sort_values(
            by=["notice_id", "_queue_recommendation_rank", "_queue_score", "_queue_project_name"],
            ascending=[True, False, False, True],
            na_position="last",
        )
        best = working.drop_duplicates(subset=["notice_id"], keep="first").copy()
        return best[
            [
                "notice_id",
                "_queue_recommendation",
                "_queue_project_name",
                "_queue_budget",
                "_queue_reason",
                "_queue_keywords",
            ]
        ].reset_index(drop=True)

    def _enrich_notice_rows(rows: pd.DataFrame, opportunity_df: pd.DataFrame) -> pd.DataFrame:
        if rows is None or rows.empty:
            return pd.DataFrame()

        enriched = rows.copy()
        enriched["_notice_id"] = _safe_series(enriched, ["공고ID", "notice_id"])
        summary_df = _build_notice_analysis_summary(opportunity_df)
        if not summary_df.empty:
            enriched = enriched.merge(
                summary_df,
                left_on="_notice_id",
                right_on="notice_id",
                how="left",
                suffixes=("", "_analysis"),
            )
        for column in (
            "_queue_recommendation",
            "_queue_project_name",
            "_queue_budget",
            "_queue_reason",
            "_queue_keywords",
        ):
            if column not in enriched.columns:
                enriched[column] = ""
            enriched[column] = enriched[column].fillna("").astype(str).str.strip()
        enriched["_queue_analysis"] = enriched["_queue_project_name"]
        enriched.loc[
            enriched["_queue_analysis"].eq(""),
            "_queue_analysis",
        ] = enriched["_queue_reason"]
        enriched.loc[
            enriched["_queue_analysis"].eq(""),
            "_queue_analysis",
        ] = enriched["_queue_keywords"]
        return enriched

    def _matches_search(rows: pd.DataFrame, search_text: str) -> pd.Series:
        query = clean(search_text).lower()
        if rows.empty or not query:
            return pd.Series(True, index=rows.index)

        columns = [
            "공고명",
            "notice_title",
            "_queue_project_name",
            "전문기관",
            "agency",
            "소관부처",
            "주관부처",
            "ministry",
            "매체",
            "source_label",
            "공고번호",
            "notice_no",
        ]
        stacked = pd.Series("", index=rows.index, dtype="object")
        for column in columns:
            if column in rows.columns:
                stacked = stacked + " " + rows[column].fillna("").astype(str)
        return stacked.str.lower().str.contains(query, na=False)

    def _apply_notice_filters(rows: pd.DataFrame, status_filter: str, recommendation_filter: str, search_text: str) -> pd.DataFrame:
        if rows is None or rows.empty:
            return pd.DataFrame()

        filtered = rows.copy()
        normalized_status = _normalize_status_filter(status_filter)
        normalized_recommendation = _normalize_recommendation_filter(recommendation_filter)

        if normalized_status == "진행중":
            filtered = filtered[filtered["_notice_scope"].fillna("").astype(str).str.strip().eq("current")].copy()
        elif normalized_status == "예정":
            filtered = filtered[filtered["_notice_scope"].fillna("").astype(str).str.strip().eq("scheduled")].copy()
        elif normalized_status == "마감":
            filtered = filtered[filtered["_notice_scope"].fillna("").astype(str).str.strip().eq("archive")].copy()
        if normalized_recommendation != "전체":
            filtered = filtered[filtered["_queue_recommendation"].eq(normalized_recommendation)].copy()

        search_mask = _matches_search(filtered, search_text)
        return filtered[search_mask].copy()

    def _render_filter_control(title: str, options: list[tuple[str, str]], state_key: str) -> str:
        option_values = [value for value, _ in options]
        option_labels = {value: label for value, label in options}
        current_value = clean(st.session_state.get(state_key, option_values[0]))
        if current_value not in option_labels:
            st.session_state[state_key] = option_values[0]

        st.markdown(f'<div class="notice-filter-group-title">{escape(title)}</div>', unsafe_allow_html=True)
        selected_value = st.radio(
            title,
            options=option_values,
            key=state_key,
            horizontal=True,
            label_visibility="collapsed",
            format_func=lambda value: option_labels.get(value, value),
        )
        return clean(selected_value)

    def _inject_notice_queue_dashboard_styles() -> None:
        st.markdown(
            """
            <style>
            .notice-filter-group {
              margin: 0.9rem 0 0.35rem;
            }
            .notice-filter-group-title {
              color: var(--text-muted);
              font-size: 0.83rem;
              font-weight: 800;
              margin-bottom: 0.45rem;
            }
            div[data-testid="stRadio"] > div {
              gap: 0.55rem;
              flex-wrap: wrap;
            }
            div[data-testid="stRadio"] label {
              margin: 0;
            }
            div[data-testid="stRadio"] label p {
              font-size: 0.92rem;
              font-weight: 700;
            }
            .notice-queue-note {
              margin: 0.9rem 0 0.35rem;
              color: var(--text-muted);
              font-size: 0.92rem;
              line-height: 1.6;
            }
            .notice-queue-list {
              display: flex;
              flex-direction: column;
              width: 100%;
              margin-top: 0.7rem;
              border-top: 1px solid rgba(226, 232, 240, 0.95);
            }
            .notice-queue-row-shell {
              position: relative;
              width: 100%;
              border-bottom: 1px solid rgba(226, 232, 240, 0.95);
            }
            .notice-queue-row-link {
              position: absolute;
              inset: 0;
              z-index: 1;
              display: block;
            }
            .notice-queue-row-link:focus-visible {
              outline: 2px solid #60a5fa;
              outline-offset: -2px;
            }
            .notice-queue-row {
              display: block;
              position: relative;
              z-index: 2;
              width: 100%;
              padding: 1.05rem 0.15rem;
              pointer-events: none;
              transition: background-color 140ms ease;
            }
            .notice-queue-row-shell:hover .notice-queue-row {
              background: #f8fafc;
            }
            .notice-queue-row-main {
              display: grid;
              grid-template-columns: minmax(0, 1fr) 220px;
              align-items: flex-start;
              gap: 1.25rem;
              width: 100%;
            }
            .notice-queue-row-left {
              min-width: 0;
              width: 100%;
            }
            .notice-queue-row-right {
              width: 220px;
              min-width: 220px;
              display: flex;
              flex-direction: column;
              align-items: flex-end;
              justify-self: end;
              gap: 0.55rem;
            }
            .notice-queue-topline {
              display: flex;
              align-items: center;
              flex-wrap: wrap;
              gap: 0.45rem;
              margin-bottom: 0.5rem;
            }
            .notice-chip {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              min-height: 24px;
              padding: 0 10px;
              border-radius: 999px;
              font-size: 0.78rem;
              font-weight: 800;
              line-height: 1;
              white-space: nowrap;
            }
            .notice-chip-source {
              background: #f1f5f9;
              color: #334155;
            }
            .notice-chip-status {
              background: #eff6ff;
              color: #1d4ed8;
            }
            .notice-chip-status.is-archive {
              background: #fff1f2;
              color: #be123c;
            }
            .notice-chip-status.is-scheduled {
              background: #fff7ed;
              color: #c2410c;
            }
            .notice-chip-recommend {
              background: #ecfdf5;
              color: #047857;
            }
            .notice-chip-review {
              background: #fffbeb;
              color: #b45309;
            }
            .notice-chip-neutral {
              background: #f8fafc;
              color: #475569;
            }
            .notice-chip-favorite {
              background: #fff7ed;
              color: #c2410c;
            }
            .notice-queue-title {
              color: var(--text-strong);
              font-size: 1.12rem;
              font-weight: 900;
              line-height: 1.45;
            }
            .notice-queue-analysis-label {
              margin-top: 0.45rem;
              color: var(--text-muted);
              font-size: 0.78rem;
              font-weight: 800;
              letter-spacing: 0.02em;
            }
            .notice-queue-analysis {
              margin-top: 0.2rem;
              color: var(--text-body);
              font-size: 0.98rem;
              font-weight: 700;
              line-height: 1.55;
            }
            .notice-queue-analysis.is-empty {
              color: #94a3b8;
              font-weight: 600;
            }
            .notice-queue-meta {
              display: flex;
              flex-wrap: wrap;
              gap: 0.55rem 1rem;
              margin-top: 0.7rem;
            }
            .notice-queue-meta-item {
              color: var(--text-body);
              font-size: 0.91rem;
              line-height: 1.5;
            }
            .notice-queue-meta-label {
              color: var(--text-muted);
              font-weight: 800;
              margin-right: 0.28rem;
            }
            .notice-queue-cta {
              color: var(--text-muted);
              font-size: 0.84rem;
              font-weight: 700;
            }
            .notice-queue-row-action {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              position: relative;
              z-index: 3;
              pointer-events: auto;
              min-height: 38px;
              padding: 0 14px;
              border-radius: 10px;
              border: 1px solid rgba(203, 213, 225, 0.92);
              background: #ffffff;
              color: #334155 !important;
              font-size: 0.84rem;
              font-weight: 800;
              line-height: 1;
              text-decoration: none !important;
              white-space: nowrap;
            }
            .notice-queue-row-action:hover {
              background: #f8fafc;
              text-decoration: none !important;
            }
            .notice-queue-row-action.is-active {
              background: #fff7ed;
              border-color: #fdba74;
              color: #c2410c !important;
            }
            @media (max-width: 960px) {
              .notice-queue-row-main {
                grid-template-columns: 1fr;
                gap: 0.95rem;
              }
              .notice-queue-row-right {
                width: 100%;
                min-width: 0;
                align-items: flex-start;
                justify-self: stretch;
              }
            }
            @media (max-width: 640px) {
              .notice-queue-title {
                font-size: 1.02rem;
              }
              div[data-testid="stRadio"] > div {
                gap: 0.35rem;
              }
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

    def _status_badge_class(status: str) -> str:
        if status == "마감":
            return "notice-chip notice-chip-status is-archive"
        if status == "예정":
            return "notice-chip notice-chip-status is-scheduled"
        return "notice-chip notice-chip-status"

    def _recommendation_badge_html(value: str) -> str:
        normalized = _normalize_recommendation_value(value)
        if not normalized:
            return '<span class="notice-chip notice-chip-neutral">분석대기</span>'
        if normalized == "추천":
            class_name = "notice-chip notice-chip-recommend"
        elif normalized == "검토권장":
            class_name = "notice-chip notice-chip-review"
        else:
            class_name = "notice-chip notice-chip-neutral"
        return f'<span class="{class_name}">{escape(normalized)}</span>'

    def _queue_click_href(row: pd.Series, collection_id: str, source_key: str) -> str:
        if clean(collection_id):
            return build_route_href(detail_page_key, collection_id, source_key=source_key)
        if callable(resolve_external_detail_link):
            return clean(resolve_external_detail_link(row, source_key=source_key))
        return clean(first_non_empty(row, "상세링크", "detail_link"))

    def _queue_click_href(row: pd.Series, collection_id: str, source_key: str) -> str:
        del collection_id
        notice_id = _resolve_notice_id(row)
        if notice_id:
            return build_route_href(detail_page_key, notice_id, source_key=source_key)
        return ""

    def render_crawled_notice_rows(
        rows: pd.DataFrame,
        *,
        key_prefix: str,
        limit: int = 30,
        page_key: str = detail_page_key,
        empty_message: str = "표시할 공고가 없습니다.",
    ) -> None:
        del key_prefix
        if rows is None or rows.empty:
            st.info(empty_message)
            return

        row_html: list[str] = []
        for _, row in rows.head(limit).iterrows():
            collection_id = clean(row.get("_collection_id"))
            notice_id = clean(first_non_empty(row, "공고ID", "notice_id"))
            source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
            title = clean(first_non_empty(row, "공고명", "notice_title")) or "-"
            ministry = clean(first_non_empty(row, "소관부처", "주관부처", "ministry")) or "-"
            agency = clean(first_non_empty(row, "전문기관", "agency", "수행기관")) or "-"
            period = clean(first_non_empty(row, "접수기간", "notice_period", "period", "신청기간")) or "-"
            budget = clean(row.get("_queue_budget")) or "미기재"
            recommendation = clean(row.get("_queue_recommendation"))
            analysis_text = clean(row.get("_queue_analysis")) or "연결된 RFP 분석이 아직 없습니다."
            review_value = _review_value(row)
            source_label = clean(first_non_empty(row, "매체", "source_label")) or (source_key or "IRIS").upper()
            is_favorite = _is_favorite(review_value)
            scope = clean(first_non_empty(row, "_notice_scope"))
            status = normalize_notice_status_label(first_non_empty(row, "공고상태", "status", "rcve_status"))
            if not status:
                if scope == "archive":
                    status = "마감"
                elif scope == "scheduled":
                    status = "예정"
                else:
                    status = "진행중"
            action_href = (
                _build_favorite_href(
                    page_key=page_key,
                    notice_id=notice_id,
                    current_value=review_value,
                    source_key=source_key or "iris",
                    notice_title=title,
                )
                if notice_id
                else ""
            )
            click_href = _queue_click_href(row, collection_id, source_key)
            cta_label = "Notice Detail"
            cta_label = "상세 보기" if clean(collection_id) else "원문 보기"

            cta_label = "Notice Detail"
            title_badges = [f'<span class="notice-chip notice-chip-source">{escape(source_label)}</span>']
            if is_favorite:
                title_badges.append(_favorite_badge_html())

            analysis_class = "notice-queue-analysis" if clean(row.get("_queue_analysis")) else "notice-queue-analysis is-empty"
            right_badges = [
                f'<span class="{_status_badge_class(status)}">{escape(status)}</span>',
                _recommendation_badge_html(recommendation),
            ]

            link_html = ""
            if clean(click_href):
                safe_href = escape(click_href, quote=True)
                link_html = f'<a class="notice-queue-row-link" href="{safe_href}" aria-label="{escape(title, quote=True)}"></a>'

            row_html.append(
                '<div class="notice-queue-row-shell">'
                f"{link_html}"
                '<div class="notice-queue-row">'
                '<div class="notice-queue-row-main">'
                '<div class="notice-queue-row-left">'
                f'<div class="notice-queue-topline">{"".join(title_badges)}</div>'
                f'<div class="notice-queue-title">{escape(title)}</div>'
                '<div class="notice-queue-analysis-label">과제 분석</div>'
                f'<div class="{analysis_class}">{escape(analysis_text)}</div>'
                '<div class="notice-queue-meta">'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">기관</span>{escape(ministry)} / {escape(agency)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">기간</span>{escape(period)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">예산</span>{escape(budget)}</div>'
                "</div>"
                "</div>"
                '<div class="notice-queue-row-right">'
                f'{"".join(right_badges)}'
                f'<div class="notice-queue-cta">{escape(cta_label)}</div>'
                f'{_favorite_button_html(action_href, review_value) if action_href else ""}'
                "</div>"
                "</div>"
                "</div>"
                "</div>"
            )

        st.markdown(f'<div class="notice-queue-list">{"".join(row_html)}</div>', unsafe_allow_html=True)

    def _ensure_collection_for_favorites(
        notice_view_df: pd.DataFrame,
        source_datasets: dict[str, object] | None,
    ) -> pd.DataFrame:
        if notice_view_df is not None and not notice_view_df.empty and "_collection_id" in notice_view_df.columns:
            return notice_view_df.copy()
        datasets = {
            "notice_current": notice_view_df if isinstance(notice_view_df, pd.DataFrame) else pd.DataFrame(),
            "pending": pd.DataFrame(),
            "notice_archive": pd.DataFrame(),
        }
        return build_crawled_notice_collection(datasets, source_datasets)

    def _render_notice_queue_screen(
        source_df: pd.DataFrame,
        opportunity_df: pd.DataFrame,
        detail_opportunity_df: pd.DataFrame,
    ) -> None:
        status_filter_key = _status_filter_state_key()
        recommendation_filter_key = _recommendation_filter_state_key()
        search_key = _search_state_key()

        _consume_notice_filter_query_actions()
        consume_favorite_toggle_query_action()

        source_df = _enrich_notice_rows(source_df, detail_opportunity_df)

        current_view, selected_id = get_route_state(detail_page_key)
        if current_view == "detail":
            selected_notice_id = clean(selected_id) or clean(st.session_state.get(_selected_notice_state_key(), ""))
            if clean(selected_id):
                st.session_state[_selected_notice_state_key()] = clean(selected_id)
            selected_row = _get_notice_row_by_id(source_df, selected_notice_id)
            back_col, info_col = st.columns([1, 5])
            with back_col:
                if st.button("목록으로", key=f"{detail_page_key}_back_to_table", use_container_width=True):
                    switch_to_table(detail_page_key)
            with info_col:
                st.markdown('<div class="page-note">공고 탐색 Queue에서 선택한 상세 화면입니다.</div>', unsafe_allow_html=True)
            if not selected_row:
                st.info("?좏깮???怨듦퀬 ?곸꽭瑜?李얠쓣 ???놁뒿?덈떎.")
                return
            render_notice_detail_from_row(selected_row, detail_opportunity_df)
            return

        render_page_header(
            "Notice Queue",
            "빠르게 훑고, 바로 판단하고, 곧바로 상세로 들어갈 수 있게 공고 탐색 Queue 중심으로 정리했습니다.",
            eyebrow="Notices",
        )
        render_notice_queue_ui_styles()
        _inject_notice_queue_dashboard_styles()
        if source_df is None or source_df.empty:
            st.info("표시할 공고가 없습니다.")
            return

        st.session_state.setdefault(status_filter_key, "all")
        st.session_state.setdefault(recommendation_filter_key, "all")
        st.session_state.setdefault(search_key, "")

        st.markdown(
            '<div class="notice-queue-note">공고 상태와 추천 상태만 빠르게 좁히고, 카드 전체를 눌러 상세 화면으로 바로 이동할 수 있습니다.</div>',
            unsafe_allow_html=True,
        )
        selected_status = _normalize_status_filter(
            _render_filter_control("공고상태 필터", STATUS_FILTER_OPTIONS, status_filter_key)
        )
        st.session_state[status_filter_key] = selected_status
        selected_recommendation = _normalize_recommendation_filter(
            _render_filter_control("추천여부 필터", RECOMMENDATION_FILTER_OPTIONS, recommendation_filter_key)
        )
        st.session_state[recommendation_filter_key] = selected_recommendation

        search_col, reset_col = st.columns([6, 1])
        with search_col:
            search_text = st.text_input(
                "검색",
                key=search_key,
                placeholder="공고명 / 과제명 / 기관명 검색",
            )
        with reset_col:
            st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
            if st.button("초기화", key=f"{detail_page_key}_search_reset", use_container_width=True):
                st.session_state[search_key] = ""
                st.session_state[status_filter_key] = "all"
                st.session_state[recommendation_filter_key] = "all"
                st.rerun()

        filtered_source_df = _apply_notice_filters(
            source_df,
            selected_status,
            selected_recommendation,
            search_text,
        )

        st.caption(f"결과 {len(filtered_source_df)}건")
        render_crawled_notice_rows(
            filtered_source_df,
            key_prefix=f"{detail_page_key}_list",
            page_key=detail_page_key,
        )

    def _notice_filters_state_key() -> str:
        return "notice_filters"

    def _notice_filter_widget_key(field_name: str) -> str:
        return f"{detail_page_key}_notice_filter_widget_{field_name}"

    def _default_notice_filters() -> dict[str, str]:
        return {
            "status": "전체",
            "recommendation": "전체",
            "search": "",
        }

    def _get_notice_filters() -> dict[str, str]:
        defaults = _default_notice_filters()
        current_value = st.session_state.get(_notice_filters_state_key(), {})
        filters = defaults.copy()
        if isinstance(current_value, dict):
            filters.update(
                {
                    "status": _normalize_status_filter(current_value.get("status", defaults["status"])),
                    "recommendation": _normalize_recommendation_filter(
                        current_value.get("recommendation", defaults["recommendation"])
                    ),
                    "search": clean(current_value.get("search", defaults["search"])),
                }
            )
        st.session_state[_notice_filters_state_key()] = filters
        return filters

    def _set_notice_filters(filters: dict[str, str]) -> dict[str, str]:
        next_filters = {
            "status": _normalize_status_filter(filters.get("status", "전체")),
            "recommendation": _normalize_recommendation_filter(filters.get("recommendation", "전체")),
            "search": clean(filters.get("search", "")),
        }
        st.session_state[_notice_filters_state_key()] = next_filters
        return next_filters

    def _sync_notice_filter(field_name: str) -> None:
        filters = _get_notice_filters()
        widget_key = _notice_filter_widget_key(field_name)
        widget_value = st.session_state.get(widget_key, filters.get(field_name, ""))
        if field_name == "status":
            filters["status"] = _normalize_status_filter(widget_value)
        elif field_name == "recommendation":
            filters["recommendation"] = _normalize_recommendation_filter(widget_value)
        else:
            filters["search"] = clean(widget_value)
        _set_notice_filters(filters)

    def _reset_notice_filters() -> None:
        filters = _set_notice_filters(_default_notice_filters())
        st.session_state[_notice_filter_widget_key("status")] = filters["status"]
        st.session_state[_notice_filter_widget_key("recommendation")] = filters["recommendation"]
        st.session_state[_notice_filter_widget_key("search")] = filters["search"]

    def _notice_detail_state_key() -> str:
        return f"{detail_page_key}_notice_detail_state"

    def _css_safe_key(value: str) -> str:
        return re.sub(r"[^0-9A-Za-z_-]", "-", clean(value))

    def _render_notice_title_button(title: str, *, button_key: str, row: pd.Series) -> None:
        safe_key = _css_safe_key(button_key)
        st.markdown(
            f"""
            <style>
            .st-key-{safe_key} {{
              width: 100%;
            }}
            .st-key-{safe_key} button {{
              display: block;
              width: 100%;
              padding: 0;
              margin: 0;
              border: 0 !important;
              background: transparent !important;
              box-shadow: none !important;
              color: var(--text-strong) !important;
              font-size: 1.12rem !important;
              font-weight: 900 !important;
              line-height: 1.45 !important;
              text-align: left !important;
              justify-content: flex-start !important;
              white-space: normal !important;
              min-height: 0 !important;
            }}
            .st-key-{safe_key} button:hover,
            .st-key-{safe_key} button:active,
            .st-key-{safe_key} button:focus {{
              border: 0 !important;
              background: transparent !important;
              box-shadow: none !important;
              color: var(--text-strong) !important;
            }}
            .st-key-{safe_key} button p {{
              font-size: 1.12rem !important;
              font-weight: 900 !important;
              line-height: 1.45 !important;
              text-align: left !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
        if st.button(title, key=button_key, use_container_width=True):
            _open_notice_detail(row)

    def _filter_rows_for_tab(rows: pd.DataFrame, tab_key: str) -> pd.DataFrame:
        if rows is None or rows.empty:
            return pd.DataFrame()
        if tab_key == "iris":
            return rows[rows["source_key"].fillna("").astype(str).str.strip().eq("iris")].copy()
        if tab_key == "tipa":
            return rows[rows["source_key"].fillna("").astype(str).str.strip().eq("tipa")].copy()
        if tab_key == "nipa":
            return rows[rows["source_key"].fillna("").astype(str).str.strip().eq("nipa")].copy()
        if tab_key == "favorite":
            return rows[_review_series(rows).eq(FAVORITE_REVIEW_STATUS)].copy()
        if tab_key == "archive":
            return rows[rows["_notice_scope"].fillna("").astype(str).str.strip().eq("archive")].copy()
        return rows.copy()

    def _default_notice_detail_state() -> dict[str, str]:
        return {
            "view": "table",
            "selected_notice_id": "",
            "source": "",
        }

    def _get_notice_detail_state() -> dict[str, str]:
        current_value = st.session_state.get(_notice_detail_state_key(), {})
        state = _default_notice_detail_state()
        if isinstance(current_value, dict):
            state.update(
                {
                    "view": clean(current_value.get("view", state["view"])) or "table",
                    "selected_notice_id": clean(current_value.get("selected_notice_id", "")),
                    "source": clean(current_value.get("source", "")),
                }
            )
        st.session_state[_notice_detail_state_key()] = state
        return state

    def _set_notice_detail_state(view: str, notice_id: str = "", source: str = "") -> dict[str, str]:
        next_state = {
            "view": clean(view) or "table",
            "selected_notice_id": clean(notice_id),
            "source": clean(source),
        }
        st.session_state[_notice_detail_state_key()] = next_state
        st.session_state["selected_notice_id"] = next_state["selected_notice_id"]
        st.session_state[_selected_notice_state_key()] = next_state["selected_notice_id"]
        return next_state

    def _open_notice_detail(row: pd.Series) -> None:
        notice_id = _resolve_notice_id(row)
        if not notice_id:
            return
        source_value = clean(first_non_empty(row, "source_site", "source_key", "_source_key"))
        _set_notice_detail_state("notice_detail", notice_id, source_value)
        st.rerun()

    def _close_notice_detail() -> None:
        _set_notice_detail_state("table", "", "")
        st.rerun()

    def render_crawled_notice_rows(
        rows: pd.DataFrame,
        *,
        key_prefix: str,
        limit: int = 30,
        page_key: str = detail_page_key,
        empty_message: str = "표시할 공고가 없습니다.",
    ) -> None:
        del page_key
        if rows is None or rows.empty:
            st.info(empty_message)
            return

        for position, (_, row) in enumerate(rows.head(limit).iterrows()):
            notice_id = _resolve_notice_id(row)
            source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
            title = clean(first_non_empty(row, "notice_title", "공고명")) or notice_id or "-"
            ministry = clean(first_non_empty(row, "ministry", "소관부처")) or "-"
            agency = clean(first_non_empty(row, "agency", "전문기관", "담당부서")) or "-"
            period = clean(first_non_empty(row, "notice_period", "period", "접수기간", "요청기간")) or "-"
            budget = clean(row.get("_queue_budget")) or "-"
            recommendation = clean(row.get("_queue_recommendation"))
            analysis_text = clean(row.get("_queue_analysis")) or "-"
            review_value = _review_value(row)
            source_label = clean(first_non_empty(row, "source_label", "매체")) or (source_key or "IRIS").upper()
            is_favorite = _is_favorite(review_value)
            scope = clean(first_non_empty(row, "_notice_scope"))
            status = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "공고상태"))
            if not status:
                if scope == "archive":
                    status = "마감"
                elif scope == "scheduled":
                    status = "예정"
                else:
                    status = "진행중"

            with st.container(border=True):
                left_col, right_col = st.columns([5.2, 1.8], gap="medium")
                with left_col:
                    title_badges = [f'<span class="notice-chip notice-chip-source">{escape(source_label)}</span>']
                    if is_favorite:
                        title_badges.append(_favorite_badge_html())
                    st.markdown(f'<div class="notice-queue-topline">{"".join(title_badges)}</div>', unsafe_allow_html=True)
                    _render_notice_title_button(
                        title,
                        button_key=f"{key_prefix}_open_notice_{notice_id}_{position}",
                        row=row,
                    )
                    analysis_class = "notice-queue-analysis" if clean(row.get("_queue_analysis")) else "notice-queue-analysis is-empty"
                    st.markdown('<div class="notice-queue-analysis-label">Analysis</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="{analysis_class}">{escape(analysis_text)}</div>', unsafe_allow_html=True)
                    st.markdown(
                        (
                            '<div class="notice-queue-meta">'
                            f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">Org</span>{escape(ministry)} / {escape(agency)}</div>'
                            f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">Period</span>{escape(period)}</div>'
                            f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">Budget</span>{escape(budget)}</div>'
                            "</div>"
                        ),
                        unsafe_allow_html=True,
                    )
                with right_col:
                    st.markdown(
                        (
                            f'<div class="{_status_badge_class(status)}">{escape(status)}</div>'
                            f'{_recommendation_badge_html(recommendation)}'
                            '<div class="notice-queue-cta">Notice Detail</div>'
                        ),
                        unsafe_allow_html=True,
                    )
                    if notice_id:
                        render_favorite_scrap_button(
                            notice_id=notice_id,
                            current_value=review_value,
                            source_key=source_key or "iris",
                            notice_title=title,
                            button_key=f"{key_prefix}_favorite_{notice_id}_{position}",
                            compact=True,
                        )

    def _render_notice_queue_screen(
        source_df: pd.DataFrame,
        opportunity_df: pd.DataFrame,
        detail_opportunity_df: pd.DataFrame,
    ) -> None:
        del opportunity_df
        source_df = _enrich_notice_rows(source_df, detail_opportunity_df)

        detail_state = _get_notice_detail_state()
        if detail_state["view"] == "notice_detail":
            selected_row = _get_notice_row_by_id(source_df, detail_state["selected_notice_id"])
            back_col, info_col = st.columns([1, 5])
            with back_col:
                if st.button("목록으로", key=f"{detail_page_key}_back_to_table", use_container_width=True):
                    _close_notice_detail()
            with info_col:
                st.markdown('<div class="page-note">선택한 Notice 상세 화면입니다.</div>', unsafe_allow_html=True)
            if not selected_row:
                st.info("선택한 공고 상세를 찾을 수 없습니다.")
                return
            render_notice_detail_from_row(selected_row, detail_opportunity_df)
            return

        render_page_header(
            "Notice Queue",
            "빠르게 훑고, 바로 판단하고, 같은 앱 안에서 Notice 상세로 진입하는 공고 탐색 Queue입니다.",
            eyebrow="Notices",
        )
        render_notice_queue_ui_styles()
        _inject_notice_queue_dashboard_styles()
        if source_df is None or source_df.empty:
            st.info("표시할 공고가 없습니다.")
            return

        filters = _get_notice_filters()
        status_widget_key = _notice_filter_widget_key("status")
        recommendation_widget_key = _notice_filter_widget_key("recommendation")
        search_widget_key = _notice_filter_widget_key("search")
        st.session_state.setdefault(status_widget_key, filters["status"])
        st.session_state.setdefault(recommendation_widget_key, filters["recommendation"])
        st.session_state.setdefault(search_widget_key, filters["search"])

        st.markdown(
            '<div class="notice-queue-note">탭으로 범위를 나누고, 필터와 검색은 그대로 유지한 채 같은 앱 안에서 Notice 상세를 확인합니다.</div>',
            unsafe_allow_html=True,
        )
        st.markdown('<div class="notice-filter-group-title">공고상태 필터</div>', unsafe_allow_html=True)
        st.radio(
            "status-filter",
            options=[value for value, _ in STATUS_FILTER_OPTIONS],
            key=status_widget_key,
            horizontal=True,
            label_visibility="collapsed",
            format_func=lambda value: dict(STATUS_FILTER_OPTIONS).get(value, value),
            on_change=_sync_notice_filter,
            args=("status",),
        )
        st.markdown('<div class="notice-filter-group-title">추천여부 필터</div>', unsafe_allow_html=True)
        st.radio(
            "recommendation-filter",
            options=[value for value, _ in RECOMMENDATION_FILTER_OPTIONS],
            key=recommendation_widget_key,
            horizontal=True,
            label_visibility="collapsed",
            format_func=lambda value: dict(RECOMMENDATION_FILTER_OPTIONS).get(value, value),
            on_change=_sync_notice_filter,
            args=("recommendation",),
        )

        search_col, reset_col = st.columns([6, 1])
        with search_col:
            st.text_input(
                "search-filter",
                key=search_widget_key,
                placeholder="공고명 / 과제명 / 기관명 검색",
                label_visibility="collapsed",
                on_change=_sync_notice_filter,
                args=("search",),
            )
        with reset_col:
            st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
            if st.button("초기화", key=f"{detail_page_key}_search_reset", use_container_width=True):
                _reset_notice_filters()
                st.rerun()

        filters = _get_notice_filters()
        filtered_source_df = _apply_notice_filters(
            source_df,
            filters["status"],
            filters["recommendation"],
            filters["search"],
        )

        st.caption(f"결과 {len(filtered_source_df)}건")
        tabs = st.tabs([label for label, _ in TOP_TAB_OPTIONS])
        for tab, (label, tab_key) in zip(tabs, TOP_TAB_OPTIONS):
            with tab:
                tab_rows = _filter_rows_for_tab(filtered_source_df, tab_key)
                render_crawled_notice_rows(
                    tab_rows,
                    key_prefix=f"{detail_page_key}_{tab_key}_list",
                    page_key=detail_page_key,
                    empty_message=f"{label} 탭에 표시할 공고가 없습니다.",
                )
    def render_favorite_notice_page(
        notice_view_df: pd.DataFrame,
        opportunity_df: pd.DataFrame,
        source_datasets: dict[str, object] | None = None,
    ) -> None:
        current_view, selected_id = get_route_state("favorites")
        source_df = _ensure_collection_for_favorites(notice_view_df, source_datasets)
        source_df = _enrich_notice_rows(source_df, opportunity_df)
        if current_view == "detail":
            selected_row = get_row_by_column_value(source_df, "_collection_id", selected_id)
            back_col, info_col = st.columns([1, 5])
            with back_col:
                if st.button("목록으로", key="favorites_back_to_table", use_container_width=True):
                    switch_to_table("favorites")
            with info_col:
                st.markdown('<div class="page-note">관심공고 목록에서 선택한 상세 화면입니다.</div>', unsafe_allow_html=True)
            render_notice_detail_from_row(selected_row, opportunity_df)
            return

        st.subheader("관심공고")
        st.caption("검토 여부가 관심공고인 공고만 모아 봅니다.")
        if source_df is None or source_df.empty:
            st.info("아직 관심공고로 지정한 공고가 없습니다.")
            return
        favorite_rows = source_df[_review_series(source_df).eq(FAVORITE_REVIEW_STATUS)].copy()
        if favorite_rows.empty:
            st.info("아직 관심공고로 지정한 공고가 없습니다.")
            return
        render_crawled_notice_rows(
            favorite_rows,
            key_prefix=f"{detail_page_key}_favorite_page",
            page_key="favorites",
            empty_message="아직 관심공고로 지정한 공고가 없습니다.",
        )

    def render_notice_queue_page(datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None) -> None:
        source_df = build_crawled_notice_collection(datasets, source_datasets)
        _render_notice_queue_screen(
            source_df,
            datasets.get("opportunity", pd.DataFrame()),
            datasets["opportunity_all"],
        )

    def render_notices_source(
        source_config,
        mode_config,
        datasets: dict[str, pd.DataFrame],
        source_datasets: dict[str, object] | None,
        *,
        show_internal_tabs: bool = True,
    ) -> None:
        del source_config, mode_config, show_internal_tabs
        source_df = build_crawled_notice_collection(datasets, source_datasets)
        _render_notice_queue_screen(
            source_df,
            datasets.get("opportunity", pd.DataFrame()),
            datasets["opportunity_all"],
        )

    ns["consume_favorite_toggle_query_action"] = consume_favorite_toggle_query_action
    ns["render_favorite_scrap_button"] = render_favorite_scrap_button
    ns["favorite_button_props"] = favorite_button_props
    ns["render_crawled_notice_rows"] = render_crawled_notice_rows
    ns["render_favorite_notice_page"] = render_favorite_notice_page
    ns["render_notice_queue_page"] = render_notice_queue_page
    if "render_notices_source" in ns:
        ns["render_notices_source"] = render_notices_source
