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
    ("추천", "추천"),
    ("보통", "보통"),
]

TOP_TAB_OPTIONS: list[tuple[str, str]] = [
    ("IRIS", "iris"),
    ("MSS", "tipa"),
    ("NIPA", "nipa"),
    ("관심공고", "favorite"),
    ("보관/마감", "archive"),
]

RECOMMENDATION_RANK = {
    "추천": 3,
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
    build_project_analysis_text = ns.get("build_project_analysis_text")
    pill_html = ns.get("_pill_html")
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

    def _normalize_key_text(value: object) -> str:
        return re.sub(r"\s+", " ", clean(value)).strip().lower()

    def _truncate_queue_text(value: object, max_chars: int = 170) -> str:
        text = re.sub(r"\s+", " ", clean(value)).strip()
        if len(text) <= max_chars:
            return text
        trimmed = text[:max_chars].rsplit(" ", 1)[0].strip()
        return (trimmed or text[:max_chars].strip()).rstrip("., ") + "..."

    def _compose_queue_analysis(row: dict | pd.Series | None) -> str:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
        title_text = _normalize_key_text(first_non_empty(row_dict, "notice_title", "공고명"))
        project_text = clean(first_non_empty(row_dict, "_queue_project_name"))
        reason_text = clean(first_non_empty(row_dict, "_queue_reason"))
        field_text = clean(first_non_empty(row_dict, "_queue_application_field"))
        market_text = clean(first_non_empty(row_dict, "_queue_target_market"))
        keyword_text = clean(first_non_empty(row_dict, "_queue_keywords"))

        if callable(build_project_analysis_text):
            try:
                analysis_text = clean(build_project_analysis_text(row_dict))
                if analysis_text:
                    first_paragraph = next(
                        (part.strip() for part in re.split(r"\n\s*\n", analysis_text) if clean(part)),
                        analysis_text,
                    )
                    compact = _truncate_queue_text(first_paragraph)
                    if compact and _normalize_key_text(compact) != title_text:
                        return compact
            except Exception:
                pass

        if reason_text and _normalize_key_text(reason_text) != title_text:
            return _truncate_queue_text(reason_text)
        if project_text and _normalize_key_text(project_text) != title_text:
            if field_text:
                return _truncate_queue_text(f"{project_text}. {field_text} 분야와 연결된 과제로 검토할 수 있습니다.")
            return _truncate_queue_text(project_text)
        if market_text and field_text:
            return _truncate_queue_text(f"{market_text}과 {field_text} 분야 확장 가능성이 있는 과제로 보입니다.")
        if field_text:
            return _truncate_queue_text(f"{field_text} 분야 중심의 과제로 판단됩니다.")
        if market_text:
            return _truncate_queue_text(f"{market_text} 시장과의 연결성이 높은 과제로 보입니다.")
        if keyword_text:
            return _truncate_queue_text(f"{keyword_text} 중심의 기술 Opportunity로 검토할 수 있습니다.")
        return ""

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
        icon_only: bool = False,
        use_container_width: bool | None = None,
    ) -> None:
        if not clean(notice_id):
            return
        is_favorite, button_label, _ = favorite_button_props(current_value)
        if icon_only:
            button_label = "★" if is_favorite else "☆"
        next_value = UNFAVORITE_REVIEW_STATUS if is_favorite else FAVORITE_REVIEW_STATUS
        safe_key = _css_safe_key(button_key)
        if compact:
            active_bg = "#fff7ed" if is_favorite else "#ffffff"
            active_border = "#fdba74" if is_favorite else "#cbd5e1"
            active_color = "#c2410c" if is_favorite else "#64748b"
            min_width = "42px" if icon_only else "auto"
            padding = "0" if icon_only else "0.15rem 0.8rem"
            st.markdown(
                f"""
                <style>
                .st-key-{safe_key} {{
                  display: flex;
                  justify-content: flex-end;
                }}
                .st-key-{safe_key} button {{
                  min-height: 36px !important;
                  min-width: {min_width} !important;
                  padding: {padding} !important;
                  border-radius: 999px !important;
                  border: 1px solid {active_border} !important;
                  background: {active_bg} !important;
                  color: {active_color} !important;
                  font-size: {("1.02rem" if icon_only else "0.88rem")} !important;
                  font-weight: 800 !important;
                  white-space: nowrap !important;
                  box-shadow: none !important;
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
            return "보통"
        if "보통" in text:
            return "보통"
        if "추천" in text or "recommend" in lowered:
            return "추천"
        if "검토" in text or "보류" in text or "hold" in lowered:
            return "보통"
        return text

    def _normalize_recommendation_filter(value: str) -> str:
        normalized = _normalize_recommendation_value(value)
        if normalized in {option for option, _ in RECOMMENDATION_FILTER_OPTIONS}:
            return normalized
        return RECOMMENDATION_FILTER_OPTIONS[0][0]

    def _status_filter_state_key() -> str:
        return f"{detail_page_key}_selected_status_filter"

    def _recommendation_filter_state_key() -> str:
        return f"{detail_page_key}_selected_recommendation_filter"

    def _search_state_key() -> str:
        return f"{detail_page_key}_search_text"

    def _selected_notice_state_key() -> str:
        return f"{detail_page_key}_selected_notice_id"

    def _notice_detail_state_key() -> str:
        return f"{detail_page_key}_notice_detail_state"

    def _css_safe_key(value: str) -> str:
        return re.sub(r"[^0-9A-Za-z_-]", "-", clean(value))

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

        route_page = clean(get_query_param("page"))
        route_view = clean(get_query_param("view"))
        route_notice_id = clean(get_query_param("id"))
        route_source = clean(get_query_param("source"))
        if route_page == detail_page_key and route_view == "detail" and route_notice_id:
            state.update(
                {
                    "view": "notice_detail",
                    "selected_notice_id": route_notice_id,
                    "source": route_source,
                }
            )
        elif route_page == detail_page_key and route_view == "table":
            state = _default_notice_detail_state()

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

        params = get_query_params_dict()
        params["page"] = detail_page_key
        if next_state["source"]:
            params["source"] = next_state["source"]
        if next_state["view"] == "notice_detail" and next_state["selected_notice_id"]:
            params["view"] = "detail"
            params["id"] = next_state["selected_notice_id"]
        else:
            params["view"] = "table"
            params.pop("id", None)
        _replace_params(_auth_params(params))
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
        if opportunity_df is None or opportunity_df.empty:
            return pd.DataFrame(
                columns=[
                    "notice_id",
                    "_queue_recommendation",
                    "_queue_project_name",
                    "_queue_budget",
                    "_queue_reason",
                    "_queue_keywords",
                    "_queue_application_field",
                    "_queue_target_market",
                    "_queue_support_type",
                    "_queue_notice_period",
                    "_queue_notice_no",
                    "_queue_notice_date",
                ]
            )

        working = opportunity_df.copy()
        working["notice_id"] = _safe_series(working, ["notice_id", "공고ID", "Notice ID", "source_notice_id"])
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
                    "_queue_application_field",
                    "_queue_target_market",
                    "_queue_support_type",
                    "_queue_notice_period",
                    "_queue_notice_no",
                    "_queue_notice_date",
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
        working["_queue_application_field"] = _safe_series(
            working,
            ["llm_application_field", "application_field", "Application Field"],
        )
        working["_queue_target_market"] = _safe_series(
            working,
            ["target_market", "대표관심영역", "llm_score_target_markets"],
        )
        working["_queue_support_type"] = _safe_series(
            working,
            ["pbofr_type", "공모유형", "support_type", "project_type"],
        )
        working["_queue_notice_period"] = _safe_series(
            working,
            ["notice_period", "period", "접수기간", "신청기간", "요청기간"],
        )
        working["_queue_notice_no"] = _safe_series(
            working,
            ["notice_no", "ancm_no", "공고번호"],
        )
        working["_queue_notice_date"] = _safe_series(
            working,
            ["registered_at", "ancm_de", "공고일자", "등록일"],
        )
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
                "_queue_application_field",
                "_queue_target_market",
                "_queue_support_type",
                "_queue_notice_period",
                "_queue_notice_no",
                "_queue_notice_date",
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
            "_queue_application_field",
            "_queue_target_market",
            "_queue_support_type",
            "_queue_notice_period",
            "_queue_notice_no",
            "_queue_notice_date",
        ):
            if column not in enriched.columns:
                enriched[column] = ""
            enriched[column] = enriched[column].fillna("").astype(str).str.strip()
        enriched["notice_no"] = _safe_series(enriched, ["notice_no", "공고번호", "ancm_no", "_queue_notice_no"])
        enriched["registered_at"] = _safe_series(enriched, ["registered_at", "공고일자", "ancm_de", "_queue_notice_date"])
        enriched["pbofr_type"] = _safe_series(enriched, ["pbofr_type", "공모유형", "support_type", "_queue_support_type"])
        enriched["notice_period"] = _safe_series(enriched, ["notice_period", "접수기간", "period", "신청기간", "_queue_notice_period"])
        enriched["_queue_analysis"] = enriched.apply(_compose_queue_analysis, axis=1)
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

    def _normalize_status_filter_values(value: object) -> list[str]:
        allowed_values = [option for option, _ in STATUS_FILTER_OPTIONS if clean(option) and option != "전체"]
        if isinstance(value, (list, tuple, set)):
            normalized: list[str] = []
            for item in value:
                item_value = _normalize_status_filter(item)
                if item_value in allowed_values and item_value not in normalized:
                    normalized.append(item_value)
            return normalized
        normalized_value = _normalize_status_filter(value)
        return [normalized_value] if normalized_value in allowed_values else []

    def _normalize_recommendation_filter_values(value: object) -> list[str]:
        allowed_values = [option for option, _ in RECOMMENDATION_FILTER_OPTIONS if clean(option) and option != "전체"]
        if isinstance(value, (list, tuple, set)):
            normalized: list[str] = []
            for item in value:
                item_value = _normalize_recommendation_filter(item)
                if item_value in allowed_values and item_value not in normalized:
                    normalized.append(item_value)
            return normalized
        normalized_value = _normalize_recommendation_filter(value)
        return [normalized_value] if normalized_value in allowed_values else []

    def _apply_notice_filters(
        rows: pd.DataFrame,
        status_filter: object,
        recommendation_filter: object,
        search_text: str,
    ) -> pd.DataFrame:
        if rows is None or rows.empty:
            return pd.DataFrame()

        filtered = rows.copy()
        normalized_statuses = _normalize_status_filter_values(status_filter)
        normalized_recommendations = _normalize_recommendation_filter_values(recommendation_filter)

        if normalized_statuses:
            scope_map = {
                "진행중": "current",
                "예정": "scheduled",
                "마감": "archive",
            }
            allowed_scopes = [scope_map[value] for value in normalized_statuses if value in scope_map]
            if allowed_scopes:
                filtered = filtered[
                    filtered["_notice_scope"].fillna("").astype(str).str.strip().isin(allowed_scopes)
                ].copy()
        if normalized_recommendations:
            filtered = filtered[filtered["_queue_recommendation"].isin(normalized_recommendations)].copy()

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
            :root {
              --app-bg: #eef4fc;
              --surface: #ffffff;
              --surface-soft: #f8fbff;
              --surface-tint: #edf4ff;
              --border: #d8e3f2;
              --border-strong: #c8d7ec;
              --text-strong: #15233b;
              --text-body: #21314d;
              --text-muted: #6c7f9d;
              --text-subtle: #8fa0ba;
              --blue: #3c63f0;
              --blue-soft: #dfe9ff;
            }
            .notice-queue-note,
            .notice-queue-card-row,
            .notice-queue-card-shell,
            .notice-queue-card-topline,
            .notice-queue-header-label,
            .notice-queue-cell,
            .notice-queue-row-shell,
            .notice-queue-row,
            .notice-queue-breadcrumb,
            .notice-queue-meta,
            .notice-queue-meta-item,
            .notice-queue-meta-label {
              font-family: "Segoe UI", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
            }
            .notice-queue-note {
              margin: 0.85rem 0 0.35rem;
              color: var(--text-muted);
              font-size: 0.9rem;
              line-height: 1.55;
            }
            .notice-queue-card-row {
              display: grid;
              grid-template-columns: minmax(0, 1fr) 44px;
              gap: 0.5rem;
              align-items: flex-start;
              margin: 0.45rem 0;
            }
            .notice-queue-card-shell {
              display: block;
              text-decoration: none !important;
            }
            .notice-queue-card-topline {
              display: flex;
              flex-wrap: wrap;
              gap: 0.42rem;
              margin-bottom: 0.42rem;
            }
            .notice-queue-favorite-wrap {
              padding-top: 0;
              display: flex;
              justify-content: flex-end;
              align-items: flex-start;
            }
            .notice-queue-favorite-wrap div[data-testid="stButton"] {
              width: 100%;
            }
            .notice-queue-favorite-wrap div[data-testid="stButton"] > button {
              width: 42px;
              min-width: 42px;
              height: 42px;
              border-radius: 999px;
              border: 1px solid rgba(148, 163, 184, 0.26);
              background: #ffffff;
              color: #111827;
              box-shadow: none;
              padding: 0;
              transition: border-color 120ms ease, background-color 120ms ease, transform 120ms ease;
            }
            .notice-queue-favorite-wrap div[data-testid="stButton"] > button:hover {
              border-color: rgba(17, 24, 39, 0.22);
              background: #f8fafc;
              color: #111827;
              transform: none;
            }
            .notice-queue-header-label {
              color: #64748b;
              font-size: 0.78rem;
              font-weight: 800;
              text-transform: uppercase;
              letter-spacing: 0.04em;
            }
            .notice-queue-divider {
              width: 100%;
              height: 1px;
              background: rgba(226, 232, 240, 0.95);
              margin: 0.15rem 0;
            }
            .notice-chip {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              min-height: 22px;
              padding: 0 8px;
              border-radius: 999px;
              border: 1px solid transparent;
              font-size: 0.72rem;
              font-weight: 700;
              line-height: 1;
              white-space: nowrap;
            }
            .notice-chip-status {
              background: #ffffff;
              border-color: rgba(17, 24, 39, 0.12);
              color: #111827;
            }
            .notice-chip-status.is-archive {
              background: #ffffff;
              border-color: rgba(17, 24, 39, 0.12);
              color: #111827;
            }
            .notice-chip-status.is-scheduled {
              background: #ffffff;
              border-color: rgba(17, 24, 39, 0.12);
              color: #111827;
            }
            .notice-chip-recommend {
              background: #ffffff;
              border-color: rgba(17, 24, 39, 0.12);
              color: #111827;
            }
            .notice-chip-neutral {
              background: #ffffff;
              border-color: rgba(17, 24, 39, 0.12);
              color: #111827;
            }
            .notice-queue-title {
              color: #111827;
              font-size: 0.95rem;
              font-weight: 800;
              line-height: 1.35;
            }
            .notice-queue-title-meta {
              margin-top: 0.18rem;
              color: #111827;
              font-size: 0.78rem;
              line-height: 1.35;
            }
            .notice-queue-cell {
              padding: 0.45rem 0.1rem 0.45rem 0;
              min-width: 0;
            }
            .notice-queue-cell-text {
              color: #111827;
              font-size: 0.9rem;
              line-height: 1.4;
            }
            .notice-queue-cell-muted {
              color: #4b5563;
              font-size: 0.78rem;
              line-height: 1.35;
            }
            .notice-queue-right-rail {
              width: 100%;
              max-width: 320px;
              margin-left: auto;
            }
            .notice-queue-summary-text {
              color: #4b5563;
              font-size: 0.78rem;
              line-height: 1.35;
              display: -webkit-box;
              -webkit-line-clamp: 2;
              -webkit-box-orient: vertical;
              overflow: hidden;
              text-align: left;
              word-break: keep-all;
            }
            .notice-queue-summary-text.is-empty {
              color: #6b7280;
            }
            .notice-queue-status-cell,
            .notice-queue-recommend-cell,
            .notice-queue-favorite-cell {
              display: flex;
              align-items: flex-start;
              justify-content: flex-start;
              min-height: 100%;
            }
            .notice-queue-favorite-cell {
              justify-content: center;
            }
            .notice-queue-empty {
              color: #94a3b8;
              font-weight: 600;
            }
            @media (max-width: 640px) {
              .notice-queue-title {
                font-size: 0.96rem;
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
        else:
            class_name = "notice-chip notice-chip-neutral"
        return f'<span class="{class_name}">{escape(normalized)}</span>'

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
        empty_message: str = "??? ??? ????.",
    ) -> None:
        del page_key
        if rows is None or rows.empty:
            st.info(empty_message)
            return

        for position, (_, row) in enumerate(rows.head(limit).iterrows()):
            notice_id = _resolve_notice_id(row)
            source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
            title = clean(first_non_empty(row, "notice_title", "???")) or notice_id or "-"
            ministry = clean(first_non_empty(row, "ministry", "????", "????")) or "-"
            agency = clean(first_non_empty(row, "agency", "????", "????")) or "-"
            notice_no = clean(first_non_empty(row, "notice_no", "????", "ancm_no")) or "-"
            notice_date = _queue_display_date_text(row)
            period_text = clean(first_non_empty(row, "notice_period", "????", "period", "_queue_notice_period", "????")) or "-"
            budget_text = clean(first_non_empty(row, "_queue_budget", "budget", "total_budget_text", "???")) or "???"
            recommendation = clean(row.get("_queue_recommendation"))
            recommendation_text = _normalize_recommendation_value(recommendation) or "??"
            review_value = _review_value(row)
            source_label = clean(first_non_empty(row, "source_label", "source_site", "??")) or (source_key or "IRIS").upper()
            scope = clean(first_non_empty(row, "_notice_scope"))
            status = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "????"))
            if not status:
                if scope == "archive":
                    status = "??"
                elif scope == "scheduled":
                    status = "??"
                else:
                    status = "???"

            agency_parts = [part for part in [ministry, agency] if clean(part) and part != "-"]
            agency_text = " / ".join(agency_parts) if agency_parts else source_label
            analysis_text = clean(first_non_empty(row, "_queue_analysis", "_queue_reason", "_queue_project_name"))
            subtitle_parts = [source_label]
            if notice_no and notice_no != "-":
                subtitle_parts.append(f"???? {notice_no}")
            subtitle_text = " ? ".join(subtitle_parts)
            meta_html = (
                '<div class="queue-list-card-meta">'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">??</div><div class="queue-list-card-meta-value">{escape(agency_text)}</div></div>'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">???</div><div class="queue-list-card-meta-value">{escape(notice_date)}</div></div>'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">????</div><div class="queue-list-card-meta-value">{escape(period_text)}</div></div>'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">??</div><div class="queue-list-card-meta-value">{escape(budget_text)}</div></div>'
                '</div>'
            )
            badges = "".join(
                [
                    _queue_card_badge_html(status, kind="status"),
                    _queue_card_badge_html(recommendation_text, kind="recommendation"),
                    _queue_card_badge_html(source_label, kind="neutral"),
                ]
            )
            summary_html = (
                f'<div class="notice-queue-summary-text">{escape(_truncate_queue_text(analysis_text, max_chars=120))}</div>'
                if analysis_text
                else '<div class="notice-queue-summary-text is-empty">연결된 RFP 분석이 아직 없습니다.</div>'
            )
            detail_href = build_route_href(detail_page_key, notice_id, source_key=source_key) if notice_id else ""
            card_html = (
                f'<a class="queue-list-link notice-queue-card-shell" href="{escape(detail_href, quote=True)}" target="_self">'
                '<div class="queue-card queue-list-card">'
                f'<div class="notice-queue-card-topline">{badges}</div>'
                f'<div class="queue-list-card-title">{escape(_truncate_queue_text(title, max_chars=110))}</div>'
                f'<div class="queue-list-card-subtitle">{escape(subtitle_text)}</div>'
                f'{meta_html}'
                '</div></a>'
            ) if detail_href else (
                '<div class="queue-card queue-list-card">'
                f'<div class="notice-queue-card-topline">{badges}</div>'
                f'<div class="queue-list-card-title">{escape(_truncate_queue_text(title, max_chars=110))}</div>'
                f'<div class="queue-list-card-subtitle">{escape(subtitle_text)}</div>'
                f'{meta_html}'
                '</div>'
            )

            card_left, card_right = st.columns([13, 5], gap="medium")
            with card_left:
                st.markdown(card_html, unsafe_allow_html=True)
            with card_right:
                st.markdown('<div class="notice-queue-right-rail">', unsafe_allow_html=True)
                summary_col, favorite_col = st.columns([5, 1], gap="small")
                with summary_col:
                    st.markdown(summary_html, unsafe_allow_html=True)
                with favorite_col:
                    st.markdown('<div class="notice-queue-favorite-wrap">', unsafe_allow_html=True)
                    if notice_id:
                        render_favorite_scrap_button(
                            notice_id=notice_id,
                            current_value=review_value,
                            source_key=source_key or "iris",
                            notice_title=title,
                            button_key=f"{key_prefix}_favorite_{notice_id}_{position}",
                            compact=True,
                            icon_only=True,
                            use_container_width=False,
                        )
                    else:
                        st.markdown('<div class="notice-queue-empty">-</div>', unsafe_allow_html=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)

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
                if st.button("????", key=f"{detail_page_key}_back_to_table", use_container_width=True):
                    _close_notice_detail()
            with info_col:
                st.markdown('<div class="page-note">??? Notice ?? ?????.</div>', unsafe_allow_html=True)
            if not selected_row:
                st.info("??? ?? ??? ?? ? ????.")
                return
            render_notice_detail_from_row(selected_row, detail_opportunity_df)
            return

        render_page_header(
            "Notice Queue",
            "RFP Queue? ?? ???? ??? ??, ??? ???, ?? ? ??? Notice ??? ?? ??? ? ?? ??????.",
            eyebrow="Notices",
        )
        render_notice_queue_ui_styles()
        _inject_notice_queue_dashboard_styles()
        if source_df is None or source_df.empty:
            st.info("??? ??? ????.")
            return

        filters = _get_notice_filters()
        status_widget_key = _notice_filter_widget_key("status")
        recommendation_widget_key = _notice_filter_widget_key("recommendation")
        search_widget_key = _notice_filter_widget_key("search")
        st.session_state.setdefault(status_widget_key, filters["status"])
        st.session_state.setdefault(recommendation_widget_key, filters["recommendation"])
        st.session_state.setdefault(search_widget_key, filters["search"])

        st.markdown(
            '<div class="queue-shell-note">?? ??? ?? ??? ??? ???, ?? ???? Notice ??? ?? RFP ???? ?? ??? ? ?? ??????.</div>',
            unsafe_allow_html=True,
        )
        st.markdown('<div class="queue-filter-label">?? / ??</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="queue-filter-help">?? ??? ??? ???, ??? ??? RFP Queue? ?? ???? ?????.</div>',
            unsafe_allow_html=True,
        )
        filter_cols = st.columns(2)
        with filter_cols[0]:
            st.multiselect(
                "?? ??",
                options=[value for value, _ in RECOMMENDATION_FILTER_OPTIONS if value != "??"],
                key=recommendation_widget_key,
                placeholder="??",
                on_change=_sync_notice_filter,
                args=("recommendation",),
            )
        with filter_cols[1]:
            st.multiselect(
                "?? ??",
                options=[value for value, _ in STATUS_FILTER_OPTIONS if value != "??"],
                key=status_widget_key,
                placeholder="??",
                on_change=_sync_notice_filter,
                args=("status",),
            )

        st.markdown('<div class="queue-search-label">??</div>', unsafe_allow_html=True)
        search_col, reset_col = st.columns([6, 1])
        with search_col:
            st.text_input(
                "search-filter",
                key=search_widget_key,
                placeholder="??? / ??? / ??? ??",
                label_visibility="collapsed",
                on_change=_sync_notice_filter,
                args=("search",),
            )
        with reset_col:
            st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
            if st.button("???", key=f"{detail_page_key}_search_reset", use_container_width=True):
                _reset_notice_filters()
                st.rerun()

        filters = _get_notice_filters()
        filtered_source_df = _apply_notice_filters(
            source_df,
            filters["status"],
            filters["recommendation"],
            filters["search"],
        )

        st.caption(f"?? {len(filtered_source_df)}?")
        tab_specs: list[tuple[str, str, pd.DataFrame]] = []
        for label, tab_key in TOP_TAB_OPTIONS:
            tab_rows = _filter_rows_for_tab(filtered_source_df, tab_key)
            tab_specs.append((label, tab_key, tab_rows))
        tabs = st.tabs([f"{label} {len(tab_rows)}?" for label, _, tab_rows in tab_specs])
        for tab, (label, tab_key, tab_rows) in zip(tabs, tab_specs):
            with tab:
                render_crawled_notice_rows(
                    tab_rows,
                    key_prefix=f"{detail_page_key}_{tab_key}_list",
                    page_key=detail_page_key,
                    empty_message=f"{label} ?? ??? ??? ????.",
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
                if st.button("????", key="favorites_back_to_table", use_container_width=True):
                    switch_to_table("favorites")
            with info_col:
                st.markdown('<div class="page-note">???? ???? ??? ?? ?????.</div>', unsafe_allow_html=True)
            render_notice_detail_from_row(selected_row, opportunity_df)
            return

        st.subheader("????")
        st.caption("?? ??? ????? ??? ?? ???.")
        if source_df is None or source_df.empty:
            st.info("?? ????? ??? ??? ????.")
            return
        favorite_rows = source_df[_review_series(source_df).eq(FAVORITE_REVIEW_STATUS)].copy()
        if favorite_rows.empty:
            st.info("?? ????? ??? ??? ????.")
            return
        render_crawled_notice_rows(
            favorite_rows,
            key_prefix=f"{detail_page_key}_favorite_page",
            page_key="favorites",
            empty_message="?? ????? ??? ??? ????.",
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
