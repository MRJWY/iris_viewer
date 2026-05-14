from __future__ import annotations

import json
import re
from html import escape
from urllib.parse import urlencode

import pandas as pd
import streamlit as st


FAVORITE_REVIEW_STATUS = "관심공고"
UNFAVORITE_REVIEW_STATUS = "검토전"

SOURCE_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("all", "전체"),
    ("iris", "IRIS"),
    ("tipa", "MSS"),
    ("nipa", "NIPA"),
]

STATUS_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("all", "전체"),
    ("current", "진행중"),
    ("scheduled", "예정"),
    ("archive", "마감"),
    ("favorite", "관심공고"),
]


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
    filter_notice_queue_rows = ns["filter_notice_queue_rows"]
    get_query_param = ns["get_query_param"]
    get_query_params_dict = ns["get_query_params_dict"]
    series_from_candidates = ns["series_from_candidates"]
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
        return is_favorite, "★ 관심공고 등록됨" if is_favorite else "📌 관심공고 등록"

    def _favorite_badge_html() -> str:
        return '<span class="notice-favorite-badge">관심</span>'

    def _build_favorite_href(*, page_key: str, notice_id: str, current_value: str, source_key: str, notice_title: str) -> str:
        try:
            return build_favorite_toggle_href(
                page_key=page_key,
                notice_id=notice_id,
                current_value=current_value,
                source_key=source_key,
                notice_title=notice_title,
            )
        except TypeError:
            return build_favorite_toggle_href(
                page_key=page_key,
                notice_id=notice_id,
                current_value=current_value,
                source_key=source_key,
            )

    def _favorite_button_html(href: str, current_value: str, *, absolute: bool) -> str:
        is_favorite, label = _favorite_button_label(current_value)
        class_name = "notice-queue-row-action is-active" if is_favorite else "notice-queue-row-action"
        inline_style = "position:absolute;top:32px;right:44px;z-index:4;" if absolute else ""
        return (
            f'<a class="{class_name}" href="{escape(href, quote=True)}" style="{inline_style}" '
            'onclick="event.preventDefault(); event.stopPropagation(); window.location.href=this.href;">'
            f"{escape(label)}"
            "</a>"
        )

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
        if get_query_param("favorite_toggle") != "1":
            return
        notice_id = clean(get_query_param("favorite_notice_id"))
        source_key = clean(get_query_param("favorite_source_key")) or "iris"
        current_value = clean(get_query_param("favorite_current_value"))
        notice_title = clean(get_query_param("favorite_notice_title"))
        next_value = UNFAVORITE_REVIEW_STATUS if current_value == FAVORITE_REVIEW_STATUS else FAVORITE_REVIEW_STATUS
        try:
            if notice_id:
                _persist_review_status(
                    notice_id=notice_id,
                    source_key=source_key,
                    review_status=next_value,
                    notice_title=notice_title,
                )
        finally:
            params = get_query_params_dict()
            for key in (
                "favorite_toggle",
                "favorite_notice_id",
                "favorite_source_key",
                "favorite_current_value",
                "favorite_notice_title",
            ):
                params.pop(key, None)
            _replace_params(_auth_params(params))
            st.rerun()

    def render_favorite_scrap_button(
        *,
        notice_id: str,
        current_value: str,
        source_key: str = "iris",
        notice_title: str = "",
        button_key: str,
    ) -> None:
        del button_key
        if not clean(notice_id):
            return
        action_href = _build_favorite_href(
            page_key=clean(get_query_param("page")) or detail_page_key,
            notice_id=notice_id,
            current_value=clean(current_value),
            source_key=clean(source_key) or "iris",
            notice_title=clean(notice_title),
        )
        st.markdown(
            '<div style="display:flex;justify-content:flex-end;align-items:flex-start;">'
            f"{_favorite_button_html(action_href, current_value, absolute=False)}"
            "</div>",
            unsafe_allow_html=True,
        )

    def favorite_button_props(current_value: str) -> tuple[bool, str, str]:
        is_favorite, label = _favorite_button_label(current_value)
        return is_favorite, label, "primary" if is_favorite else "secondary"

    def _coerce_links(raw_value: object, default_label: str) -> list[dict[str, str]]:
        items: list[dict[str, str]] = []

        def push(url: object, label: object = "") -> None:
            normalized_url = clean(url)
            if not normalized_url:
                return
            items.append({"url": normalized_url, "label": clean(label) or default_label})

        def visit(value: object) -> None:
            if value is None:
                return
            try:
                if pd.isna(value):
                    return
            except TypeError:
                pass

            if isinstance(value, list):
                for entry in value:
                    visit(entry)
                return

            if isinstance(value, dict):
                nested = value.get("attachments") or value.get("items") or value.get("files") or value.get("links")
                if isinstance(nested, list):
                    visit(nested)
                push(
                    first_non_empty(value, "download_url", "url", "file_url", "attachment_url", "link", "href"),
                    first_non_empty(value, "file_name", "name", "title", "text", "label"),
                )
                return

            text = clean(value)
            if not text:
                return
            if text.startswith("[") or text.startswith("{"):
                try:
                    visit(json.loads(text))
                    return
                except Exception:
                    pass
            if re.match(r"^https?://", text, flags=re.IGNORECASE):
                push(text, default_label)

        visit(raw_value)
        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, str]] = []
        for item in items:
            key = (clean(item.get("url")), clean(item.get("label")))
            if not key[0] or key in seen:
                continue
            seen.add(key)
            deduped.append({"url": key[0], "label": key[1] or default_label})
        return deduped

    def _extract_assets(row: dict | pd.Series | None) -> dict[str, object]:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
        attachments: list[dict[str, str]] = []
        for field_name in ("attachments", "attachment_links", "rfp_files", "첨부파일", "첨부파일목록"):
            attachments.extend(_coerce_links(row_dict.get(field_name), "첨부파일"))

        rfp_candidates: list[dict[str, str]] = []
        direct_url = first_non_empty(row_dict, "download_url", "file_url", "attachment_url")
        direct_label = first_non_empty(row_dict, "file_name", "rfp_title") or "RFP 다운로드"
        if clean(direct_url):
            rfp_candidates.append({"url": clean(direct_url), "label": clean(direct_label) or "RFP 다운로드"})
        rfp_candidates.extend(_coerce_links(row_dict.get("rfp_files"), "RFP 다운로드"))

        if not rfp_candidates:
            for item in attachments:
                label_key = clean(item.get("label")).lower()
                if any(keyword in label_key for keyword in ("rfp", "proposal")):
                    rfp_candidates.append(item)

        deduped_attachments: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        for item in attachments:
            url = clean(item.get("url"))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            deduped_attachments.append({"url": url, "label": clean(item.get("label")) or "첨부파일"})

        rfp_download = rfp_candidates[0] if rfp_candidates else {}
        return {
            "rfp_download": {
                "url": clean(rfp_download.get("url")),
                "label": clean(rfp_download.get("label")) or "RFP 다운로드",
            },
            "attachments": deduped_attachments,
        }

    def _normalize_source_filter(value: str) -> str:
        normalized = clean(value).lower()
        if normalized in {option for option, _ in SOURCE_FILTER_OPTIONS}:
            return normalized
        return "all"

    def _normalize_status_filter(value: str) -> str:
        normalized = clean(value).lower()
        if normalized in {option for option, _ in STATUS_FILTER_OPTIONS}:
            return normalized
        return "all"

    def _source_filter_state_key() -> str:
        return f"{detail_page_key}_selected_source_filter"

    def _status_filter_state_key() -> str:
        return f"{detail_page_key}_selected_status_filter"

    def _search_state_key() -> str:
        return f"{detail_page_key}_search_text"

    def _build_filter_href(*, source_value: str | None = None, status_value: str | None = None) -> str:
        params = get_query_params_dict()
        params["page"] = detail_page_key
        params["view"] = "table"
        if source_value is not None:
            params["notice_source_filter_select"] = _normalize_source_filter(source_value)
        if status_value is not None:
            params["notice_status_filter_select"] = _normalize_status_filter(status_value)
        return f"?{urlencode(_auth_params(params))}"

    def _consume_notice_filter_query_actions() -> None:
        source_param = get_query_param("notice_source_filter_select")
        status_param = get_query_param("notice_status_filter_select")
        st.session_state.setdefault(_source_filter_state_key(), "all")
        st.session_state.setdefault(_status_filter_state_key(), "all")
        if not clean(source_param) and not clean(status_param):
            return
        if clean(source_param):
            st.session_state[_source_filter_state_key()] = _normalize_source_filter(source_param)
        if clean(status_param):
            st.session_state[_status_filter_state_key()] = _normalize_status_filter(status_param)
        params = get_query_params_dict()
        params["page"] = detail_page_key
        params["view"] = "table"
        params.pop("notice_source_filter_select", None)
        params.pop("notice_status_filter_select", None)
        _replace_params(_auth_params(params))
        st.rerun()

    def _apply_notice_filters(rows: pd.DataFrame, *, source_filter: str, status_filter: str) -> pd.DataFrame:
        if rows is None or rows.empty:
            return pd.DataFrame()

        filtered = rows.copy()
        normalized_source = _normalize_source_filter(source_filter)
        normalized_status = _normalize_status_filter(status_filter)

        if normalized_source != "all" and "source_key" in filtered.columns:
            filtered = filtered[filtered["source_key"].fillna("").astype(str).str.strip().eq(normalized_source)].copy()

        if normalized_status == "current":
            filtered = filtered[filtered["_notice_scope"].fillna("").astype(str).str.strip().eq("current")].copy()
        elif normalized_status == "scheduled":
            filtered = filtered[filtered["_notice_scope"].fillna("").astype(str).str.strip().eq("scheduled")].copy()
        elif normalized_status == "archive":
            filtered = filtered[filtered["_notice_scope"].fillna("").astype(str).str.strip().eq("archive")].copy()
        elif normalized_status == "favorite":
            filtered = filtered[_review_series(filtered).eq(FAVORITE_REVIEW_STATUS)].copy()

        return filtered

    def _recommended_rfp_count(opportunity_df: pd.DataFrame) -> int:
        if opportunity_df is None or opportunity_df.empty:
            return 0
        recommendation_series = _safe_series(
            opportunity_df,
            ["llm_recommendation", "recommendation", "추천여부", "추천 상태"],
        )
        return int(recommendation_series.str.contains("추천", regex=False).sum())

    def _build_kpi_items(rows: pd.DataFrame, opportunity_df: pd.DataFrame) -> list[tuple[str, str]]:
        review_series = _review_series(rows)
        scope_series = _safe_series(rows, ["_notice_scope"])
        return [
            ("전체 공고", str(len(rows))),
            ("진행중", str(int(scope_series.eq("current").sum()))),
            ("예정", str(int(scope_series.eq("scheduled").sum()))),
            ("마감/보관", str(int(scope_series.eq("archive").sum()))),
            ("관심공고", str(int(review_series.eq(FAVORITE_REVIEW_STATUS).sum()))),
            ("추천 RFP", str(_recommended_rfp_count(opportunity_df))),
        ]

    def _render_kpi_summary_cards(items: list[tuple[str, str]]) -> None:
        cards = []
        for label, value in items:
            cards.append(
                '<div class="notice-summary-card">'
                f'<div class="notice-summary-label">{escape(label)}</div>'
                f'<div class="notice-summary-value">{escape(value)}</div>'
                "</div>"
            )
        st.markdown(f'<div class="notice-summary-grid">{"".join(cards)}</div>', unsafe_allow_html=True)

    def _render_filter_bar(title: str, options: list[tuple[str, str]], *, current_value: str, filter_kind: str) -> None:
        links = []
        for option_value, label in options:
            active_class = " is-active" if option_value == current_value else ""
            href = _build_filter_href(
                source_value=option_value if filter_kind == "source" else None,
                status_value=option_value if filter_kind == "status" else None,
            )
            links.append(
                f'<a class="notice-filter-link{active_class}" href="{escape(href, quote=True)}">{escape(label)}</a>'
            )
        st.markdown(
            '<div class="notice-filter-group">'
            f'<div class="notice-filter-group-title">{escape(title)}</div>'
            f'<div class="notice-filter-bar">{"".join(links)}</div>'
            "</div>",
            unsafe_allow_html=True,
        )

    def _inject_notice_queue_dashboard_styles() -> None:
        st.markdown(
            """
            <style>
            .notice-summary-grid {
              display: grid;
              grid-template-columns: repeat(6, minmax(0, 1fr));
              gap: 1rem;
              margin: 1.35rem 0 1.1rem;
            }
            .notice-summary-card {
              padding: 1.15rem 1.35rem;
              border-radius: 24px;
              border: 1px solid rgba(203, 213, 225, 0.92);
              background: #ffffff;
              box-shadow: 0 16px 36px rgba(148, 163, 184, 0.10);
              cursor: default;
            }
            .notice-summary-label {
              color: var(--text-muted);
              font-size: 0.88rem;
              font-weight: 800;
              line-height: 1.4;
            }
            .notice-summary-value {
              margin-top: 0.6rem;
              color: var(--text-strong);
              font-size: 2.05rem;
              font-weight: 900;
              line-height: 1;
            }
            .notice-filter-group {
              margin: 0.95rem 0 0.35rem;
            }
            .notice-filter-group-title {
              color: var(--text-muted);
              font-size: 0.83rem;
              font-weight: 800;
              margin-bottom: 0.45rem;
            }
            .notice-filter-bar {
              display: flex;
              flex-wrap: wrap;
              gap: 1rem;
              border-bottom: 1px solid rgba(203, 213, 225, 0.82);
              margin-bottom: 0.6rem;
            }
            .notice-filter-link {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              padding: 0.1rem 0 0.78rem;
              color: var(--text-muted);
              font-size: 0.95rem;
              font-weight: 700;
              text-decoration: none !important;
              border-bottom: 2px solid transparent;
              transition: color 140ms ease, border-color 140ms ease;
            }
            .notice-filter-link:hover {
              color: var(--text-strong);
            }
            .notice-filter-link.is-active {
              color: #2563eb !important;
              border-bottom-color: #2563eb;
            }
            .notice-favorite-badge {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              padding: 4px 10px;
              border-radius: 999px;
              background: #ffedd5;
              color: #c2410c;
              font-size: 12px;
              font-weight: 800;
              line-height: 1;
            }
            .notice-queue-row-action {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              height: 36px;
              padding: 0 14px;
              background: #ffffff;
              border: 1px solid rgba(203, 213, 225, 0.9);
              border-radius: 8px;
              color: #374151 !important;
              font-size: 13px;
              font-weight: 700;
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
              border-color: #fb923c;
              color: #c2410c !important;
              font-weight: 800;
            }
            @media (max-width: 1180px) {
              .notice-summary-grid {
                grid-template-columns: repeat(3, minmax(0, 1fr));
              }
            }
            @media (max-width: 760px) {
              .notice-summary-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
              }
            }
            @media (max-width: 560px) {
              .notice-summary-grid {
                grid-template-columns: 1fr;
              }
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

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
            review_value = _review_value(row)
            is_favorite = _is_favorite(review_value)
            assets = _extract_assets(row)

            title = clean(first_non_empty(row, "공고명", "notice_title")) or "-"
            notice_date = clean(first_non_empty(row, "공고일자", "notice_date")) or "-"
            notice_no = clean(first_non_empty(row, "공고번호", "notice_no")) or "-"
            period = clean(first_non_empty(row, "접수기간", "notice_period", "period", "신청기간")) or "-"
            source_label = clean(first_non_empty(row, "매체", "source_label")) or (source_key or "IRIS").upper()
            ministry = clean(first_non_empty(row, "소관부처", "주관부처", "ministry"))
            agency = clean(first_non_empty(row, "전문기관", "agency", "수행기관"))
            breadcrumb_parts = [part for part in (ministry, agency) if clean(part) and part != "-"]
            breadcrumb = " > ".join(breadcrumb_parts) if breadcrumb_parts else source_label
            status = normalize_notice_status_label(first_non_empty(row, "공고상태", "status", "rcve_status"))
            scope = clean(first_non_empty(row, "_notice_scope"))
            if not status:
                if scope == "archive":
                    status = "마감"
                elif scope == "scheduled":
                    status = "예정"
                else:
                    status = "진행중"
            support_type = clean(first_non_empty(row, "공모유형", "pbofr_type", "support_type")) or "-"
            detail_href = build_route_href(detail_page_key, collection_id, source_key=source_key) if collection_id else "#"
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

            rfp = assets["rfp_download"]
            if clean(rfp.get("url")):
                rfp_html = (
                    f'<a href="{escape(clean(rfp.get("url")), quote=True)}" target="_blank" rel="noopener noreferrer" '
                    'onclick="event.stopPropagation()">RFP 다운로드</a>'
                )
            else:
                rfp_html = '<span style="color:#94a3b8;">첨부파일 없음</span>'

            attachments = assets["attachments"]
            if attachments:
                attachment_html = " / ".join(
                    f'<a href="{escape(clean(item.get("url")), quote=True)}" target="_blank" rel="noopener noreferrer" '
                    f'onclick="event.stopPropagation()">{escape(clean(item.get("label")) or "첨부파일")}</a>'
                    for item in attachments
                )
            else:
                attachment_html = '<span style="color:#94a3b8;">첨부파일 없음</span>'

            row_html.append(
                '<div class="notice-queue-row-shell">'
                f'{_favorite_button_html(action_href, review_value, absolute=True) if action_href else ""}'
                f'<div class="notice-queue-row" role="link" tabindex="0" onclick="window.location.href=\'{escape(detail_href, quote=True)}\'" '
                f'onkeydown="if(event.key===\'Enter\'||event.key===\' \'){{event.preventDefault();window.location.href=\'{escape(detail_href, quote=True)}\';}}">'
                f'<div class="notice-queue-breadcrumb">{escape(breadcrumb)}</div>'
                '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:0.45rem;">'
                f'<div class="notice-queue-title" style="margin-bottom:0;">{escape(title)}</div>'
                f'{_favorite_badge_html() if is_favorite else ""}'
                "</div>"
                '<div class="notice-queue-meta">'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">공고번호</span> {escape(notice_no)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">공고일자</span> {escape(notice_date)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">공고상태</span> {escape(status)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">공모유형</span> {escape(support_type)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">매체</span> {escape(source_label)}</div>'
                f'<div class="notice-queue-meta-item"><span class="notice-queue-meta-label">접수기간</span> {escape(period)}</div>'
                "</div>"
                '<div style="margin-top:0.7rem;display:flex;flex-wrap:wrap;gap:0.75rem 1.25rem;font-size:0.88rem;line-height:1.5;color:#475569;">'
                f'<div><span style="color:#64748b;font-weight:800;">RFP</span> {rfp_html}</div>'
                f'<div><span style="color:#64748b;font-weight:800;">첨부파일</span> {attachment_html}</div>'
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
        *,
        opportunity_df: pd.DataFrame,
        detail_opportunity_df: pd.DataFrame,
    ) -> None:
        source_filter_key = _source_filter_state_key()
        status_filter_key = _status_filter_state_key()
        search_key = _search_state_key()

        _consume_notice_filter_query_actions()
        consume_favorite_toggle_query_action()

        current_view, selected_id = get_route_state(detail_page_key)
        if current_view == "detail":
            selected_row = get_row_by_column_value(source_df, "_collection_id", selected_id)
            back_col, info_col = st.columns([1, 5])
            with back_col:
                if st.button("목록으로", key=f"{detail_page_key}_back_to_table", use_container_width=True):
                    switch_to_table(detail_page_key)
            with info_col:
                st.markdown('<div class="page-note">통합 공고 목록에서 선택한 상세 화면입니다.</div>', unsafe_allow_html=True)
            render_notice_detail_from_row(selected_row, detail_opportunity_df)
            return

        render_page_header(
            "Notice Queue",
            "IRIS, MSS, NIPA에서 크롤링한 공고를 한 곳에서 확인합니다.",
            eyebrow="Notices",
        )
        render_notice_queue_ui_styles()
        _inject_notice_queue_dashboard_styles()
        if source_df is None or source_df.empty:
            st.info("표시할 공고가 없습니다.")
            return

        st.session_state.setdefault(source_filter_key, "all")
        st.session_state.setdefault(status_filter_key, "all")
        st.session_state.setdefault(search_key, "")

        _render_kpi_summary_cards(_build_kpi_items(source_df, opportunity_df))
        _render_filter_bar(
            "Source Filter",
            SOURCE_FILTER_OPTIONS,
            current_value=_normalize_source_filter(st.session_state.get(source_filter_key, "all")),
            filter_kind="source",
        )
        _render_filter_bar(
            "Status Filter",
            STATUS_FILTER_OPTIONS,
            current_value=_normalize_status_filter(st.session_state.get(status_filter_key, "all")),
            filter_kind="status",
        )

        search_col, reset_col = st.columns([6, 1])
        with search_col:
            search_text = st.text_input("공고명", key=search_key, placeholder="공고명을 입력하세요")
        with reset_col:
            st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
            if st.button("초기화", key=f"{detail_page_key}_search_reset", use_container_width=True):
                st.session_state[search_key] = ""
                st.session_state[source_filter_key] = "all"
                st.session_state[status_filter_key] = "all"
                st.rerun()

        filtered_source_df = _apply_notice_filters(
            source_df,
            source_filter=st.session_state.get(source_filter_key, "all"),
            status_filter=st.session_state.get(status_filter_key, "all"),
        )
        filtered_source_df = filter_notice_queue_rows(filtered_source_df, search_text=search_text)

        if clean(search_text):
            st.caption(f"검색 결과 {len(filtered_source_df)}건")
        else:
            st.caption(f"전체 {len(filtered_source_df)}건")

        render_crawled_notice_rows(
            filtered_source_df,
            key_prefix=f"{detail_page_key}_list",
            page_key=detail_page_key,
        )

    def render_favorite_notice_page(
        notice_view_df: pd.DataFrame,
        opportunity_df: pd.DataFrame,
        source_datasets: dict[str, object] | None = None,
    ) -> None:
        consume_favorite_toggle_query_action()
        current_view, selected_id = get_route_state("favorites")
        source_df = _ensure_collection_for_favorites(notice_view_df, source_datasets)
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
            opportunity_df=datasets.get("opportunity", pd.DataFrame()),
            detail_opportunity_df=datasets["opportunity_all"],
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
            opportunity_df=datasets.get("opportunity", pd.DataFrame()),
            detail_opportunity_df=datasets["opportunity_all"],
        )

    ns["consume_favorite_toggle_query_action"] = consume_favorite_toggle_query_action
    ns["render_favorite_scrap_button"] = render_favorite_scrap_button
    ns["favorite_button_props"] = favorite_button_props
    ns["render_crawled_notice_rows"] = render_crawled_notice_rows
    ns["render_favorite_notice_page"] = render_favorite_notice_page
    ns["render_notice_queue_page"] = render_notice_queue_page
    if "render_notices_source" in ns:
        ns["render_notices_source"] = render_notices_source
