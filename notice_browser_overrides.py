from __future__ import annotations

import json
import re
from html import escape

import pandas as pd
import streamlit as st


FAVORITE_REVIEW_STATUS = "관심공고"
UNFAVORITE_REVIEW_STATUS = "검토전"


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
    get_notice_queue_filter_state_key = ns["get_notice_queue_filter_state_key"]
    consume_notice_queue_filter_query_action = ns["consume_notice_queue_filter_query_action"]
    normalize_notice_queue_filter = ns["normalize_notice_queue_filter"]
    build_notice_queue_metric_items = ns["build_notice_queue_metric_items"]
    render_notice_queue_kpi_cards = ns["render_notice_queue_kpi_cards"]
    apply_notice_queue_kpi_filter = ns["apply_notice_queue_kpi_filter"]
    reset_notice_queue_controls = ns["reset_notice_queue_controls"]
    get_query_param = ns["get_query_param"]
    get_query_params_dict = ns["get_query_params_dict"]
    save_review_status = ns.get("save_review_status")
    update_notice_review_status = ns.get("update_notice_review_status")
    update_mss_review_status = ns.get("update_mss_review_status")
    update_nipa_review_status = ns.get("update_nipa_review_status")
    is_user_scoped_operations_enabled = ns.get("is_user_scoped_operations_enabled")
    upsert_user_review_status = ns.get("upsert_user_review_status")
    get_current_operation_scope_key = ns.get("get_current_operation_scope_key")
    replace_query_params = ns.get("replace_query_params")
    with_auth_params = ns.get("with_auth_params")

    def _clear_notice_caches() -> None:
        for name in (
            "load_sheet_as_dataframe",
            "load_optional_sheet_as_dataframe",
            "load_app_datasets",
            "build_source_datasets",
            "load_user_review_statuses",
        ):
            fn = ns.get(name)
            clear_fn = getattr(fn, "clear", None)
            if callable(clear_fn):
                clear_fn()

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

    def _coerce_links(raw_value: object, default_label: str) -> list[dict[str, str]]:
        items: list[dict[str, str]] = []

        def push(url: object, label: object = "") -> None:
            normalized_url = clean(url)
            if not normalized_url:
                return
            items.append(
                {
                    "url": normalized_url,
                    "label": clean(label) or default_label,
                }
            )

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

    def _review_value(row: dict | pd.Series | None) -> str:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
        return clean(first_non_empty(row_dict, "review_status", "검토 여부", "검토여부"))

    def _is_favorite(row_or_value: dict | pd.Series | str | None) -> bool:
        value = _review_value(row_or_value) if isinstance(row_or_value, (dict, pd.Series)) else clean(row_or_value)
        return value == FAVORITE_REVIEW_STATUS

    def _favorite_state(current_value: str) -> tuple[bool, str]:
        is_favorite = _is_favorite(current_value)
        return is_favorite, "★ 관심공고 등록됨" if is_favorite else "📌 관심공고 등록"

    def _favorite_badge_html() -> str:
        return (
            '<span style="display:inline-flex;align-items:center;justify-content:center;'
            'padding:4px 10px;border-radius:999px;background:#ffedd5;color:#c2410c;'
            'font-size:12px;font-weight:800;line-height:1;">관심</span>'
        )

    def _favorite_button_html(href: str, current_value: str, *, absolute: bool) -> str:
        is_favorite, label = _favorite_state(current_value)
        styles = [
            "display:inline-flex",
            "align-items:center",
            "justify-content:center",
            "height:36px",
            "padding:0 14px",
            "border-radius:8px",
            "font-size:13px",
            f"font-weight:{'800' if is_favorite else '700'}",
            "line-height:1",
            "text-decoration:none",
            "white-space:nowrap",
            "border:1px solid rgba(203, 213, 225, 0.9)",
            "background:#ffffff",
            "color:#374151",
        ]
        if absolute:
            styles.extend(
                [
                    "position:absolute",
                    "top:32px",
                    "right:44px",
                    "z-index:4",
                ]
            )
        if is_favorite:
            styles.extend(
                [
                    "background:#fff7ed",
                    "border-color:#fb923c",
                    "color:#c2410c",
                ]
            )
        style_attr = ";".join(styles)
        return (
            f'<a class="notice-queue-row-action{" is-active" if is_favorite else ""}" '
            f'href="{escape(href, quote=True)}" style="{style_attr}" '
            'onclick="event.preventDefault(); event.stopPropagation(); window.location.href=this.href;" '
            'onmouseover="this.style.background=\'#f8fafc\'" '
            f'onmouseout="this.style.background=\'{"#fff7ed" if is_favorite else "#ffffff"}\'">'
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
        action_href = build_favorite_toggle_href(
            page_key=clean(get_query_param("page")) or detail_page_key,
            notice_id=notice_id,
            current_value=clean(current_value),
            source_key=clean(source_key) or "iris",
            notice_title=clean(notice_title),
        )
        st.markdown(
            (
                '<div style="display:flex;justify-content:flex-end;align-items:flex-start;">'
                f'{_favorite_button_html(action_href, current_value, absolute=False)}'
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    def favorite_button_props(current_value: str) -> tuple[bool, str, str]:
        is_favorite, label = _favorite_state(current_value)
        return is_favorite, label, "primary" if is_favorite else "secondary"

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
            period = clean(first_non_empty(row, "접수기간", "notice_period", "period")) or "-"
            source_label = clean(first_non_empty(row, "매체", "source_label")) or (source_key or "IRIS").upper()
            ministry = clean(first_non_empty(row, "소관부처", "ministry"))
            agency = clean(first_non_empty(row, "전문기관", "agency", "담당부처"))
            breadcrumb_parts = [part for part in (ministry, agency) if clean(part) and part != "-"]
            breadcrumb = " > ".join(breadcrumb_parts) if breadcrumb_parts else source_label
            status = normalize_notice_status_label(first_non_empty(row, "공고상태", "status", "rcve_status"))
            scope = clean(first_non_empty(row, "_notice_scope"))
            if not status:
                status = "마감" if scope == "archive" else "예정" if scope == "scheduled" else "접수중"
            support_type = clean(first_non_empty(row, "공모유형", "pbofr_type", "support_type")) or "-"
            detail_href = build_route_href(detail_page_key, collection_id, source_key=source_key) if collection_id else "#"
            action_href = (
                build_favorite_toggle_href(
                    page_key=page_key,
                    notice_id=notice_id,
                    current_value=review_value,
                    source_key=source_key or "iris",
                    notice_title=title,
                )
                if notice_id
                else ""
            )
            title_badge = _favorite_badge_html() if is_favorite else ""
            favorite_button = _favorite_button_html(action_href, review_value, absolute=True) if action_href else ""

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
                (
                    '<div class="notice-queue-row-shell">'
                    f"{favorite_button}"
                    f'<div class="notice-queue-row" role="link" tabindex="0" onclick="window.location.href=\'{escape(detail_href, quote=True)}\'" '
                    f'onkeydown="if(event.key===\'Enter\'||event.key===\' \'){{event.preventDefault();window.location.href=\'{escape(detail_href, quote=True)}\';}}">'
                    f'<div class="notice-queue-breadcrumb">{escape(breadcrumb)}</div>'
                    '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:0.45rem;">'
                    f'<div class="notice-queue-title" style="margin-bottom:0;">{escape(title)}</div>'
                    f"{title_badge}"
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
            )

        st.markdown(f'<div class="notice-queue-list">{"".join(row_html)}</div>', unsafe_allow_html=True)

    def _render_notice_queue_screen(source_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
        filter_state_key = get_notice_queue_filter_state_key(detail_page_key)
        search_key = f"{detail_page_key}_search_text"
        consume_notice_queue_filter_query_action(page_key=detail_page_key, state_key=filter_state_key)
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
            render_notice_detail_from_row(selected_row, opportunity_df)
            return

        render_page_header(
            "Notice Queue",
            "IRIS, MSS, NIPA에서 크롤링한 공고를 한 곳에서 확인합니다.",
            eyebrow="Notices",
        )
        render_notice_queue_ui_styles()
        if source_df.empty:
            st.info("아직 표시할 공고가 없습니다.")
            return

        current_search_text = clean(st.session_state.get(search_key, ""))
        selected_filter = normalize_notice_queue_filter(st.session_state.get(filter_state_key, "all"))
        render_notice_queue_kpi_cards(
            build_notice_queue_metric_items(filter_notice_queue_rows(source_df, search_text=current_search_text)),
            selected_filter=selected_filter,
            page_key=detail_page_key,
        )

        search_col, reset_col = st.columns([6, 1])
        with search_col:
            search_text = st.text_input(
                "공고명",
                key=search_key,
                placeholder="공고명을 입력하세요",
            )
        with reset_col:
            st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
            st.button(
                "초기화",
                key=f"{detail_page_key}_search_reset",
                use_container_width=True,
                on_click=reset_notice_queue_controls,
                args=(search_key, filter_state_key),
            )

        search_filtered_df = filter_notice_queue_rows(source_df, search_text=search_text)
        selected_filter = normalize_notice_queue_filter(st.session_state.get(filter_state_key, "all"))
        filtered_source_df = apply_notice_queue_kpi_filter(search_filtered_df, selected_filter)

        if clean(search_text) or selected_filter != "all":
            st.caption(f"검색 결과 {len(filtered_source_df)}건")
        else:
            st.caption(f"전체 {len(source_df)}건")

        iris_rows = filtered_source_df[
            filtered_source_df["source_key"].eq("iris") & filtered_source_df["_notice_scope"].isin(["current", "scheduled"])
        ].copy()
        mss_rows = filtered_source_df[
            filtered_source_df["source_key"].eq("tipa") & filtered_source_df["_notice_scope"].eq("current")
        ].copy()
        nipa_rows = filtered_source_df[
            filtered_source_df["source_key"].eq("nipa") & filtered_source_df["_notice_scope"].eq("current")
        ].copy()
        archive_rows = filtered_source_df[filtered_source_df["_notice_scope"].eq("archive")].copy()
        favorite_rows = filtered_source_df[
            filtered_source_df["검토 여부"].fillna("").astype(str).str.strip().eq(FAVORITE_REVIEW_STATUS)
        ].copy()

        tab_iris, tab_mss, tab_nipa, tab_archive, tab_favorites = st.tabs(
            ["IRIS", "MSS", "NIPA", "Archive", "Favorites"]
        )
        with tab_iris:
            render_crawled_notice_rows(iris_rows, key_prefix=f"{detail_page_key}_iris", page_key=detail_page_key)
        with tab_mss:
            render_crawled_notice_rows(mss_rows, key_prefix=f"{detail_page_key}_mss", page_key=detail_page_key)
        with tab_nipa:
            render_crawled_notice_rows(nipa_rows, key_prefix=f"{detail_page_key}_nipa", page_key=detail_page_key)
        with tab_archive:
            render_crawled_notice_rows(archive_rows, key_prefix=f"{detail_page_key}_archive", page_key=detail_page_key)
        with tab_favorites:
            render_crawled_notice_rows(
                favorite_rows,
                key_prefix=f"{detail_page_key}_favorites",
                page_key="favorites",
                empty_message="아직 관심공고로 저장한 공고가 없습니다.",
            )

    def render_favorite_notice_page(
        source_df: pd.DataFrame,
        opportunity_df: pd.DataFrame,
        source_datasets: dict[str, object] | None = None,
    ) -> None:
        del opportunity_df, source_datasets
        consume_favorite_toggle_query_action()
        st.subheader("관심공고")
        st.caption("'관심공고'로 지정된 공고만 모아봅니다.")
        if source_df is None or source_df.empty:
            st.info("아직 관심공고로 저장한 공고가 없습니다.")
            return
        favorite_rows = source_df[
            source_df["검토 여부"].fillna("").astype(str).str.strip().eq(FAVORITE_REVIEW_STATUS)
        ].copy()
        if favorite_rows.empty:
            st.info("아직 관심공고로 저장한 공고가 없습니다.")
            return
        render_crawled_notice_rows(
            favorite_rows,
            key_prefix=f"{detail_page_key}_favorite_page",
            page_key="favorites",
            empty_message="아직 관심공고로 저장한 공고가 없습니다.",
        )

    def render_notice_queue_page(datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None) -> None:
        source_df = build_crawled_notice_collection(datasets, source_datasets)
        _render_notice_queue_screen(source_df, datasets["opportunity_all"])

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
        _render_notice_queue_screen(source_df, datasets["opportunity_all"])

    ns["consume_favorite_toggle_query_action"] = consume_favorite_toggle_query_action
    ns["render_favorite_scrap_button"] = render_favorite_scrap_button
    ns["favorite_button_props"] = favorite_button_props
    ns["render_crawled_notice_rows"] = render_crawled_notice_rows
    ns["render_favorite_notice_page"] = render_favorite_notice_page
    ns["render_notice_queue_page"] = render_notice_queue_page
    if "render_notices_source" in ns:
        ns["render_notices_source"] = render_notices_source
