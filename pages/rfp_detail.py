from __future__ import annotations

from components.detail_blocks import (
    filter_meta_items,
    filter_points,
    first_present,
    inject_detail_workspace_styles,
    present_value,
    render_detail_action_panel,
    render_detail_breadcrumb,
    render_detail_compact_meta_card,
    render_detail_decision_card,
    render_detail_header_card,
    render_detail_kpi_strip,
    render_detail_related_items_card,
    render_detail_review_card,
    render_detail_schedule_card,
    render_detail_summary_card,
    render_detail_support_card,
    truncate,
)


def _deadline_tone(deadline_text: str) -> str:
    normalized = (deadline_text or "").strip()
    if not normalized:
        return "neutral"
    if normalized == "D-Day":
        return "danger"
    if normalized.startswith("D-"):
        try:
            days = int(normalized.split("-", 1)[1])
        except ValueError:
            return "neutral"
        if days <= 7:
            return "danger"
        if days <= 30:
            return "warning"
        return "primary"
    return "neutral"


def _source_label(source_key: str) -> str:
    return {
        "tipa": "MSS",
        "nipa": "NIPA",
    }.get((source_key or "").lower(), "IRIS")


def _as_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "to_dict"):
        return row.to_dict()
    return dict(row)


def render_page(st, row, *, api) -> None:
    inject_detail_workspace_styles(st)

    row_dict = _as_dict(row)
    if not row_dict:
        st.info("표시할 RFP가 없습니다.")
        return

    source_key = first_present(api, row_dict, "source_key") or api.resolve_notice_source_key(row_dict)
    source_label = _source_label(source_key)
    notice_id = first_present(api, row_dict, "notice_id", "공고ID")
    row_id = first_present(api, row_dict, "_row_id", "document_id")
    title = first_present(
        api,
        row_dict,
        "llm_project_name",
        "project_name",
        "해당 과제명",
        "llm_rfp_title",
        "rfp_title",
        "file_name",
    )
    if not title:
        st.info("RFP 제목을 확인할 수 없습니다.")
        return

    detail_link = api.resolve_external_detail_link(row_dict, source_key=source_key)
    download_path = api.resolve_local_file_path(row_dict)
    period_text = first_present(api, row_dict, "notice_period", "period", "접수기간", "신청기간")
    status_text = api.normalize_notice_status_label(
        first_present(api, row_dict, "notice_status", "공고상태", "status", "rcve_status")
    )
    deadline_text = present_value(api, api.format_dashboard_deadline_badge(period_text, status_text))
    recommendation_text = first_present(api, row_dict, "llm_recommendation", "recommendation", "추천여부")
    score_text = first_present(api, row_dict, "llm_fit_score", "rfp_score")
    budget_text = first_present(api, row_dict, "llm_total_budget_text", "total_budget_text", "budget", "예산")
    support_type = first_present(api, row_dict, "support_type", "사업유형", "business_type", "document_type")
    ministry_text = first_present(api, row_dict, "ministry", "소관부처", "주관부처")
    agency_text = first_present(api, row_dict, "agency", "전문기관", "담당부서")
    notice_title = first_present(api, row_dict, "notice_title", "공고명")

    story = api.build_analysis_story_bundle(row_dict, period_text=period_text)
    ai_summary = present_value(api, story.get("summary_text")) or present_value(
        api,
        api.build_project_analysis_text(row_dict),
    )
    rationale_points = filter_points(
        api,
        [
            present_value(api, api.build_project_analysis_text(row_dict)),
            present_value(api, story.get("objective_text")),
            first_present(api, row_dict, "target_market"),
            first_present(api, row_dict, "llm_application_field", "application_field"),
        ],
    )
    risk_points = filter_points(
        api,
        [
            present_value(api, story.get("support_need_text")),
            present_value(api, story.get("eligibility_text")),
            first_present(api, row_dict, "llm_development_content", "development_content"),
        ],
    )

    support_items = filter_meta_items(
        api,
        [
            ("지원 유형", support_type),
            ("지원 요건", story.get("eligibility_text")),
            ("지원 구조", story.get("support_plan_text")),
            ("활용 분야", first_present(api, row_dict, "llm_application_field", "application_field")),
        ],
    )
    schedule_items = filter_meta_items(
        api,
        [
            ("접수 기간", period_text),
            ("마감 기준", deadline_text),
            ("예산", budget_text),
            ("소관부처", ministry_text),
            ("전문기관", agency_text),
            ("연결 공고", notice_title),
        ],
    )

    related_notice_items: list[dict[str, object]] = []
    if notice_id and notice_title:
        related_notice_items.append(
            {
                "title": truncate(api, notice_title, max_chars=96),
                "href": api.build_route_href("notice_queue", notice_id, source_key="notices"),
                "subtitle": "연결 공고",
                "meta": " / ".join(
                    part
                    for part in [ministry_text, agency_text, period_text]
                    if part
                ),
                "badges": filter_points(api, [status_text, deadline_text]),
            }
        )

    original_items: list[dict[str, object]] = []
    if detail_link:
        original_items.append(
            {
                "title": "원문 공고 바로가기",
                "href": detail_link,
                "subtitle": f"{source_label} 공고 원문",
                "meta": notice_title,
                "badges": [source_label],
            }
        )
    if download_path:
        original_items.append(
            {
                "title": download_path.name,
                "href": "",
                "subtitle": "로컬 RFP 문서",
                "meta": str(download_path),
                "badges": ["RFP"],
            }
        )

    breadcrumbs = [
        ("RFP Queue", f"?source={source_key or 'iris'}&page=rfp_queue&view=table"),
        (truncate(api, title, max_chars=42), None),
    ]
    render_detail_breadcrumb(st, breadcrumbs)

    def render_header_actions() -> None:
        if notice_id:
            api.render_favorite_scrap_button(
                notice_id=notice_id,
                current_value=first_present(api, row_dict, "검토 여부", "검토여부", "review_status"),
                source_key=source_key,
                notice_title=notice_title,
                button_key=f"rfp_detail_header_favorite_{row_id or notice_id}",
                compact=True,
                use_container_width=True,
            )
        if download_path and not detail_link:
            try:
                with open(download_path, "rb") as file_handle:
                    st.download_button(
                        "원문 문서",
                        data=file_handle.read(),
                        file_name=download_path.name,
                        mime="application/octet-stream",
                        key=f"rfp_detail_header_download_{row_id or notice_id}",
                        use_container_width=True,
                    )
            except OSError:
                pass
        elif detail_link:
            st.link_button("원문 공고", detail_link, use_container_width=True)
        if st.button("뒤로", key=f"rfp_detail_back_{row_id or notice_id}", use_container_width=True):
            api.switch_to_table("rfp_queue")

    badges = [
        (recommendation_text, "primary"),
        (f"점수 {score_text}" if score_text else "", "neutral"),
        (deadline_text, _deadline_tone(deadline_text)),
        (status_text, "neutral"),
    ]
    render_detail_header_card(
        st,
        title=title,
        kicker=f"{source_label} RFP",
        subtitle=truncate(api, notice_title, max_chars=100),
        badges=badges,
        action_renderer=render_header_actions,
        container_key=f"rfp_detail_header_{row_id or notice_id or 'unknown'}",
    )

    render_detail_kpi_strip(
        st,
        [
            ("마감", deadline_text),
            ("예산", budget_text),
            ("소관부처", ministry_text),
            ("전문기관", agency_text),
            ("접수기간", period_text),
            ("적합 점수", score_text),
        ],
    )

    main_col, side_col = st.columns([1.75, 0.95], gap="large")
    with main_col:
        render_detail_summary_card(
            st,
            title="AI 요약",
            body=ai_summary,
            key=f"rfp_detail_summary_{row_id or notice_id or 'unknown'}",
        )
        render_detail_decision_card(
            st,
            title="추천 근거",
            points=rationale_points,
            key=f"rfp_detail_rationale_{row_id or notice_id or 'unknown'}",
        )
        render_detail_decision_card(
            st,
            title="리스크 / 검토 포인트",
            points=risk_points,
            key=f"rfp_detail_risk_{row_id or notice_id or 'unknown'}",
        )
        render_detail_support_card(
            st,
            title="지원 요건",
            items=support_items,
            key=f"rfp_detail_support_{row_id or notice_id or 'unknown'}",
        )
        render_detail_schedule_card(
            st,
            title="일정 및 제출 정보",
            items=schedule_items,
            key=f"rfp_detail_schedule_{row_id or notice_id or 'unknown'}",
        )
        render_detail_related_items_card(
            st,
            title="관련 공고",
            items=related_notice_items,
            key=f"rfp_detail_notice_{row_id or notice_id or 'unknown'}",
            empty_text="연결된 공고 정보가 없습니다.",
        )
        render_detail_related_items_card(
            st,
            title="원문 / 첨부",
            items=original_items,
            key=f"rfp_detail_original_{row_id or notice_id or 'unknown'}",
            empty_text="확인 가능한 원문 또는 첨부가 없습니다.",
        )

    with side_col:
        with st.container(key="detail_side_rfp"):
            def render_side_actions() -> None:
                if detail_link:
                    st.link_button("원문 공고 열기", detail_link, use_container_width=True)
                if notice_id and st.button(
                    "관련 공고 보기",
                    key=f"rfp_detail_notice_jump_{row_id or notice_id}",
                    use_container_width=True,
                ):
                    api.navigate_to_notice_detail(source_key, notice_id)
                if download_path:
                    try:
                        with open(download_path, "rb") as file_handle:
                            st.download_button(
                                "RFP 문서 다운로드",
                                data=file_handle.read(),
                                file_name=download_path.name,
                                mime="application/octet-stream",
                                key=f"rfp_detail_download_{row_id or notice_id}",
                                use_container_width=True,
                            )
                    except OSError:
                        st.caption("연결된 문서를 찾을 수 없습니다.")
                if st.button(
                    "RFP Queue로 돌아가기",
                    key=f"rfp_detail_side_back_{row_id or notice_id}",
                    use_container_width=True,
                ):
                    api.switch_to_table("rfp_queue")

            render_detail_action_panel(
                st,
                key=f"rfp_detail_action_panel_{row_id or notice_id or 'unknown'}",
                render_actions=render_side_actions,
                title="작업",
            )

            compact_meta = filter_meta_items(
                api,
                [
                    ("출처", source_label),
                    ("문서 ID", row_id),
                    ("연결 공고 ID", notice_id),
                    ("검토 상태", first_present(api, row_dict, "review_status", "검토 여부", "검토여부")),
                ],
            )
            render_detail_compact_meta_card(
                st,
                title="빠른 메타데이터",
                items=compact_meta,
                key=f"rfp_detail_meta_{row_id or notice_id or 'unknown'}",
            )

            if notice_id:
                render_detail_review_card(
                    st,
                    key=f"rfp_detail_review_{row_id or notice_id}",
                    review_caption="검토 상태와 메모는 연결된 공고 기준으로 저장됩니다.",
                    render_review=lambda: api.render_review_editor(
                        notice_id=notice_id,
                        current_value=first_present(api, row_dict, "검토 여부", "검토여부", "review_status"),
                        form_key=f"rfp_review_form_{row_id or notice_id}",
                        source_key=source_key,
                    ),
                    render_comments=lambda: api.render_notice_comments(
                        row_dict,
                        section_key=f"rfp_{notice_id or row_id}",
                        modern_layout=True,
                    ),
                )
