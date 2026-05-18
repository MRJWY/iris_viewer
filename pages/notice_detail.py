from __future__ import annotations

import pandas as pd

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


def _status_badge_tone(status: str) -> str:
    normalized = (status or "").strip()
    if normalized == "접수중":
        return "primary"
    if normalized == "마감임박":
        return "warning"
    if normalized == "마감":
        return "danger"
    if normalized == "예정":
        return "neutral"
    return "neutral"


def _dday_badge_tone(dday_text: str) -> str:
    normalized = (dday_text or "").strip()
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


def render_page(st, row, opportunity_df: pd.DataFrame, *, api) -> None:
    inject_detail_workspace_styles(st)

    row_dict = _as_dict(row)
    if not row_dict:
        st.info("표시할 공고가 없습니다.")
        return

    source_key = api.resolve_notice_source_key(row_dict)
    source_label = _source_label(source_key)
    notice_id = first_present(api, row_dict, "공고ID", "notice_id")
    notice_title = first_present(api, row_dict, "공고명", "notice_title", "title")
    if not notice_title:
        st.info("공고 제목을 확인할 수 없습니다.")
        return

    related = api.find_related_opportunities_for_notice(row_dict, opportunity_df)
    top_related = {}
    if not related.empty:
        related = api.ensure_opportunity_row_ids(related.copy())
        related = related.sort_values(
            by=["rfp_score", "project_name"],
            ascending=[False, True],
            na_position="last",
        )
        top_related = related.iloc[0].to_dict()
        row_dict = api.ensure_notice_analysis_fallback(row_dict, top_related)

    period_text = first_present(api, row_dict, "접수기간", "신청기간", "period")
    status_text = api.normalize_notice_status_label(
        first_present(api, row_dict, "공고상태", "status", "rcve_status")
    )
    dday_text = present_value(api, api.format_dashboard_deadline_badge(period_text, status_text))
    score_text = first_present(api, row_dict, "대표점수", "추천점수", "rfp_score", "llm_fit_score")
    recommendation_text = first_present(
        api,
        row_dict,
        "대표추천도",
        "추천정도",
        "recommendation",
        "llm_recommendation",
    )
    budget_text = first_present(api, row_dict, "대표예산", "사업비", "예산")
    ministry_text = first_present(api, row_dict, "소관부처", "주관부처", "ministry")
    agency_text = first_present(api, row_dict, "전문기관", "담당부서", "agency")
    detail_link = api.resolve_external_detail_link(row_dict, source_key=source_key)

    story = api.build_analysis_story_bundle(
        top_related or {},
        notice_row=row_dict,
        period_text=period_text,
    )

    ai_summary = (
        first_present(api, row_dict, "대표추천이유", "summary", "analysis_summary")
        or present_value(api, story.get("summary_text"))
        or present_value(api, api.build_project_analysis_text(row_dict, top_related))
    )
    rationale_points = filter_points(
        api,
        [
            first_present(api, row_dict, "대표추천이유"),
            present_value(api, story.get("objective_text")),
            present_value(api, story.get("background_text")),
            first_present(api, top_related, "target_market"),
            first_present(api, top_related, "llm_application_field", "application_field"),
        ],
    )
    risk_points = filter_points(
        api,
        [
            present_value(api, story.get("support_need_text")),
            present_value(api, story.get("eligibility_text")),
            "연결된 RFP가 아직 없습니다." if related.empty else "",
        ],
    )

    support_items = filter_meta_items(
        api,
        [
            ("지원 요건", story.get("eligibility_text")),
            ("지원 구조", story.get("support_plan_text")),
            ("지원 필요성", story.get("support_need_text")),
            ("활용 분야", first_present(api, top_related, "llm_application_field", "application_field")),
        ],
    )
    schedule_items = filter_meta_items(
        api,
        [
            ("공고 상태", status_text),
            ("접수 기간", period_text),
            ("공고 등록일", first_present(api, row_dict, "공고일자", "registered_at", "Date")),
            ("공고번호", first_present(api, row_dict, "공고번호", "ancm_no")),
            ("소관부처", ministry_text),
            ("전문기관", agency_text),
        ],
    )

    related_items: list[dict[str, object]] = []
    if not related.empty:
        for _, related_row in related.head(6).iterrows():
            related_dict = related_row.to_dict()
            row_id = first_present(api, related_dict, "_row_id", "document_id")
            project_title = first_present(
                api,
                related_dict,
                "llm_project_name",
                "project_name",
                "해당 과제명",
                "rfp_title",
                "file_name",
            )
            if not row_id or not project_title:
                continue
            related_items.append(
                {
                    "title": truncate(api, project_title, max_chars=88),
                    "href": api.build_route_href("rfp_queue", row_id, source_key=source_key),
                    "subtitle": truncate(
                        api,
                        first_present(api, related_dict, "notice_title", "공고명"),
                        max_chars=110,
                    ),
                    "meta": " / ".join(
                        part
                        for part in [
                            first_present(api, related_dict, "llm_recommendation", "recommendation"),
                            first_present(api, related_dict, "llm_fit_score", "rfp_score"),
                            first_present(
                                api,
                                related_dict,
                                "budget",
                                "llm_total_budget_text",
                                "total_budget_text",
                            ),
                        ]
                        if part
                    ),
                    "badges": filter_points(
                        api,
                        [
                            first_present(api, related_dict, "llm_recommendation", "recommendation"),
                            first_present(api, related_dict, "llm_fit_score", "rfp_score"),
                        ],
                    ),
                }
            )

    original_items: list[dict[str, object]] = []
    if detail_link:
        original_items.append(
            {
                "title": "원문 공고 바로가기",
                "href": detail_link,
                "subtitle": f"{source_label} 공고 원문",
                "meta": first_present(api, row_dict, "공고번호", "ancm_no"),
                "badges": [source_label],
            }
        )

    breadcrumbs = [
        ("Notice Queue", "?source=notices&page=notice_queue&view=table"),
        (truncate(api, notice_title, max_chars=42), None),
    ]
    render_detail_breadcrumb(st, breadcrumbs)

    badges = [
        (status_text, _status_badge_tone(status_text)),
        (recommendation_text, "primary"),
        (dday_text, _dday_badge_tone(dday_text)),
        (f"점수 {score_text}" if score_text else "", "neutral"),
    ]

    def render_header_actions() -> None:
        if notice_id:
            api.render_favorite_scrap_button(
                notice_id=notice_id,
                current_value=first_present(api, row_dict, "검토 여부", "검토여부", "review_status"),
                source_key=source_key,
                notice_title=notice_title,
                button_key=f"notice_detail_header_favorite_{notice_id}",
                compact=True,
                use_container_width=True,
            )
        if detail_link:
            st.link_button("원문 공고", detail_link, use_container_width=True)
        if st.button("뒤로", key=f"notice_detail_back_{notice_id}", use_container_width=True):
            api.switch_to_table("notice_queue")

    render_detail_header_card(
        st,
        title=notice_title,
        kicker=f"{source_label} Notice",
        subtitle="공고를 빠르게 검토하고 연결된 RFP를 함께 판단하는 워크스페이스",
        badges=badges,
        action_renderer=render_header_actions,
        container_key=f"notice_detail_header_{notice_id or 'unknown'}",
    )

    render_detail_kpi_strip(
        st,
        [
            ("마감", dday_text),
            ("예산", budget_text),
            ("소관부처", ministry_text),
            ("전문기관", agency_text),
            ("접수기간", period_text),
            ("관련 RFP", str(len(related)) if not related.empty else ""),
        ],
    )

    main_col, side_col = st.columns([1.75, 0.95], gap="large")
    with main_col:
        render_detail_summary_card(
            st,
            title="AI 요약",
            body=ai_summary,
            key=f"notice_detail_summary_{notice_id or 'unknown'}",
        )
        render_detail_decision_card(
            st,
            title="추천 근거",
            points=rationale_points,
            key=f"notice_detail_rationale_{notice_id or 'unknown'}",
        )
        render_detail_decision_card(
            st,
            title="리스크 / 검토 포인트",
            points=risk_points,
            key=f"notice_detail_risk_{notice_id or 'unknown'}",
        )
        render_detail_support_card(
            st,
            title="지원 요건",
            items=support_items,
            key=f"notice_detail_support_{notice_id or 'unknown'}",
        )
        render_detail_schedule_card(
            st,
            title="일정 및 제출 정보",
            items=schedule_items,
            key=f"notice_detail_schedule_{notice_id or 'unknown'}",
        )
        render_detail_related_items_card(
            st,
            title="관련 RFP",
            items=related_items,
            key=f"notice_detail_related_rfp_{notice_id or 'unknown'}",
            empty_text="연결된 RFP가 아직 없습니다.",
        )
        render_detail_related_items_card(
            st,
            title="원문 / 첨부",
            items=original_items,
            key=f"notice_detail_original_{notice_id or 'unknown'}",
            empty_text="확인 가능한 원문 링크가 없습니다.",
        )

    with side_col:
        with st.container(key="detail_side_notice"):
            def render_side_actions() -> None:
                if detail_link:
                    st.link_button("원문 공고 열기", detail_link, use_container_width=True)
                if related_items and not related.empty:
                    first_related = related.iloc[0].to_dict()
                    first_related_id = first_present(api, first_related, "_row_id", "document_id")
                    if first_related_id and st.button(
                        "관련 RFP 바로 보기",
                        key=f"notice_detail_related_jump_{notice_id}",
                        use_container_width=True,
                    ):
                        api.navigate_to_opportunity_detail(source_key, first_related_id)
                if st.button(
                    "Notice Queue로 돌아가기",
                    key=f"notice_detail_side_back_{notice_id}",
                    use_container_width=True,
                ):
                    api.switch_to_table("notice_queue")

            render_detail_action_panel(
                st,
                key=f"notice_detail_action_panel_{notice_id or 'unknown'}",
                render_actions=render_side_actions,
                title="작업",
            )

            compact_meta = filter_meta_items(
                api,
                [
                    ("출처", source_label),
                    ("공고ID", notice_id),
                    ("검토 상태", first_present(api, row_dict, "검토 여부", "검토여부", "review_status")),
                    ("현재 공고", first_present(api, row_dict, "is_current")),
                ],
            )
            render_detail_compact_meta_card(
                st,
                title="빠른 메타데이터",
                items=compact_meta,
                key=f"notice_detail_meta_{notice_id or 'unknown'}",
            )

            if notice_id:
                render_detail_review_card(
                    st,
                    key=f"notice_detail_review_{notice_id}",
                    review_caption="검토 상태와 메모는 원본 공고 기준으로 저장됩니다.",
                    render_review=lambda: api.render_review_editor(
                        notice_id=notice_id,
                        current_value=first_present(api, row_dict, "검토 여부", "검토여부", "review_status"),
                        form_key=f"notice_review_form_{notice_id}",
                        source_key=source_key,
                    ),
                    render_comments=lambda: api.render_notice_comments(
                        row_dict,
                        section_key=f"notice_{notice_id}",
                    ),
                )
